package violet.trending.flink.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import violet.trending.flink.common.pojo.Action;
import violet.trending.flink.common.pojo.ActionBatch;
import violet.trending.flink.common.pojo.Creation;
import violet.trending.flink.connectors.kafka.KafkaSourceFactory;
import violet.trending.flink.connectors.redis.RedisHotRankingSink;
import violet.trending.flink.processing.aggregators.CategoryTopKAggregator;
import violet.trending.flink.processing.functions.ActionBatchSplitter;
import violet.trending.flink.processing.processors.CategoryRankingManager;
import violet.trending.flink.processing.processors.CreationStateUpdater;
import violet.trending.flink.processing.processors.TrendingCalculator;

import java.time.Duration;
import java.util.Objects;

/**
 * Builds the Trending pipeline end-to-end: Creation ingestion, Action processing, window aggregation and Redis sink.
 */
public final class TrendingJob {

    private TrendingJob() {
    }

    public static void build(StreamExecutionEnvironment env, TrendingJobOptions options) {
        Objects.requireNonNull(env, "env");
        Objects.requireNonNull(options, "options");

        // --- Sources ---
        WatermarkStrategy<Creation> creationWatermarks = WatermarkStrategy.noWatermarks();
        KafkaSource<Creation> creationSource =
                KafkaSourceFactory.create(options.creationSourceOptions, Creation.class, creationWatermarks);

        DataStreamSource<Creation> creationStream = env.fromSource(
                creationSource,
                creationWatermarks,
                "creation-source");

        WatermarkStrategy<ActionBatch> actionWatermarks = WatermarkStrategy.noWatermarks();
        KafkaSource<ActionBatch> actionSource =
                KafkaSourceFactory.create(options.actionSourceOptions, ActionBatch.class, actionWatermarks);

        DataStreamSource<ActionBatch> actionBatchStream = env.fromSource(
                actionSource,
                actionWatermarks,
                "action-batch-source");

        // --- Creation state enrichment & removal signal ---
        SingleOutputStreamOperator<Creation> creationUpdateStream = creationStream
                .name("creation-stream")
                .keyBy(Creation::getCreationId)
                .process(new CreationStateUpdater())
                .name("creation-state-updater");

        // Removal signal: creations with status != 0 -> TrendingResult with removed=true
        DataStream<TrendingCalculator.TrendingResult> removalStream = creationUpdateStream
                .filter(c -> c.getStatus() != null && c.getStatus() != 0)
                .map(c -> {
                    TrendingCalculator.TrendingResult r = new TrendingCalculator.TrendingResult();
                    r.setCreationId(c.getCreationId());
                    r.setCategory(c.getCategory());
                    r.setScore(0);
                    r.setRemoved(true);
                    return r;
                })
                .returns(TypeInformation.of(TrendingCalculator.TrendingResult.class))
                .name("creation-removal-signal");

        // --- Action processing & trending score update ---
        DataStream<Action> actionStream = actionBatchStream
                .name("action-batch-stream")
                .flatMap(new ActionBatchSplitter())
                .name("action-batch-splitter");

        SingleOutputStreamOperator<TrendingCalculator.TrendingResult> trendingStream = actionStream
                .keyBy(Action::getCreationId)
                .process(new TrendingCalculator(options.calculatorHalfLifeMillis))
                .name("trending-calculator");

        // --- Per-category ranking -> trend:{category} ---
        DataStream<TrendingCalculator.TrendingResult> categoryInput = trendingStream
                .filter(r -> r.getCategory() != null)
                .union(removalStream.filter(r -> r.getCategory() != null));

        DataStream<CategoryTopKAggregator.CategoryTopKResult> categoryRanking = categoryInput
                .keyBy(TrendingCalculator.TrendingResult::getCategory)
                .process(new CategoryRankingManager(options.windowSize.toMillis(), options.windowDecayHalfLifeMillis))
                .name("category-ranking-manager");

        categoryRanking
                .sinkTo(new RedisHotRankingSink(options.redisUri))
                .name("redis-category-sink");

        // --- Global ranking -> trend:all ---
        DataStream<TrendingCalculator.TrendingResult> allInput = trendingStream
                .union(removalStream);

        DataStream<CategoryTopKAggregator.CategoryTopKResult> globalRanking = allInput
                .keyBy(r -> "all")
                .process(new CategoryRankingManager(options.windowSize.toMillis(), options.windowDecayHalfLifeMillis))
                .name("global-ranking-manager");

        globalRanking
                .sinkTo(new RedisHotRankingSink(options.redisUri))
                .name("redis-all-sink");
    }

    public static final class TrendingJobOptions {
        private final KafkaSourceFactory.KafkaSourceOptions creationSourceOptions;
        private final KafkaSourceFactory.KafkaSourceOptions actionSourceOptions;
        private final Duration windowSize;
        private final long calculatorHalfLifeMillis;
        private final long windowDecayHalfLifeMillis;
        private final String redisUri;

        private TrendingJobOptions(Builder builder) {
            this.creationSourceOptions = Objects.requireNonNull(builder.creationSourceOptions, "creationSourceOptions");
            this.actionSourceOptions = Objects.requireNonNull(builder.actionSourceOptions, "actionSourceOptions");
            this.windowSize = Objects.requireNonNull(builder.windowSize, "windowSize");
            this.redisUri = Objects.requireNonNull(builder.redisUri, "redisUri");
            this.calculatorHalfLifeMillis = builder.calculatorHalfLifeMillis;
            this.windowDecayHalfLifeMillis = builder.windowDecayHalfLifeMillis;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private KafkaSourceFactory.KafkaSourceOptions creationSourceOptions;
            private KafkaSourceFactory.KafkaSourceOptions actionSourceOptions;
            private Duration windowSize = Duration.ofMinutes(5);
            private long calculatorHalfLifeMillis = Duration.ofMinutes(10).toMillis();
            private long windowDecayHalfLifeMillis = Duration.ofMinutes(10).toMillis();
            private String redisUri = "redis://localhost:6379";

            public Builder withCreationSourceOptions(KafkaSourceFactory.KafkaSourceOptions options) {
                this.creationSourceOptions = options;
                return this;
            }

            public Builder withActionSourceOptions(KafkaSourceFactory.KafkaSourceOptions options) {
                this.actionSourceOptions = options;
                return this;
            }

            public Builder withWindowSize(Duration windowSize) {
                if (windowSize != null) {
                    this.windowSize = windowSize;
                }
                return this;
            }

            public Builder withCalculatorHalfLife(Duration halfLife) {
                if (halfLife != null) {
                    this.calculatorHalfLifeMillis = halfLife.toMillis();
                }
                return this;
            }

            public Builder withWindowDecayHalfLife(Duration halfLife) {
                if (halfLife != null) {
                    this.windowDecayHalfLifeMillis = halfLife.toMillis();
                }
                return this;
            }

            public Builder withRedisUri(String redisUri) {
                if (redisUri != null && !redisUri.isBlank()) {
                    this.redisUri = redisUri;
                }
                return this;
            }

            public TrendingJobOptions build() {
                return new TrendingJobOptions(this);
            }
        }
    }
}
