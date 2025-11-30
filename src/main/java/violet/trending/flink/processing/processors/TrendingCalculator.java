package violet.trending.flink.processing.processors;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import violet.trending.flink.common.pojo.Action;
import violet.trending.flink.common.pojo.Creation;

import java.time.Duration;

/**
 * 示例：Action 流 keyedBy creationId 后，读取 Creation 状态并更新热度值。
 */
public class TrendingCalculator extends KeyedProcessFunction<Long, Action, TrendingCalculator.TrendingResult> {

    private transient ValueState<Creation> creationState;
    private transient ValueState<Double> trendingScoreState;
    private transient ValueState<Long> lastScoreUpdateTsState;

    private final double decayRatePerMillis;

    public TrendingCalculator() {
        this(Duration.ofMinutes(10).toMillis());
    }

    public TrendingCalculator(long halfLifeMillis) {
        if (halfLifeMillis <= 0) {
            throw new IllegalArgumentException("halfLifeMillis must be positive");
        }
        this.decayRatePerMillis = Math.log(2) / halfLifeMillis;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Creation> creationDescriptor =
                new ValueStateDescriptor<>("creation-state", Creation.class);
        creationState = getRuntimeContext().getState(creationDescriptor);

        ValueStateDescriptor<Double> scoreDescriptor =
                new ValueStateDescriptor<>("trending-score", Double.class);
        trendingScoreState = getRuntimeContext().getState(scoreDescriptor);

        ValueStateDescriptor<Long> lastUpdateDescriptor =
                new ValueStateDescriptor<>("trending-score-last-update-ts", Long.class);
        lastScoreUpdateTsState = getRuntimeContext().getState(lastUpdateDescriptor);
    }

    @Override
    public void processElement(Action action, Context ctx, Collector<TrendingResult> out) throws Exception {
        Creation creation = creationState.value();
        if (creation == null) {
            // Creation 还没入 state，可以选择跳过或打回重试
            return;
        }

        double currentScore = trendingScoreState.value() == null ? 0d : trendingScoreState.value();
        Long lastUpdatedTs = lastScoreUpdateTsState.value();

        double updatedScore = calculateScore(action, currentScore, lastUpdatedTs);
        trendingScoreState.update(updatedScore);
        lastScoreUpdateTsState.update(action.getActionTs());

        TrendingResult result = new TrendingResult();
        result.setCreationId(action.getCreationId());
        result.setCategory(creation.getCategory());
        result.setScore(updatedScore);
        result.setLastActionTs(action.getActionTs());
        out.collect(result);
    }

    private double calculateScore(Action action, double previousScore, Long lastUpdatedTs) {
        double actionWeight = switch (action.getActionType()) {
            case 2 -> 1.0;  // 点击
            case 3 -> 2.0;  // 点赞
            case 4 -> 3.0;  // 评论
            case 5 -> 3.0;  // 转发
            default -> 0.0; // other signals
        };

        if (previousScore < 0) {
            previousScore = 0;
        }

        if (lastUpdatedTs == null) {
            return previousScore + actionWeight;
        }

        long deltaMillis = Math.max(0L, action.getActionTs() - lastUpdatedTs);
        double decayFactor = Math.exp(-decayRatePerMillis * deltaMillis);
        double decayed = previousScore * decayFactor;
        return decayed + actionWeight;
    }

    @Setter
    @Getter
    public static class TrendingResult {
        private Long creationId;
        private String category;
        private double score;
        private Long lastActionTs;
    }
}
