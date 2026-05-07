package violet.trending.flink.processing.processors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import violet.trending.flink.processing.aggregators.CategoryTopKAggregator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CategoryRankingManager extends KeyedProcessFunction<String, TrendingCalculator.TrendingResult, CategoryTopKAggregator.CategoryTopKResult> {

    private static final double SCORE_THRESHOLD = 0.01;
    private static final int TOP_K = 100;

    private transient MapState<Long, TrendingCalculator.TrendingResult> creationStateMap;

    private final long windowIntervalMillis;
    private final double decayRatePerMillis;

    public CategoryRankingManager(long windowIntervalMillis, long halfLifeMillis) {
        this.windowIntervalMillis = windowIntervalMillis;
        this.decayRatePerMillis = Math.log(2) / halfLifeMillis;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        creationStateMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("creation-state-map", Long.class, TrendingCalculator.TrendingResult.class));
    }

    @Override
    public void processElement(TrendingCalculator.TrendingResult result, Context ctx, Collector<CategoryTopKAggregator.CategoryTopKResult> out) throws Exception {
        if (result.getCreationId() == null) {
            return;
        }
        if (result.isRemoved()) {
            creationStateMap.remove(result.getCreationId());
            return;
        }
        creationStateMap.put(result.getCreationId(), result);
        long nextWindow = ((ctx.timerService().currentProcessingTime() / windowIntervalMillis) + 1) * windowIntervalMillis;
        ctx.timerService().registerProcessingTimeTimer(nextWindow);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CategoryTopKAggregator.CategoryTopKResult> out) throws Exception {
        List<TrendingCalculator.TrendingResult> decayedResults = new ArrayList<>();
        List<Long> toRemove = new ArrayList<>();

        for (Map.Entry<Long, TrendingCalculator.TrendingResult> entry : creationStateMap.entries()) {
            TrendingCalculator.TrendingResult result = entry.getValue();
            long lastTs = result.getLastActionTs() == null ? timestamp : result.getLastActionTs();
            double deltaMillis = Math.max(0, timestamp - lastTs);
            double decayedScore = result.getScore() * Math.exp(-decayRatePerMillis * deltaMillis);

            if (decayedScore < SCORE_THRESHOLD) {
                toRemove.add(entry.getKey());
            } else {
                TrendingCalculator.TrendingResult decayedResult = new TrendingCalculator.TrendingResult();
                decayedResult.setCreationId(result.getCreationId());
                decayedResult.setCategory(result.getCategory());
                decayedResult.setScore(decayedScore);
                decayedResult.setLastActionTs(result.getLastActionTs());
                decayedResults.add(decayedResult);
            }
        }

        for (Long id : toRemove) {
            creationStateMap.remove(id);
        }

        if (!decayedResults.isEmpty()) {
            decayedResults.sort(Comparator.comparingDouble(TrendingCalculator.TrendingResult::getScore).reversed());
            List<TrendingCalculator.TrendingResult> topItems = decayedResults.stream()
                    .limit(TOP_K)
                    .toList();

            CategoryTopKAggregator.CategoryTopKResult output = new CategoryTopKAggregator.CategoryTopKResult();
            output.setKey(ctx.getCurrentKey());
            output.setTopItems(topItems);
            out.collect(output);
        }

        if (!creationStateMap.isEmpty()) {
            long nextWindow = timestamp + windowIntervalMillis;
            ctx.timerService().registerProcessingTimeTimer(nextWindow);
        }
    }
}