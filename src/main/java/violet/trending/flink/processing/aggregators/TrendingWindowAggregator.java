package violet.trending.flink.processing.aggregators;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import violet.trending.flink.processing.processors.TrendingCalculator;

/**
 * Aggregator that:
 * 1) uses a reduce step to keep the latest TrendingResult per window,
 * 2) applies an exponential decay referencing the window end when emitting the final score.
 *
 * This lets us treat the window end as the "current time" for the exponential integral.
 */
public class TrendingWindowAggregator extends ProcessWindowFunction<
        TrendingCalculator.TrendingResult,
        TrendingWindowAggregator.WindowedTrendingResult,
        Long,
        TimeWindow> {

    private final double decayRatePerMillis;

    /**
     * @param halfLifeMillis configure by domain needs; decayRate = ln(2)/halfLife
     */
    public TrendingWindowAggregator(long halfLifeMillis) {
        if (halfLifeMillis <= 0) {
            throw new IllegalArgumentException("halfLifeMillis must be positive");
        }
        this.decayRatePerMillis = Math.log(2) / halfLifeMillis;
    }

    /**
     * Reduce function retaining the element with the latest logical time.
     */
    public static ReduceFunction<TrendingCalculator.TrendingResult> latestReducer() {
        return (left, right) -> {
            long leftTs = left.getLastActionTs() == null ? Long.MIN_VALUE : left.getLastActionTs();
            long rightTs = right.getLastActionTs() == null ? Long.MIN_VALUE : right.getLastActionTs();
            return rightTs >= leftTs ? right : left;
        };
    }

    @Override
    public void process(Long key,
                        Context context,
                        Iterable<TrendingCalculator.TrendingResult> elements,
                        Collector<WindowedTrendingResult> out) {
        TrendingCalculator.TrendingResult latest = elements.iterator().hasNext()
                ? elements.iterator().next()
                : null;
        if (latest == null) {
            return;
        }

        long windowEnd = context.window().getEnd();
        long eventTs = latest.getLastActionTs() == null ? windowEnd : latest.getLastActionTs();
        double deltaMillis = Math.max(0, windowEnd - eventTs);
        double decayedScore = latest.getScore() * Math.exp(-decayRatePerMillis * deltaMillis);

        WindowedTrendingResult result = new WindowedTrendingResult();
        result.setCreationId(latest.getCreationId());
        result.setCategory(latest.getCategory());
        result.setScore(decayedScore);
        result.setLastActionTs(latest.getLastActionTs());
        result.setWindowStart(context.window().getStart());
        result.setWindowEnd(windowEnd);
        out.collect(result);
    }

    @Getter
    @Setter
    public static class WindowedTrendingResult extends TrendingCalculator.TrendingResult {
        private long windowStart;
        private long windowEnd;
    }
}
