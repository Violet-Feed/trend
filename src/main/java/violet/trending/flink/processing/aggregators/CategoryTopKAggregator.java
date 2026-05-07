package violet.trending.flink.processing.aggregators;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import violet.trending.flink.processing.processors.TrendingCalculator;

import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

public class CategoryTopKAggregator extends ProcessWindowFunction<
        TrendingCalculator.TrendingResult,
        CategoryTopKAggregator.CategoryTopKResult,
        String,
        TimeWindow> {

    private final int topK;

    public CategoryTopKAggregator(int topK) {
        this.topK = topK;
    }

    @Override
    public void process(String key, Context context,
                        Iterable<TrendingCalculator.TrendingResult> elements,
                        Collector<CategoryTopKResult> out) {
        List<TrendingCalculator.TrendingResult> topItems = StreamSupport.stream(elements.spliterator(), false)
                .sorted(Comparator.comparingDouble(TrendingCalculator.TrendingResult::getScore).reversed())
                .limit(topK)
                .toList();

        if (!topItems.isEmpty()) {
            CategoryTopKResult result = new CategoryTopKResult();
            result.setKey(key);
            result.setTopItems(topItems);
            out.collect(result);
        }
    }

    @Getter
    @Setter
    public static class CategoryTopKResult {
        private String key;
        private List<TrendingCalculator.TrendingResult> topItems;
    }
}