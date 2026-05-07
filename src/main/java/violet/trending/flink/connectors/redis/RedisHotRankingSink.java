package violet.trending.flink.connectors.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import violet.trending.flink.processing.aggregators.CategoryTopKAggregator;
import violet.trending.flink.processing.processors.TrendingCalculator;

public class RedisHotRankingSink implements Sink<CategoryTopKAggregator.CategoryTopKResult> {

    private final String redisUri;

    public RedisHotRankingSink(String redisUri) {
        this.redisUri = redisUri;
    }

    @Override
    public SinkWriter<CategoryTopKAggregator.CategoryTopKResult> createWriter(WriterInitContext context) {
        return new RedisHotRankingWriter(redisUri);
    }

    private static final class RedisHotRankingWriter implements SinkWriter<CategoryTopKAggregator.CategoryTopKResult> {

        private final RedisClient redisClient;
        private final StatefulRedisConnection<String, String> connection;
        private final RedisCommands<String, String> syncCommands;

        private RedisHotRankingWriter(String redisUri) {
            this.redisClient = RedisClient.create(redisUri);
            this.connection = redisClient.connect();
            this.syncCommands = connection.sync();
        }

        @Override
        public void write(CategoryTopKAggregator.CategoryTopKResult value, Context context) {
            if (value == null || value.getKey() == null || value.getTopItems() == null || value.getTopItems().isEmpty()) {
                return;
            }
            String redisKey = "trend:" + value.getKey();
            String tempKey = redisKey + ":new";

            syncCommands.del(tempKey);
            for (TrendingCalculator.TrendingResult item : value.getTopItems()) {
                String member = String.valueOf(item.getCreationId());
                syncCommands.zadd(tempKey, item.getScore(), member);
            }
            syncCommands.rename(tempKey, redisKey);
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }
}