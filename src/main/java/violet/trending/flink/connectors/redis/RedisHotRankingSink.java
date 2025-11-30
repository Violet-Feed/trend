package violet.trending.flink.connectors.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import violet.trending.flink.processing.processors.TrendingCalculator;

/**
 * Flink 2.0 sink that writes TrendingResult into Redis sorted sets using hot:{category}.
 */
public class RedisHotRankingSink implements Sink<TrendingCalculator.TrendingResult> {

    private final String redisUri;

    public RedisHotRankingSink(String redisUri) {
        this.redisUri = redisUri;
    }

    @Override
    public SinkWriter<TrendingCalculator.TrendingResult> createWriter(WriterInitContext context) {
        return new RedisHotRankingWriter(redisUri);
    }

    private static final class RedisHotRankingWriter implements SinkWriter<TrendingCalculator.TrendingResult> {

        private final RedisClient redisClient;
        private final StatefulRedisConnection<String, String> connection;
        private final RedisCommands<String, String> syncCommands;

        private RedisHotRankingWriter(String redisUri) {
            this.redisClient = RedisClient.create(redisUri);
            this.connection = redisClient.connect();
            this.syncCommands = connection.sync();
        }

        @Override
        public void write(TrendingCalculator.TrendingResult value, Context context) {
            if (value == null || value.getCategory() == null) {
                return;
            }
            String redisKey = "hot:" + value.getCategory();
            String member = String.valueOf(value.getCreationId());
            syncCommands.zadd(redisKey, value.getScore(), member);
            // Trim to top 100 by removing lowest scores when size exceeds limit.
            long size = syncCommands.zcard(redisKey);
            int maxSize = 100;
            if (size > maxSize) {
                long trimCount = size - maxSize;
                syncCommands.zremrangebyrank(redisKey, 0, trimCount - 1);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // Lettuce sync client writes immediately; no buffered flush needed.
        }

        @Override
        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }
}
