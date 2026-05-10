package violet.trending.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import violet.trending.flink.connectors.kafka.KafkaSourceFactory;
import violet.trending.flink.jobs.TrendingJob;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JobMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100_000L);

        Map<String, String> envVars = System.getenv();
        String bootstrapServers = envVars.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "10.66.0.1:9094");

        KafkaSourceFactory.KafkaSourceOptions creationOptions =
                new KafkaSourceFactory.KafkaSourceOptions(
                        bootstrapServers,
                        parseTopics(envVars.get("CREATION_TOPICS"), "mysql.violet.creation"),
                        envVars.getOrDefault("CREATION_GROUP_ID", "flink_creation_consumer"));

        KafkaSourceFactory.KafkaSourceOptions actionOptions =
                new KafkaSourceFactory.KafkaSourceOptions(
                        bootstrapServers,
                        parseTopics(envVars.get("ACTION_TOPICS"), "action"),
                        envVars.getOrDefault("ACTION_GROUP_ID", "flink_action_consumer"));

        TrendingJob.TrendingJobOptions jobOptions = TrendingJob.TrendingJobOptions.builder()
                .withCreationSourceOptions(creationOptions)
                .withActionSourceOptions(actionOptions)
                .withWindowSize(parseDuration(envVars.get("WINDOW_SIZE"), Duration.ofMinutes(5)))
                .withCalculatorHalfLife(parseDuration(envVars.get("CALCULATOR_HALF_LIFE"), Duration.ofMinutes(10)))
                .withWindowDecayHalfLife(parseDuration(envVars.get("WINDOW_DECAY_HALF_LIFE"), Duration.ofMinutes(10)))
                .withRedisUri(envVars.getOrDefault("TRENDING_REDIS_URI", "redis://10.66.0.1:6666"))
                .build();

        TrendingJob.build(env, jobOptions);

        env.execute("Trending Job");
    }

    private static List<String> parseTopics(String raw, String defaultTopic) {
        if (raw == null || raw.isBlank()) {
            return List.of(defaultTopic);
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static Duration parseDuration(String raw, Duration fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        return Duration.parse(raw);
    }
}
