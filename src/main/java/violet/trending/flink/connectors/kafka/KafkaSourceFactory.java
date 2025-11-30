package violet.trending.flink.connectors.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import violet.trending.flink.common.utils.jsonUtils.JsonDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public final class KafkaSourceFactory {

    private KafkaSourceFactory() {}

    public static <T> KafkaSource<T> create(KafkaSourceOptions options, Class<T> targetClass, WatermarkStrategy<T> watermarkStrategy) {
        DeserializationSchema<T> schema = new JsonDeserializer<>(targetClass);

        // 1. 先把用户自定义的属性拷进来
        Properties props = new Properties();
        props.putAll(options.getAdditionalProperties());

        // 2. 根据 startFromEarliest 统一决定 auto.offset.reset
        String autoOffsetReset =
                options.isStartFromEarliest() ? "earliest" : options.getAutoOffsetReset();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        // 3. 固定一些我们想强控的行为
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, options.getSecurityProtocol());

        // 分区发现：通过 partition.discovery.interval.ms 控制
        Duration interval = options.getPartitionDiscoveryInterval();
        if (options.getBoundedness() == Boundedness.CONTINUOUS_UNBOUNDED
                && interval != null
                && !interval.isZero()
                && !interval.isNegative()) {
            props.setProperty(
                    "partition.discovery.interval.ms",
                    String.valueOf(interval.toMillis())
            );
        }

        // 4. 起始 offset 策略（老分区：用 OffsetsInitializer，新分区：用 auto.offset.reset）
        OffsetsInitializer startingOffsets = options.isStartFromEarliest()
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest();

        KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
                .setBootstrapServers(options.getBootstrapServers())
                .setTopics(options.getTopics())
                .setGroupId(options.getGroupId())
                .setClientIdPrefix(options.getClientIdPrefix())
                .setValueOnlyDeserializer(schema)
                .setStartingOffsets(startingOffsets)
                .setProperties(props);

        // 5. bounded 语义：如果是 BOUNDED，就设置「读到 latest 就停」
        if (options.getBoundedness() == Boundedness.BOUNDED) {
            builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.build();
    }

    @Getter
    @Setter
    @ToString
    @Accessors(chain = true)
    public static final class KafkaSourceOptions {
        private final String bootstrapServers;
        private final List<String> topics;
        private final String groupId;

        // Optional defaults
        private String autoOffsetReset = "latest";  // 逻辑参数，由工厂转成 OffsetsInitializer
        private boolean startFromEarliest = false;
        private String clientIdPrefix = "trending-flink";
        private Duration partitionDiscoveryInterval = Duration.ofMinutes(1);
        private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        private String securityProtocol = "PLAINTEXT";
        private Properties additionalProperties = new Properties();

        public KafkaSourceOptions(String bootstrapServers, List<String> topics, String groupId) {
            this.bootstrapServers = bootstrapServers;
            this.topics = topics;
            this.groupId = groupId;
        }

        public KafkaSourceOptions withAdditionalProperties(Properties additionalProperties) {
            if (additionalProperties != null) {
                this.additionalProperties.putAll(additionalProperties);
            }
            return this;
        }

        public KafkaSourceOptions withAdditionalProperty(String key, String value) {
            this.additionalProperties.put(key, value);
            return this;
        }
    }
}
