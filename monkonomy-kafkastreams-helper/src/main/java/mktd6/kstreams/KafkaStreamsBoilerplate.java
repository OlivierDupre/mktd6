package mktd6.kstreams;

import mktd6.topic.TopicDef;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreamsBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsBoilerplate.class);

    private final String bootstrapServers;
    private final String applicationId;

    public KafkaStreamsBoilerplate(String bootstrapServers, String applicationId) {
        this.bootstrapServers = bootstrapServers;
        this.applicationId = applicationId;
    }

    public <K, V> Properties consumerConfig(TopicDef<K, V> topic, Map<String, String> additional) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, topic.getKeyDeserializerClass().getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, topic.getValueDeserializerClass().getName());
        consumerConfig.putAll(additional);
        return consumerConfig;
    }

    public <K, V> Properties consumerConfig(TopicDef<K, V> topic) {
        return consumerConfig(topic, Collections.emptyMap());
    }

    public <K, V> Properties producerConfig(TopicDef<K, V> topic, boolean exactlyOnce,  Map<String, String> additional) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, topic.getKeySerializerClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, topic.getValueSerializerClass().getName());
        if (exactlyOnce) {
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        }
        properties.putAll(additional);
        return properties;
    }

    public <K, V> Properties producerConfig(TopicDef<K, V> topic, boolean exactlyOnce) {
        return producerConfig(topic, exactlyOnce, Collections.emptyMap());
    }

    public Properties streamsConfigProps(boolean exactlyOnce, String offset) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
        if (exactlyOnce) {
            streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        }
        return streamsConfiguration;
    }

    public Properties streamsConfigProps(boolean exactlyOnce) {
        return streamsConfigProps(exactlyOnce, "earliest");
    }

    protected <K, V> Properties streamsConfigProps(TopicDef<K, V> topicDef, boolean exactlyOnce) {
        return streamsConfigProps(topicDef, exactlyOnce, "earliest");
    }

    protected <K, V> Properties streamsConfigProps(TopicDef<K, V> topicDef, boolean exactlyOnce, String offset) {
        Properties properties = streamsConfigProps(exactlyOnce, offset);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, topicDef.getKeySerde().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, topicDef.getValueSerde().getClass().getName());
        return properties;
    }

    public StreamsConfig streamsConfig(boolean exactlyOnce) {
        return new StreamsConfig(streamsConfigProps(exactlyOnce));
    }

    public <K,V> StreamsConfig streamsConfig(boolean exactlyOnce, String offset) {
        return new StreamsConfig(streamsConfigProps(exactlyOnce, offset));
    }

    public <K,V> StreamsConfig streamsConfig(TopicDef<K,V> topicDef, boolean exactlyOnce) {
        return new StreamsConfig(streamsConfigProps(topicDef, exactlyOnce));
    }

    public <K,V> Consumed<K,V> consumed(TopicDef<K, V> topicDef) {
        return Consumed
            .with(topicDef.getKeySerde(), topicDef.getValueSerde())
            .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp());
    }

    public <K,V> Produced<K,V> produced(TopicDef<K, V> topicDef) {
        return Produced.with(topicDef.getKeySerde(), topicDef.getValueSerde());
    }

    public File tempDirectory() {
        String prefix = "kafka-";
        final File file;
        try {
            Path dir = Paths.get(System.getProperty("java.io.tmpdir"));
            file = Files.createTempDirectory(dir, prefix).toFile();
        } catch (IOException var4) {
            throw new RuntimeException("Failed to create a temp dir", var4);
        }
        file.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    Utils.delete(file);
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }

            }
        });
        return file;
    }

    public <K,VL,VR> Joined<K, VL, VR> joined(TopicDef<K, VL> tl, TopicDef<K, VR> tr) {
        return Joined.with(tl.getKeySerde(), tl.getValueSerde(), tr.getValueSerde());
    }

    public <K,V> Serialized<K,V> serialized(TopicDef<K,V> topicDef) {
        return Serialized.with(topicDef.getKeySerde(), topicDef.getValueSerde());
    }
}
