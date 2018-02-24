package io.monkeypatch.mktd6.kstreams;

import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

    public <K, V> Properties producerConfig(TopicDef<K, V> topic, Map<String, String> additional) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, topic.getKeySerializerClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, topic.getValueSerializerClass().getName());
        properties.putAll(additional);
        return properties;
    }

    public <K, V> Properties producerConfig(TopicDef<K, V> topic) {
        return producerConfig(topic, Collections.emptyMap());
    }

    protected <K, V> StreamsConfig getStreamsConfig(TopicDef<K, V> topicDef) {
        Properties properties = new Properties();//TODO
        return new StreamsConfig(properties);
    }

    public Properties getStreamsConfig(String bootstrapServers, String keySerdeClassName, String valueSerdeClassName, Properties additional) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        streamsConfiguration.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000");
        streamsConfiguration.put("default.key.serde", keySerdeClassName);
        streamsConfiguration.put("default.value.serde", valueSerdeClassName);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.putAll(additional);
        return streamsConfiguration;
    }


    public File tempDirectory() {
        String prefix = "kafka-";
        final File file;
        try {
            file = Files.createTempDirectory(Paths.get("/tmp"), prefix).toFile();
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
}
