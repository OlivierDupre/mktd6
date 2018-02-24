package io.monkeypatch.mktd6.utils;

import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.ClassRule;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public abstract class EmbeddedClusterBoilerplate {

    protected static final int NUM_BROKERS = 1;
    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    protected static final Time mockTime = Time.SYSTEM;

    protected static KafkaStreams kafkaStreams;

    protected void createTopics(TopicDef<?,?>... topicDefs) throws InterruptedException {
        for (TopicDef<?,?> topicDef: topicDefs) {
            EMBEDDED_KAFKA.createTopic(topicDef.getTopicName());
        }
    }

    protected <K, V> void buildTopologyAndLaunchKafka(TopicDef<K,V> topicDef) {
        StreamsConfig streamsConfig = getStreamsConfig(topicDef);

        // Create the StreamsBuilder which will help us declare
        // the streaming topology.
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // Create the topology.
        buildStreamTopology(streamsBuilder);

        // Run the cluster...
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        kafkaStreams.start();
    }

    protected abstract void buildStreamTopology(StreamsBuilder streamsBuilder);

    public static <K, V> Properties consumerConfig(TopicDef<K, V> topic) {
        return TestUtils.consumerConfig(
                EMBEDDED_KAFKA.bootstrapServers(),
                topic.getKeyDeserializerClass(),
                topic.getValueDeserializerClass());
    }

    public static <K, V> Properties producerConfig(TopicDef<K, V> topic) {
        return TestUtils.producerConfig(
                EMBEDDED_KAFKA.bootstrapServers(),
                topic.getKeySerializerClass(),
                topic.getValueSerializerClass());
    }

    protected <K, V> StreamsConfig getStreamsConfig(TopicDef<K, V> topicDef) {
        Properties properties = getStreamsConfigProperties(topicDef);
        return new StreamsConfig(properties);
    }

    protected <K, V> Properties getStreamsConfigProperties(TopicDef<K, V> topicDef) {
        Properties properties = StreamsTestUtils.getStreamsConfig(
                this.getClass().getSimpleName(),
                EMBEDDED_KAFKA.bootstrapServers(),
                topicDef.getKeySerdeClass().getName(),
                topicDef.getValueSerdeClass().getName(),
                new Properties());
        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        return properties;
    }

    protected <K,V> void sendValues(TopicDef<K, V> topicDef, List<V> values) throws Exception {
        IntegrationTestUtils.produceValuesSynchronously(
                topicDef.getTopicName(),
                values,
                producerConfig(topicDef),
                mockTime);
    }

    protected <K,V> void assertValuesReceivedOnTopic(TopicDef<K, V> topicDef, List<V> expected) throws Exception {
        List<String> actual = IntegrationTestUtils
                .waitUntilMinValuesRecordsReceived(
                        consumerConfig(topicDef),
                        topicDef.getTopicName(),
                        expected.size());
        assertThat(actual, equalTo(expected));
    }

    @After
    public void tearDownAll() {
        if (kafkaStreams != null) { kafkaStreams.close(); }
    }
}
