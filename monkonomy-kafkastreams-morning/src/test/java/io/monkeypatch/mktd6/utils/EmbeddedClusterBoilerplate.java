package io.monkeypatch.mktd6.utils;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.vavr.Tuple3;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public abstract class EmbeddedClusterBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedClusterBoilerplate.class);

    protected static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    protected final KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(EMBEDDED_KAFKA.bootstrapServers(), this.getClass().getSimpleName());

    protected static final Time mockTime = Time.SYSTEM;

    protected static KafkaStreams kafkaStreams;

    protected void createTopics(TopicDef<?,?>... topicDefs) throws InterruptedException {
        for (TopicDef<?,?> topicDef: topicDefs) {
            EMBEDDED_KAFKA.createTopic(topicDef.getTopicName());
        }
        String[] topics = Arrays.stream(topicDefs)
            .map(TopicDef::getTopicName)
            .collect(Collectors.toList())
            .toArray(new String[]{});
        EMBEDDED_KAFKA.waitForRemainingTopics(10000, topics);
    }

    protected <K, V> void buildTopologyAndLaunchKafka(TopicDef<K,V> topicDef) {
        StreamsConfig streamsConfig = helper.streamsConfig(topicDef, false);

        // Create the StreamsBuilder which will help us declare
        // the streaming topology.
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // Create the topology.
        buildStreamTopology(streamsBuilder);

        // Run the cluster...
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        kafkaStreams.start();
    }

    protected abstract void buildStreamTopology(StreamsBuilder builder);

    protected <K,V> void sendValues(TopicDef<K, V> topicDef, List<V> values) throws Exception {
        IntegrationTestUtils.produceValuesSynchronously(
            topicDef.getTopicName(),
            values,
            helper.producerConfig(topicDef, false),
            mockTime);
    }

    protected <K,V> void sendKeyValues(TopicDef<K, V> topicDef, List<KeyValue<K, V>> kvs) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topicDef.getTopicName(),
            kvs,
            helper.producerConfig(topicDef, false),
            mockTime);
    }

    public void sendKeyValues(
        Collection<Tuple3<TopicDef<?,?>, Object, Object>> tkvs,
        long wait
    ) throws ExecutionException, InterruptedException {

        Set<TopicDef<?,?>> topics = tkvs.stream()
            .map(Tuple3::_1)
            .collect(Collectors.toSet());

        Map<String, Producer> producers = topics.stream()
            .collect(Collectors.toMap(
                t -> t.getTopicName(),
                t -> new KafkaProducer<>(helper.producerConfig(t, false)),
                (a,b) -> a
            ));

        tkvs.stream().forEach(tkv -> {
            TopicDef topicDef = tkv._1;
            String topicName = topicDef.getTopicName();
            Object key = tkv._2;
            Object value = tkv._3;
            LOG.info("######## {} - {}={}", topicName, key, value);
            producers
                .get(topicName)
                .send(new ProducerRecord(topicName, key, value));
            sleep(wait);
        });

        producers.values().forEach(Producer::close);
    }

    public <K,V> String keyToString(TopicDef<K,V> topicDef, K key) {
        return new String(topicDef.getKeySerde().serializer().serialize(topicDef.getTopicName(), key));
    }
    public <K,V> String valueToString(TopicDef<K,V> topicDef, V value) {
        return new String(topicDef.getValueSerde().serializer().serialize(topicDef.getTopicName(), value));
    }

    private void sleep(long wait) {
        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    protected <K,V> List<KeyValue<K,V>> recordsConsumedOnTopic(
        TopicDef<K, V> topicDef,
        int number
    ) throws Exception {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            helper.consumerConfig(topicDef),
            topicDef.getTopicName(),
            number);
    }

    protected <K,V> void consume(
            TopicDef<K, V> topicDef,
            BiConsumer<K,V> kvConsumer,
            AtomicBoolean stop
    ) throws Exception {
        Properties properties = helper.consumerConfig(topicDef);
        Consumer<K,V> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicDef.getTopicName()));

        new Thread(() -> {
            while(!stop.get()) {
                ConsumerRecords<K, V> record = consumer.poll(10L);
                record.iterator()
                    .forEachRemaining(r -> kvConsumer.accept(r.key(), r.value()));
            }
            consumer.close();
        }).start();
    }


    protected <K,V> void assertValuesReceivedOnTopic(TopicDef<K, V> topicDef, List<V> expected) throws Exception {
        List<String> actual = IntegrationTestUtils
            .waitUntilMinValuesRecordsReceived(
                helper.consumerConfig(topicDef),
                topicDef.getTopicName(),
                expected.size());
        assertThat(actual, equalTo(expected));
    }

    @After
    public void tearDownAll() {
        if (kafkaStreams != null) { kafkaStreams.close(); }
    }
}
