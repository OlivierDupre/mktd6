package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In this chapter, we will aggregate data which are temporally close
 * to each other.
 *
 * <h3>On the importance of partitions, and keys</h3>
 *
 * <p>KafkaStreams being based on Kafka, messages are assigned to a partition.</p>
 *
 * <p>The key of the key/value pair determine which partition the message is sent to.</p>
 *
 * <p><u>Conclusion:</u></p>
 * <p>Messages can only be grouped if they are on the same partition, so
 * we must make sure to give the same key to all the messages we want to group
 * together.</p>
 *
 * <p>In this chapter, we will get a stream with keys already well-structured
 * keys. We only care about grouping messages together.</p>
 *
 * <p>In order to work with grouped values, there is no other way but to
 * transit through {@link KTable}s.</p>
 *
 * <h3>What are {@link KTable}s?</h3>
 *
 * <p>Kafka allows to work using the
 * "<a href="https://martinfowler.com/eaaDev/EventSourcing.html">event sourcing</a>"
 * pattern.</p>
 *
 * <p>So streams are supposed to represent events, which describe how some
 * piece of data evolves. A {@link KTable} is a view at a certain time of
 * this data.</p>
 *
 * <p>In Kafka Streams, the data which evolves is identified by the record key.
 * The record value represents the way the key evolves. So a {@link KTable}
 * will represent an aggregation, or a reduction, of the values for a given key.
 * You can think of a table as a snapshot in time of a state for each key.
 * </p>
 *
 * <p>A {@link KTable} can be converted back to a {@link KStream}, retrieving
 * the events that make the data evolve.</p>
 *
 * <p>For more information on the so-called stream/table duality, read this
 * <a href="https://docs.confluent.io/current/streams/concepts.html#duality-of-streams-and-tables"article</a>.
 * </p>
 */
public class Chapter03_GroupingAndAggregating extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

    private static final Logger LOG = LoggerFactory.getLogger(Chapter03_GroupingAndAggregating.class);

    public static final TopicDef<String, Investment> INVESTMENTS = new TopicDef<>(
        "investments",
        new JsonSerde.StringSerde(),
        new JsonSerde.InvestmentSerde()
    );

    public static final TopicDef<String, Investment> GROUPED_INVESTMENTS = new TopicDef<>(
            "grouped-investments",
            new JsonSerde.StringSerde(),
            new JsonSerde.InvestmentSerde()
    );
    public static final int TIME_WINDOW_MILLIS = 100;

    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * The input topic sends investment orders from players.
     *
     * <p>Keys are the player names, and values are the investment orders.
     *
     * <p>The logic behind this exercise is that players may send a lot
     * of investment orders, and if they are close in time, we wish
     * to group them together in order to lower the load on the servers
     * which will manage them.</p>
     *
     * <p>{@link Investment}s are easy to combine. The given method
     * {@link #combineInvestments(Investment, Investment)} allows to do this.</p>
     *
     * <p>The relevant methods would be these:</p>
     * <ul>
     *     <li>{@link KStream#groupByKey()}, returning a {@link KGroupedStream}</li>
     *     <li>{@link KGroupedStream#windowedBy(Windows)}, returning a {@link TimeWindowedKStream}</li>
     *     <li>{@link TimeWindowedKStream#reduce(Reducer)}, returning a {@link KTable}</li>
     *     <li>{@link KTable#toStream(KeyValueMapper)}, returning back a {@link KStream}</li>
     * </ul>
     * <p><small><i>
     *     Many of these methods have variants, with different parameters.
     *     It is instructive to have a look at them.
     * </i></small></p>
     *
     * <p>Note the sequence of classes:</p>
     * <ul>
     *     <li>we start with a regular {@link KStream}</li>
     *     <li>transformed into a {@link KGroupedStream}, which is an
     *     abstraction over KStreams (most of its methods are now
     *     deprecated, so make sure to read their doc to know if there
     *     is a better way to do the same thing)</li>
     *     <li>converted into a {@link TimeWindowedKStream}, providing several
     *     ways to create {@link KTable}s</li>
     * </ul>
     */
    @Override
    protected void buildStreamTopology(StreamsBuilder builder) {

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:

        // stream data from the INVESTMENTS topic
        // group the stream by key
        // window stream messages in TIME_WINDOW_MILLIS time windows
        // reduce the grouped events using the combineInvestments
        // convert back the reduced values to a stream
        // write the messages to the GROUPED_INVESTMENTS topic

        // <<< Your job ends here.

        // If you feel you spend too much time on this and need a hand, here is my solution.
        // If you use it, just don't copy paste it!
        // https://gist.github.com/glmxndr/ed97d7aa4b5766db598c9405413eb139

    }

    //==========================================================================
    //==== GIVEN METHODS

    private Investment combineInvestments(Investment i1, Investment i2) {
        return Investment.make(
            i1.getTxnId(),
            i1.getInvested() + i2.getInvested());
    }

    //==========================================================================
    //==== TEST LOGIC

    private final int iterations = 20;
    private final CountDownLatch sendingLatch = new CountDownLatch(iterations);
    private final CountDownLatch receivingLatch = new CountDownLatch(1);
    private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(4);
    private final Producer<String, Investment> producer = new KafkaProducer<>(helper.producerConfig(INVESTMENTS, false));

    @Before
    public void setUp() throws Exception {
        createTopics(INVESTMENTS, GROUPED_INVESTMENTS);
        buildTopologyAndLaunchKafka(INVESTMENTS);
    }

    @Test
    public void test() throws Exception {
        // We send groups of investment orders every TIME_WINDOW_MILLIS
        exec.execute(() -> iterateSendingInvestments(TIME_WINDOW_MILLIS, () -> {
            sendInvestment("player1", Investment.make("txn01", 1));   // 1
            sendInvestment("player1", Investment.make("txn02", 2));   // 1
            sendInvestment("player2", Investment.make("txn03", 3));   // 2
            sendInvestment("player1", Investment.make("txn04", 4));   // 1
            sendInvestment("player2", Investment.make("txn05", 5));   // 2
            sendInvestment("player1", Investment.make("txn06", 6));   // 1
            sendInvestment("player3", Investment.make("txn06", 100)); // 3
        }));

        // We read the grouping topic and keep only the max values invested for each player
        // (With very high probability, we will have all events for each batch summed up
        // at least one time.)
        Map<String, Double> maxResults = exec
            .submit(this::readMaxValuesInGroupedInvestments)
            .get();

        // Wait for all async sending/reading to finish
        sendingLatch.await();
        receivingLatch.await();
        LOG.info("Max Results: {}", maxResults);

        // We should have both players and the correct grouped investments
        assertThat(maxResults).containsKeys("player1", "player2", "player3");
        assertThat(maxResults.get("player1")).isEqualTo(13); // 13 = 1+2+4+6
        assertThat(maxResults.get("player2")).isEqualTo(8);  //  8 = 3+5
        assertThat(maxResults.get("player3")).isEqualTo(100);
    }



    private Map<String, Double> readMaxValuesInGroupedInvestments() {
        Map<String, Double> maxResults = new ConcurrentHashMap<>();
        try {
            recordsConsumedOnTopic(GROUPED_INVESTMENTS, iterations * 2)
                .forEach(kv -> {
                    if (!maxResults.containsKey(kv.key) ||
                        maxResults.get(kv.key) < kv.value.getInvested()
                    ) {
                        maxResults.put(kv.key, kv.value.getInvested());
                    }
                });
        }
        catch (Exception e) { e.printStackTrace(); }
        finally { receivingLatch.countDown(); }
        return maxResults;
    }

    private void sendInvestment(String player, Investment inv) {
        producer.send(new ProducerRecord<>(INVESTMENTS.getTopicName(),player, inv));
    }

    private void iterateSendingInvestments(int timeWindowMillis, Runnable run) {
        try {
            run.run();
        }
        finally {
            sendingLatch.countDown();
            if (sendingLatch.getCount() > 0) {
                exec.schedule(
                    () -> iterateSendingInvestments(timeWindowMillis, run),
                    timeWindowMillis,
                    TimeUnit.MILLISECONDS);
            }
        }
    }

}
