package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrderType;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * Linear transformations are useful, but being able to "split"
 * stream transformations is even more powerful.
 *
 * <p>
 * In this chapter, we use two KStream methods:
 *   <ul>
 *   <li>{@link KStream#branch(Predicate[])}</li>
 *   <li>{@link KStream#merge(KStream)}</li>
 *   </ul>
 * </p>
 *
 * <h3>Splitting by applying several transformations to the same stream</h3>
 *
 * <p>One may split a topology by saving a KStream instance and apply
 * several transformations to it. For instance, you may choose to do this:</p>
 *
 * <pre>
 *              print
 *          .---o
 *          |
 *      .---o--------o-------
 *      |   filter   map
 * -----o
 *      |
 *      .---o----------------
 *          map
 * </pre>
 * <p>The code would look something like this:</p>
 * <pre><code>
 *     KStream&lt;K,V> start = ...;
 *     KStream&lt;K,V> filtered = start.filter((k,v) -> somePredicate(k,v));
 *     filtered.print(Printed.&lt;K,V>toSysOut());
 *     KStream&lt;K,V2> filterMapped = filtered.mapValues(v1 -> someTransformation(v1));
 *     KStream&lt;K2,V3> startMapped = start.map((k,v) -> someOtherTransformation(k,v));
 * </code></pre>
 *
 * <h3>Merging streams having the same key and value types</h3>
 *
 * <p>Once streams are split, they even can be merged back together as long as
 * their key/value types are the same.
 *
 */
public class Chapter02_BranchingAndMerging extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

    private static final Logger LOG = LoggerFactory.getLogger(Chapter02_BranchingAndMerging.class);

    public static final TopicDef<Void, String> ORDER_TEXT =
        new TopicDef<>(
            "order-texts",
            new JsonSerde.VoidSerde(),
            new JsonSerde.StringSerde());

    public static final TopicDef<String, MarketOrder> VALID_ORDERS =
        new TopicDef<>(
            "valid-orders",
            new JsonSerde.StringSerde(),
            new JsonSerde.MarketOrderSerde());

    public static final TopicDef<Void, String> INVALID_ORDERS =
        new TopicDef<>(
            "invalid-orders",
            new JsonSerde.VoidSerde(),
            new JsonSerde.StringSerde());

    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * Traders can send {@link MarketOrder}s of two kinds:
     * {@link MarketOrderType#BUY} and {@link MarketOrderType#SELL}.
     *
     * <p>The {@link #ORDER_TEXT} input topic should contain null keys, and values of the form:
     * <tt>trader transactionId (BUY|SELL) 999</tt> where 999 is a valid
     * positive number.
     *
     * <h3>Changing keys from <tt>null</tt> to the trader name</h3>
     *
     * <p>First, our topic has null keys. We will modify this to keep
     * the name of the trader as the key. This can be done in several ways:</p>
     * <ul>
     *     <li>using {@link KStream#selectKey(KeyValueMapper)} and then {@link KStream#mapValues(ValueMapper)}</li>
     *     <li>or directly using {@link KStream#map(KeyValueMapper)}, which allows to modify both keys and values</li>
     * </ul>
     *
     * <p>Here, we will parse strings into market orders using the given
     * {@link #parseOrderOrNull(Void, String)} method.</p>
     *
     * <small><i>
     *     <p>This step changes the distribution of the keys, and thus may
     *     provoke a repartition in the internal topics used by Kafka Streams.
     *     This operation may be costly and thus, changing the keys should be
     *     done as few times as possible.</p>
     *
     *     <p>More information about partitioning can be found
     *     <a href="https://docs.confluent.io/current/streams/architecture.html#stream-partitions-and-tasks">there</a>.
     *     </p>
     * </i></small>
     *
     * <h3>Branching</h3>
     *
     * <p>We will then branch into 3 different streams, using {@link KStream#branch(Predicate[])}:
     * <ul>
     *     <li>BUY orders</li>
     *     <li>SELL orders</li>
     *     <li>invalid orders</li>
     * </ul>
     *
     * <p>Note that in case of invalid orders, we keep the whole text value
     * as the key.</p>
     *
     * <h3>Writing invalid orders to the {@link #INVALID_ORDERS} topic</h3>
     *
     * <p>We will write the invalid orders to a separate topic to be handled
     * by a specific treatment, for instance security verifications, logging,
     * etc.</p>
     *
     * <p>In order to do this, we must convert back the key/values to
     * Void/String, keeping the whole content of the initial </p>
     *
     * <h3>Filtering both valid order streams</h3>
     *
     * <p>For both the BUY and SELL streams, we apply the
     * {@link #tooManySharesInOrder(String, MarketOrder)} filter.</p>
     *
     * <p><small><i>Obviously,
     * this could be done in a single stream, but for the sake of the
     * exercise, we'll do it on both. Feel free to modify the topology
     * at will after you have done it this way.</i></small></p>
     *
     * <h3>Merge the valid order streams and output to {@link #VALID_ORDERS} topic</h3>
     *
     * <p>The topology we aim for looks like this:</pre>
     *
     * <pre>
     *                       filter
     *            .->-(BUY)--o-------.
     *     branch |          filter  | merge  to
     * o-->-------o->-(SELL)-o-------o--------o
     * stream     |
     *            .->-(INVALID)--o to
     * </pre>
     */
    @SuppressWarnings("unchecked")
    protected void buildStreamTopology(StreamsBuilder builder) {

        builder.stream(ORDER_TEXT.getTopicName(), helper.consumed(ORDER_TEXT));

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:
        // map using the parseOrderOrNull method
        // branch using 3 predicates

        // map the invalid orders back into key:Void/value:String
        // to INVALID_ORDERS topic

        // filter/filterNot the BUY  orders using tooManySharesInOrder
        // filter/filterNot the SELL orders using tooManySharesInOrder
        // merge back the valid orders
        // to the VALID_ORDERS topic

        // You may find these useful for the 'to' operations:
        // invalidOrders.to(INVALID_ORDERS.getTopicName(), helper.produced(INVALID_ORDERS));
        // validOrders.to(VALID_ORDERS.getTopicName(), helper.produced(VALID_ORDERS));

        // <<< Your job ends here.

        // If you feel you spend too much time on this and need a hand, here is my solution.
        // If you use it, just don't copy paste it!
        // https://gist.github.com/glmxndr/b9c929adcba949b77baac73d3770c307
    }


    //==========================================================================
    //==== GIVEN METHODS

    private KeyValue<String, MarketOrder> parseOrderOrNull(Void key, String input) {
        try {
            Pattern p = Pattern.compile("(?i)^(?<player>[a-z0-9]+) (?<id>[a-z0-9]+) (?<type>BUY|SELL) (?<shares>[0-9]+)$");
            Matcher m = p.matcher(input);
            if (m.matches()) {
                String id = m.group("id");
                MarketOrderType type = MarketOrderType.valueOf(m.group("type"));
                int shares = Integer.parseInt(m.group("shares"), 10);
                return KeyValue.pair(
                        m.group("player"),
                        MarketOrder.make(id, type, shares)
                );
            }
            else {
                return KeyValue.pair(input, null);
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
            return KeyValue.pair(input, null);
        }
    }

    /**
     * We don't want Kerviel level catastrophes anymore.
     * Orders too big are forbidden.
     * Use this method to keep only the orders buying/selling
     * less than 1000 shares.
     */
    private boolean tooManySharesInOrder(String s, MarketOrder marketOrder) {
        return marketOrder.getShares() > 1000;
    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setUp() throws Exception {
        createTopics(ORDER_TEXT, VALID_ORDERS, INVALID_ORDERS);
        buildTopologyAndLaunchKafka(ORDER_TEXT);
    }

    @Test
    public void testUpOrDown() throws Exception {
        sendValues(ORDER_TEXT, Arrays.asList(
            "player1 txn01 BUY 5",
            "player2 txn02 BUY 99999",
            "player2 txn03 SELL 8",
            "player3 txn04 INVALID ORDER",
            "player1 txn01 SELL 5"
        ));

        assertValuesReceivedOnTopic(INVALID_ORDERS, Arrays.asList(
            "player3 txn04 INVALID ORDER"
        ));

        List<String> records = recordsConsumedOnTopic(VALID_ORDERS, 3)
            .stream()
            .map(kv -> String.format("%s %s %s %d",
                    kv.key,
                    kv.value.getTxnId(),
                    kv.value.getType().name(),
                    kv.value.getShares())
            )
            .collect(Collectors.toList());

        assertThat(records).containsExactlyInAnyOrder(
            "player1 txn01 BUY 5",
            "player2 txn03 SELL 8",
            "player1 txn01 SELL 5"
        );
    }

}
