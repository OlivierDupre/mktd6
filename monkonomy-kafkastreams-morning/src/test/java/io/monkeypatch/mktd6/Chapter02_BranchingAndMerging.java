package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrderType;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * <p>The topology we aim for looks like this:</pre>
 *
 * <pre>
 *        .---o---(...)---.
 * branch |               | merge
 * -------o---o---(...)---o---o------
 *        |                   | merge
 *        .---o---(...)-------.
 * </pre>
 *
 * <p>(Note that merging can be done only with streams with the same types
 * for keys/values.)</p>
 */
public class Chapter02_BranchingAndMerging extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

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

    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * Traders can send {@link MarketOrder}s of two kinds:
     * {@link MarketOrderType#BUY} and {@link MarketOrderType#SELL}.
     *
     * <p>Here, we will parse strings into market orders using the given
     * {@link #parseOrderOrBuy0(String)} method.
     * <p>The input topic should contain null keys, and values of the form:
     * <tt>trader transactionId (BUY|SELL) 999</tt> where 999 is a valid
     * positive number.
     *
     * <p>First, our topic has null keys. We will modify this to keep
     * the </p>
     *
     * <p>We will then branch into 3 different streams:
     * <ul>
     *     <li>BUY orders</li>
     *     <li>SELL orders</li>
     *     <li>invalid orders, transformed to BUY 0 orders</li>
     * </ul>
     *
     *
     *
     *
     * <p>Our input topic has </p>
     *
     *
     */
    protected void buildStreamTopology(StreamsBuilder builder) {

        // TODO

    }

    //==========================================================================
    //==== GIVEN METHODS

    private MarketOrder parseOrderOrBuy0(String input) {
        try {
            Pattern p = Pattern.compile("^(?<id>[a-z]+) (?<type>BUY|SELL) (?<shares>-?\\d+)$");
            Matcher m = p.matcher(input);
            String id = m.group("id");
            MarketOrderType type = MarketOrderType.valueOf(m.group("type"));
            int shares = Integer.parseInt(m.group("shares"), 10);
            return MarketOrder.make(id, type, shares);
        }
        catch(Exception e) {
            return null;
        }
    }


    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setUp() throws Exception {
        createTopics(ORDER_TEXT, VALID_ORDERS);
        buildTopologyAndLaunchKafka(ORDER_TEXT);
    }

    @Test
    public void testUpOrDown() throws Exception {

    }

}
