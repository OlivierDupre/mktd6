package mktd6;

import mktd6.model.market.SharePriceInfo;
import mktd6.serde.JsonSerde;
import mktd6.topic.TopicDef;
import mktd6.utils.EmbeddedClusterBoilerplate;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Here, we write and run our very first Kafka Streams app.
 *
 * <p>Most of the boilerplate is hidden away in the parent class,
 * which will be used in all chapters. You may take a look at
 * this class, or skip it entirely. Its purpose it to hide away
 * the tedious details of how to configure and run a Kafka Streams
 * app. This is not very important, but often clunky and repetitive
 * in actual code, so some of it is hidden away, and only the
 * strictly necessary boilerplate code is shown.
 *
 * <p>Your job is to complete the {@link #buildStreamTopology(StreamsBuilder)}
 * method.
 *
 * <p>In order to run our first simple KafkaStreams application,
 * we use an embedded kafka cluster. This technique can be used in
 * tests and during development, to prevent the need for an
 * actual Kafka cluster, but it is heavy on resources and should
 * not necessarily be the default choice for unit tests.
 */
public class Chapter00_RunMyFirstKafkaStreamsApp extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

    // We will be reading from this topic.
    // (TopicDef is a helper class that is not part of Kafka.)
    public static final TopicDef<String, SharePriceInfo> SHARE_PRICE_TOPIC = TopicDef.SHARE_PRICE;
    public static final String SHARE_PRICE_TOPIC_NAME = SHARE_PRICE_TOPIC .getTopicName();

    // And we will be writing to this topic:
    public static final String BUY_OR_SELL_TOPIC_NAME = "buy-or-sell";
    public static final TopicDef<String, String> BUY_OR_SELL_TOPIC = new TopicDef<>(
        BUY_OR_SELL_TOPIC_NAME,
        new JsonSerde.StringSerde(),  // Keys are Strings
        new JsonSerde.StringSerde()); // Values are Strings


    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * We want to decide whether we should buy or sell shares in the market.
     *
     * <p>We are given a stream of SharePriceInfo, containing a price forecast.
     * The forecast is a multiplicator (always a positive number):
     * <ul>
     * <li>if it is greater than 1, it means the price is likely to go up
     * <li>if it is lower than 1, it means the price is likely to go down.
     * </ul>
     *
     * <p>We will be coding how to create such a forecast in a later chapter.
     *
     * <p>For now, we want to convert each SharePriceInfo into a String
     * containing "BUY" if we think we should buy, or "SELL" if we think
     * we should sell, based only on the forecast.
     */
    protected void buildStreamTopology(StreamsBuilder builder) {
        // We read from the share price topic
        builder.<String, SharePriceInfo>stream(SHARE_PRICE_TOPIC_NAME)

            // .peek allows to add dirty IO for stream events
            .peek((k, v) -> System.out.println(k + " = " + v))

            /*====
               You should do something here, using the fluent API.
               This may help you:
               https://kafka.apache.org/10/documentation/streams/developer-guide/dsl-api.html#stateless-transformations
               https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/kstream/KStream.html#mapValues-org.apache.kafka.streams.kstream.ValueMapper-

               If you run this test without doing anything here, you will get a
               SerializationException: you are sending the wrong kind of data
               to the sink topic.
            ====*/

            // We write our results to the buy-or-sell topic.
            .to(BUY_OR_SELL_TOPIC_NAME);
    }

    //==========================================================================
    //==== TEST LOGIC

    // Now for some tedious ceremony for this stream.
    // Look at it once, and then ignore it in future chapters.
    @Before
    public void setUp() throws Exception {
        // Create the topics in the embedded cluster
        createTopics(SHARE_PRICE_TOPIC, BUY_OR_SELL_TOPIC);
        // Create the configurations, connecting to the correct cluster
        // and providing the correct serialization/deserialization logic
        buildTopologyAndLaunchKafka(SHARE_PRICE_TOPIC);
    }

    @Test
    public void test() throws Exception {
        // Send this list of objects in the source topic.
        // (Don't worry about keys for now, only values are sent.)
        sendValues(SHARE_PRICE_TOPIC, Arrays.asList(
            SharePriceInfo.make(1.0f, 3.14f),   // mult is > 1, BUY
            SharePriceInfo.make(1.1f, 0.42f),   // mult is < 1, SELL
            SharePriceInfo.make(0.9f, 0.9999f), // mult is < 1, SELL
            SharePriceInfo.make(1.05f,1.0001f)  // mult is > 1, BUY
        ));

        // These are the values we expect to receive in the sink topic.
        // Here again, keys are ignored, we care only about values.
        assertValuesReceivedOnTopic(BUY_OR_SELL_TOPIC, Arrays.asList(
            "BUY",
            "SELL",
            "SELL",
            "BUY"
        ));

    }

}
