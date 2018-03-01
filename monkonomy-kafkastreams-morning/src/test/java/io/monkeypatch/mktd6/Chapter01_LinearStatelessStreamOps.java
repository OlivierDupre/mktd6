package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Here, we do more things with our streams, but we keep things simple with
 * a linear topology.
 *
 * <pre>
 *     ---o----o----o----o----
 * </pre>
 *
 * <h3>What are these <a href="https://docs.confluent.io/current/streams/architecture.html#processor-topology">topologies</a> anyway?</h3>
 *
 * <p>A topology, in Kafka Streams jargon, is a <b>directed acyclic graph (DAG)</b>
 * of operations. In more cluster-oriented technologies, like Flink or Storm,
 * a topology would correspond to a set of nodes in the cluster, each doing a
 * specific job. Here, with Kafka Streams, every part of this topology runs
 * in the same JVM process.
 *
 * <p>The Kafka Streams Processor API allows to declare topologies in great detail,
 * but we will stick to the StreamBuilder API which provides a high-level DSL
 * doing most of the work for us. This way, we can focus on the logic internal
 * to each processing step, and leave even more boilerplate to Kafka Streams.
 */
public class Chapter01_LinearStatelessStreamOps extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

    enum Sentiment { POS, NEG, NEUTRAL }

    enum PriceInfluence { UP, DOWN }

    public static final TopicDef<String, Gibb> GIBB_TOPIC =
            new TopicDef<>("gibb-topic", new JsonSerde.StringSerde(), new JsonSerde.GibbSerde());

    public static final TopicDef<String, String> PRICE_INFLUENCE_TOPIC =
            new TopicDef<>("price-influence-topic", new JsonSerde.StringSerde(), new JsonSerde.StringSerde());

    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * Monkeys talk to each other, and influence each other. These conversations
     * can build up confidence in the economy, or negatively impact it.
     *
     * <p>Here we wish to analyse the content of some texts sent on the Gibber
     * social network and harvested into a kafka topic. Our source is this topic.
     *
     * <p>This analysis will allow to influence the share value price: the more
     * monkeys send positive comments on the Gibber topic, the more the price
     * of shares will go up.
     *
     * <p>In order to do this analysis, we need to:
     * <ul>
     *   <li>read gibbs from the input gibb topic using
     *     {@link StreamsBuilder#stream(String, Consumed)}
     *
     *   <li>filter only the texts involving the #mktd6 and #bananacoins hashtags
     *     (using {@link KStream#filter(Predicate)})
     *
     *   <li>detect the text sentiment
     *     (using {@link KStream#mapValues(ValueMapper)}
     *     and the given method {@link #gibbAnalysis(Gibb)})
     *
     *   <li>filter out the texts with neutral sentiment
     *     (using {@link KStream#filterNot(Predicate)})
     *
     *   <li>split on the number of influencing characters
     *     (using {@link KStream#flatMapValues(ValueMapper)} and
     *     the given method {@link #influencingChars(Sentiment, String)})
     *     <br>For instance, the text "#mktd6 #bananacoins are good!!!"
     *     has 3 influencing characters '!'
     *
     *   <li>write the resulting stream events of share price updates
     *     (UP or DOWN) to an output topic (using {@link KStream#to(String, Produced)}).
     * </ul>
     *
     * <p>The resulting topology would look like this (read top to bottom):</p>
     * <pre>
     *     o stream
     *     |
     *     o filter
     *     |
     *     o mapValues
     *     |
     *     o filterNot
     *     |
     *     o flatMapValues
     *     |
     *     o to
     * </pre>
     *
     * <strong>Important note!!!</strong>
     * <p>
     *     Notice the {@link Produced} and {@link Consumed} classes in the
     *     start and end methods. These classes allows KafkaStreams to know
     *     how to serialize/deserialize the keys/values.
     *
     *     <br>These parameters may be omitted when the keys/values have
     *     the StreamsBuilder default key/value serializers/deserializers,
     *     but it is good practice to always provide them explicitly.
     *
     *     <br>If you don't provide them, the program may fail at runtime
     *     and KafkaStreams does not help you much in identifying exactly
     *     where the serialization error occurred.
     *
     *     <br>Helper methods for creating these classes exist in the given
     *     'helper' field.
     * </p>
     *
     */
    protected void buildStreamTopology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .stream(
                GIBB_TOPIC.getTopicName(),
                helper.consumed(GIBB_TOPIC)
            )
            // >>> Your job starts here.

                // ######## Read the javadoc !!! ########

                // Summary:
                // filter on #mktd6 and #bananacoins
                // mapValues to convert to sentiments
                // filterNot to eliminate useless sentiments
                // flatMapValues to convert to influencing characters

                // The following line is there only to make things compile and must be removed
                .map((k, v) -> KeyValue.<String, String>pair(null, null))
            // <<< Your jobs ends there.

            .to(
                PRICE_INFLUENCE_TOPIC.getTopicName(),
                helper.produced(PRICE_INFLUENCE_TOPIC)
            );

        // If you feel you spend too much time on this and need a hand, here is my solution.
        // If you use it, just don't copy paste it!
        // https://gist.github.com/glmxndr/ed6f14fc3f22897527c796f35b0a1281
    }



    //==========================================================================
    //==== GIVEN METHODS

    private Tuple2<Sentiment, String> gibbAnalysis(Gibb gibb) {
        String text = gibb.getText();
        Sentiment sentiment
            = text.matches(".*\\b(smile|happy|good|yes)\\b.*") ? Sentiment.POS
            : text.matches(".*\\b(frown|sad|bad|no)\\b.*") ? Sentiment.NEG
            : Sentiment.NEUTRAL;
        return Tuple.of(sentiment, text);
    }

    private Stream<PriceInfluence> influencingChars(Sentiment s, String text) {
        return text.chars()
            .filter(c -> c == '!')
            .mapToObj(c -> sentimentToInfluence(s));
    }

    private PriceInfluence sentimentToInfluence(Sentiment s) {
        return s == Sentiment.POS ? PriceInfluence.UP : PriceInfluence.DOWN;
    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setUp() throws Exception {
        createTopics(GIBB_TOPIC, PRICE_INFLUENCE_TOPIC);
        buildTopologyAndLaunchKafka(GIBB_TOPIC);
    }

    @Test
    public void testUpOrDown() throws Exception {
        sendValues(GIBB_TOPIC, Arrays.asList(
            new Gibb("001", DateTime.now(DateTimeZone.UTC), "#mktd6 this is ignored"),
            new Gibb("002", DateTime.now(DateTimeZone.UTC), "#mktd6 #bananacoins are good!!!"),
            new Gibb("003", DateTime.now(DateTimeZone.UTC), "#mktd6 #bananacoins make me sad!!"),
            new Gibb("004", DateTime.now(DateTimeZone.UTC), "smile happy good !!! (ignored)"),
            new Gibb("005", DateTime.now(DateTimeZone.UTC), "#mktd6 smile! #bananacoins")
        ));

        assertValuesReceivedOnTopic(PRICE_INFLUENCE_TOPIC, Arrays.asList(
            "UP", "UP", "UP",
            "DOWN", "DOWN",
            "UP"
        ));
    }

}
