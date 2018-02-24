package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Here, we do more things with our streams, but we keep things simple with
 * a linear topology.
 *
 * ## What are these topologies anyway?
 *
 * A topology, in Kafka Streams jargon, is a directed acyclic graph (DAG)
 * of operations. In more cluster-oriented technologies, like Flink or Storm,
 * a topology would correspond to a set of nodes in the cluster, each doing a
 * specific job. Here, with Kafka Streams, every part of this topology runs
 * in the same JVM process.
 *
 * The Kafka Streams Processor API allows to declare topologies in great detail,
 * but we will stick to the StreamBuilder API which provides a high-level DSL
 * doing most of the work for us. This way, we can focus on the logic internal
 * to each processing step, and leave even more boilerplate to Kafka Streams.
 */
public class Chapter01_LinearStatelessStreamOps extends EmbeddedClusterBoilerplate {

    /** Sentiment analysis result */
    enum Sentiment {
        POS, NEG, NEUTRAL
    }

    enum PriceInfluence { UP, DOWN }

    public static final TopicDef<Void, Gibb> GIBB_TOPIC =
            new TopicDef<>("gibb-topic", new JsonSerde.VoidSerde(), new JsonSerde.GibbSerde());

    public static final TopicDef<Void, String> PRICE_INFLUENCE_TOPIC =
            new TopicDef<>("price-influence-topic", new JsonSerde.VoidSerde(), new JsonSerde.StringSerde());

    @Before
    public void setUp() throws Exception {
        createTopics(GIBB_TOPIC, PRICE_INFLUENCE_TOPIC);
        buildTopologyAndLaunchKafka(GIBB_TOPIC);
    }

    /**
     * Monkeys talk to each other, and influence each other. These conversations
     * can build up confidence in the economy, or negatively impact it.
     *
     * Here we wish to analyse the content of some texts sent on the Gibber
     * social network and harvested into a kafka topic. Our source is this topic.
     *
     * This analysis will allow to influence the share value price: the more
     * monkeys send positive comments on the Gibber topic, the more the price
     * of shares will go up.
     *
     * In order to do this analysis, we need to:
     *
     *   - filter only the texts involving the #mktd6 and #bananacoins hashtags
     *     (using KStream#filter)
     *
     *   - detect the text sentiment
     *     (using KStream#mapValues and #gibbAnalysis)
     *
     *   - filter out the texts with neutral sentiment
     *     (using KStream#filterNot)
     *
     *   - split on the number of influencing characters
     *     (using KStream#flatMapValues and  and #influencingChars)
     *     For instance, the text "#mktd6 #bananacoins are good!!!"
     *     has 3 influencing characters '!'
     *
     *   - provide a stream of share price updates (UP or DOWN) for each of these
     *     influencing characters
     *     (using KStream#to)
     */
    protected void buildStreamTopology(StreamsBuilder streamsBuilder) {
        KStream<Void, String> upOrDown = streamsBuilder.<Void, Gibb>stream(GIBB_TOPIC.getTopicName())
            .filter((k, v) -> v.getText().contains("#mktd6") && v.getText().contains("#bananacoins"))
            .mapValues(this::gibbAnalysis)
            .filterNot((k, t) -> t._1 == Sentiment.NEUTRAL)
            .flatMapValues(t -> influencingChars(t._1, t._2).map(Enum::name).collect(Collectors.toList()));

        // As an alternative to #peek, use #print() to display the key/value pairs passing through the stream
        upOrDown.print(Printed.<Void, String>toSysOut().withLabel("upOrDown"));

        upOrDown
            .to(PRICE_INFLUENCE_TOPIC.getTopicName());
    }




    /*
     * Some given methods are given below, in order to do the
     * sentiment analysis.
     */

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

    @Test
    public void testUpOrDown() throws Exception {
        sendValues(GIBB_TOPIC, Arrays.asList(
            new Gibb("#mktd6 this is ignored"),
            new Gibb("#mktd6 #bananacoins are good!!!"),
            new Gibb("#mktd6 #bananacoins make me sad!!"),
            new Gibb("smile happy good !!! (ignored)"),
            new Gibb("#mktd6 smile! #bananacoins")
        ));

        assertValuesReceivedOnTopic(PRICE_INFLUENCE_TOPIC, Arrays.asList(
            "UP", "UP", "UP",
            "DOWN", "DOWN",
            "UP"
        ));
    }

}
