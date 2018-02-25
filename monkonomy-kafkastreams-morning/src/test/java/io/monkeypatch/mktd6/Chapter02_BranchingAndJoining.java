package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Linear transformations are useful, but being able to split and
 * join streams is a lot more powerful.
 *
 * In this chapter, we use two KStream methods allowing this:
 *   - {@link KStream#branch(Predicate[])}
 *   - {@link KStream#join(KStream, ValueJoiner, JoinWindows)}
 *
 * The curious developer can already see that joining can be
 * done with several method names (join, leftJoin, outerJoin, etc.)
 * and several input types (KStream, KTable, etc.). We will
 * address these differences in a next chapter, for now just focus
 * on `join` between two KStream instances.
 *
 * The topology may then look like this:
 *
 *   branch         join
 *        o--(...)--o
 *       /           \
 * o----o-o--(...)--o-o-----o
 *       \           /
 *        o--(...)--o
 */
public class Chapter02_BranchingAndJoining extends EmbeddedClusterBoilerplate {

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
            new Gibb(id, time, "#mktd6 this is ignored"),
            new Gibb(id, time, "#mktd6 #bananacoins are good!!!"),
            new Gibb(id, time, "#mktd6 #bananacoins make me sad!!"),
            new Gibb(id, time, "smile happy good !!! (ignored)"),
            new Gibb(id, time, "#mktd6 smile! #bananacoins")
        ));

        assertValuesReceivedOnTopic(PRICE_INFLUENCE_TOPIC, Arrays.asList(
            "UP", "UP", "UP",
            "DOWN", "DOWN",
            "UP"
        ));
    }

}
