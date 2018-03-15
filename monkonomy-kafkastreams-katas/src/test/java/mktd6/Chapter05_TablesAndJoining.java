package mktd6;


import mktd6.model.Team;
import mktd6.model.trader.Trader;
import mktd6.model.trader.TraderState;
import mktd6.serde.JsonSerde;
import mktd6.topic.TopicDef;
import mktd6.utils.EmbeddedClusterBoilerplate;
import io.vavr.Tuple;
import org.apache.kafka.streams.StreamsBuilder;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * In this exercise we will explore joining.
 *
 * <p>Joining can happen between streams, between tables or between streams
 * and tables. In all of these cases, there may be left joins, inner joins or
 * outer joins. Reading the javadoc for each of these methods provides
 * clear information about how they work.</p>
 *
 * <p>In this exercise, we will create a table, and left-join a stream
 * with this table.</p>
 *
 * <h3>Joining</h3>
 *
 * <p>Joining can happen only between streams/tables having the same key type.</p>
 *
 * <p>When joining two streams/tables, what we do is actually get values
 * corresponding to the same key, and applying a function to both values.
 * Values may be of different types.</p>
 *
 * <p>Joining happens in time windows: events too far apart from each other
 * in the joined streams will not be joined. The time windowing is configurable.
 * </p>
 *
 * <p>When joining a KStream with a KTable, however, the time windowing that
 * was used to create the table is used, so there is no need to specify it
 * in this particular case (idem when joining two tables).</p>
 *
 * <p>A rather lengthy but complete article about joining in Kafka Streams can be found
 * <a href="https://blog.codecentric.de/en/2017/02/crossing-streams-joins-apache-kafka/">there</a>,
 * with nice visual examples. Skimming over it can be rewarding.</p>
 *
 */
public class Chapter05_TablesAndJoining extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

    private static final Logger LOG = LoggerFactory.getLogger(Chapter05_TablesAndJoining.class);

    public static final TopicDef<Trader, TraderState> STATES = new TopicDef<>(
        "trader-states",
        new JsonSerde.TraderSerde(),
        new JsonSerde.TraderStateSerde()
    );

    public static final TopicDef<Trader, Integer> FED_MONKEYS_EVENTS = new TopicDef<>(
        "trader-fed-monkeys",
        new JsonSerde.TraderSerde(),
        new JsonSerde.IntegerSerde()
    );

    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * In this exercise, we will get a table of {@link TraderState}s.
     *
     * <p>This table associates traders to their latest known state (which
     * includes the number of monkeys the given trader has already fed).</p>
     *
     * <p>We will then take the stream of events, counting how many monkeys
     * a player has already fed. We will join this stream with the table of states,
     * and apply the "fed monkey" update to the player's state.
     * </p>
     *
     * <p><b>Note:</b> what is done in this example could more easily achieved
     * with a simple reduce. What is asked from you does not represent well
     * the use case for joins, look at this example as a simple way to familiarize
     * yourself with the API.</p>
     */
    @Override
    protected void buildStreamTopology(StreamsBuilder builder) {

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:
        // create a KTable from the STATES topic
        // create a KStream from the FED_MONKEYS_EVENTS topic
        // left join the stream with the table
        // write the new states back to the STATES topic

        // <<< Your job ends here.

    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setUp() throws Exception {
        createTopics(STATES, FED_MONKEYS_EVENTS);
        buildTopologyAndLaunchKafka(FED_MONKEYS_EVENTS);
        Thread.sleep(5000); // Leave time to embedded kafka to load correctly
    }

    @Test
    public void test() throws Exception {
        AtomicBoolean stop = new AtomicBoolean();

        Trader trader1 = new Trader(Team.ALOUATE, "trader1");
        Trader trader2 = new Trader(Team.BONOBO, "trader2");

        Map<String, Integer> fedMonkeys = new HashMap<>();

        consume(STATES, (k,v) -> {
            LOG.info("New state for {}: {}",
                keyToString(STATES, k),
                valueToString(STATES, v));
            fedMonkeys.put(k.getName(), v.getFedMonkeys());
        }, stop);

        sendKeyValues(Lists.newArrayList(
            Tuple.of(STATES, trader1, TraderState.init()),
            Tuple.of(FED_MONKEYS_EVENTS, trader1, 1),
            Tuple.of(STATES, trader2, TraderState.init()),
            Tuple.of(FED_MONKEYS_EVENTS, trader1, 2),
            Tuple.of(FED_MONKEYS_EVENTS, trader2, 1),
            Tuple.of(FED_MONKEYS_EVENTS, trader1, 4),
            Tuple.of(FED_MONKEYS_EVENTS, trader2, 2),
            Tuple.of(FED_MONKEYS_EVENTS, trader2, 4)
        ), 250);

        assertThat(fedMonkeys.get("trader1")).isEqualTo(7);
        assertThat(fedMonkeys.get("trader2")).isEqualTo(7);
    }
}
