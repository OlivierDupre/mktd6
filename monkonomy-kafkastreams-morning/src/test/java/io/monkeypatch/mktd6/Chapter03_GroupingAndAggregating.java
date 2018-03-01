package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Chapter03_GroupingAndAggregating extends EmbeddedClusterBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter03_GroupingAndAggregating.class);

    public static final TopicDef<String, Investment> INVESTMENTS = new TopicDef<>(
        "investments",
        new JsonSerde.StringSerde(),
        new JsonSerde.InvestmentSerde()
    );

    @Override
    protected void buildStreamTopology(StreamsBuilder builder) {
        builder
            .stream(INVESTMENTS.getTopicName(), helper.consumed(INVESTMENTS))
            .groupByKey()
            .windowedBy(TimeWindows.of(100))
            .reduce(this::combineInvestments)
            .toStream((wk,investment) -> wk.key())
            .peek((k,v) -> LOG.info("#### {} -> {}", k, v.getInvested()));

    }

    private Investment combineInvestments(Investment i1, Investment i2) {
        return Investment.make(
            i1.getTxnId(),
            i1.getInvested() + i2.getInvested());
    }


    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setUp() throws Exception {
        createTopics(INVESTMENTS);
        buildTopologyAndLaunchKafka(INVESTMENTS);
    }

    @Test
    public void testUpOrDown() throws Exception {

        CountDownLatch latch = new CountDownLatch(100);

        ScheduledExecutorService exec = Executors.newScheduledThreadPool(4);
        Runnable sendData = new Runnable() {
            public void run () {
                try {
                    sendKeyValues(INVESTMENTS, Arrays.asList(
                        KeyValue.pair("player1", Investment.make("txn01", 1)),
                        KeyValue.pair("player1", Investment.make("txn02", 2)),
                        KeyValue.pair("player2", Investment.make("txn03", 3)),
                        KeyValue.pair("player2", Investment.make("txn04", 4)),
                        KeyValue.pair("player1", Investment.make("txn05", 5)),
                        KeyValue.pair("player1", Investment.make("txn06", 6))
                    ));
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                finally {
                    latch.countDown();
                    if (latch.getCount() > 0) {
                        exec.schedule(this, 150, TimeUnit.MILLISECONDS);
                    }
                }
            }
        };
        exec.execute(sendData);

        latch.await();
    }



}
