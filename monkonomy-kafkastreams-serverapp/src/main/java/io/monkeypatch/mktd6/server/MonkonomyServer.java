package io.monkeypatch.mktd6.server;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.server.market.MarketServer;
import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.priceinfo.SharePriceMultMeter;
import io.monkeypatch.mktd6.server.priceinfo.SharePriceServer;
import io.monkeypatch.mktd6.server.trader.SimpleTrader;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MonkonomyServer {

    private static final Logger LOG = LoggerFactory.getLogger(MonkonomyServer.class);
    public static final String ONE_KEY = "GNOU";

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(32);

    private final KafkaStreamsBoilerplate boilerplate;
    private final GibberServer gibberServer;
    private final SharePriceMultMeter sharePriceMultMeter;

    public MonkonomyServer(KafkaStreamsBoilerplate boilerplate) {
        this.boilerplate = boilerplate;
        this.gibberServer = new GibberServer(boilerplate);
        this.sharePriceMultMeter = new SharePriceMultMeter(boilerplate);
    }

    public void run() {
        executor.execute(() ->
            gibberServer.run(Arrays.asList("banana")));

        executor.execute(sharePriceMultMeter::run);

        KafkaStreams kafkaStreams = new KafkaStreams(
            buildTopology(),
            boilerplate.streamsConfig(false));

        executor.scheduleAtFixedRate(
            () -> LOG.info(kafkaStreams.toString()),
            10,
            10,
            TimeUnit.SECONDS);

        executor.execute(kafkaStreams::start);
    }

    private Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        return getTopologyBuilders()
            .reduce(
                builder,
                (b, supplier) -> {
                    LOG.info("TopologyBuilder: {}", supplier.getClass().getName());
                    return supplier.apply(boilerplate, b);
                },
                (l, r) -> l)
            .build();
    }

    private Stream<TopologySupplier> getTopologyBuilders() {
        return Stream.of(
            (helper, builder) -> ServerStores.PRICE_VALUE_STORE.addTo(builder),
            (helper, builder) -> ServerStores.TXN_INVESTMENT_STORE.addTo(builder),
            new SharePriceServer(),
            new MarketServer(),
            new SimpleTrader("st0")
        );
    }

    public static void main(String[] args) {
        KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(
                "172.16.238.3:9092",
                "monkonomy-server");

        new MonkonomyServer(helper).run();
    }

}
