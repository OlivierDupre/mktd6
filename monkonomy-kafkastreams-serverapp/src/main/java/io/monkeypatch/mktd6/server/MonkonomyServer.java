package io.monkeypatch.mktd6.server;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.server.model.StateStores;
import io.monkeypatch.mktd6.server.priceinfo.SharePriceMultMeter;
import io.monkeypatch.mktd6.server.priceinfo.SharePriceServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Executor;
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
                (b, supplier) -> supplier.apply(boilerplate, b),
                (l, r) -> l)
            .build();
    }

    private Stream<TopologySupplier> getTopologyBuilders() {
        return Stream.of(
            (helper, builder) -> StateStores.PRICE_VALUE_STORE.addTo(builder),
            new SharePriceServer()
        );
    }

    public static void main(String[] args) {
        KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(
                "172.16.238.3:9092",
                "monkonomy-server");
        new MonkonomyServer(helper).run();
    }

}
