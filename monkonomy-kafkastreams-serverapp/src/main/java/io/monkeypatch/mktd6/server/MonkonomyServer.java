package io.monkeypatch.mktd6.server;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.server.invest.InvestmentServer;
import io.monkeypatch.mktd6.server.market.MarketServer;
import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.model.ServerTopics;
import io.monkeypatch.mktd6.server.priceinfo.SharePriceMultMeter;
import io.monkeypatch.mktd6.server.priceinfo.SharePriceServer;
import io.monkeypatch.mktd6.server.trader.SimpleTrader;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MonkonomyServer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MonkonomyServer.class);
    public static final String ONE_KEY = "GNOU";

    private final ScheduledExecutorService executor =
        Executors.newScheduledThreadPool(32);

    private final KafkaStreamsBoilerplate boilerplate;
    private final GibberServer gibberServer;
    private final SharePriceMultMeter sharePriceMultMeter;
    private final InvestmentServer investmentServer;

    public MonkonomyServer(KafkaStreamsBoilerplate boilerplate) {
        this.boilerplate = boilerplate;
        this.gibberServer = new GibberServer(boilerplate);
        this.sharePriceMultMeter = new SharePriceMultMeter(boilerplate);
        this.investmentServer = new InvestmentServer(boilerplate);
    }

    @Override
    public void run() {
        executor.execute(() -> gibberServer.run(Arrays.asList("banana")));
        executor.execute(investmentServer);
        executor.execute(sharePriceMultMeter);

        KafkaStreams kafkaStreams = new KafkaStreams(
            buildTopology(),
            boilerplate.streamsConfig(false));

        /*//
        executor.scheduleAtFixedRate(
            () -> LOG.info(kafkaStreams.toString()),
            10,
            10,
            TimeUnit.SECONDS);
        //*/

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
            (helper, builder) -> ServerStores.STATE_STORE.addTo(builder),
            (helper, builder) -> ServerStores.TRADER_INVESTMENT.addTo(builder),
            new SharePriceServer(),
            new MarketServer(),
            new SimpleTrader("st0")
        );
    }

    public static void main(String[] args) {
        String bootstrapServerConfig = "172.16.238.3:9092";

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);

        AdminClient admin = AdminClient.create(config);

        Map<String, String> configs = new HashMap<>();
        int partitions = 3;
        short replication = 1;

        admin.createTopics(Arrays.asList(
            new NewTopic(TopicDef.SHARE_PRICE.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(TopicDef.GIBBS.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(TopicDef.INVESTMENT_ORDERS.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(TopicDef.MARKET_ORDERS.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(TopicDef.TXN_RESULTS.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(TopicDef.FEED_MONKEYS.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(TopicDef.SHARE_PRICE_OUTSIDE_EVOLUTION_METER.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(ServerTopics.TRADER_UPDATES.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(ServerTopics.SHARE_HYPE.getTopicName(), partitions, replication).configs(configs),
            new NewTopic(ServerTopics.INVESTMENT_TXN_EVENTS.getTopicName(), partitions, replication).configs(configs)
        ));

        KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(
                bootstrapServerConfig,
                "monkonomy-server");

        new MonkonomyServer(helper).run();
    }

}
