package mktd6.server;

import mktd6.kstreams.KafkaStreamsBoilerplate;
import mktd6.kstreams.TopologySupplier;
import mktd6.server.gibber.GibberServer;
import mktd6.server.invest.InvestmentServer;
import mktd6.server.market.MarketServer;
import mktd6.server.model.ServerStores;
import mktd6.server.model.ServerTopics;
import mktd6.server.priceinfo.SharePriceMultMeter;
import mktd6.server.priceinfo.SharePriceServer;
import mktd6.topic.TopicDef;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static mktd6.kstreams.LaunchHelper.allTopicsExist;
import static mktd6.kstreams.LaunchHelper.getLocalIp;

public class MonkonomyServer implements Runnable {

    public static final String KAFKA_HOST = getLocalIp();
    public static final String ZK_PORT = "2181";
    public static final String KAFKA_PORT = "9092";

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

        //displayTopology(kafkaStreams);

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
            (h, builder) -> ServerStores.PRICE_VALUE_STORE.addTo(builder),
            (h, builder) -> ServerStores.TXN_INVESTMENT_STORE.addTo(builder),
            (h, builder) -> ServerStores.STATE_STORE.addTo(builder),
            (h, builder) -> ServerStores.TRADER_INVESTMENT_STORE.addTo(builder),
            (h, builder) -> ServerStores.BURST_STEP_STORE.addTo(builder),
            new SharePriceServer(),
            new MarketServer()
        );
    }

    public static void main(String[] args) throws Exception {
        String zkHostPort = KAFKA_HOST + ":" + ZK_PORT;
        ZkClient zk = new ZkClient(zkHostPort);
        ZkUtils zkUtils = new ZkUtils(zk, new ZkConnection(zkHostPort), false);

        String bootstrapServer = KAFKA_HOST + ":" + KAFKA_PORT;

        LOG.info("Connecting to kafka bootstrapServer: {}", bootstrapServer);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        AdminClient admin = AdminClient.create(config);

        Map<String, String> configs = new HashMap<>();
        int partitions = 3;
        short replication = 1;

        List<TopicDef<?,?>> topicDefs = Lists.newArrayList(
            TopicDef.SHARE_PRICE,
            TopicDef.GIBBS,
            TopicDef.INVESTMENT_ORDERS,
            TopicDef.MARKET_ORDERS,
            TopicDef.TXN_RESULTS,
            TopicDef.FEED_MONKEYS,
            TopicDef.SHARE_PRICE_OUTSIDE_EVOLUTION_METER,
            ServerTopics.TRADER_STATES,
            ServerTopics.TRADER_UPDATES,
            ServerTopics.SHARE_HYPE,
            ServerTopics.INVESTMENT_TXN_EVENTS
        );

        admin.createTopics(
            topicDefs.stream()
                .map(td -> new NewTopic(td.getTopicName(), partitions, replication).configs(configs))
                .collect(Collectors.toSet()));

        KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(
                bootstrapServer,
                "monkonomy-server");

        while(!allTopicsExist(zkUtils, topicDefs)) {
            LOG.info("Waiting for topics to be created...");
            Thread.sleep(1000);
        }

        new MonkonomyServer(helper).run();
    }

    private void displayTopology(KafkaStreams kafkaStreams) {
        // Show the topology every 10 seconds
        executor.scheduleAtFixedRate(
                () -> LOG.info(kafkaStreams.toString()),
                10,
                10,
                TimeUnit.SECONDS);
    }


}
