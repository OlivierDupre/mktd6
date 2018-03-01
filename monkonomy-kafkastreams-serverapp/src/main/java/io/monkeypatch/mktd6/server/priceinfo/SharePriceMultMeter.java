package io.monkeypatch.mktd6.server.priceinfo;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.model.market.SharePriceMult;
import io.monkeypatch.mktd6.server.MonkonomyServer;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class simulates econometric measures of how the outside economy
 * evolves and its influence on the share price.
 *
 * It writes share price multiplicators every second to the
 * share price evolution topic.
 */
public class SharePriceMultMeter {

    /**
     * Slightly biased increasing log-normal, very close to 1 though.
     * The share prices will increase over long periods.
     */
    private final LogNormalDistribution logNorm = new LogNormalDistribution(0.0001, 0.01);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private final KafkaStreamsBoilerplate boilerplate;

    public SharePriceMultMeter(KafkaStreamsBoilerplate boilerplate) {
        this.boilerplate = boilerplate;
    }

    public SharePriceMult getMult() {
        return SharePriceMult.make(logNorm.sample());
    }

    public void run() {
        TopicDef<String, SharePriceMult> topic = TopicDef.SHARE_PRICE_OUTSIDE_EVOLUTION_METER;
        String topicName = topic.getTopicName();
        Properties producerConfig = boilerplate.producerConfig(topic, false);
        KafkaProducer<String, SharePriceMult> producer = new KafkaProducer<>(producerConfig);

        executor.scheduleAtFixedRate(
            () -> producer.send(new ProducerRecord<>(topicName, MonkonomyServer.ONE_KEY, getMult())),
            0,
            1,
            TimeUnit.SECONDS
        );
    }

}
