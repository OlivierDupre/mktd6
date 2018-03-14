package io.monkeypatch.mktd6.server.invest;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.server.model.ServerTopics;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import io.monkeypatch.mktd6.server.model.TxnEvent;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InvestmentServer implements Runnable {

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

    private final KafkaStreamsBoilerplate helper;

    public InvestmentServer(KafkaStreamsBoilerplate helper) {
        this.helper = helper;
    }

    @Override
    public void run() {
        Properties producerProps = helper.producerConfig(ServerTopics.TRADER_UPDATES, false);
        Properties consumerProps = helper.consumerConfig(ServerTopics.INVESTMENT_TXN_EVENTS);
        try (
            KafkaConsumer<Trader, TxnEvent> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<Trader, TraderStateUpdater> producer = new KafkaProducer<>(producerProps)
        ) {
            consumer.subscribe(Arrays.asList(ServerTopics.INVESTMENT_TXN_EVENTS.getTopicName()));
            while (true) {
                ConsumerRecords<Trader, TxnEvent> records = consumer.poll(500);
                records.forEach(record -> process(record, producer));
            }
        }
    }

    private void process(ConsumerRecord<Trader, TxnEvent> record, KafkaProducer<Trader, TraderStateUpdater> producer) {
        TxnEvent event = record.value();
        TxnResult result = event.getTxnResult();

        double totalInvestments = record.value().getTotalInvestments();
        long timeToWait = (long) totalInvestments;
        double logNormalBiasReturn = Math.exp(-1 - (totalInvestments / 1000d));
        LogNormalDistribution logNormal = new LogNormalDistribution(0.035 + logNormalBiasReturn, 0.01);
        double investmentReturn = logNormal.sample();

        executor.schedule(() -> {
            TraderStateUpdater updater = new TraderStateUpdater(
                result.getTxnId(),
                TraderStateUpdater.Type.RETURN,
                investmentReturn * event.getInvestedCoins(),
                0,
                false,
                0,
                -1
            );
            producer.send(new ProducerRecord<>(
                ServerTopics.TRADER_UPDATES.getTopicName(),
                record.key(),
                updater));
        }, timeToWait, TimeUnit.MILLISECONDS);
    }
}
