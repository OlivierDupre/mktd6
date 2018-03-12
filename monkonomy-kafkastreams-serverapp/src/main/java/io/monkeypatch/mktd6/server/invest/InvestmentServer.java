package io.monkeypatch.mktd6.server.invest;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.server.model.ServerTopics;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import io.monkeypatch.mktd6.server.model.TxnEvent;
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

        executor.schedule(() -> {
            TraderStateUpdater updater = new TraderStateUpdater(
                    result.getTxnId()  + "-return",
                    TraderStateUpdater.Type.RETURN,
                    2 * event.getInvestedCoins(),
                    0,
                    false,
                    0
            );
            producer.send(new ProducerRecord<Trader, TraderStateUpdater>(
                ServerTopics.TRADER_UPDATES.getTopicName(),
                record.key(),
                updater));
        }, (long)record.value().getTotalInvestments(), TimeUnit.MILLISECONDS);
    }
}
