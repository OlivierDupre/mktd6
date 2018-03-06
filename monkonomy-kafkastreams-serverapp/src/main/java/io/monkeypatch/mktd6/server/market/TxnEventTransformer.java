package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.model.ServerTopics;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import io.monkeypatch.mktd6.server.model.TxnEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;
import java.util.Properties;

public class TxnEventTransformer implements ValueTransformer<TxnEvent, TxnEvent> {

    public static final String TOTAL_INVESTMENTS = "TOTAL_INVESTMENTS";
    private ProcessorContext context;
    private KeyValueStore<String, Double> stateStore;

    private final KafkaStreamsBoilerplate helper;
    private final Producer<Trader, TraderStateUpdater> updateProducer;

    public TxnEventTransformer(KafkaStreamsBoilerplate helper) {
        this.helper = helper;
        Properties properties = helper.producerConfig(ServerTopics.TRADER_UPDATES, false);
        this.updateProducer =  new KafkaProducer<>(properties);
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore) this.context.getStateStore(ServerStores.TXN_INVESTMENT_STORE.getStoreName());
    }

    @Override
    public TxnEvent transform(TxnEvent value) {
        double newInvestment = value.getInvestedCoins();
        double prevInvestments = Optional.ofNullable(stateStore.get(TOTAL_INVESTMENTS)).orElse(0d);
        double total = newInvestment + prevInvestments;
        stateStore.put(TOTAL_INVESTMENTS, total);
        return new TxnEvent(value.getTxnResult(), value.getInvestedCoins(), total);
    }

    @Override
    public TxnEvent punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
