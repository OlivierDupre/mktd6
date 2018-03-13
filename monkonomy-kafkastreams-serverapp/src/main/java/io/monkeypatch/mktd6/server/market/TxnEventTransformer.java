package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.model.TxnEvent;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

public class TxnEventTransformer implements ValueTransformer<TxnEvent, TxnEvent> {

    public static final String TOTAL_INVESTMENTS = "TOTAL_INVESTMENTS";
    private KeyValueStore<String, Double> stateStore;


    public TxnEventTransformer() {}

    @Override
    public void init(ProcessorContext context) {
        this.stateStore = (KeyValueStore) context.getStateStore(ServerStores.TXN_INVESTMENT_STORE.getStoreName());
    }

    @Override
    public TxnEvent transform(TxnEvent value) {
        double newInvestment = value.getInvestedCoins();
        double prevInvestments = Optional.ofNullable(stateStore.get(TOTAL_INVESTMENTS)).orElse(0d);
        double total = Math.abs(newInvestment) + prevInvestments;
        stateStore.put(TOTAL_INVESTMENTS, total);
        return new TxnEvent(value.getTxnResult(), value.getInvestedCoins(), total);
    }

    @Override
    public TxnEvent punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
