package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.market.ops.TxnResultType;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.TraderState;
import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import io.monkeypatch.mktd6.server.model.TxnEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

public class TraderUpdaterToStateTransformer implements Transformer<Trader, TraderStateUpdater, KeyValue<Trader, TxnEvent>> {

    private KeyValueStore<Trader, TraderState> stateStore;

    public TraderUpdaterToStateTransformer() {}

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        stateStore = (KeyValueStore)
            context.getStateStore(ServerStores.STATE_STORE.getStoreName());
    }

    @Override
    public KeyValue<Trader, TxnEvent> transform(Trader trader, TraderStateUpdater upd) {
        TraderState state = Optional.ofNullable(stateStore.get(trader)).orElse(TraderState.init());
        TxnResult result = upd.update(state);
        double investedCoins =
                result.getStatus() == TxnResultType.ACCEPTED &&
                upd.getType() == TraderStateUpdater.Type.INVEST
            ? upd.getCoinsDiff()
            : 0;
        TxnEvent event = new TxnEvent(result, investedCoins);
        stateStore.put(trader, result.getState());
        return KeyValue.pair(trader, event);
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValue<Trader, TxnEvent> punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}