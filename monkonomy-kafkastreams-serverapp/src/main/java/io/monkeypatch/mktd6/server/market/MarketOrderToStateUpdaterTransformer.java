package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;
import java.util.Optional;

import static io.monkeypatch.mktd6.server.model.ServerStoreConstants.CURRENT_SHARE_PRICE_KEY;

public class MarketOrderToStateUpdaterTransformer implements ValueTransformer<MarketOrder, TraderStateUpdater> {

    private KeyValueStore<String, Double> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public MarketOrderToStateUpdaterTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    public TraderStateUpdater transform(MarketOrder value) {
        double currentPrice = Optional.ofNullable(stateStore.get(CURRENT_SHARE_PRICE_KEY)).orElse(1d);
        return TraderStateUpdater.from(value, currentPrice);
    }

    @Override
    @SuppressWarnings("deprecation")
    public TraderStateUpdater punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
