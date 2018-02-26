package io.monkeypatch.mktd6.server.priceinfo;

import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public class SharePriceBandTransformer implements ValueTransformer<Double, SharePriceInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceBandTransformer.class);

    private static final String EMA = "EMA";

    private KeyValueStore<String, Double> stateStore;
    private final String storeName;
    private ProcessorContext context;
    private final double factor;

    public SharePriceBandTransformer(String storeName, double factor) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        if (factor <= 0d || factor >= 1d) {
            throw new IllegalArgumentException("factor should be strictly between 0 and 1");
        }
        this.storeName = storeName;
        this.factor = factor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    public SharePriceInfo transform(Double value) {
        double hypeComponent = Optional.ofNullable(stateStore.get(StateKeys.PRICE_HYPE_COMPONENT)).orElse(0d);
        double newValue = value + hypeComponent;

        double previousValue = Optional.ofNullable(stateStore.get(EMA)).orElse(newValue);
        double movingAverage = previousValue * (1d - factor) + newValue * factor;
        stateStore.put(EMA, movingAverage);
        double forecastMult = movingAverage / newValue;
        SharePriceInfo result = SharePriceInfo.make(newValue, forecastMult);
        LOG.info("PriceInfo: {}", newValue);
        return result;
    }

    @Override
    @SuppressWarnings("deprecation")
    public SharePriceInfo punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
