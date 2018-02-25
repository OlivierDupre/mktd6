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
        double previousValue = Optional.ofNullable(stateStore.get(EMA)).orElse(value);
        double movingAverage = previousValue * (1d - factor) + value * factor;
        stateStore.put(EMA, movingAverage);
        double forecastMult = movingAverage / value;
        SharePriceInfo result = SharePriceInfo.make(value, forecastMult);
        LOG.info("PriceInfo: {}", value);
        return result;
    }

    @Override
    @SuppressWarnings("deprecation")
    public SharePriceInfo punctuate(long timestamp) {
        return null;  //no-op null values not forwarded.
    }

    @Override
    public void close() {
        //no-op
    }
}
