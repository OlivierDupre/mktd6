package io.monkeypatch.mktd6.server.priceinfo;

import io.monkeypatch.mktd6.server.model.StateConstants;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;

public class SharePriceHypeInfluenceTransformer implements ValueTransformer<Double, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceHypeInfluenceTransformer.class);

    private KeyValueStore<String, Double> stateStore;
    private final String storeName;
    private ProcessorContext context;
    private final Random random = new Random();

    public SharePriceHypeInfluenceTransformer(String storeName) {
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
    public Double transform(Double value) {
        stateStore.putIfAbsent(StateConstants.BURSTS_KEY, 0d);

        double bursts = stateStore.get(StateConstants.BURSTS_KEY);
        double diff = value - bursts;

        // The more hype, the more risk of the burst of a hype bubble...
        if (random.nextDouble() < diff * 0.01) {
            diff = diff / 2;
            bursts = bursts + diff;
            stateStore.put(StateConstants.BURSTS_KEY, bursts);
            LOG.info("BubbleBurst!!!: -{}", diff);
        }

        stateStore.put(StateConstants.PRICE_HYPE_COMPONENT_KEY, diff);
        LOG.info(String.format("Influence: %.5f - %.5f = %.5f", value, bursts, diff));
        return diff;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Double punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
