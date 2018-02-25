package io.monkeypatch.mktd6.server.priceinfo;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;

public class SharePriceHypeInfluenceTransformer implements ValueTransformer<Double, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceHypeInfluenceTransformer.class);

    private static final String BURSTS = "BURSTS";

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
        stateStore.putIfAbsent(BURSTS, 0d);

        double bursts = stateStore.get(BURSTS);
        double diff = value - bursts;

        // The more hype, the more risk of the burst of a hype bubble...
        if (random.nextDouble() < diff * 0.01) {
            diff = diff / 2;
            bursts = bursts + diff;
            stateStore.put(BURSTS, bursts);
            LOG.info("BubbleBurst!!!: -{}", diff);
        }

        LOG.info(String.format("Influence: %.5f - %.5f = %.5f", value, bursts, diff));
        return diff;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Double punctuate(long timestamp) {
        return null;  //no-op null values not forwarded.
    }

    @Override
    public void close() {
        //no-op
    }
}
