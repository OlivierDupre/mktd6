package mktd6.server.priceinfo;

import mktd6.server.model.BurstStep;
import mktd6.server.model.ServerStoreConstants;
import mktd6.server.model.ServerStores;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SharePriceHypeInfluenceTransformer implements ValueTransformer<Double, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceHypeInfluenceTransformer.class);
    public static final String BURST_STEP_KEY = "BURST_STEP";

    private KeyValueStore<String, Double> priceStore;
    private final Random random = new Random();
    private KeyValueStore<String, BurstStep> burstStore;

    public SharePriceHypeInfluenceTransformer() {}

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        priceStore = (KeyValueStore) context.getStateStore(ServerStores.PRICE_VALUE_STORE.getStoreName());
        burstStore = (KeyValueStore) context.getStateStore(ServerStores.BURST_STEP_STORE.getStoreName());
    }

    @Override
    public Double transform(Double value) {
        priceStore.putIfAbsent(ServerStoreConstants.BURSTS_KEY, 0d);

        double bursts = priceStore.get(ServerStoreConstants.BURSTS_KEY);
        double diff = value - bursts;

        BurstStep step = burstStore.get("BURST_STEP");

        if (step != null) {
            double burstDiff = diff * step.getMult();
            double bursted = diff - burstDiff;
            bursts += bursted;
            diff = burstDiff;
            LOG.info("Bubble burst!!! {} - x{} (bursted: {})", step.name(), step.getMult(), bursted);
            priceStore.put(ServerStoreConstants.BURSTS_KEY, bursts);
            burstStore.put(BURST_STEP_KEY, step.getNext().orElse(null));
        }
        else if (random.nextDouble() < diff * 0.01) {
            // The more hype, the more risk of the burst of a hype bubble...
            burstStore.put(BURST_STEP_KEY, BurstStep.STEP1);
        }

        priceStore.put(ServerStoreConstants.PRICE_HYPE_COMPONENT_KEY, diff);
        LOG.info(String.format("Influence: %.5f - %.5f = %.5f", value, bursts, diff));
        return diff;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Double punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
