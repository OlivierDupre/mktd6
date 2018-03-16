package mktd6.server.priceinfo;

import mktd6.server.model.BurstStep;
import mktd6.server.model.ServerStoreConstants;
import mktd6.server.model.ServerStores;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

public class SharePriceHypeInfluenceTransformer implements ValueTransformer<Double, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceHypeInfluenceTransformer.class);
    public static final String BURST_STEP_KEY = "BURST_STEP";

    private KeyValueStore<String, Double> priceStore;
    private final Random random = new Random();
    private KeyValueStore<String, BurstStep> burstStore;
    private ProcessorContext context;


    public SharePriceHypeInfluenceTransformer() {}

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        priceStore = (KeyValueStore) context.getStateStore(ServerStores.PRICE_VALUE_STORE.getStoreName());
        burstStore = (KeyValueStore) context.getStateStore(ServerStores.BURST_STEP_STORE.getStoreName());
    }

    @Override
    public Double transform(Double value) {
        priceStore.putIfAbsent(ServerStoreConstants.BURSTS_KEY, 0d);

        double bursts = priceStore.get(ServerStoreConstants.BURSTS_KEY);
        double diff = value - bursts;

        BurstStep step = burstStore.get(BURST_STEP_KEY);

        if (step == null && random.nextDouble() < diff * 0.01) {
            // The more hype, the more risk of the burst of a hype bubble...
            burstStore.put(BURST_STEP_KEY, BurstStep.STEP1);

            AtomicReference<Cancellable> cancel = new AtomicReference<>();
            cancel.set(context.schedule(
                5000,
                PunctuationType.WALL_CLOCK_TIME,
                t -> bubbleBurstStep(value, cancel)));
        }

        priceStore.put(ServerStoreConstants.PRICE_HYPE_COMPONENT_KEY, diff);
        LOG.info(String.format("Influence: %.5f - %.5f = %.5f", value, bursts, diff));
        return diff;
    }

    private void bubbleBurstStep(Double value, AtomicReference<Cancellable> cancel) {
        BurstStep currentStep = burstStore.get(BURST_STEP_KEY);
        if (currentStep == null) {
            cancel.get().cancel();
            return;
        }
        double bursts = priceStore.get(ServerStoreConstants.BURSTS_KEY);
        double diff = value - bursts;
        double burstDiff = diff * currentStep.getMult();
        double bursted = diff - burstDiff;
        LOG.info("Bubble burst!!! {} - x{} (bursted: {})", currentStep.name(), currentStep.getMult(), bursted);
        priceStore.put(ServerStoreConstants.BURSTS_KEY, bursts + bursted);
        burstStore.put(BURST_STEP_KEY, currentStep.getNext().orElse(null));
    }

    @Override
    @SuppressWarnings("deprecation")
    public Double punctuate(long timestamp) { return null; }

    @Override
    public void close() {}
}
