package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.market.ops.TxnResultType;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import io.monkeypatch.mktd6.server.model.TxnEvent;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class TxnEventProcessor implements Processor<Trader, TxnEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TxnEventProcessor.class);


    public static final String TOTAL_INVESTMENTS = "TOTAL_INVESTMENTS";
    private ProcessorContext context;
    private KeyValueStore<String, Double> stateStore;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore) this.context.getStateStore(ServerStores.TXN_INVESTMENT_STORE.getStoreName());
    }

    @Override
    public void process(Trader key, TxnEvent value) {
        TxnResult txnResult = value.getTxnResult();
        if (txnResult.getStatus() != TxnResultType.ACCEPTED) {
            return;
        }

        double newInvestment = value.getUpdater().getCoinsDiff();
        double prevInvestments = Optional.ofNullable(stateStore.get(TOTAL_INVESTMENTS)).orElse(0d);
        double total = newInvestment + prevInvestments;
        stateStore.put(TOTAL_INVESTMENTS, total);

        long waitFor = (long)new LogNormalDistribution(total * 1000, total * 100).sample();

        AtomicReference<Cancellable> cancel = new AtomicReference<>();
        cancel.set(this.context.schedule(waitFor, PunctuationType.WALL_CLOCK_TIME, l -> {
            TraderStateUpdater updater = new TraderStateUpdater(
                txnResult.getTxnId()  + "-return",
                TraderStateUpdater.Type.RETURN,
                2 * value.getUpdater().getCoinsDiff(),
                0,
                false,
                0
            );
            LOG.info("WRITE TO TXN_RESULTS TOPIC: {}", updater);
            cancel.get().cancel();
        }));
    }

    @Override
    public void punctuate(long timestamp) {}

    @Override
    public void close() {}
}
