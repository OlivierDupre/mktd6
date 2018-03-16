package mktd6.trader;

import mktd6.model.market.SharePriceInfo;
import mktd6.model.trader.Trader;
import mktd6.model.trader.ops.FeedMonkeys;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This transformer only acts as a single record emitter, and then does nothing.
 * It allows to bootstrap the trader by making a first transaction, allowing the
 * server to send a TxnResult for this trader. This gives the trader his/her
 * current state, and he/she can go on deciding which operations to do
 * after that.
 */
public class TraderBootstrapTransformer implements Transformer<String, SharePriceInfo, KeyValue<Trader, FeedMonkeys>> {

    private static final Logger LOG = LoggerFactory.getLogger(TraderBootstrapTransformer.class);

    private final AtomicBoolean hasFired = new AtomicBoolean();
    private final Trader trader;

    public TraderBootstrapTransformer(Trader trader) {
        this.trader = trader;
    }

    @Override
    public void init(ProcessorContext context) {
        AtomicReference<Cancellable> cancelRef = new AtomicReference<>();
        cancelRef.set(context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, t -> {
            if (!hasFired.get()) {
                LOG.info("Bootstrap transformer firing in {}", TraderBootstrapTransformer.this);
                context.forward(trader, FeedMonkeys.bootstrap(trader));
                hasFired.set(true);
            }
           cancelRef.get().cancel();
        }));
    }

    @Override
    public KeyValue<Trader, FeedMonkeys> transform(String key, SharePriceInfo value) {
        // This transformer ignores all the input values,
        // it only send a single record from the init scheduled punctuator.
        return null;
    }

    @Override
    public KeyValue<Trader, FeedMonkeys> punctuate(long timestamp) {
        return null;
    }

    @Override
    public void close() {

    }
}
