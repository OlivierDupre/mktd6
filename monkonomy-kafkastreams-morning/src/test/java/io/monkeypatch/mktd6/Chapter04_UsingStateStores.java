package io.monkeypatch.mktd6;


import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.topic.TopicDef;
import io.monkeypatch.mktd6.utils.EmbeddedClusterBoilerplate;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Until now, we have not used any external state in our exercises.
 *
 * <p>The exercise this time will be to compute a moving average on
 * share prices, using a state store.</p>
 *
 * <h2>State in Kafka Streams</h2>
 *
 * <p>{@link StateStore} and {@link Transformer} are the main interfaces
 * to know about when dealing with state.</p>
 *
 * <p>{@link StateStore} is where you'll keep track of data. Think of it
 * as a key/value store in which you can put any types of keys/values, and
 * which will be shared between your Kafka brokers for each partition.
 * It can be hosted in memory, or persisting on disk.
 * </p>
 *
 * <p>{@link Transformer} (and {@link ValueTransformer} which does not
 * transform keys, only values) is a kind of {@link KStream#map(KeyValueMapper)}
 * which allows to use one or several stores during the transformation.
 * </p>
 *
 */
public class Chapter04_UsingStateStores extends EmbeddedClusterBoilerplate {

    //==========================================================================
    //==== ASSETS

    private static final Logger LOG = LoggerFactory.getLogger(Chapter04_UsingStateStores.class);

    public static final TopicDef<String, SharePriceInfo> PRICES = TopicDef.SHARE_PRICE;

    public static final TopicDef<String, Double> PRICE_EMA = new TopicDef<>(
        "price-ema",
        new JsonSerde.StringSerde(),
        new JsonSerde.DoubleSerde()
    );

    //==========================================================================
    //==== YOUR MISSION, SHOULD YOU DECIDE TO ACCEPT IT

    /**
     * We want to compute an
     * <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">
     * exponential moving average</a> of prices.
     *
     * <p>We receive price info from the {@link #PRICES} topic, and use the
     * given {@link EmaTransformer} class to compute the EMA.</p>
     *
     * <p>In order to do that, we need to create a store, which is done in
     * several steps.</p>
     *
     * <ul>
     *     <li>First, create a {@link StoreSupplier} using the {@link Stores}
     *         helper methods.</li>
     *     <li>Then, create a {@link StoreBuilder} using the {@link Stores}
     *         helper methods.</li>
     *     <li>Finally, add the store builder to the StreamsBuilder.</li>
     * </ul>
     *
     * <p>StateStores are identified by a name (String), use the constant
     * {@link #EMA_STORE_NAME} for the store name.</p>
     *
     * <p>Once you have registered the store, the rest is pretty much
     * straightforward. You need to have a look at the
     * {@link KStream#transformValues(ValueTransformerSupplier, String...)}.
     * </p>
     */
    @Override
    protected void buildStreamTopology(StreamsBuilder builder) {

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:
        // create a StoreSupplier using Stores
        // create a StoreBuilder using Stores
        // add the StoreBuilder to the StreamsBuilder parameter
        //
        // create a stream from the PRICES topic
        // compute the EMA using transformValues and the given EmaTransformer class
        // send the EMA values to the PRICE_EMA topic

        // <<< Your job ends here.

        // If you feel you spend too much time on this and need a hand, here is my solution.
        // If you use it, just don't copy paste it!
        // https://gist.github.com/glmxndr/737fee390101ec15f33a0af26a8c6508

    }

    //==========================================================================
    //==== GIVEN METHODS

    private static final double EMA_ALPHA = 0.1d;
    private static final String EMA_STORE_NAME = "ema-store";
    private static final String EMA_STORE_KEY = "EMA";

    static class EmaTransformer implements ValueTransformer<SharePriceInfo, Double> {

        private KeyValueStore<String, Double> stateStore;

        public EmaTransformer() {}

        public void init(ProcessorContext context) {
            stateStore = (KeyValueStore) context.getStateStore(EMA_STORE_NAME);
        }

        public Double transform(SharePriceInfo value) {
            return Optional.ofNullable(stateStore.get(EMA_STORE_KEY))
                .map(prevEma -> {
                    double currentEma = value.getCoins() * EMA_ALPHA
                            + prevEma * (1 - EMA_ALPHA);
                    stateStore.put("EMA", currentEma);
                    return currentEma;
                })
                .orElseGet(() -> {
                    double price = value.getCoins();
                    stateStore.put("EMA", price);
                    return price;
                });
        }

        public Double punctuate(long timestamp) { return null; }
        public void close() {}
    }


    //==========================================================================
    //==== TEST LOGIC


    @Before
    public void setUp() throws Exception {
        createTopics(PRICES, PRICE_EMA);
        buildTopologyAndLaunchKafka(PRICES);
    }

    @Test
    public void test() throws Exception {
        sendValues(PRICES, Lists.newArrayList(
            priceInfo(1),
            priceInfo(2),
            priceInfo(3),
            priceInfo(4),
            priceInfo(3),
            priceInfo(2),
            priceInfo(1)
        ));

        List<Double> emas = recordsConsumedOnTopic(PRICE_EMA, 7)
            .stream()
            .map(kv -> kv.value)
            .collect(Collectors.toList());
        LOG.info("Result: {}", emas);

        assertThat(emas).containsExactlyInAnyOrder(
            1.0,
            1.1,
            1.29,
            1.561,
            1.7049,
            1.73441,
            1.6609690000000001
        );
    }

    private SharePriceInfo priceInfo(double value) {
        return SharePriceInfo.make(value, 1d);
    }
}
