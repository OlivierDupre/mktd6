package io.monkeypatch.mktd6.server.priceinfo;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import io.monkeypatch.mktd6.model.market.SharePriceMult;
import io.monkeypatch.mktd6.serde.JsonSerde;
import io.monkeypatch.mktd6.server.model.ShareHypePiece;
import io.monkeypatch.mktd6.server.model.Topics;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.monkeypatch.mktd6.server.priceinfo.StateConstants.PRICE_STATE_STORE;

/**
 * This class generates the data in the share price topic.
 *
 * Share prices contain the actual share price and a simple
 * forecast.
 *
 * In order to do this, this class has to:
 * - use share price multiplicators in the dedicated topic
 * - use gibber topic to manipulate the price (according to
 *   the players' gibbs/tweets)
 * - compute the probability of a krach based on the gibber
 *   messages, and potentially apply it (nullifying half of the
 *   positive gibbs)
 * - compute the SMA on 10 minutes, and generate the mult
 *   for the forecast.
 */
public class SharePriceServer implements TopologySupplier {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceServer.class);

    @Override
    public StreamsBuilder apply(
        KafkaStreamsBoilerplate helper,
        StreamsBuilder builder
    ) {
        TopicDef<String, Gibb> gibbs = TopicDef.GIBBS;
        TopicDef<String, SharePriceMult> priceMults = TopicDef.SHARE_PRICE_OUTSIDE_EVOLUTION_METER;
        TopicDef<String, ShareHypePiece> shareHypeTopic = Topics.SHARE_HYPE;

        addPriceState(builder);

        // Fetch the stream of (random) multipliers
        KStream<String, Double> sharePriceBase = builder
            .stream(priceMults.getTopicName(), helper.consumed(priceMults))
            .mapValues(SharePriceMult::getMult)
            .groupByKey(Serialized.with(Serdes.String(), Serdes.Double()))
            .aggregate(
                () -> 1d,
                (k, mult, acc) -> acc * mult,
                Materialized.with(Serdes.String(), Serdes.Double()))
            .toStream()
        ;

        // Compute the hype
        KStream<String, ShareHypePiece> hypePieces = builder
            .stream(gibbs.getTopicName(), helper.consumed(gibbs))
            .filter((k, v) -> selectGibb(v))
            .flatMapValues(ShareHypePiece::hypePieces)
            .peek((k,v) -> LOG.info("HypePiece: {}", v.getWord()))
        ;
        hypePieces
            .to(shareHypeTopic.getTopicName(), helper.produced(shareHypeTopic));

        // Compute the total hype influence
        KTable<String, Double> hypePriceInfluence = hypePieces
            .mapValues(hypePiece -> hypePiece.getInfluence() * 0.01d)
            .groupByKey(Serialized.with(Serdes.String(), Serdes.Double()))
            .aggregate(
                () -> 0d,
                (k, points, acc) -> acc + points,
                Materialized.with(Serdes.String(), Serdes.Double()));

        // Compute the hype taking bubble bursts into account, using state
        hypePriceInfluence
            .toStream()
            .transformValues(
                () -> new SharePriceHypeInfluenceTransformer(PRICE_STATE_STORE),
                PRICE_STATE_STORE)
            .peek((k,v) -> LOG.info("HypePriceAdder: {}", v))
        ;

        // Compute the prices and output them to the dedicated topic
        KStream<String, SharePriceInfo> sharePrices = sharePriceBase
            .transformValues(
                () -> new SharePriceBandTransformer(PRICE_STATE_STORE, 0.1d),
                PRICE_STATE_STORE)
        ;
        sharePrices
            .to(TopicDef.SHARE_PRICE.getTopicName(), helper.produced(TopicDef.SHARE_PRICE));

        return builder;
    }

    private void addPriceState(StreamsBuilder builder) {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(PRICE_STATE_STORE);
        StoreBuilder<KeyValueStore<String, Double>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Double());
        builder.addStateStore(storeBuilder);
    }

    private boolean selectGibb(Gibb v) {
        return v.getText().contains("banana");
    }

}
