package io.monkeypatch.mktd6.server.priceinfo;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import io.monkeypatch.mktd6.model.market.SharePriceMult;
import io.monkeypatch.mktd6.server.model.ServerStores;
import io.monkeypatch.mktd6.server.model.ServerTopics;
import io.monkeypatch.mktd6.server.model.ShareHypePiece;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        TopicDef<String, ShareHypePiece> shareHypeTopic = ServerTopics.SHARE_HYPE;
        String priceStoreName = ServerStores.PRICE_VALUE_STORE.getStoreName();
        String burstStoreName = ServerStores.BURST_STEP_STORE.getStoreName();

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
            //.peek((k,v) -> LOG.info("sharePriceBase = {}/{}", k, v))
        ;

        // Compute the hype
        KStream<String, ShareHypePiece> hypePieces = builder
            .stream(gibbs.getTopicName(), helper.consumed(gibbs))
            .filter((k, v) -> selectGibb(v))
            .flatMapValues(ShareHypePiece::hypePieces)
            //.peek((k,v) -> LOG.info("HypePiece: {}", v.getWord()))
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
                SharePriceHypeInfluenceTransformer::new,
                priceStoreName, burstStoreName)
            //.peek((k,v) -> LOG.info("HypePriceAdder: {}", v))
        ;

        // Compute the prices and output them to the dedicated topic
        KStream<String, SharePriceInfo> sharePrices = sharePriceBase
            .transformValues(
                () -> new SharePriceBandTransformer(priceStoreName, 0.1d),
                priceStoreName)
        ;
        sharePrices
            .to(TopicDef.SHARE_PRICE.getTopicName(), helper.produced(TopicDef.SHARE_PRICE));

        return builder;
    }

    private boolean selectGibb(Gibb v) {
        return v.getText().contains("banana");
    }

}
