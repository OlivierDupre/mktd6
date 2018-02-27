package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.ops.FeedMonkeys;
import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.server.model.StateStores;
import io.monkeypatch.mktd6.server.model.Topics;
import io.monkeypatch.mktd6.server.model.TraderStateUpdater;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * The market server does several things:
 * - receive market orders, investments and monkey feeding from the players,
 *   from 3 separate topics,
 * - merges them into a single topic of state update requests,
 * - maintains state for each player and applies or rejects the
 *   state updates according to their internal balance,
 * - automatically assigns bailouts to players who need it,
 * - writes the updated states to a topic.
 */
public class MarketServer  implements TopologySupplier {


    @Override
    public StreamsBuilder apply(KafkaStreamsBoilerplate helper, StreamsBuilder builder) {
        String priceStoreName = StateStores.PRICE_VALUE_STORE.getStoreName();
        TopicDef<Trader, MarketOrder> marketOrders = TopicDef.MARKET_ORDERS;
        TopicDef<Trader, FeedMonkeys> feedMonkeys = TopicDef.FEED_MONKEYS;
        TopicDef<Trader, Investment> investments = TopicDef.INVESTMENT_ORDERS;
        TopicDef<Trader, TraderStateUpdater> traderUpdates = Topics.TRADER_UPDATES;

        KStream<Trader, TraderStateUpdater> orderStateUpdaters = builder
                .stream(marketOrders.getTopicName(), helper.consumed(marketOrders))
                .transformValues(() -> new MarketOrderToStateUpdaterTransformer(priceStoreName), priceStoreName);

        KStream<Trader, TraderStateUpdater> feedMonkeysStateUpdaters = builder
                .stream(feedMonkeys.getTopicName(), helper.consumed(feedMonkeys))
                .mapValues(fm -> TraderStateUpdater.from(fm));

        KStream<Trader, TraderStateUpdater> investmentsStateUpdaters = builder
                .stream(investments.getTopicName(), helper.consumed(investments))
                .mapValues(investment -> TraderStateUpdater.from(investment));

        KStream<Trader, TraderStateUpdater> updates =
            orderStateUpdaters
                .merge(feedMonkeysStateUpdaters)
                .merge(investmentsStateUpdaters);

        updates.to(traderUpdates.getTopicName(), helper.produced(traderUpdates));
        
        return builder;
    }

}
