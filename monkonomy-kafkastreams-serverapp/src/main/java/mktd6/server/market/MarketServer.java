package mktd6.server.market;

import mktd6.kstreams.KafkaStreamsBoilerplate;
import mktd6.kstreams.TopologySupplier;
import mktd6.model.market.ops.TxnResultType;
import mktd6.model.trader.Trader;
import mktd6.server.model.ServerStores;
import mktd6.server.model.TraderStateUpdater;
import mktd6.server.model.TxnEvent;
import mktd6.server.model.ServerTopics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import static mktd6.server.model.ServerStores.STATE_STORE;
import static mktd6.topic.TopicDef.*;

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
        final String priceStoreName = ServerStores.PRICE_VALUE_STORE.getStoreName();

        KStream<Trader, TraderStateUpdater> orderStateUpdaters = builder
            .stream(MARKET_ORDERS.getTopicName(), helper.consumed(MARKET_ORDERS))
            //.mapValues(mo -> TraderStateUpdater.from(mo, 1d));
            .transformValues(() -> new MarketOrderToStateUpdaterTransformer(priceStoreName), priceStoreName);

        KStream<Trader, TraderStateUpdater> feedMonkeysStateUpdaters = builder
            .stream(FEED_MONKEYS.getTopicName(), helper.consumed(FEED_MONKEYS))
            .mapValues(fm -> TraderStateUpdater.from(fm));

        KStream<Trader, TraderStateUpdater> investmentsStateUpdaters = builder
            .stream(INVESTMENT_ORDERS.getTopicName(), helper.consumed(INVESTMENT_ORDERS))
            .mapValues(investment -> TraderStateUpdater.from(investment));

        KStream<Trader, TraderStateUpdater> updates = orderStateUpdaters
            .merge(feedMonkeysStateUpdaters)
            .merge(investmentsStateUpdaters)
            .through(ServerTopics.TRADER_UPDATES.getTopicName(), helper.produced(ServerTopics.TRADER_UPDATES));
        // Updates are published to their own topic. This is useful because
        // return on investment will also be written to this topic.

        KStream<Trader, TxnEvent> txnEvents = updates.transform(
            TraderUpdaterToStateTransformer::new,
            STATE_STORE.getStoreName());

        // Send
        txnEvents
            .filter((k,v) -> isAcceptedInvestment(v))
            .transformValues(
                TxnEventTransformer::new,
                ServerStores.TXN_INVESTMENT_STORE.getStoreName())
            .to(ServerTopics.INVESTMENT_TXN_EVENTS.getTopicName(), helper.produced(ServerTopics.INVESTMENT_TXN_EVENTS));

        txnEvents
            .mapValues(TxnEvent::getTxnResult)
            .to(TXN_RESULTS.getTopicName(), helper.produced(TXN_RESULTS));

        txnEvents
            .filter((trader, event) -> event.getTxnResult().getStatus() == TxnResultType.ACCEPTED)
            .mapValues(event -> event.getTxnResult().getState())
            .to(ServerTopics.TRADER_STATES.getTopicName(), helper.produced(ServerTopics.TRADER_STATES));

        return builder;
    }

    private boolean isAcceptedInvestment(TxnEvent v) {
        return v.getTxnResult().getStatus() == TxnResultType.ACCEPTED
        && v.getTxnResult().getType().equals(TraderStateUpdater.Type.INVEST.name());
    }

}
