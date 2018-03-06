package io.monkeypatch.mktd6.server.trader;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import io.monkeypatch.mktd6.model.Team;
import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrderType;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class SimpleTrader implements TopologySupplier {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTrader.class);

    private final String playerName;
    public final Trader trader;

    private final AtomicInteger txn = new AtomicInteger();

    public SimpleTrader(String playerName) {
        this.playerName = playerName;
        this.trader = new Trader(Team.ALOUATE, playerName);
    }

    private String txnId() {
        return playerName + "_" + txn.incrementAndGet();
    }

    @Override
    public StreamsBuilder apply(KafkaStreamsBoilerplate helper,
                                StreamsBuilder builder) {

        TopicDef<String, SharePriceInfo> sharePrice = TopicDef.SHARE_PRICE;
        TopicDef<Trader, MarketOrder> marketOrders = TopicDef.MARKET_ORDERS;
        TopicDef<Trader, TxnResult> txnResults = TopicDef.TXN_RESULTS;
        TopicDef<Trader, Investment> investmentOrders = TopicDef.INVESTMENT_ORDERS;

        // Look at the price forecast and follow the advice:
        // - if the forecast is > 1, meaning the price should increase,
        //   then BUY 1 share,
        // - if the forecast is < 1, meaning the price should decrease,
        //   then SELL 1 share.
        // Don't even care about looking at our assets, let the
        // market accept/reject the transaction.
        builder
            .stream(sharePrice.getTopicName(), helper.consumed(sharePrice))
            .map((k, info) -> {
                MarketOrderType type = info.getForecast().getMult() > 1
                    ? MarketOrderType.BUY
                    : MarketOrderType.SELL;
                LOG.info("Trader order: {}", type);
                return KeyValue.pair(
                        trader,
                        MarketOrder.make(txnId(), type, 1)
                );
            })
            .to(marketOrders.getTopicName(), helper.produced(marketOrders));

        // Invest all your money whenever you have some.
        builder
            .stream(txnResults.getTopicName(), helper.consumed(txnResults))
            .filter((k, v) -> k.getName().equals(playerName) && v.getState().getCoins() > 0.1)
            .mapValues(v -> Investment.make(txnId(), v.getState().getCoins()))
            .to(investmentOrders.getTopicName(), helper.produced(investmentOrders));

        // Never feed monkeys... (bad!)

        return builder;
    }
}
