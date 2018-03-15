package mktd6.trader;

import mktd6.kstreams.KafkaStreamsBoilerplate;
import mktd6.kstreams.TopologySupplier;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.ops.TxnResultType;
import mktd6.model.trader.Trader;
import mktd6.model.trader.ops.Investment;
import mktd6.model.trader.ops.MarketOrder;
import mktd6.model.trader.ops.MarketOrderType;
import mktd6.serde.JsonSerde;
import mktd6.trader.helper.TraderStores;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static mktd6.topic.TopicDef.*;

@SuppressWarnings("unchecked")
public class TraderTopology implements TopologySupplier {

    private static final Logger LOG = LoggerFactory.getLogger(TraderTopology.class);

    private final String playerName;
    public final Trader trader;

    private final AtomicInteger txn = new AtomicInteger();

    public TraderTopology(Trader trader) {
        this.playerName = trader.getName();
        this.trader = trader;
    }

    private String txnId() {
        return playerName + "_" + Integer.toString(Math.abs(UUID.randomUUID().hashCode()), 36);
    }

    @Override
    public StreamsBuilder apply(KafkaStreamsBoilerplate helper,
                                StreamsBuilder builder) {

        // Look at the price forecast and follow the advice:
        // - if the forecast is > 1, meaning the price should increase,
        //   then BUY 1 share,
        // - if the forecast is < 1, meaning the price should decrease,
        //   then SELL 1 share.
        // Don't even care about looking at our assets, let the
        // market accept/reject the transaction.
        KStream<String, SharePriceInfo> sharePrices = builder
                .stream(SHARE_PRICE.getTopicName(), helper.consumed(SHARE_PRICE));
        sharePrices
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
            .to(MARKET_ORDERS.getTopicName(), helper.produced(MARKET_ORDERS));

        // Get the table containing the latest share price,
        // re-keyed with my trader instance (to allow joins)
        KTable<Trader, SharePriceInfo> myPrices = sharePrices
            .selectKey((k,v) -> trader)
            .groupByKey(Serialized.with(
                new JsonSerde.TraderSerde(),
                new JsonSerde.SharePriceInfoSerde()))
            .reduce((a, b) -> b);



        // Invest all your money whenever you have some.
        builder
            .stream(TXN_RESULTS.getTopicName(), helper.consumed(TXN_RESULTS))
            .filter((k,v) -> trader.equals(k))
            .peek((k,v) -> LOG.info("Transaction result: {}", v.toString()))
            // Ignore rejected transactions
            .filter((k,v) -> v.getStatus() == TxnResultType.ACCEPTED)
            .mapValues(v -> v.getState().getCoins())
            // Keep the price for 1 share
            .join(
                myPrices, // We join with the price tables
                (coins, priceInfo) -> coins - priceInfo.getCoins(), // We compute how much we can invest while keeping the price of 1 share
                Joined.with(new JsonSerde.TraderSerde(), Serdes.Double(), new JsonSerde.SharePriceInfoSerde())
            )
            .filter((k, v) -> v > 0)
            // Throttle to not get more than 1 investment by second
            .transform(() -> new TraderInvestmentTransformer(trader), TraderStores.TRADER_INVESTMENT_STORE.getStoreName())
            .peek((k,v) -> LOG.info("Investing {}!!!", v))
            // Create the investment
            .mapValues(v -> Investment.make(txnId(), v))
            .to(INVESTMENT_ORDERS.getTopicName(), helper.produced(INVESTMENT_ORDERS));

        // Never feed monkeys... (bad!)

        return builder;
    }
}
