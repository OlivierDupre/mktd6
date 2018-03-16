package mktd6.topic;

import mktd6.model.gibber.Gibb;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.SharePriceMult;
import mktd6.model.market.ops.TxnResult;
import mktd6.model.trader.Trader;
import mktd6.model.trader.ops.FeedMonkeys;
import mktd6.model.trader.ops.Investment;
import mktd6.model.trader.ops.MarketOrder;
import mktd6.serde.JsonSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TopicDef<K, V> {
    
    // Traders write to:

    public static final TopicDef<Trader, MarketOrder> MARKET_ORDERS = new TopicDef<>(
            "market-orders",
            new JsonSerde.TraderSerde(),
            new JsonSerde.MarketOrderSerde(), 8);

    public static final TopicDef<Trader, Investment> INVESTMENT_ORDERS = new TopicDef<>(
            "investment-orders",
            new JsonSerde.TraderSerde(),
            new JsonSerde.InvestmentSerde(), 8);

    public static final TopicDef<Trader, FeedMonkeys> FEED_MONKEYS = new TopicDef<>(
            "feed-monkeys",
            new JsonSerde.TraderSerde(),
            new JsonSerde.FeedMonkeysSerde(), 8);

    // Traders read from:

    public static final TopicDef<Trader, TxnResult> TXN_RESULTS = new TopicDef<>(
            "txn-results",
            new JsonSerde.TraderSerde(),
            new JsonSerde.TxnResultSerde(), 8);

    public static final TopicDef<String, SharePriceMult> SHARE_PRICE_OUTSIDE_EVOLUTION_METER = new TopicDef<>(
            "share-price-outside-evolution-meter",
            new JsonSerde.StringSerde(),
            new JsonSerde.SharePriceMultSerde(), 1);

    public static final TopicDef<String, SharePriceInfo> SHARE_PRICE = new TopicDef<>(
            "share-price",
            new JsonSerde.StringSerde(),
            new JsonSerde.SharePriceInfoSerde(), 1);

    public static final TopicDef<String, Gibb> GIBBS = new TopicDef<>(
            "gibber-gibbs",
            new JsonSerde.StringSerde(),
            new JsonSerde.GibbSerde(),
            1);

    private final String topicName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final int partitions;

    public TopicDef(String topicName, Serde<K> keySerde, Serde<V> valueSerde, int partitions) {
        this.topicName = topicName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.partitions = partitions;
    }

    public TopicDef(String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        this(topicName, keySerde, valueSerde, 1);
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitions() {
        return partitions;
    }

    public Serde<K> getKeySerde() { return keySerde; }
    public Serde<V> getValueSerde() { return valueSerde; }

    public Class<? extends Serializer<K>> getKeySerializerClass() {
        return (Class<? extends Serializer<K>>)((Serializer<K>)getKeySerde()).getClass();
    }
    public Class<? extends Serializer<V>> getValueSerializerClass() {
        return (Class<? extends Serializer<V>>)((Serializer<K>)getValueSerde()).getClass();
    }

    public Class<? extends Deserializer<K>> getKeyDeserializerClass() {
        return (Class<? extends Deserializer<K>>)((Deserializer<K>)getKeySerde()).getClass();
    }
    public Class<? extends Deserializer<V>> getValueDeserializerClass() {
        return (Class<? extends Deserializer<V>>)((Deserializer<K>)getValueSerde()).getClass();
    }

    public Class<? extends Serde<K>> getKeySerdeClass() {
        return (Class<? extends Serde<K>>)((Serde<K>)getKeySerde()).getClass();
    }
    public Class<? extends Serde<V>> getValueSerdeClass() {
        return (Class<? extends Serde<V>>)((Serde<K>)getValueSerde()).getClass();
    }

}
