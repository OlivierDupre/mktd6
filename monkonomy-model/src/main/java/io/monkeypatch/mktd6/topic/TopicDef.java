package io.monkeypatch.mktd6.topic;

import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.ops.FeedMonkeys;
import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.serde.BaseJsonSerde;
import io.monkeypatch.mktd6.serde.JsonSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TopicDef<K, V> {
    
    // Traders write to:

    public static final TopicDef<Trader, MarketOrder> MARKET_ORDERS = new TopicDef<>(
            "market-orders",
            new JsonSerde.TraderSerde(),
            new JsonSerde.MarketOrderSerde());

    public static final TopicDef<Trader, Investment> INVESTMENT_ORDERS = new TopicDef<>(
            "investment-orders",
            new JsonSerde.TraderSerde(),
            new JsonSerde.InvestmentSerde());

    public static final TopicDef<Trader, FeedMonkeys> FEED_MONKEYS = new TopicDef<>(
            "feed-monkeys",
            new JsonSerde.TraderSerde(),
            new JsonSerde.FeedMonkeysSerde());

    // Traders read from:

    public static final TopicDef<Trader, TxnResult> TXN_RESULTS = new TopicDef<>(
            "txn-results",
            new JsonSerde.TraderSerde(),
            new JsonSerde.TxnResultSerde());

    public static final TopicDef<Void, SharePriceInfo> SHARE_PRICE = new TopicDef<>(
            "share-price",
            new JsonSerde.VoidSerde(),
            new JsonSerde.SharePriceInfoSerde());



    private final String topicName;
    private final BaseJsonSerde<K> keySerde;
    private final BaseJsonSerde<V> valueSerde;

    public TopicDef(String topicName, BaseJsonSerde<K> keySerde, BaseJsonSerde<V> valueSerde) {
        this.topicName = topicName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public String getTopicName() {
        return topicName;
    }

    public BaseJsonSerde<K> getKeySerde() { return keySerde; }
    public BaseJsonSerde<V> getValueSerde() { return valueSerde; }

    public Class<K> getKeyType() { return keySerde.getType(); }
    public Class<V> getValueType() { return valueSerde.getType(); }

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
