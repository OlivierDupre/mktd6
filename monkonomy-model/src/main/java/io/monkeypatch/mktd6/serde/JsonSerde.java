package io.monkeypatch.mktd6.serde;

import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.model.market.SharePriceInfo;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.ops.FeedMonkeys;
import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;

public class JsonSerde {

    public static class StringSerde extends BaseJsonSerde<String> {
        public StringSerde() { super(String.class); }
    }
    public static class VoidSerde extends BaseJsonSerde<Void> {
        public VoidSerde() { super(Void.class); }
    }

    public static class TraderSerde extends BaseJsonSerde<Trader> {
        public TraderSerde() { super(Trader.class); }
    }
    public static class MarketOrderSerde extends BaseJsonSerde<MarketOrder> {
        public MarketOrderSerde() { super(MarketOrder.class); }
    }
    public static class InvestmentSerde extends BaseJsonSerde<Investment> {
        public InvestmentSerde() { super(Investment.class); }
    }
    public static class FeedMonkeysSerde extends BaseJsonSerde<FeedMonkeys> {
        public FeedMonkeysSerde() { super(FeedMonkeys.class); }
    }
    public static class TxnResultSerde extends BaseJsonSerde<TxnResult> {
        public TxnResultSerde() { super(TxnResult.class); }
    }
    public static class SharePriceInfoSerde extends BaseJsonSerde<SharePriceInfo> {
        public SharePriceInfoSerde() { super(SharePriceInfo.class); }
    }
    public static class GibbSerde extends BaseJsonSerde<Gibb> {
        public GibbSerde() { super(Gibb.class); }
    }
}
