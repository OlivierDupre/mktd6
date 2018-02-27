package io.monkeypatch.mktd6.model.trader.ops;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MarketOrder extends TraderOp {

    private final MarketOrderType type;
    private final int shares;

    public MarketOrder(DateTime time, String txnId, MarketOrderType type, int shares) {
        super(time, txnId);
        if (shares < 1) {
            throw new IllegalArgumentException("Shares must be > 0, but was " + shares);
        }
        this.type = type;
        this.shares = shares;
    }

    public static MarketOrder make(String txnId, MarketOrderType type, int shares) {
        return new MarketOrder(DateTime.now(DateTimeZone.UTC), txnId, type, shares);
    }

    public MarketOrderType getType() {
        return type;
    }

    public int getShares() {
        return shares;
    }

}
