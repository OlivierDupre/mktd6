package io.monkeypatch.mktd6.model.trader.ops;

import io.monkeypatch.mktd6.model.trader.Trader;
import org.joda.time.DateTime;

public abstract class TraderOp {

    private final DateTime time;
    private final String txnId;
    private final Trader trader;

    public TraderOp(DateTime time, String txnId, Trader trader) {
        this.time = time;
        this.txnId = txnId;
        this.trader = trader;
    }

    public DateTime getTime() {
        return time;
    }

    public String getTxnId() {
        return txnId;
    }

    public Trader getTrader() {
        return trader;
    }
}
