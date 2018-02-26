package io.monkeypatch.mktd6.model.trader.ops;

import org.joda.time.DateTime;

public abstract class TraderOp {

    private final DateTime time;
    private final String txnId;

    public TraderOp(DateTime time, String txnId) {
        this.time = time;
        this.txnId = txnId;
    }

    public DateTime getTime() {
        return time;
    }

    public String getTxnId() {
        return txnId;
    }

}
