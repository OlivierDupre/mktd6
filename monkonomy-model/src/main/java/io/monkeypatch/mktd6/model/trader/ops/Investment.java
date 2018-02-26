package io.monkeypatch.mktd6.model.trader.ops;

import org.joda.time.DateTime;

public class Investment extends TraderOp {

    private final float invested;

    public Investment(DateTime time, String txnId, float invested) {
        super(time, txnId);
        if (invested <= 0) {
            throw new IllegalArgumentException("Invested coins must be > 0, but was: " + invested);
        }
        this.invested = invested;
    }

    public float getInvested() {
        return invested;
    }
}
