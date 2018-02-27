package io.monkeypatch.mktd6.model.trader.ops;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Investment extends TraderOp {

    private final double invested;

    public Investment(DateTime time, String txnId, double invested) {
        super(time, txnId);
        if (invested <= 0) {
            throw new IllegalArgumentException("Invested coins must be > 0, but was: " + invested);
        }
        this.invested = invested;
    }

    public static Investment make(String txnId, double invested) {
        return new Investment(DateTime.now(DateTimeZone.UTC), txnId, invested);
    }

    public double getInvested() {
        return invested;
    }
}
