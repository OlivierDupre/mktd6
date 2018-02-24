package io.monkeypatch.mktd6.model.trader.ops;

import io.monkeypatch.mktd6.model.trader.Trader;
import org.joda.time.DateTime;

public class FeedMonkeys extends TraderOp {

    private final int monkeys;

    public FeedMonkeys(DateTime time, String txnId, Trader trader, int monkeys) {
        super(time, txnId, trader);
        if (monkeys < 1) {
            throw new IllegalArgumentException("Monkey no happy.");
        }
        this.monkeys = monkeys;
    }

    public int getMonkeys() {
        return monkeys;
    }

}
