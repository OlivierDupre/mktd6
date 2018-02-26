package io.monkeypatch.mktd6.model.trader.ops;

import org.joda.time.DateTime;

public class FeedMonkeys extends TraderOp {

    private final int monkeys;

    public FeedMonkeys(DateTime time, String txnId, int monkeys) {
        super(time, txnId);
        if (monkeys < 1) {
            throw new IllegalArgumentException("Monkey no happy.");
        }
        this.monkeys = monkeys;
    }

    public int getMonkeys() {
        return monkeys;
    }

}
