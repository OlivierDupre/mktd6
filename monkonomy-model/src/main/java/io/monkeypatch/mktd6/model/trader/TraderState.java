package io.monkeypatch.mktd6.model.trader;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.monkeypatch.mktd6.model.market.ops.TxnResultType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class TraderState {

    private final DateTime time;

    private final float coins;
    private final int shares;
    private final int bailouts;
    private final int fedMonkeys;

    @JsonCreator
    public TraderState(float coins, int shares, int bailouts, int fedMonkeys) {
        this.time = now();
        this.coins = coins;
        this.shares = shares;
        this.bailouts = bailouts;
        this.fedMonkeys = fedMonkeys;
    }

    public DateTime getTime() {
        return time;
    }

    public float getCoins() {
        return coins;
    }

    public int getShares() {
        return shares;
    }

    public int getBailouts() {
        return bailouts;
    }

    public int getFedMonkeys() {
        return fedMonkeys;
    }

    public TxnResultType validate() {
        return
            (coins < 0) ? TxnResultType.INSUFFICIENT_COINS :
            (shares < 0) ? TxnResultType.INSUFFICIENT_SHARES :
            TxnResultType.ACCEPTED;
    }

    public static TraderState init() {
        return new TraderState(
            10,
            5,
            0,
            0
        );
    }

    private static DateTime now() {
        return DateTime.now(DateTimeZone.UTC);
    }

}

