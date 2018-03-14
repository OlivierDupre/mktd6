package io.monkeypatch.mktd6.model.trader;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.market.ops.TxnResultType;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.text.DecimalFormat;

public class TraderState {

    private final DateTime time;

    private final double coins;
    private final int shares;
    private final int bailouts;
    private final int fedMonkeys;
    private final int inFlightInvestments;

    @JsonCreator
    public TraderState(
            @JsonProperty("time") DateTime time,
            @JsonProperty("coins") double coins,
            @JsonProperty("shares") int shares,
            @JsonProperty("bailouts") int bailouts,
            @JsonProperty("fedMonkeys") int fedMonkeys,
            @JsonProperty("inFlightInvestments") int inFlightInvestments) {
        this.time = time;
        this.coins = coins;
        this.shares = shares;
        this.bailouts = bailouts;
        this.fedMonkeys = fedMonkeys;
        this.inFlightInvestments = inFlightInvestments;
    }

    public TraderState(double coins, int shares, int bailouts, int fedMonkeys, int inFlightInvestments) {
        this.time = now();
        this.coins = coins;
        this.shares = shares;
        this.bailouts = bailouts;
        this.fedMonkeys = fedMonkeys;
        this.inFlightInvestments = inFlightInvestments;
    }

    public DateTime getTime() {
        return time;
    }

    public double getCoins() {
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

    public int getInFlightInvestments() {
        return inFlightInvestments;
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
            0,
            0);
    }

    private static DateTime now() {
        return DateTime.now(DateTimeZone.UTC);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraderState that = (TraderState) o;
        return new EqualsBuilder()
                .append(coins, that.coins)
                .append(shares, that.shares)
                .append(bailouts, that.bailouts)
                .append(fedMonkeys, that.fedMonkeys)
                .append(inFlightInvestments, that.inFlightInvestments)
                .append(time, that.time)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(time)
                .append(coins)
                .append(shares)
                .append(bailouts)
                .append(fedMonkeys)
                .append(inFlightInvestments)
                .toHashCode();
    }

    @Override
    public String toString() {
        DecimalFormat num = new DecimalFormat("#.###");
        return "TraderState{" +
                "time=" + time +
                ", coins=" + num.format(coins) +
                ", shares=" + shares +
                ", bailouts=" + bailouts +
                ", fedMonkeys=" + fedMonkeys +
                ", inFlightInvestments=" + inFlightInvestments +
                '}';
    }
}

