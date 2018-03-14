package io.monkeypatch.mktd6.model.trader.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MarketOrder extends TraderOp {

    private final MarketOrderType type;
    private final int shares;

    @JsonCreator
    public MarketOrder(@JsonProperty("time") DateTime time,
                       @JsonProperty("txnId") String txnId,
                       @JsonProperty("type") MarketOrderType type,
                       @JsonProperty("shares") int shares) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MarketOrder that = (MarketOrder) o;
        return new EqualsBuilder()
                .append(shares, that.shares)
                .append(type, that.type)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(type)
                .append(shares)
                .toHashCode();
    }
}
