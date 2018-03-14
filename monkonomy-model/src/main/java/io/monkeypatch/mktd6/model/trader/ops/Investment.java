package io.monkeypatch.mktd6.model.trader.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Investment extends TraderOp {

    private final double invested;

    @JsonCreator
    public Investment(@JsonProperty("time") DateTime time,
                      @JsonProperty("txnId") String txnId,
                      @JsonProperty("invested") double invested) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Investment that = (Investment) o;
        return new EqualsBuilder()
                .append(invested, that.invested)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(invested)
                .toHashCode();
    }
}
