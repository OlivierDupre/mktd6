package io.monkeypatch.mktd6.model.market;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * The monkonomy lives outside of the traders, and the share price
 * evolves. This multiplicator is a measure of how it has evolved
 * in the previous timeframe.
 */
public class SharePriceMult {

    private final DateTime time;
    private final double mult;

    @JsonCreator
    public SharePriceMult(@JsonProperty("time") DateTime time,
                          @JsonProperty("mult") double mult) {
        this.time = time;
        this.mult = mult;
    }

    public static SharePriceMult make(double mult) {
        return new SharePriceMult(DateTime.now(DateTimeZone.UTC), mult);
    }

    public DateTime getTime() {
        return time;
    }

    public double getMult() {
        return mult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SharePriceMult that = (SharePriceMult) o;
        return new EqualsBuilder()
                .append(mult, that.mult)
                .append(time, that.time)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(time)
                .append(mult)
                .toHashCode();
    }
}
