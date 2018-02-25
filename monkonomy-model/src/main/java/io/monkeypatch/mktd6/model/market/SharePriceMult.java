package io.monkeypatch.mktd6.model.market;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.serde.BaseJsonSerde;
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

}
