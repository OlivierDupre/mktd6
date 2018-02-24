package io.monkeypatch.mktd6.model.market;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class SharePriceInfo {

    @JsonProperty
    private final DateTime time;
    @JsonProperty
    private final float coins;
    @JsonProperty
    private final SharePriceSimpleForecast forecast;

    @JsonCreator
    public SharePriceInfo(@JsonProperty("time") DateTime time,
                          @JsonProperty("coins") float coins,
                          @JsonProperty("forecast") SharePriceSimpleForecast forecast) {
        this.time = time;
        this.coins = coins;
        this.forecast = forecast;
    }

    public static SharePriceInfo make(float coins, float mult) {
        return new SharePriceInfo(DateTime.now(DateTimeZone.UTC), coins, new SharePriceSimpleForecast(mult));
    }

    public float getCoins() {
        return coins;
    }

    public DateTime getTime() {
        return time;
    }

    public SharePriceSimpleForecast getForecast() {
        return forecast;
    }
}