package io.monkeypatch.mktd6.model.market;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class SharePriceInfo {

    @JsonProperty
    private final DateTime time;
    @JsonProperty
    private final double coins;
    @JsonProperty
    private final SharePriceSimpleForecast forecast;

    @JsonCreator
    public SharePriceInfo(
            @JsonProperty("time") DateTime time,
            @JsonProperty("coins") double coins,
            @JsonProperty("forecast") SharePriceSimpleForecast forecast
    ) {
        this.time = time;
        this.coins = coins;
        this.forecast = forecast;
    }

    public static SharePriceInfo make(double coins, double mult) {
        return new SharePriceInfo(
            DateTime.now(DateTimeZone.UTC),
            coins,
            new SharePriceSimpleForecast(mult)
        );
    }

    public double getCoins() {
        return coins;
    }

    public DateTime getTime() {
        return time;
    }

    public SharePriceSimpleForecast getForecast() {
        return forecast;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SharePriceInfo that = (SharePriceInfo) o;
        return new EqualsBuilder()
                .append(coins, that.coins)
                .append(time, that.time)
                .append(forecast, that.forecast)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(time)
                .append(coins)
                .append(forecast)
                .toHashCode();
    }
}
