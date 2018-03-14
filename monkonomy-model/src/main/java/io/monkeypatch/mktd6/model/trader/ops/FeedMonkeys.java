package io.monkeypatch.mktd6.model.trader.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;

public class FeedMonkeys extends TraderOp {

    private final int monkeys;

    @JsonCreator
    public FeedMonkeys(@JsonProperty("time") DateTime time,
                       @JsonProperty("txnId") String txnId,
                       @JsonProperty("monkeys") int monkeys) {
        super(time, txnId);
        if (monkeys < 1) {
            throw new IllegalArgumentException("Monkey no happy.");
        }
        this.monkeys = monkeys;
    }

    public int getMonkeys() {
        return monkeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FeedMonkeys that = (FeedMonkeys) o;
        return new EqualsBuilder()
                .append(monkeys, that.monkeys)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(monkeys)
                .toHashCode();
    }
}
