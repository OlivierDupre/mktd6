package mktd6.model.trader.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.trader.Trader;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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

    public static FeedMonkeys bootstrap(Trader trader) {
        return new FeedMonkeys(
            DateTime.now(DateTimeZone.UTC),
            "bootstrap_" + trader.getName(),
            1);
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
