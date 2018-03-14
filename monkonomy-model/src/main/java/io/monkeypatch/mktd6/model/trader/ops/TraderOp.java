package io.monkeypatch.mktd6.model.trader.ops;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;

public abstract class TraderOp {

    private final DateTime time;
    private final String txnId;

    public TraderOp(DateTime time, String txnId) {
        this.time = time;
        this.txnId = txnId;
    }

    public DateTime getTime() {
        return time;
    }

    public String getTxnId() {
        return txnId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraderOp traderOp = (TraderOp) o;
        return new EqualsBuilder()
                .append(time, traderOp.time)
                .append(txnId, traderOp.txnId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(time)
                .append(txnId)
                .toHashCode();
    }
}
