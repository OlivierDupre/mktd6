package io.monkeypatch.mktd6.model.market.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.TraderState;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TxnResult {

    private final String txnId;
    private final String type;
    private final TraderState state;
    private final TxnResultType status;

    @JsonCreator
    public TxnResult(
            @JsonProperty("txnId") String txnId,
            @JsonProperty("type") String type,
            @JsonProperty("state") TraderState state,
            @JsonProperty("status") TxnResultType status
    ) {
        this.txnId = txnId;
        this.type = type;
        this.state = state;
        this.status = status;
    }

    public String getTxnId() {
        return txnId;
    }

    public String getType() {
        return type;
    }

    public TraderState getState() {
        return state;
    }

    public TxnResultType getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnResult txnResult = (TxnResult) o;
        return new EqualsBuilder()
                .append(txnId, txnResult.txnId)
                .append(type, txnResult.type)
                .append(state, txnResult.state)
                .append(status, txnResult.status)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(txnId)
                .append(type)
                .append(state)
                .append(status)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "TxnResult{" +
                "txnId='" + txnId + '\'' +
                ", type='" + type + '\'' +
                ", status=" + status +
                ", state=" + state +
                '}';
    }
}
