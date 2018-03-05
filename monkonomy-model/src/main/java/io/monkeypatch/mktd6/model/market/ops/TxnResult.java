package io.monkeypatch.mktd6.model.market.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.TraderState;

public class TxnResult {

    private final String txnId;
    private final TraderState state;
    private final TxnResultType status;

    @JsonCreator
    public TxnResult(
        @JsonProperty("txnId") String txnId,
        @JsonProperty("state") TraderState state,
        @JsonProperty("status") TxnResultType status
    ) {
        this.txnId = txnId;
        this.state = state;
        this.status = status;
    }

    public String getTxnId() {
        return txnId;
    }

    public TraderState getState() {
        return state;
    }

    public TxnResultType getStatus() {
        return status;
    }
}
