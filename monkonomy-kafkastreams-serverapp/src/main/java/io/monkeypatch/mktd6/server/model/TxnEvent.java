package io.monkeypatch.mktd6.server.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.trader.TraderState;

public class TxnEvent {

    private final TxnResult txnResult;
    private final TraderStateUpdater updater;

    @JsonCreator
    public TxnEvent(
            @JsonProperty("txnResult") TxnResult txnResult,
            @JsonProperty("updater") TraderStateUpdater updater) {
        this.txnResult= txnResult;
        this.updater = updater;
    }

    public TxnResult getTxnResult() {
        return txnResult;
    }

    public TraderStateUpdater getUpdater() {
        return updater;
    }
}
