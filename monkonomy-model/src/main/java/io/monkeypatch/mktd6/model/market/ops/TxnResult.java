package io.monkeypatch.mktd6.model.market.ops;

import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.TraderState;

public class TxnResult {

    private final Trader trader;
    private final String txnId;
    private final TraderState state;
    private final TxnResultType status;

    public TxnResult(
        Trader trader,
        String txnId,
        TraderState state,
        TxnResultType status
    ) {
        this.trader = trader;
        this.txnId = txnId;
        this.state = state;
        this.status = status;
    }

    public Trader getTrader() {
        return trader;
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
