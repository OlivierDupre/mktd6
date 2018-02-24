package io.monkeypatch.mktd6.model.trader;

import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.market.ops.TxnResultType;
import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.model.trader.ops.FeedMonkeys;

public final class TraderStateUpdater {

    private final String txnId;
    private final Trader trader;

    private final float coinsDiff;
    private final int sharesDiff;
    private final boolean addBailout;
    private final int fedMonkeys;

    private TraderStateUpdater(String txnId, Trader trader, float coinsDiff, int sharesDiff, boolean addBailout, int fedMonkeys) {
        this.txnId = txnId;
        this.trader = trader;
        this.coinsDiff = coinsDiff;
        this.sharesDiff = sharesDiff;
        this.addBailout = addBailout;
        this.fedMonkeys = fedMonkeys;
    }

    public float getCoinsDiff() {
        return coinsDiff;
    }

    public int getSharesDiff() {
        return sharesDiff;
    }

    public boolean getAddBailout() {
        return addBailout;
    }

    public int getFedMonkeys() {
        return fedMonkeys;
    }

    public String getTxnId() {
        return txnId;
    }

    public Trader getTrader() {
        return trader;
    }

    public TxnResult update(TraderState state) {
        TraderState newState = new TraderState(
            state.getCoins() + getCoinsDiff(),
            state.getShares() + getSharesDiff(),
            state.getBailouts() + (getAddBailout() ? 1 : 0),
            state.getFedMonkeys() + getFedMonkeys()
        );
        TxnResultType status = newState.validate();
        TraderState keptState = status == TxnResultType.ACCEPTED
            ? newState
            : state;
        return new TxnResult(trader, txnId, keptState, status);
    }

    public static TraderStateUpdater from(MarketOrder order, float sharePrice) {
        return new TraderStateUpdater(
            order.getTxnId(),
            order.getTrader(),
            order.getType().getCoinSign() * order.getShares() * sharePrice,
            order.getType().getShareSign() * order.getShares(),
            false,
            0);
    }

    public static TraderStateUpdater from(Investment investment) {
        return new TraderStateUpdater(
            investment.getTxnId(),
            investment.getTrader(),
            -1 * investment.getInvested(),
            0,
            false,
            0);
    }

    public static TraderStateUpdater from(FeedMonkeys feed) {
        return new TraderStateUpdater(
                feed.getTxnId(),
                feed.getTrader(),
                0,
                -1 * feed.getMonkeys(),
                false,
                feed.getMonkeys());
    }

}
