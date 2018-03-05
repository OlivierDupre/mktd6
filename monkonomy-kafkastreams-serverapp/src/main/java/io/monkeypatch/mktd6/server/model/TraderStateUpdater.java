package io.monkeypatch.mktd6.server.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.market.ops.TxnResult;
import io.monkeypatch.mktd6.model.market.ops.TxnResultType;
import io.monkeypatch.mktd6.model.trader.TraderState;
import io.monkeypatch.mktd6.model.trader.ops.FeedMonkeys;
import io.monkeypatch.mktd6.model.trader.ops.Investment;
import io.monkeypatch.mktd6.model.trader.ops.MarketOrder;
import io.monkeypatch.mktd6.serde.BaseJsonSerde;

public final class TraderStateUpdater {

    public enum Type {
        MARKET,
        INVEST,
        FEED,
        RETURN;
    }

    private final String txnId;
    private final Type type;

    private final double coinsDiff;
    private final int sharesDiff;
    private final boolean addBailout;
    private final int fedMonkeys;

    @JsonCreator
    public TraderStateUpdater(
            @JsonProperty("txnId") String txnId,
            @JsonProperty("type") Type type,
            @JsonProperty("coinsDiff") double coinsDiff,
            @JsonProperty("sharesDiff") int sharesDiff,
            @JsonProperty("addBailout") boolean addBailout,
            @JsonProperty("fedMonkeys") int fedMonkeys
    ) {
        this.txnId = txnId;
        this.type = type;
        this.coinsDiff = coinsDiff;
        this.sharesDiff = sharesDiff;
        this.addBailout = addBailout;
        this.fedMonkeys = fedMonkeys;
    }

    public double getCoinsDiff() {
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

    public Type getType() {
        return type;
    }

    public String getTxnId() {
        return txnId;
    }

    public static class Serde extends BaseJsonSerde<TraderStateUpdater> {
        public Serde() { super(TraderStateUpdater.class); }
    }

    public TxnResult update(TraderState state) {
        if (state == null) {
            state = TraderState.init();
        }
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
        return new TxnResult(txnId, keptState, status);
    }

    public static TraderStateUpdater from(MarketOrder order, double sharePrice) {
        return new TraderStateUpdater(
            order.getTxnId(),
            Type.MARKET,
            order.getType().getCoinSign() * order.getShares() * sharePrice,
            order.getType().getShareSign() * order.getShares(),
            false,
            0);
    }

    public static TraderStateUpdater from(Investment investment) {
        return new TraderStateUpdater(
            investment.getTxnId(),
            Type.INVEST,
            -1 * investment.getInvested(),
            0,
            false,
            0);
    }

    public static TraderStateUpdater from(FeedMonkeys feed) {
        return new TraderStateUpdater(
            feed.getTxnId(),
            Type.FEED,
            0,
            -1 * feed.getMonkeys(),
            false,
            feed.getMonkeys());
    }

}
