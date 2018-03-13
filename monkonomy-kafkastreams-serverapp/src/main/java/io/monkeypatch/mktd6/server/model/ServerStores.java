package io.monkeypatch.mktd6.server.model;

import io.monkeypatch.mktd6.kstreams.StateStoreHelper;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.model.trader.TraderState;
import io.monkeypatch.mktd6.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;

public class ServerStores {

    public static final StateStoreHelper<String, Double> PRICE_VALUE_STORE =
        new StateStoreHelper<>(
            "price-state-store",
            new JsonSerde.StringSerde(),
            Serdes.Double());

    public static final StateStoreHelper<Trader, TraderState> STATE_STORE =
        new StateStoreHelper<>(
            "trader-state-store",
            new JsonSerde.TraderSerde(),
            new JsonSerde.TraderStateSerde());

    public static final StateStoreHelper<String, Double> TXN_INVESTMENT_STORE =
        new StateStoreHelper<>(
            "txn-investment-store",
            new JsonSerde.StringSerde(),
            Serdes.Double());

    public static final StateStoreHelper<Trader, Double> TRADER_INVESTMENT_STORE =
        new StateStoreHelper<>(
            "trader-investment-store",
            new JsonSerde.TraderSerde(),
            Serdes.Double());

    public static final StateStoreHelper<String, BurstStep> BURST_STEP_STORE =
        new StateStoreHelper<>(
            "burst-step-store",
            new JsonSerde.StringSerde(),
            new BurstStep.Serde());
}
