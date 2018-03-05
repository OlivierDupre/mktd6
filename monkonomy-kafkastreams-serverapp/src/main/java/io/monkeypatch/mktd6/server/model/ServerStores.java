package io.monkeypatch.mktd6.server.model;

import io.monkeypatch.mktd6.kstreams.StateStoreHelper;
import io.monkeypatch.mktd6.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;

public class ServerStores {

    public static final StateStoreHelper<String, Double> PRICE_VALUE_STORE = new StateStoreHelper(
        "price-state-store",
        new JsonSerde.StringSerde(),
        Serdes.Double());

    public static final StateStoreHelper<String, Double> TXN_INVESTMENT_STORE = new StateStoreHelper(
        "txn-investment-store",
        new JsonSerde.StringSerde(),
        Serdes.Double());

}
