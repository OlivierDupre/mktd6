package io.monkeypatch.mktd6.server.model;

import io.monkeypatch.mktd6.kstreams.StateStoreHelper;
import io.monkeypatch.mktd6.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;

public class StateStores {

    public static final StateStoreHelper<String, Double> PRICE_VALUE_STORE = new StateStoreHelper(
        "price-state-store",
        new JsonSerde.StringSerde(),
        Serdes.Double());

}
