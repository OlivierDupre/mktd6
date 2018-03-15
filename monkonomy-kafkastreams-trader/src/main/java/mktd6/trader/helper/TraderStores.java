package mktd6.trader.helper;

import mktd6.kstreams.StateStoreHelper;
import mktd6.model.trader.Trader;
import mktd6.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;

public class TraderStores {

    public static final StateStoreHelper<Trader, Double> TRADER_INVESTMENT_STORE =
        new StateStoreHelper<>(
            "trader-investment-store",
            new JsonSerde.TraderSerde(),
            Serdes.Double());

}
