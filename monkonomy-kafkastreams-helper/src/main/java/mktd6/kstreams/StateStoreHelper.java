package mktd6.kstreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class StateStoreHelper<K, V> {

    private final String storeName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public StateStoreHelper(String storeName, Serde<K> keySerde, Serde<V> valueSerde) {
        this.storeName = storeName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public String getStoreName() {
        return storeName;
    }

    public Serde<K> getKeySerde() { return keySerde; }
    public Serde<V> getValueSerde() { return valueSerde; }

    public StreamsBuilder addTo(StreamsBuilder builder) {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, keySerde, valueSerde);
        return builder.addStateStore(storeBuilder);
    }
}
