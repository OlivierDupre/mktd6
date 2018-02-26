package io.monkeypatch.mktd6.server.market;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.kstreams.TopologySupplier;
import org.apache.kafka.streams.StreamsBuilder;

/**
 * The market server does several things:
 * - receive market orders, investments and monkey feeding from the players,
 *   from 3 separate topics,
 * - merges them into a single topic of state update requests,
 * - maintains state for each player and applies or rejects the
 *   state updates according to their internal balance,
 * - automatically assigns bailouts to players who need it,
 * - writes the updated states to a topic.
 */
public class MarketServer  implements TopologySupplier {


    @Override
    public StreamsBuilder apply(KafkaStreamsBoilerplate helper, StreamsBuilder builder) {

        return builder;
    }

}
