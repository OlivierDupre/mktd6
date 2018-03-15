package mktd6.kstreams;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.function.BiFunction;

public interface TopologySupplier extends
    BiFunction<
        KafkaStreamsBoilerplate, StreamsBuilder,
        StreamsBuilder
    > {}