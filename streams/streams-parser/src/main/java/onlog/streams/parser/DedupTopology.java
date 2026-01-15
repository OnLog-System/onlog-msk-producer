package onlog.streams.parser;

import onlog.common.model.CanonicalEvent;
import onlog.common.serde.CanonicalEventSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

public class DedupTopology {

    public static Topology build() {

        Topology topology = new Topology();

        topology.addSource(
                "source",
                Serdes.String().deserializer(),
                new CanonicalEventSerde().deserializer(),
                ParserConfig.INTERMEDIATE_TOPIC
        );

        topology.addProcessor(
                "dedup",
                DedupTransformer::new,
                "source"
        );

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        DedupStoreSupplier.supplier(),
                        Serdes.String(),
                        Serdes.Long()
                ),
                "dedup"
        );

        topology.addSink(
                "sink",
                ParserConfig.OUTPUT_TOPIC,
                Serdes.String().serializer(),
                new CanonicalEventSerde().serializer(),
                "dedup"
        );

        return topology;
    }
}
