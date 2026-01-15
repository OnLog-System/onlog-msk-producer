package onlog.streams.parser;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamsParserApp {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                ParserConfig.APPLICATION_ID + "-dedup");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv("KAFKA_BOOTSTRAP"));

        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.EXACTLY_ONCE_V2);

        KafkaStreams streams =
                new KafkaStreams(DedupTopology.build(), props);

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );

        streams.start();
    }
}
