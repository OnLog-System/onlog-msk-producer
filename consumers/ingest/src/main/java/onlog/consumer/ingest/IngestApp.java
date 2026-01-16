package onlog.consumer.ingest;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class IngestApp {

    private static final Logger log =
            LoggerFactory.getLogger(IngestApp.class);

    public static void main(String[] args) {

        Properties props =
                KafkaConsumerConfig.base("ingest-consumer");

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(props)) {

            consumer.subscribe(
                    List.of("sensor.parsed", "kpi.event")
            );

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                records.forEach(r -> {
                    try {
                        Dispatcher.dispatch(r.topic(), r.value());
                    } catch (Exception e) {
                        log.error(
                            "ingest failed topic={} offset={}",
                            r.topic(), r.offset(), e
                        );
                        // ❗ 죽이지 않고 skip
                    }
                });

                consumer.commitSync();
            }
        }
    }
}
