package kafka;

import model.RawLogRow;
import org.apache.kafka.clients.producer.*;

public class KafkaSender {

    private final KafkaProducer<String, String> producer;

    public KafkaSender(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void send(RawLogRow row) {

        String topic = row.topic;      // sensor.env.raw 등
        String key   = row.devEui;     // ✅ transport-level key

        String value = String.format("""
            {
              "received_at": "%s",
              "tenant_id": "%s",
              "line_id": "%s",
              "process": "%s",
              "device_type": "%s",
              "metric": "%s",
              "payload": %s
            }
            """,
            row.receivedAt,
            row.tenantId,
            row.lineId,
            row.process,
            row.deviceType,
            row.metric,
            row.payload
        );

        producer.send(new ProducerRecord<>(topic, key, value));
    }
}
