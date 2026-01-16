package kafka;

import model.RawLogRow;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSender {

    private final KafkaProducer<String, String> producer;

    public KafkaSender(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void send(RawLogRow row) {
        producer.send(
            new ProducerRecord<>(
                row.topic,
                row.devEui,
                buildValue(row)
            )
        );
    }

    public void flush() {
        producer.flush();
    }

    private String buildValue(RawLogRow row) {
        return String.format("""
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
    }
}
