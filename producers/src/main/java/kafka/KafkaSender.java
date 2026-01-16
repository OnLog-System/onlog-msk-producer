package kafka;

import model.RawLogRow;
import org.apache.kafka.clients.producer.*;

public class KafkaSender {

    private final KafkaProducer<String, String> producer;
    private final String topicPrefix;

    public KafkaSender(KafkaProducer<String, String> producer, String topicPrefix) {
        this.producer = producer;
        this.topicPrefix = topicPrefix;
    }

    public void send(RawLogRow row) {

        String topic = topicPrefix + "." + resolveTopic(row.topic);
        String key = row.tenantId + "." + row.lineId + "." + row.deviceType + "." + row.metric;

        String value = String.format("""
            {
              "received_at":"%s",
              "tenant_id":"%s",
              "line_id":"%s",
              "process":"%s",
              "device_type":"%s",
              "metric":"%s",
              "payload":%s
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

    private String resolveTopic(String rawTopic) {
        if (rawTopic.contains("sensor.env")) return "sensor_env";
        if (rawTopic.contains("sensor.scale")) return "sensor_scale";
        if (rawTopic.contains("machine")) return "machine";
        return "unknown";
    }
}
