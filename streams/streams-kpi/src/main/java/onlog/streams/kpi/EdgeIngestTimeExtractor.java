package onlog.streams.kpi;

import onlog.common.model.CanonicalEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class EdgeIngestTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        Object value = record.value();

        if (value instanceof CanonicalEvent) {
            CanonicalEvent e = (CanonicalEvent) value;
            if (e.edgeIngestTime != null) {
                return e.edgeIngestTime.toEpochMilli();
            }
        }

        // fallback (안전장치)
        return record.timestamp();
    }
}
