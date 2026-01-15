package onlog.streams.parser;

import onlog.common.model.CanonicalEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class DedupTransformer
        implements Processor<String, CanonicalEvent, String, CanonicalEvent> {

    private KeyValueStore<String, Long> store;
    private ProcessorContext<String, CanonicalEvent> context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext<String, CanonicalEvent> context) {
        this.context = context;
        this.store =
            (KeyValueStore<String, Long>)
                context.getStateStore(DedupStoreSupplier.STORE_NAME);
    }

    @Override
    public void process(Record<String, CanonicalEvent> record) {
        CanonicalEvent v = record.value();

        if (v == null || v.devEui == null || v.fCnt == null || v.edgeIngestTime == null) {
            context.forward(record);
            return;
        }

        String key = v.devEui + ":" + v.fCnt;
        long now = v.edgeIngestTime.toEpochMilli();
        long ttl = ParserConfig.DEDUP_TTL.toMillis();

        Long last = store.get(key);

        if (last == null || now - last > ttl) {
            store.put(key, now);
            context.forward(record);
        }
        // else: duplicate → drop
    }
}
