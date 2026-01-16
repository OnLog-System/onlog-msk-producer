import kafka.KafkaProducerFactory;
import kafka.KafkaSender;
import model.RawLogRow;
import sqlite.RawLogRepository;
import sqlite.SqliteClient;
import time.TimeSlot;

import java.io.File;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {

        String bootstrap = getenv("KAFKA_BOOTSTRAP_SERVERS");
        String basePath = getenv("DB_BASE_PATH");
        String prefix = getenv("TOPIC_PREFIX");

        var producer = KafkaProducerFactory.create(bootstrap);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        var sender = new KafkaSender(producer, prefix);

        Instant lastSlot = null;

        while (true) {

            Instant slot = TimeSlot.currentSlot();

            if (!slot.equals(lastSlot)) {

                for (File db : new File(basePath).listFiles(f -> f.getName().endsWith(".sqlite"))) {

                    try (Connection conn = SqliteClient.connect(db.getAbsolutePath())) {
                        RawLogRepository repo = new RawLogRepository(conn);
                        List<RawLogRow> rows = repo.findBySlot(slot);

                        for (RawLogRow row : rows) {
                            sender.send(row);
                        }
                    }
                }

                lastSlot = slot;
            }

            Thread.sleep(1000);
        }
    }

    private static String getenv(String key) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            throw new RuntimeException(key + " not set");
        }
        return v;
    }
}
