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
        String basePath  = getenv("DB_BASE_PATH");
        String mode      = getenvOrDefault("PRODUCER_MODE", "realtime");

        var producer = KafkaProducerFactory.create(bootstrap);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                producer.flush();
            } finally {
                producer.close();
            }
        }));

        var sender = new KafkaSender(producer);

        System.out.println("[Producer mode] " + mode);

        if ("backfill".equals(mode)) {
            runBackfill(basePath, sender);
            sender.flush();
            System.out.println("[Backfill completed]");
            return;
        }

        runRealtime(basePath, sender);
    }

    // ==================================================
    // Realtime (slot catch-up, miss-free)
    // ==================================================
    private static void runRealtime(String basePath, KafkaSender sender) throws Exception {

        Instant lastProcessedSlot = null;

        while (true) {

            Instant currentSlot = TimeSlot.currentSlot();

            if (lastProcessedSlot == null) {
                lastProcessedSlot = currentSlot.minusSeconds(10);
                System.out.println("[Realtime start] from slot " + lastProcessedSlot);
            }

            while (lastProcessedSlot.isBefore(currentSlot)) {

                Instant slot = lastProcessedSlot.plusSeconds(10);

                File[] dbFiles = new File(basePath)
                        .listFiles(f -> f.getName().endsWith(".sqlite"));

                if (dbFiles != null) {
                    for (File db : dbFiles) {

                        try (Connection conn = SqliteClient.connect(db.getAbsolutePath())) {
                            RawLogRepository repo = new RawLogRepository(conn);
                            List<RawLogRow> rows = repo.findBySlot(slot);

                            if (!rows.isEmpty()) {
                                System.out.printf(
                                    "[Realtime] slot=%s db=%s rows=%d%n",
                                    slot, db.getName(), rows.size()
                                );
                            }

                            for (RawLogRow row : rows) {
                                sender.send(row);
                            }
                        }
                    }
                }

                lastProcessedSlot = slot;
            }

            Thread.sleep(500);
        }
    }

    // ==================================================
    // Backfill (batch + flush + pacing)
    // ==================================================
    private static void runBackfill(String basePath, KafkaSender sender) throws Exception {

        File[] dbFiles = new File(basePath)
                .listFiles(f -> f.getName().endsWith(".sqlite"));

        if (dbFiles == null) return;

        final int FLUSH_EVERY = 5_000;
        final int SLEEP_MS   = 5;

        for (File db : dbFiles) {

            System.out.println("[Backfill] " + db.getName());

            try (Connection conn = SqliteClient.connect(db.getAbsolutePath())) {

                RawLogRepository repo = new RawLogRepository(conn);
                List<RawLogRow> rows = repo.findAllOrdered();

                int sent = 0;

                for (RawLogRow row : rows) {
                    sender.send(row);
                    sent++;

                    if (sent % FLUSH_EVERY == 0) {
                        sender.flush();
                        Thread.sleep(SLEEP_MS);
                        System.out.printf(
                            "[Backfill] %s sent=%d%n",
                            db.getName(), sent
                        );
                    }
                }

                sender.flush();
                System.out.printf(
                    "[Backfill completed] %s total=%d%n",
                    db.getName(), sent
                );
            }
        }
    }

    // ==================================================
    // Utils
    // ==================================================
    private static String getenv(String key) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            throw new RuntimeException(key + " not set");
        }
        return v;
    }

    private static String getenvOrDefault(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
