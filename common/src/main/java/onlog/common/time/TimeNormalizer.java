package onlog.common.time;

import java.time.Instant;

/**
 * Time normalization utilities
 * All downstream logic MUST use edge_ingest_time
 */
public class TimeNormalizer {

    public static Instant parseIso(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return Instant.parse(value);
    }

    public static Instant nowIfNull(Instant t) {
        return t == null ? Instant.now() : t;
    }
}
