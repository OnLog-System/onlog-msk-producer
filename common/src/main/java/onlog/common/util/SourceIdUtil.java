package onlog.common.util;

/**
 * source_id = tenant.line.process.device.metric
 *
 * Example:
 * F01.L01.FRY.FRYER_OIL.OIL_TEMP
 */
public class SourceIdUtil {

    public static String build(
            String tenantId,
            String lineId,
            String process,
            String deviceType,
            String metric
    ) {
        return String.join(".",
                safe(tenantId),
                safe(lineId),
                safe(process),
                safe(deviceType),
                safe(metric)
        );
    }

    private static String safe(String v) {
        return v == null ? "UNKNOWN" : v;
    }
}
