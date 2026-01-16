package onlog.common.model;

import java.time.Instant;

public class KpiEvent {

    public Instant snapshotTime;

    public String tenantId;
    public String lineId;

    public String kpiType;
    public String kpiKey;

    public Double valueNum;
    public String valueText;
    public Boolean valueBool;

    public static KpiEvent production(Instant ts, String key, double total) {
        String[] parts = key.split("\\|");

        KpiEvent e = new KpiEvent();
        e.snapshotTime = ts;
        e.tenantId = parts[0];
        e.lineId   = parts[1];

        e.kpiType  = "production";
        e.kpiKey   = "total_weight";
        e.valueNum = total;
        return e;
    }

    public static KpiEvent yield(Instant ts, String key, double ratio) {
        String[] parts = key.split("\\|");

        KpiEvent e = new KpiEvent();
        e.snapshotTime = ts;
        e.tenantId = parts[0];
        e.lineId   = parts[1];

        e.kpiType  = "yield";
        e.kpiKey   = "yield_ratio";
        e.valueNum = ratio;
        return e;
    }
}
