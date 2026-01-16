package model;

import java.time.Instant;

public class RawLogRow {
    public long id;
    public Instant receivedAt;
    public String topic;
    public String tenantId;
    public String lineId;
    public String process;
    public String deviceType;
    public String metric;
    public String devEui;
    public String payload;
}
