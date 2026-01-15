package onlog.common.serde;

import onlog.common.model.CanonicalEvent;

public class CanonicalEventSerde extends JsonSerde<CanonicalEvent> {

    public CanonicalEventSerde() {
        super(CanonicalEvent.class);
    }
}
