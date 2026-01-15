package onlog.streams.parser;

import java.util.Base64;
import java.util.Optional;

public class BatteryPayloadDecoder {

    /**
     * Try to decode ENV sensor payload.
     * Never throws exception.
     */
    public static Optional<Decoded> decode(String base64) {

        if (base64 == null || base64.isEmpty()) {
            return Optional.empty();
        }

        byte[] data;
        try {
            data = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            // invalid base64
            return Optional.empty();
        }

        // =========================
        // Length check (NO exception)
        // =========================
        if (data.length < 6) {
            return Optional.empty();
        }

        // =========================
        // BAT (2 bytes)
        // =========================
        int batRaw = ((data[0] & 0xFF) << 8) | (data[1] & 0xFF);

        int statusBits = (batRaw >> 14) & 0b11;
        int voltageMv  = batRaw & 0x3FFF;

        String status = switch (statusBits) {
            case 0b00 -> "ULTRA_LOW";
            case 0b01 -> "LOW";
            case 0b10 -> "OK";
            case 0b11 -> "GOOD";
            default -> "UNKNOWN";
        };

        // =========================
        // Temperature (int16 / 100)
        // =========================
        short tempRaw = (short) (
                ((data[2] & 0xFF) << 8) |
                (data[3] & 0xFF)
        );
        double temperature = tempRaw / 100.0;

        // =========================
        // Humidity (uint16 / 10)
        // =========================
        int humRaw = ((data[4] & 0xFF) << 8) | (data[5] & 0xFF);
        double humidity = humRaw / 10.0;

        return Optional.of(
            new Decoded(voltageMv, status, temperature, humidity)
        );
    }

    public record Decoded(
            int batteryMv,
            String batteryStatus,
            double temperature,
            double humidity
    ) {}
}
