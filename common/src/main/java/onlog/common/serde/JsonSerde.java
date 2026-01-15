package onlog.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> targetClass;

    public JsonSerde(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("JSON serialization failed", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            try {
                return mapper.readValue(data, targetClass);
            } catch (Exception e) {
                throw new RuntimeException("JSON deserialization failed", e);
            }
        };
    }
}
