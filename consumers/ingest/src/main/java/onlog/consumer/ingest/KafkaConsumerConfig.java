
package onlog.consumer.ingest;

import onlog.common.serde.CanonicalEventSerde;
import onlog.common.serde.JsonSerde;
import onlog.common.model.KpiEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerConfig {

        public static Properties base(String groupId) {

        Properties p = new Properties();

        p.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv("KAFKA_BOOTSTRAP")
        );

        // =========================
        // MSK IAM (🔥 필수 🔥)
        // =========================
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "AWS_MSK_IAM");
        p.put(
                "sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;"
        );
        p.put(
                "sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        );

        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        p.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        p.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        return p;
        }
