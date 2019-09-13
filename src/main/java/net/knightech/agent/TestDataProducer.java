package net.knightech.agent;

import net.knightech.agent.domain.Schemas;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class TestDataProducer {

    public static void main(String[] args) {


    }


    public static <T> KafkaProducer startProducer(final String bootstrapServers,
                                                  final Schemas.Topic<String, T> topic) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "agents-sender");

        return new KafkaProducer<>(producerConfig,
                topic.keySerde().serializer(),
                topic.valueSerde().serializer());
    }

}
