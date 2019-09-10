package net.knightech.agent.domain;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.knightech.agent.Agent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class Schemas {

  public static String schemaRegistryUrl = "";

  public static class Topic<K, V> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    Topic(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
      this.name = name;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
      Topics.ALL.put(name, this);
    }

    public Serde<K> keySerde() {
      return keySerde;
    }

    public Serde<V> valueSerde() {
      return valueSerde;
    }

    public String name() {
      return name;
    }

    public String toString() {
      return name;
    }
  }

  public static class Topics {

    static final Map<String, Topic<?, ?>> ALL = new HashMap<>();
    public static Topic<String, Agent> AGENTS;

    static {
      createTopics();
    }

    private static void createTopics() {
      AGENTS = new Topic<>("agents", Serdes.String(), new SpecificAvroSerde<Agent>());
    }
  }

  public static void configureSerdesWithSchemaRegistryUrl(final String url) {
    Topics.createTopics(); //wipe cached schema registry
    for (final Topic<?, ?> topic : Topics.ALL.values()) {
      configure(topic.keySerde(), url);
      configure(topic.valueSerde(), url);
    }
    schemaRegistryUrl = url;
  }

  private static void configure(final Serde<?> serde, final String url) {
    if (serde instanceof SpecificAvroSerde) {
      serde.configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, url), false);
    }
  }

}
