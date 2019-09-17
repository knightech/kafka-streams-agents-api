package net.knightech.agent;

import lombok.extern.slf4j.Slf4j;
import net.knightech.agent.domain.Schemas;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MicroserviceUtils {


  private static final String DEFAULT_BOOTSTRAP_SERVERS = "http://192.168.5.31:9092";
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://192.168.5.31:8081";

  public static String parseArgsAndConfigure(final String[] args) {


    log.info("Connecting to Kafka cluster via bootstrap servers " + DEFAULT_BOOTSTRAP_SERVERS);
    log.info("Connecting to Confluent schema registry at " + DEFAULT_SCHEMA_REGISTRY_URL);
    Schemas.configureSerdesWithSchemaRegistryUrl(DEFAULT_SCHEMA_REGISTRY_URL);
    return DEFAULT_BOOTSTRAP_SERVERS;
  }

  public static Properties baseStreamsConfig(final String bootstrapServers,
                                             final String stateDir,
                                             final String appId) {
    return baseStreamsConfig(bootstrapServers, stateDir, appId, false);
  }

  public static Properties baseStreamsConfig(final String bootstrapServers,
                                             final String stateDir,
                                             final String appId,
                                             final boolean enableEOS) {
    Properties config = new Properties();
    // Workaround for a known issue with RocksDB in environments where you have only 1 cpu core.
    //config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    //config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final String processingGuaranteeConfig = enableEOS ? "exactly_once" : "at_least_once";
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuaranteeConfig);
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1); //commit as fast as possible
    config.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 30000);

    return config;
  }

  public static class CustomRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(final String storeName, final Options options,
        final Map<String, Object> configs) {
      // Workaround: We must ensure that the parallelism is set to >= 2.  There seems to be a known
      // issue with RocksDB where explicitly setting the parallelism to 1 causes issues (even though
      // 1 seems to be RocksDB's default for this configuration).
      final int compactionParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 2);
      // Set number of compaction threads (but not flush threads).
      options.setIncreaseParallelism(compactionParallelism);
    }
  }

  public static <T> KafkaProducer startProducer(final String bootstrapServers,
                                                final Schemas.Topic<String, T> topic) {
    final Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "order-sender");

    return new KafkaProducer<>(producerConfig,
            topic.keySerde().serializer(),
            topic.valueSerde().serializer());
  }


}
