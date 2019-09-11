package net.knightech.agent;

import lombok.extern.slf4j.Slf4j;
import net.knightech.agent.domain.Schemas;
import net.knightech.agent.domain.beans.AgentBean;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.asynchttpclient.*;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static net.knightech.agent.MicroserviceUtils.addShutdownHookAndBlock;
import static net.knightech.agent.MicroserviceUtils.baseStreamsConfig;
import static net.knightech.agent.domain.Schemas.Topics.AGENTS;
import static net.knightech.agent.domain.beans.AgentBean.fromBean;
import static net.knightech.agent.domain.beans.AgentBean.toBean;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;

@Slf4j
@Service
public class AgentServiceImpl implements AgentService {

  private static final String CALL_TIMEOUT = "10000";
  private static final String AGENTS_STORE_NAME = "agents-store";
  private final String SERVICE_APP_ID = getClass().getSimpleName();

  private KafkaStreams streams = null;
  private MetadataService metadataService;
  private KafkaProducer<String, Agent> producer;

  //In a real implementation we would need to (a) support outstanding requests for the same Id/filter from
  // different users and (b) periodically purge old entries from this map.
  private final Map<String, FilteredResponse<String, Agent>> outstandingRequests = new ConcurrentHashMap<>();


  /**
   * Create a table of agents which we can query. When the table is updated
   * we check to see if there is an outstanding HTTP GET request waiting to be
   * fulfilled.
   */
  private StreamsBuilder createAgentsMaterializedView() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.table(AGENTS.name(), Consumed.with(AGENTS.keySerde(), AGENTS.valueSerde()), Materialized.as(AGENTS_STORE_NAME))
            .toStream().foreach(this::maybeCompleteLongPollGet);
    return builder;
  }

  private void maybeCompleteLongPollGet(final String id, final Agent agent) {
    final FilteredResponse<String, Agent> callback = outstandingRequests.get(id);
    if (callback != null && callback.predicate.test(id, agent)) {
      callback.deferredResult.setResult(ResponseEntity.of(Optional.of(toBean(agent))));
    }
  }


  public void getAgent(String id, DeferredResult<ResponseEntity<?>> deferredResult){

    final HostStoreInfo hostForKey = getKeyLocationOrBlock(id,  deferredResult);

    if (hostForKey == null) { //request timed out so return
      return;
    }
    //Retrieve the agent locally or reach out to a different instance if the required partition is hosted elsewhere.
    if (thisHost(hostForKey)) {
      fetchLocal(id, deferredResult, (k, v) -> true);
    } else {
      final String path = new Paths(hostForKey.getHost(), hostForKey.getPort()).urlGet(id);
      fetchFromOtherHost(path, deferredResult);
    }
  }

  public void addAgent( AgentBean agent, DeferredResult<ResponseEntity<?>> deferredResult) {

    final Agent bean = fromBean(agent);
    producer.send(new ProducerRecord<>(AGENTS.name(), bean.getId(), bean), callback(deferredResult, bean.getId()));
  }


  class FilteredResponse<K, V> {
    private final DeferredResult<ResponseEntity<?>> deferredResult;
    private final Predicate<K, V> predicate;

    FilteredResponse(DeferredResult<ResponseEntity<?>> deferredResult, final Predicate<K, V> predicate) {
      this.deferredResult = deferredResult;
      this.predicate = predicate;
    }
  }

  /**
   * Fetch the agent from the local materialized view
   *
   * @param id ID to fetch
   * @param deferredResult the response to call once completed
   * @param predicate a filter that for this fetch, so for example we might fetch only VALIDATED
   * agents.
   */
  private void fetchLocal(final String id, DeferredResult<ResponseEntity<?>> deferredResult, final Predicate<String, Agent> predicate) {
    log.info("running GET on this node");
    try {
      final Agent agent = agentsStore().get(id);
      if (agent == null || !predicate.test(id, agent)) {
        log.info("Delaying get as agent not present for id " + id);
        outstandingRequests.put(id, new FilteredResponse<>(deferredResult, predicate));
      } else {
        deferredResult.setResult(ResponseEntity.of(Optional.of(toBean(agent))));
      }
    } catch (final InvalidStateStoreException e) {
      //Store not ready so delay
      outstandingRequests.put(id, new FilteredResponse<>(deferredResult, predicate));
    }
  }

  private ReadOnlyKeyValueStore<String, Agent> agentsStore() {
    return streams.store(AGENTS_STORE_NAME, QueryableStoreTypes.keyValueStore());
  }

  /**
   * Use Kafka Streams' Queryable State API to work out if a key/value pair is located on
   * this node, or on another Kafka Streams node. This returned HostStoreInfo can be used
   * to redirect an HTTP request to the node that has the data.
   * <p>
   * If metadata is available, which can happen on startup, or during a rebalance, block until it is.
   */
  private HostStoreInfo getKeyLocationOrBlock(final String id, DeferredResult<ResponseEntity<?>> deferredResult) {
    HostStoreInfo locationOfKey;
    while (locationMetadataIsUnavailable(locationOfKey = getHostForAgentId(id))) {
      //The metastore is not available. This can happen on startup/rebalance.
      if (deferredResult.hasResult()) {
        //The response timed out so return
        return null;
      }
      try {
        //Sleep a bit until metadata becomes available
        Thread.sleep(Math.min(Long.valueOf(CALL_TIMEOUT), 200));
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
    return locationOfKey;
  }

  private boolean locationMetadataIsUnavailable(final HostStoreInfo hostWithKey) {
    return NOT_AVAILABLE.host().equals(hostWithKey.getHost())
            && NOT_AVAILABLE.port() == hostWithKey.getPort();
  }

  private boolean thisHost(final HostStoreInfo host) {
    return host.getHost().equals("localhost") &&
            host.getPort() == 8009;
  }

  private void fetchFromOtherHost(final String path, DeferredResult<ResponseEntity<?>> deferredResult) {

    log.info("Chaining GET to a different instance: " + path);


    AsyncHttpClient client = Dsl.asyncHttpClient();
    BoundRequestBuilder request = client.prepareGet(path);

    try {

      ListenableFuture<Response> responseFuture = request.execute();

      deferredResult.setResult(ResponseEntity.of(Optional.ofNullable(responseFuture.get())));

    } catch (final Exception swallowed) {
      // do nothing
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void start(final String bootstrapServers, final String stateDir) {

    streams = startKStreams(bootstrapServers);
    log.info("Started AgentService " + getClass().getSimpleName());
  }

  private KafkaStreams startKStreams(final String bootstrapServers) {

    final KafkaStreams streams = new KafkaStreams(
            createAgentsMaterializedView().build(),
            config(bootstrapServers));
    metadataService = new MetadataService(streams);
    streams.cleanUp(); //don't do this in prod as it clears your state stores
    streams.start();

    return streams;
  }

  private Properties config(final String bootstrapServers) {
    final Properties props = baseStreamsConfig(bootstrapServers, "/tmp/kafka-streams", SERVICE_APP_ID);
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost" + ":" + "8001");
    return props;
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
    if (producer != null) {
      producer.close();
    }
  }

  // for testing only
  void cleanLocalState() {
    if (streams != null) {
      streams.cleanUp();
    }
  }

  private HostStoreInfo getHostForAgentId(final String agentId) {
    return metadataService
            .streamsMetadataForStoreAndKey(AGENTS_STORE_NAME, agentId, Serdes.String().serializer());
  }

  private Callback callback(DeferredResult<ResponseEntity<?>> deferredResult, final String agentId) {
    return (recordMetadata, e) -> {
      if (e != null) {
        deferredResult.setResult(ResponseEntity.of(Optional.of(e)));
      } else {
        try {
          //Return the location of the newly created resource
          URI uri = new URI("/v1/agents/" + agentId);
          deferredResult.setResult(ResponseEntity.created(uri).build());
        } catch (final URISyntaxException e2) {
          e2.printStackTrace();
        }
      }
    };
  }

  @PostConstruct
  public void initBroker() throws Exception {

    final String bootstrapServers = "http://192.168.5.31:9092";
    final String schemaRegistryUrl = "http://192.168.5.31:8081";
    final String restHostname = "http://192.168.5.31";
    final String restPort = "8001";

    Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
    start(bootstrapServers, "/tmp/kafka-streams");
    addShutdownHookAndBlock(this);
  }
}
