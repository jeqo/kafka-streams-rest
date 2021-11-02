package kafka.streams.rest.armeria;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import kafka.streams.rest.core.internal.DefaultKeyValueStateStoreService;
import org.apache.kafka.streams.Topology;

/**
 * HTTP Server for Kafka Streams application.
 */
public final class HttpKafkaStreamsServer {

  final DefaultApplicationService applicationService;
  final Map<String, Class<?>> kvStoreNames;

  final ServerBuilder serverBuilder;

  Server server;

  public HttpKafkaStreamsServer(final Topology topology,
      final Properties streamsConfig,
      final int port,
      final Map<String, Class<?>> kvStoreNames) {
    this.kvStoreNames = kvStoreNames;
    this.applicationService = new DefaultApplicationService(topology, streamsConfig);
    this.serverBuilder = Server.builder()
        .http(port)
        .annotatedService("/application", new HttpApplicationStateService(applicationService))
        .serviceUnder("/", DocService.builder().build());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public void startServerOnly() {
    server.start();
  }

  public void startApplicationAndServer() {
    applicationService.start();

    kvStoreNames.forEach((store, keyClass) -> serverBuilder.annotatedService(
        "/stores/key-value/" + store,
        new HttpKeyValueStateStoreService<>(new DefaultKeyValueStateStoreService<>(
            applicationService::kafkaStreams,
            store), keyClass)
    ));
    this.server = serverBuilder.build();

    server.start();
  }

  public void close() {
    applicationService.stop();
    server.stop();
  }

  public static class Builder {

    private int port = 8000;
    Map<String, Class<?>> keyValueStoreNames = new LinkedHashMap<>();

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder addServiceForKeyValueStore(String storeName) {
      this.keyValueStoreNames.put(storeName, String.class);
      return this;
    }

    public Builder addServiceForKeyValueStore(String storeName, Class<?> keyClass) {
      this.keyValueStoreNames.put(storeName, keyClass);
      return this;
    }

    public HttpKafkaStreamsServer build(Topology topology, Properties configs) {
      return new HttpKafkaStreamsServer(topology, configs, port, keyValueStoreNames);
    }
  }
}
