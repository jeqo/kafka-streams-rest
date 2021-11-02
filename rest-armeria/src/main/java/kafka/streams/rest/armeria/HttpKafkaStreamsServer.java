package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import kafka.streams.rest.core.internal.DefaultKeyValueStateStoreService;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class HttpKafkaStreamsServer {

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
        .service("/", (ctx, req) -> HttpResponse.of("Kafka Streams REST Application"))
        .annotatedService("/application", new HttpApplicationStateService(applicationService))
        .serviceUnder("/docs", DocService.builder().build());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public void startServerOnly() {
    server.start();
  }

  public void startApplicationAndServer() {
    applicationService.start();

    kvStoreNames.forEach((s, sClass) -> serverBuilder.annotatedService(
        "/stores/key-value/" + s,
            new HttpKeyValueStateStoreService<>(new DefaultKeyValueStateStoreService<>(
                    applicationService::kafkaStreams,
                    s), sClass)
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

    public Builder addServiceForKeyValueStore(String storeName, Class<?> serializer) {
      this.keyValueStoreNames.put(storeName, serializer);
      return this;
    }

    public HttpKafkaStreamsServer build(Topology topology, Properties configs) {
      return new HttpKafkaStreamsServer(topology, configs, port, keyValueStoreNames);
    }
  }
}
