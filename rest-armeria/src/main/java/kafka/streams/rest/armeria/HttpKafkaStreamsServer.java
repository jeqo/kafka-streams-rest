package kafka.streams.rest.armeria;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.streams.rest.core.KeyValueStateStoreService;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import org.apache.kafka.streams.Topology;

public class HttpKafkaStreamsServer {

  DefaultApplicationService applicationService;
  Map<String, KeyValueStateStoreService> kvStoreServices = new LinkedHashMap<>();

  ServerBuilder serverBuilder;

  Server server;

  public HttpKafkaStreamsServer(Topology topology, Properties streamsConfig, int port) {
    applicationService = new DefaultApplicationService(topology, streamsConfig);
    serverBuilder = Server.builder().http(port);
    serverBuilder
        .annotatedService("/application", new HttpApplicationStateService(applicationService))
        .serviceUnder("/docs", DocService.builder()
            .build());

  }

  public void startServerOnly() {
    server.start();
  }

  public void startApplicationAndServer() {
    applicationService.start();

    this.kvStoreServices.forEach((s, keyValueStateStoreService) -> {
      serverBuilder.annotatedService(
          "/stores/key-value/" + s,
          new HttpKeyValueStateStoreService(keyValueStateStoreService)
      );
    });
    this.server = serverBuilder.build();

    server.start();
  }

  public void close() {
    applicationService.stop();
    server.stop();
  }

  static class Builder {

    private int port = 8000;

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    HttpKafkaStreamsServer build(Topology topology, Properties configs) {
      return new HttpKafkaStreamsServer(topology, configs, port);
    }
  }
}
