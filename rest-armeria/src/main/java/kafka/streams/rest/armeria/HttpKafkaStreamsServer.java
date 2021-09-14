package kafka.streams.rest.armeria;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import java.util.Properties;
import kafka.streams.rest.core.KafkaStreamsService;
import org.apache.kafka.streams.Topology;

public class HttpKafkaStreamsServer extends KafkaStreamsService {

  ServerBuilder serverBuilder;

  Server server;

  public HttpKafkaStreamsServer(Topology topology, Properties streamsConfig) {
    super(topology, streamsConfig);
    serverBuilder = Server.builder().http(8080);
    serverBuilder
        .annotatedService("/application", new HttpApplicationStateService(applicationService()))
        .serviceUnder("/docs", DocService.builder()
            .build());

  }

  public void startServerOnly() {
    server.start();
  }

  public void startApplicationAndServer() {
    applicationService().start();

    this.kvStoreServices.forEach((s, keyValueStateStoreService) -> {
      serverBuilder.annotatedService("/stores/keyvalue/"+s, new HttpKeyValueStateStoreService(keyValueStateStoreService));
    });
    this.server = serverBuilder.build();

    server.start();
  }

  public void close() {
    applicationService().stop();
    server.stop();
  }
}
