package kafka.streams.rest.armeria;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.docs.DocService;
import java.util.Properties;
import kafka.streams.rest.core.KafkaStreamsService;
import org.apache.kafka.streams.Topology;

public class HttpKafkaStreamsServer extends KafkaStreamsService {

  Server server;

  public HttpKafkaStreamsServer(Topology topology, Properties streamsConfig) {
    super(topology, streamsConfig);
    this.server = Server.builder()
        .http(8080)
        .annotatedService("/application", new HttpApplicationStateService(applicationService()))
        .serviceUnder("/docs", DocService.builder()
            .build())
        .build();
  }

  public void startServerOnly() {
    server.start();
  }

  public void startApplicationAndServer() {
    applicationService().start();
    server.start();
  }

  public void close() {
    applicationService().stop();
    server.stop();
  }
}
