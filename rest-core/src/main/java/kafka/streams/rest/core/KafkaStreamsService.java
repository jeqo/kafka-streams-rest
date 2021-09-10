package kafka.streams.rest.core;

import java.util.Properties;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import org.apache.kafka.streams.Topology;

public class KafkaStreamsService {
  final ApplicationStateService applicationService;

  public KafkaStreamsService(final Topology topology, final Properties streamsConfig) {
    this.applicationService = new DefaultApplicationService(topology, streamsConfig);
  }

  public ApplicationStateService applicationService() {
    return applicationService;
  }
}
