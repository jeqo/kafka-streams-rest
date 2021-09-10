package kafka.streams.rest.core.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import kafka.streams.rest.core.ApplicationStateService;
import kafka.streams.rest.core.ApplicationState;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public class DefaultApplicationService implements ApplicationStateService {

  final Topology topology;
  final Properties streamsConfig;

  KafkaStreams kafkaStreams;

  public DefaultApplicationService(final Topology topology, final Properties streamsConfig) {
    this.topology = topology;
    this.streamsConfig = streamsConfig;
  }

  @Override
  public ApplicationState state() {
    return ApplicationState.build(kafkaStreams);
  }

  @Override
  public void start() {
    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    kafkaStreams.start();
  }

  @Override
  public void stop() {
    kafkaStreams.close();
  }
}
