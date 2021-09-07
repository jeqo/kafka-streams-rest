package kafka.streams.rest.core.internal;

import kafka.streams.rest.core.ApplicationService;
import kafka.streams.rest.core.ApplicationState;
import org.apache.kafka.streams.KafkaStreams;

public class DefaultApplicationService implements ApplicationService {

  final KafkaStreams kafkaStreams;

  public DefaultApplicationService(KafkaStreams kafkaStreams) {
    this.kafkaStreams = kafkaStreams;
  }

  @Override public ApplicationState state() {
    return ApplicationState.build(kafkaStreams);
  }

  @Override public void start() {
    kafkaStreams.start();
  }

  @Override public void stop() {
    kafkaStreams.close();
  }
}
