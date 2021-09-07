package kafka.streams.rest.core;

import org.apache.kafka.streams.KafkaStreams;

public record ApplicationState (String name) {
  public static ApplicationState build(KafkaStreams kafkaStreams) {
    return new ApplicationState(kafkaStreams.state().name());
  }
}
