package kafka.streams.rest.core;

import org.apache.kafka.streams.KafkaStreams;

public interface ApplicationStateService {

  // Queries
  ApplicationState state();
  ApplicationConfiguration config();
  ApplicationTopology topology();

  // Commands
  void start();
  void stop();
  void restart();

  KafkaStreams kafkaStreams();
}
