package kafka.streams.rest.core;

import org.apache.kafka.streams.KafkaStreams;

public interface ApplicationStateService {
  // Queries

  /**
   * @return current application state.
   */
  ApplicationState state();

  /**
   * Collect Kafka Streams application configuration for debugging.
   *
   * @return current application configuration.
   */
  ApplicationConfiguration config();

  /**
   * Print Kafka Streams application topology.
   *
   * @return application topology text.
   */
  ApplicationTopology topology();

  // Commands

  /**
   * Start Kafka Streams application.
   */
  void start();

  /**
   * Stop Kafka Streams application.
   */
  void stop();

  /**
   * Stop application if running, and start application.
   */
  void restart();

  /**
   * @return get Kafka Straems application instance.
   */
  KafkaStreams kafkaStreams();
}
