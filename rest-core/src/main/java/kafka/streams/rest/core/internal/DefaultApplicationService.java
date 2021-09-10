package kafka.streams.rest.core.internal;

import java.util.Properties;
import kafka.streams.rest.core.ApplicationConfiguration;
import kafka.streams.rest.core.ApplicationState;
import kafka.streams.rest.core.ApplicationStateService;
import kafka.streams.rest.core.ApplicationTopology;
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

  @Override public ApplicationState state() {
    return ApplicationState.build(kafkaStreams);
  }

  @Override public ApplicationConfiguration config() {
    return ApplicationConfiguration.build(streamsConfig);
  }

  @Override public ApplicationTopology topology() {
    return ApplicationTopology.build(topology);
  }

  @Override public void start() {
    if (kafkaStreams != null && state().isRunningOrRebalancing()) {
      throw new IllegalStateException("Application is already running.");
    } else {
      kafkaStreams = new KafkaStreams(topology, streamsConfig);
      kafkaStreams.start();
    }
  }

  @Override public void stop() {
    if (!state().isRunningOrRebalancing()) {
      throw new IllegalStateException("Application is not running");
    } else {
      kafkaStreams.close();
    }
  }

  @Override public void restart() {
    if (state().isRunningOrRebalancing()) {
      kafkaStreams.close();
    }
    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    kafkaStreams.start();
  }
}
