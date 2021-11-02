package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.streams.Topology;

public record ApplicationTopology(@JsonProperty("description") String description) {

  public static ApplicationTopology build(Topology topology) {
    return new ApplicationTopology(topology.describe().toString());
  }
}
