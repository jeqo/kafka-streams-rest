package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.Topology;

public record ApplicationTopology(@JsonProperty("description") String description) {
  static final ObjectMapper jsonMapper = new ObjectMapper();

  public static ApplicationTopology build(Topology topology) {
    return new ApplicationTopology(topology.describe().toString());
  }

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
