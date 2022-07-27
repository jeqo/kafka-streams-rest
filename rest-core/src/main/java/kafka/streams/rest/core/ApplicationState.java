package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;

public record ApplicationState(
  @JsonProperty("status") String status,
  @JsonProperty("is_running_or_rebalancing") boolean isRunningOrRebalancing
) {
  static final ObjectMapper jsonMapper = new ObjectMapper();

  public static ApplicationState build(KafkaStreams kafkaStreams) {
    return new ApplicationState(kafkaStreams.state().name(), kafkaStreams.state().isRunningOrRebalancing());
  }

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
