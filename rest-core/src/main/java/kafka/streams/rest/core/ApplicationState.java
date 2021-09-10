package kafka.streams.rest.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;

public record ApplicationState(String status, boolean isRunningOrRebalancing) {

  static final ObjectMapper jsonMapper = new ObjectMapper();

  public static ApplicationState build(KafkaStreams kafkaStreams) {
    return new ApplicationState(kafkaStreams.state().name(), kafkaStreams.state().isRunningOrRebalancing());
  }

  public JsonNode asJson() {
    return jsonMapper.createObjectNode()
        .put("status", status)
        .put("is_running_or_rebalancing", isRunningOrRebalancing);
  }
}
