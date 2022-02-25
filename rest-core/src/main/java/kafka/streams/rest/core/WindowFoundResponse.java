package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public record WindowFoundResponse(
    @JsonProperty("found") boolean found,
    @JsonProperty("window") WindowFound windowFound
    ) {

  static final ObjectMapper jsonMapper = new ObjectMapper();

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
