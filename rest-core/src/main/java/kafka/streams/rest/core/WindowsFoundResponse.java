package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public record WindowsFoundResponse(
  @JsonProperty("keyFrom") Object keyFrom,
  @JsonProperty("keyTo") Object keyTo,
  @JsonProperty("found") boolean found,
  @JsonProperty("windows") List<WindowsFound> windows
) {
  static final ObjectMapper jsonMapper = new ObjectMapper();

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
