package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;

public record WindowsFound(@JsonProperty("key") Object key, @JsonProperty("window") List<Window> windows) {
  static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JavaTimeModule());

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
