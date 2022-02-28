package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.List;

public record WindowFound(
    @JsonProperty("key") Object key,
    @JsonProperty("times") List<Instant> time
) {

  static final ObjectMapper jsonMapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
