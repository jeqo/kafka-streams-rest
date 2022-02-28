package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;
import org.apache.kafka.streams.kstream.Window;

public record SessionsFoundResponse(
    @JsonProperty("keyFrom") Object keyFrom,
    @JsonProperty("keyTo") Object keyTo,
    @JsonProperty("found") boolean found,
    @JsonProperty("sessions") List<SessionFound> sessions
) {

  static final ObjectMapper jsonMapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
