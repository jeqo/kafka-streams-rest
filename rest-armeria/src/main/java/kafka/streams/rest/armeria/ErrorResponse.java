package kafka.streams.rest.armeria;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public record ErrorResponse(String message) {
  static final ObjectMapper jsonMapper = new ObjectMapper();

  JsonNode asJson() {
    return jsonMapper.createObjectNode().put("message", message);
  }
}
