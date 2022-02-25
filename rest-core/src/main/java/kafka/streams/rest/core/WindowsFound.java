package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.kafka.streams.kstream.Window;

public record WindowsFound(
    @JsonProperty("key") Object key,
    @JsonProperty("window") List<Window> windows
) {

  static final ObjectMapper jsonMapper = new ObjectMapper();

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
