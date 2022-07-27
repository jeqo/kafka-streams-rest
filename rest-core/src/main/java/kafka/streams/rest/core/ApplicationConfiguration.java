package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public record ApplicationConfiguration(@JsonProperty("configs") Map<String, String> configs) {
  static final ObjectMapper jsonMapper = new ObjectMapper();

  public static ApplicationConfiguration build(Properties properties) {
    final var configs = new LinkedHashMap<String, String>();
    properties.forEach((k, v) -> configs.put(k.toString(), v.toString()));
    return new ApplicationConfiguration(configs);
  }

  public JsonNode asJson() {
    return jsonMapper.valueToTree(this);
  }
}
