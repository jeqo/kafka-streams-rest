package kafka.streams.rest.core;

import com.fasterxml.jackson.databind.JsonNode;

public interface KeyValueStateStoreService<K, V> {
  JsonNode get(String key);
}
