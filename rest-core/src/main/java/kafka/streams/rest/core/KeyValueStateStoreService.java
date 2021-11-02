package kafka.streams.rest.core;

import com.fasterxml.jackson.databind.JsonNode;

public interface KeyValueStateStoreService<K> {
  JsonNode checkKey(K key);

  JsonNode info();
}
