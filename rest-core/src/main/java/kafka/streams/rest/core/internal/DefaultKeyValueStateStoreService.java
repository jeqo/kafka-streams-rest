package kafka.streams.rest.core.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Supplier;
import kafka.streams.rest.core.KeyValueStateStoreService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class DefaultKeyValueStateStoreService<K, V> implements
    KeyValueStateStoreService<K, V> {

  private final ObjectMapper jsonMapper = new ObjectMapper();

  final Supplier<KafkaStreams> kafkaStreams;
  final String storeName;

  public DefaultKeyValueStateStoreService(Supplier<KafkaStreams> kafkaStreams, String name) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = name;
  }

  @Override public JsonNode get(String key) {
    var store = kafkaStreams.get().<ReadOnlyKeyValueStore<String, V>>store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    final var v = store.get(key);
    return jsonMapper.valueToTree(v);
  }
}
