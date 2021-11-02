package kafka.streams.rest.core.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.streams.rest.core.KeyValueStateStoreService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.function.Supplier;

public class DefaultKeyValueStateStoreService<K> implements KeyValueStateStoreService<K> {

  private final ObjectMapper jsonMapper = new ObjectMapper();

  final Supplier<KafkaStreams> kafkaStreams;
  final String storeName;

  public DefaultKeyValueStateStoreService(Supplier<KafkaStreams> kafkaStreams, String storeName) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
  }

  @Override public JsonNode checkKey(K key) {
    final var v = store().get(key);
    return jsonMapper.createObjectNode().put("found", v != null);
  }

  // TODO handle key type
  private ReadOnlyKeyValueStore<K, ?> store() {
    return kafkaStreams.get().store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  }

  @Override
  public JsonNode info() {
    return jsonMapper.createObjectNode()
            .put("storeName", storeName)
            .put("approximatedNumEntries", store().approximateNumEntries());
  }
}
