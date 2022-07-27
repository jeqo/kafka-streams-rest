package kafka.streams.rest.core.internal;

import java.util.function.Supplier;
import kafka.streams.rest.core.KeyFoundResponse;
import kafka.streams.rest.core.KeyValueStateStoreInfo;
import kafka.streams.rest.core.KeyValueStateStoreService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class DefaultKeyValueStateStoreService<K> implements KeyValueStateStoreService<K> {

  final Supplier<KafkaStreams> kafkaStreams;
  final String storeName;

  public DefaultKeyValueStateStoreService(Supplier<KafkaStreams> kafkaStreams, String storeName) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
  }

  @Override
  public KeyFoundResponse keyFound(K key) {
    return new KeyFoundResponse(key, store().get(key) != null);
  }

  private ReadOnlyKeyValueStore<K, ?> store() {
    return kafkaStreams
      .get()
      .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  }

  @Override
  public KeyValueStateStoreInfo info() {
    return new KeyValueStateStoreInfo(storeName, store().approximateNumEntries());
  }
}
