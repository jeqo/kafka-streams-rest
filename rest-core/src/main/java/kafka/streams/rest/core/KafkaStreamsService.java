package kafka.streams.rest.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import kafka.streams.rest.core.internal.DefaultKeyValueStateStoreService;
import org.apache.kafka.streams.Topology;

public class KafkaStreamsService {
  final ApplicationStateService applicationService;
  public final Map<String, KeyValueStateStoreService<?, ?>> kvStoreServices = new LinkedHashMap<>();

  public KafkaStreamsService(final Topology topology, final Properties streamsConfig) {
    this.applicationService = new DefaultApplicationService(topology, streamsConfig);
  }

  public ApplicationStateService applicationService() {
    return applicationService;
  }

  public <K, V> void addKeyValueStateStoreService(String storeName) {
    kvStoreServices.put(storeName, new DefaultKeyValueStateStoreService<K, V>(applicationService::kafkaStreams, storeName));
  }

  public <K, V> KeyValueStateStoreService<K, V> keyValueStateStoreService(String storeName) {
    return (KeyValueStateStoreService<K, V>) kvStoreServices.get(storeName);
  }
}
