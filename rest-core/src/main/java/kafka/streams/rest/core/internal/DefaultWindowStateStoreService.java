package kafka.streams.rest.core.internal;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;
import kafka.streams.rest.core.StateStoreInfo;
import kafka.streams.rest.core.Window;
import kafka.streams.rest.core.WindowFound;
import kafka.streams.rest.core.WindowFoundResponse;
import kafka.streams.rest.core.WindowStateStoreService;
import kafka.streams.rest.core.WindowsFound;
import kafka.streams.rest.core.WindowsFoundResponse;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

public class DefaultWindowStateStoreService<K> implements WindowStateStoreService<K> {

  final Supplier<KafkaStreams> kafkaStreams;
  final String storeName;

  public DefaultWindowStateStoreService(Supplier<KafkaStreams> kafkaStreams, String storeName) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
  }

  @Override
  public StateStoreInfo info() {
    return new StateStoreInfo(storeName);
  }

  @Override
  public WindowFoundResponse windowFound(K key, Instant timeFrom, Instant timeTo) {
    boolean found;
    var windows = new ArrayList<Instant>();
    try (final var fetch = store().backwardFetch(key, timeFrom, timeTo)) {
      found = fetch.hasNext();
      while (fetch.hasNext()) {
        var entry = fetch.next();
        windows.add(Instant.ofEpochMilli(entry.key));
      }
    }
    return new WindowFoundResponse(found, new WindowFound(key, windows));
  }

  @Override
  public WindowsFoundResponse windowsFound(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) {
    boolean found;
    var windows = new HashMap<K, List<Window>>();
    try (final var fetch = store().backwardFetch(keyFrom, keyTo, timeFrom, timeTo)) {
      found = fetch.hasNext();
      while (fetch.hasNext()) {
        var entry = fetch.next();
        windows.computeIfPresent(
          entry.key.key(),
          (k, sessionsFound) -> {
            sessionsFound.add(Window.from(entry.key.window()));
            return sessionsFound;
          }
        );
        windows.computeIfAbsent(
          entry.key.key(),
          k -> {
            var list = new ArrayList<Window>();
            list.add(Window.from(entry.key.window()));
            return list;
          }
        );
      }
    }
    return new WindowsFoundResponse(
      keyFrom,
      keyTo,
      found,
      windows.keySet().stream().map(k -> new WindowsFound(k, windows.get(k))).toList()
    );
  }

  private ReadOnlyWindowStore<K, ?> store() {
    return kafkaStreams.get().store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
  }
}
