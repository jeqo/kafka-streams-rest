package kafka.streams.rest.core.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;
import kafka.streams.rest.core.SessionFound;
import kafka.streams.rest.core.SessionFoundResponse;
import kafka.streams.rest.core.SessionStateStoreService;
import kafka.streams.rest.core.SessionsFoundResponse;
import kafka.streams.rest.core.StateStoreInfo;
import kafka.streams.rest.core.Window;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

public class DefaultSessionStateStoreService<K> implements SessionStateStoreService<K> {

  final Supplier<KafkaStreams> kafkaStreams;
  final String storeName;

  public DefaultSessionStateStoreService(Supplier<KafkaStreams> kafkaStreams, String storeName) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
  }

  @Override
  public StateStoreInfo info() {
    return new StateStoreInfo(storeName);
  }

  @Override
  public SessionFoundResponse sessionFound(K key) {
    boolean found;
    var windows = new ArrayList<Window>();
    try (final var fetch = store().fetch(key)) {
      found = fetch.hasNext();
      while (fetch.hasNext()) {
        var entry = fetch.next();
        windows.add(Window.from(entry.key.window()));
      }
    }
    return new SessionFoundResponse(found, new SessionFound(key, windows));
  }

  @Override
  public SessionsFoundResponse sessionsFound(K keyFrom, K keyTo) {
    boolean found;
    var sessions = new HashMap<K, List<Window>>();
    try (final var fetch = store().fetch(keyFrom, keyTo)) {
      found = fetch.hasNext();
      while (fetch.hasNext()) {
        var entry = fetch.next();
        sessions.computeIfPresent(
          entry.key.key(),
          (k, sessionsFound) -> {
            sessionsFound.add(Window.from(entry.key.window()));
            return sessionsFound;
          }
        );
        sessions.computeIfAbsent(
          entry.key.key(),
          k -> {
            var list = new ArrayList<Window>();
            list.add(Window.from(entry.key.window()));
            return list;
          }
        );
      }
    }
    return new SessionsFoundResponse(
      keyFrom,
      keyTo,
      found,
      sessions.keySet().stream().map(k -> new SessionFound(k, sessions.get(k))).toList()
    );
  }

  private ReadOnlySessionStore<K, ?> store() {
    return kafkaStreams
      .get()
      .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.sessionStore()));
  }
}
