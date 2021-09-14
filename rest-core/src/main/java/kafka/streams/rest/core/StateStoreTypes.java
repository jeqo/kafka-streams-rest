package kafka.streams.rest.core;

import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;

public enum StateStoreTypes {
  KEY_VALUE(QueryableStoreTypes.keyValueStore()),
  TIMESTAMPED_KEY_VALUE(QueryableStoreTypes.timestampedKeyValueStore()),

  WINDOW(QueryableStoreTypes.windowStore()),
  TIMESTAMPED_WINDOW(QueryableStoreTypes.timestampedWindowStore()),

  SESSION(QueryableStoreTypes.sessionStore()),
  ;

  final QueryableStoreType<?> type;

  StateStoreTypes(QueryableStoreType<?> type) {
    this.type = type;
  }
}
