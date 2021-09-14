package kafka.streams.rest.core;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;

public class StateStoreService<T> {
  final String name;
  final QueryableStoreType<T> type;

  final KafkaStreams kafkaStreams;

  public StateStoreService(KafkaStreams kafkaStreams, String name, QueryableStoreType<T> type) {
    this.name = name;
    this.type = type;
    this.kafkaStreams = kafkaStreams;
  }

  void read() {
    var store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(name, type));

  }
}
