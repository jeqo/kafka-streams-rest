package kafka.streams.tombstone;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

class TombstoneTopology implements Supplier<Topology> {

  private final Duration maxAge;
  private final Optional<Duration> scanFrequency;

  private final String sourceTopic;
  private final String destinationTopic;

  public TombstoneTopology(Duration maxAge, String sourceTopic) {
    this.maxAge = maxAge;
    this.scanFrequency = Optional.empty();
    this.sourceTopic = sourceTopic;
    this.destinationTopic = sourceTopic;
  }

  public TombstoneTopology(Duration maxAge, Optional<Duration> scanFrequency, String sourceTopic) {
    this.maxAge = maxAge;
    this.scanFrequency = scanFrequency;
    this.sourceTopic = sourceTopic;
    this.destinationTopic = sourceTopic;
  }

  public TombstoneTopology(Duration maxAge, Optional<Duration> scanFrequency, String sourceTopic, String destinationTopic) {
    this.maxAge = maxAge;
    this.scanFrequency = scanFrequency;
    this.sourceTopic = sourceTopic;
    this.destinationTopic = destinationTopic;
  }

  @Override
  public Topology get() {
    // return keyValueBased();
    return sessionBased();
  }

  /**
   * Session-window-based approach to emit tombstones.
   * It's based on 2 state stores: 1 for the session window, were the key is stored; and a suppression store
   * where window results are buffered until they close.
   *
   * @return session-based Kafka Streams Topology.
   */
  Topology sessionBased() {
    final var builder = new StreamsBuilder();
    builder.stream(sourceTopic, Consumed.with(Serdes.ByteBuffer(), Serdes.Bytes()).withName("read-table-source"))
        .filterNot((key, value) -> Objects.isNull(value))
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(maxAge))
        .reduce(
            (previous, latest) -> Bytes.wrap(new byte[]{}),
            Named.as("keep-only-latest-value"),
            Materialized.as("active-keys"))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()).withName("suppress-buffer"))
        .toStream(Named.as("to-stream"))
        .peek(
            (key, value) -> System.out.println("Removing key: " + new String(key.key().array()) + " at " + key.window().endTime()),
            Named.as("log-record-key-to-be-removed"))
        .selectKey((key, value) -> key.key(), Named.as("set-record-key"))
        .mapValues(value -> (Void) null, Named.as("set-tombstone"))
        .to(destinationTopic, Produced.with(Serdes.ByteBuffer(), Serdes.Void()).withName("write-tombstone-back"));
    return builder.build();
  }

  /**
   * Initial approach, based on https://kafka-tutorials.confluent.io/schedule-ktable-ttl/kstreams.html
   * @deprecated as it becomes expensive as the key cardinality grows and the scan frequency stops been enough.
   * @see #sessionBased()
   * @return key-value based topology.
   */
  @Deprecated
  Topology keyValueBased() {
    final var builder = new StreamsBuilder();
    builder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(sourceTopic),
        Serdes.ByteBuffer(),
        Serdes.Long()
    ));

    builder.stream(sourceTopic, Consumed.with(Serdes.ByteBuffer(), Serdes.Bytes()).withName("read-table-source"))
        .transform(() ->
                new TtlEmitter<ByteBuffer, Bytes, KeyValue<ByteBuffer, Void>>(
                    maxAge, scanFrequency.get(), sourceTopic
                ),
            Named.as("process-ttl-checks"), sourceTopic)
        .to(sourceTopic, Produced.with(Serdes.ByteBuffer(), Serdes.Void()).withName("write-tombstone-back"));
    return builder.build();
  }

  public static class TtlEmitter<K, V, R> implements Transformer<K, V, R> {

    private final Duration maxAge;
    private final Duration scanFrequency;
    private final String purgeStoreName;
    private ProcessorContext context;
    private KeyValueStore<K, Long> stateStore;


    /**
     * @param maxAge how old key messages can be. Older messages are candidates to be removed with tombstone.
     * @param scanFrequency how often to check key's age.
     * @param stateStoreName name for internal state store containing keys and timestamps.
     */
    public TtlEmitter(
        final Duration maxAge,
        final Duration scanFrequency,
        final String stateStoreName
    ) {
      this.maxAge = maxAge;
      this.scanFrequency = scanFrequency;
      this.purgeStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
      this.stateStore = context.getStateStore(purgeStoreName);
      // This is where the magic happens. This causes Streams to invoke the Punctuator
      // on an interval, using stream time. That is, time is only advanced by the record
      // timestamps
      // that Streams observes. This has several advantages over wall-clock time for this
      // application:
      //
      // It'll produce the exact same sequence of updates given the same sequence of data.
      // This seems nice, since the purpose is to modify the data stream itself, you want to
      // have a clear understanding of when stuff is going to get deleted. For example, if something
      // breaks down upstream for this topic, and it stops getting new data for a while, wall
      // clock time would just keep deleting data on schedule, whereas stream time will wait for
      // new updates to come in.
      //
      // You can change to wall clock time here if that is what is needed
      context.schedule(scanFrequency, PunctuationType.STREAM_TIME, timestamp -> {
        final long cutoff = timestamp - maxAge.toMillis();

        // scan over all the keys in this partition's store
        // this can be optimized, but just keeping it simple.
        // this might take a while, so the Streams timeouts should take this into account
        try (final KeyValueIterator<K, Long> all = stateStore.all()) {
          while (all.hasNext()) {
            final KeyValue<K, Long> record = all.next();
            if (record.value != null && record.value < cutoff) {
              // if a record's last update was older than our cutoff, emit a tombstone.
              context.headers().add("tombstone", "1".getBytes());
              context.forward(record.key, null);
            }
          }
        }
      });
    }

    @Override
    public R transform(K key, V value) {
      // this gets invoked for each new record we consume. If it's a tombstone, delete
      // it from our state store. Otherwise, store the record timestamp.
      if (value == null) {
        stateStore.delete(key);
      } else {
        stateStore.put(key, context.timestamp());
      }
      return null; // no need to return anything here. the punctuator will emit the tombstones
      // when necessary
    }

    @Override
    public void close() {
    }
  }
}
