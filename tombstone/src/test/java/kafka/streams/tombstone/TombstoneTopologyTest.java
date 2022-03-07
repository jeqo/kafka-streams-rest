package kafka.streams.tombstone;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.junit.jupiter.api.Test;

class TombstoneTopologyTest {

  @Test void shouldEmitTombstone() {
    final var topology = new TombstoneTopology(
        Duration.ofMinutes(1),
        Optional.empty(),
        "input",
        "output"
    ).sessionBased();
    System.out.println(topology.describe());
    final var config = new Properties();
    final var testDriver = new TopologyTestDriver(topology, config, Instant.ofEpochMilli(0));
    final var keySerde = Serdes.String();
    final var valueSerde = Serdes.String();
    final var input = testDriver.createInputTopic(
        "input",
        keySerde.serializer(),
        valueSerde.serializer()
    );

    final var output = testDriver.createOutputTopic(
        "output",
        keySerde.deserializer(),
        valueSerde.deserializer()
    );

    input.pipeInput("k1", "v1", 10L);

    final var keyBufferStore = testDriver.<ByteBuffer, Bytes>getSessionStore("active-keys");

    try (final var i = keyBufferStore.fetch(ByteBuffer.wrap("k1".getBytes(StandardCharsets.UTF_8)))) {
      assertThat(i).hasNext();
      assertThat(i.next().value).isEqualTo(Bytes.wrap("v1".getBytes(StandardCharsets.UTF_8)));
    }

    final var suppressBufferStore = (InMemoryTimeOrderedKeyValueBuffer) testDriver.getStateStore("suppress-buffer-store");
    final var records = suppressBufferStore.numRecords();
    assertThat(records).isEqualTo(1);

    testDriver.advanceWallClockTime(Duration.ofMinutes(1));

    input.pipeInput("k2", "v1", 60_010);

    assertThat(output.isEmpty()).isEqualTo(false);
    assertThat(output.readKeyValue()).isEqualTo(KeyValue.pair("k1", null));

    testDriver.close();
  }
}
