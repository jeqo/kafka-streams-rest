package kafka.streams.tombstone;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

record TombstoneConfig(String sourceTopic, Optional<Duration> scanFrequency, Duration maxAge) {
  static TombstoneConfig load(Properties props) {
    final var property = props.getProperty("tombstone.scan_frequency");
    return new TombstoneConfig(
        props.getProperty("tombstone.topic"),
        property == null || property.isBlank() ?
            Optional.empty() :
            Optional.of(Duration.ofMillis(Long.parseLong(property))),
        Duration.ofMillis(Long.parseLong(props.getProperty("tombstone.max_age"))));
  }
}
