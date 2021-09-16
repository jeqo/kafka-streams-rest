package kafka.streams.tombstone;

import java.time.Duration;
import java.util.Properties;

public record TombstoneConfig(String sourceTopic, Duration scanFrequency, Duration maxAge) {
  public static TombstoneConfig load(Properties props) {
    return new TombstoneConfig(
        props.getProperty("tombstone.topic"),
        Duration.ofMillis(Long.parseLong(props.getProperty("tombstone.scan_frequency"))),
        Duration.ofMillis(Long.parseLong(props.getProperty("tombstone.max_age"))));
  }

}
