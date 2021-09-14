package kafka.streams.tombstone;

import java.time.Duration;
import java.util.Properties;

public record TombstoneConfig(String sourceTopic, Duration scanFrequency, Duration maxAge) {
  public static TombstoneConfig load(Properties properties) {
    // TODO
    return new TombstoneConfig("", Duration.ofMinutes(10), Duration.ofMinutes(20));
  }

}
