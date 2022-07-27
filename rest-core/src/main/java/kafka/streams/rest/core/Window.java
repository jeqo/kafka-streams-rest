package kafka.streams.rest.core;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

public record Window(
  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timezone = "UTC") Instant startTime,
  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timezone = "UTC") Instant endTime
) {
  public static Window from(org.apache.kafka.streams.kstream.Window window) {
    return new Window(window.startTime(), window.endTime());
  }
}
