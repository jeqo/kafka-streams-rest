package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import kafka.streams.rest.core.WindowStateStoreService;
import org.apache.kafka.common.utils.Bytes;

/**
 * Expose Kafka Streams Key Value state stores as HTTP service.
 */
final class HttpWindowStateStoreService<K> {
  final WindowStateStoreService<K> service;
  final Class<K> keyClass;

  public HttpWindowStateStoreService(WindowStateStoreService<K> service, Class<K> keyClass) {
    this.service = service;
    this.keyClass = keyClass;
  }

  @Get("/")
  public HttpResponse info() {
    return HttpResponse.ofJson(service.info().asJson());
  }


  @Get("/{keyFrom}/{keyTo}/at/{timeFrom}/{timeTo}")
  public HttpResponse checkKeys(@Param("keyFrom") String keyFrom, @Param("keyTo") String keyTo,
      @Param("timeFrom") String timeFrom, @Param("timeTo") String timeTo) {
    var instantFrom = LocalDateTime.parse(timeFrom, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC);
    var instantTo = LocalDateTime.parse(timeTo, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC);
    if (keyClass == String.class) {
      return HttpResponse.ofJson(service.windowsFound((K) keyFrom, (K) keyTo, instantFrom, instantTo).asJson());
    } else if (keyClass == Integer.class) {
      final Integer intKeyFrom = Integer.parseInt(keyFrom);
      final Integer intKeyTo = Integer.parseInt(keyTo);
      return HttpResponse.ofJson(service.windowsFound((K) intKeyFrom, (K) intKeyTo, instantFrom, instantTo).asJson());
    } else if (keyClass == Short.class) {
      final Short shortKeyFrom = Short.parseShort(keyFrom);
      final Short shortKeyTo = Short.parseShort(keyTo);
      return HttpResponse.ofJson(service.windowsFound((K) shortKeyFrom, (K) shortKeyTo, instantFrom, instantTo).asJson());
    } else if (keyClass == Double.class) {
      final Double doubleKeyFrom = Double.parseDouble(keyFrom);
      final Double doubleKeyTo = Double.parseDouble(keyTo);
      return HttpResponse.ofJson(service.windowsFound((K) doubleKeyFrom, (K) doubleKeyTo, instantFrom, instantTo).asJson());
    } else if (keyClass == Float.class) {
      final Float floatKeyFrom = Float.parseFloat(keyFrom);
      final Float floatKeyTo = Float.parseFloat(keyTo);
      return HttpResponse.ofJson(service.windowsFound((K) floatKeyFrom, (K) floatKeyTo, instantFrom, instantTo).asJson());
    } else if (keyClass == Long.class) {
      final Long longKeyFrom = Long.parseLong(keyFrom);
      final Long longKeyTo = Long.parseLong(keyTo);
      return HttpResponse.ofJson(service.windowsFound((K) longKeyFrom, (K) longKeyTo, instantFrom, instantTo).asJson());
    } else if (keyClass == UUID.class) {
      return HttpResponse.ofJson(service.windowsFound((K) UUID.fromString(keyFrom), (K) UUID.fromString(keyTo), instantFrom, instantTo).asJson());
    } else if (keyClass == byte[].class) {
      return HttpResponse.ofJson(
          service.windowsFound(
              (K) keyFrom.getBytes(StandardCharsets.UTF_8),
              (K) keyTo.getBytes(StandardCharsets.UTF_8),
              instantFrom, instantTo).asJson());
    } else if (keyClass == Bytes.class) {
      return HttpResponse.ofJson(
          service.windowsFound(
              (K) Bytes.wrap(keyFrom.getBytes(StandardCharsets.UTF_8)),
              (K) Bytes.wrap(keyTo.getBytes(StandardCharsets.UTF_8)),
              instantFrom, instantTo).asJson());
    } else if (keyClass == ByteBuffer.class) {
      return HttpResponse.ofJson(
          service.windowsFound(
              (K) ByteBuffer.wrap(keyFrom.getBytes(StandardCharsets.UTF_8)),
              (K) ByteBuffer.wrap(keyTo.getBytes(StandardCharsets.UTF_8)),
              instantFrom, instantTo).asJson());
    } else {
      return HttpResponse.of(HttpStatus.CONFLICT, MediaType.ANY_TEXT_TYPE,
          "Key %s (type: %s) is not supported".formatted(keyFrom, keyClass.getName()));
    }
  }

  @Get("/{key}/at/{timeFrom}/{timeTo}")
  public HttpResponse checkKey(@Param("key") String key,
      @Param("timeFrom") String timeFrom, @Param("timeTo") String timeTo) {
    var instantFrom = LocalDateTime.parse(timeFrom, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC);
    var instantTo = LocalDateTime.parse(timeTo, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC);
    if (keyClass == String.class) {
      return HttpResponse.ofJson(service.windowFound((K) key, instantFrom, instantTo).asJson());
    } else if (keyClass == Integer.class) {
      final Integer intKey = Integer.parseInt(key);
      return HttpResponse.ofJson(service.windowFound((K) intKey, instantFrom, instantTo).asJson());
    } else if (keyClass == Short.class) {
      final Short shortKey = Short.parseShort(key);
      return HttpResponse.ofJson(service.windowFound((K) shortKey, instantFrom, instantTo).asJson());
    } else if (keyClass == Double.class) {
      final Double doubleKey = Double.parseDouble(key);
      return HttpResponse.ofJson(service.windowFound((K) doubleKey, instantFrom, instantTo).asJson());
    } else if (keyClass == Float.class) {
      final Float floatKey = Float.parseFloat(key);
      return HttpResponse.ofJson(service.windowFound((K) floatKey, instantFrom, instantTo).asJson());
    } else if (keyClass == Long.class) {
      final Long longKey = Long.parseLong(key);
      return HttpResponse.ofJson(service.windowFound((K) longKey, instantFrom, instantTo).asJson());
    } else if (keyClass == UUID.class) {
      return HttpResponse.ofJson(service.windowFound((K) UUID.fromString(key), instantFrom, instantTo).asJson());
    } else if (keyClass == byte[].class) {
      return HttpResponse.ofJson(
          service.windowFound((K) key.getBytes(StandardCharsets.UTF_8), instantFrom, instantTo).asJson());
    } else if (keyClass == Bytes.class) {
      return HttpResponse.ofJson(
          service.windowFound((K) Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)), instantFrom, instantTo).asJson());
    } else if (keyClass == ByteBuffer.class) {
      return HttpResponse.ofJson(
          service.windowFound((K) ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), instantFrom, instantTo).asJson());
    } else {
      return HttpResponse.of(HttpStatus.CONFLICT, MediaType.ANY_TEXT_TYPE,
          "Key %s (type: %s) is not supported".formatted(key, keyClass.getName()));
    }
  }
}
