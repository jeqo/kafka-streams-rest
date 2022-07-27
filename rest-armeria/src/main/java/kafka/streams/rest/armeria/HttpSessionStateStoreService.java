package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import kafka.streams.rest.core.KeyValueStateStoreService;
import kafka.streams.rest.core.SessionStateStoreService;
import org.apache.kafka.common.utils.Bytes;

/**
 * Expose Kafka Streams Key Value state stores as HTTP service.
 */
final class HttpSessionStateStoreService<K> {

  final SessionStateStoreService<K> service;
  final Class<K> keyClass;

  public HttpSessionStateStoreService(SessionStateStoreService<K> service, Class<K> keyClass) {
    this.service = service;
    this.keyClass = keyClass;
  }

  @Get("/")
  public HttpResponse info() {
    return HttpResponse.ofJson(service.info().asJson());
  }

  @SuppressWarnings("unchecked")
  @Get("/{keyFrom}/{keyTo}")
  public HttpResponse checkKeys(@Param("keyFrom") String keyFrom, @Param("keyTo") String keyTo) {
    if (keyClass == String.class) {
      return HttpResponse.ofJson(service.sessionsFound((K) keyFrom, (K) keyTo).asJson());
    } else if (keyClass == Integer.class) {
      final Integer intKeyFrom = Integer.parseInt(keyFrom);
      final Integer intKeyTo = Integer.parseInt(keyTo);
      return HttpResponse.ofJson(service.sessionsFound((K) intKeyFrom, (K) intKeyTo).asJson());
    } else if (keyClass == Short.class) {
      final Short shortKeyFrom = Short.parseShort(keyFrom);
      final Short shortKeyTo = Short.parseShort(keyTo);
      return HttpResponse.ofJson(service.sessionsFound((K) shortKeyFrom, (K) shortKeyTo).asJson());
    } else if (keyClass == Double.class) {
      final Double doubleKeyFrom = Double.parseDouble(keyFrom);
      final Double doubleKeyTo = Double.parseDouble(keyTo);
      return HttpResponse.ofJson(service.sessionsFound((K) doubleKeyFrom, (K) doubleKeyTo).asJson());
    } else if (keyClass == Float.class) {
      final Float floatKeyFrom = Float.parseFloat(keyFrom);
      final Float floatKeyTo = Float.parseFloat(keyTo);
      return HttpResponse.ofJson(service.sessionsFound((K) floatKeyFrom, (K) floatKeyTo).asJson());
    } else if (keyClass == Long.class) {
      final Long longKeyFrom = Long.parseLong(keyFrom);
      final Long longKeyTo = Long.parseLong(keyTo);
      return HttpResponse.ofJson(service.sessionsFound((K) longKeyFrom, (K) longKeyTo).asJson());
    } else if (keyClass == UUID.class) {
      return HttpResponse.ofJson(
        service.sessionsFound((K) UUID.fromString(keyFrom), (K) UUID.fromString(keyTo)).asJson()
      );
    } else if (keyClass == byte[].class) {
      return HttpResponse.ofJson(
        service
          .sessionsFound((K) keyFrom.getBytes(StandardCharsets.UTF_8), (K) keyTo.getBytes(StandardCharsets.UTF_8))
          .asJson()
      );
    } else if (keyClass == Bytes.class) {
      return HttpResponse.ofJson(
        service
          .sessionsFound(
            (K) Bytes.wrap(keyFrom.getBytes(StandardCharsets.UTF_8)),
            (K) Bytes.wrap(keyTo.getBytes(StandardCharsets.UTF_8))
          )
          .asJson()
      );
    } else if (keyClass == ByteBuffer.class) {
      return HttpResponse.ofJson(
        service
          .sessionsFound(
            (K) ByteBuffer.wrap(keyFrom.getBytes(StandardCharsets.UTF_8)),
            (K) ByteBuffer.wrap(keyTo.getBytes(StandardCharsets.UTF_8))
          )
          .asJson()
      );
    } else {
      return HttpResponse.of(
        HttpStatus.CONFLICT,
        MediaType.ANY_TEXT_TYPE,
        "Key %s (type: %s) is not supported".formatted(keyFrom, keyClass.getName())
      );
    }
  }

  @SuppressWarnings("unchecked")
  @Get("/{key}")
  public HttpResponse checkKey(@Param("key") String key) {
    if (keyClass == String.class) {
      return HttpResponse.ofJson(service.sessionFound((K) key).asJson());
    } else if (keyClass == Integer.class) {
      final Integer intKey = Integer.parseInt(key);
      return HttpResponse.ofJson(service.sessionFound((K) intKey).asJson());
    } else if (keyClass == Short.class) {
      final Short shortKey = Short.parseShort(key);
      return HttpResponse.ofJson(service.sessionFound((K) shortKey).asJson());
    } else if (keyClass == Double.class) {
      final Double doubleKey = Double.parseDouble(key);
      return HttpResponse.ofJson(service.sessionFound((K) doubleKey).asJson());
    } else if (keyClass == Float.class) {
      final Float floatKey = Float.parseFloat(key);
      return HttpResponse.ofJson(service.sessionFound((K) floatKey).asJson());
    } else if (keyClass == Long.class) {
      final Long longKey = Long.parseLong(key);
      return HttpResponse.ofJson(service.sessionFound((K) longKey).asJson());
    } else if (keyClass == UUID.class) {
      return HttpResponse.ofJson(service.sessionFound((K) UUID.fromString(key)).asJson());
    } else if (keyClass == byte[].class) {
      return HttpResponse.ofJson(service.sessionFound((K) key.getBytes(StandardCharsets.UTF_8)).asJson());
    } else if (keyClass == Bytes.class) {
      return HttpResponse.ofJson(service.sessionFound((K) Bytes.wrap(key.getBytes(StandardCharsets.UTF_8))).asJson());
    } else if (keyClass == ByteBuffer.class) {
      return HttpResponse.ofJson(
        service.sessionFound((K) ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8))).asJson()
      );
    } else {
      return HttpResponse.of(
        HttpStatus.CONFLICT,
        MediaType.ANY_TEXT_TYPE,
        "Key %s (type: %s) is not supported".formatted(key, keyClass.getName())
      );
    }
  }
}
