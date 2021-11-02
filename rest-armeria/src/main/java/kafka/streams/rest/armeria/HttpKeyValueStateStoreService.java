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
import org.apache.kafka.common.utils.Bytes;

public class HttpKeyValueStateStoreService<K> {

  final KeyValueStateStoreService<K> service;
  final Class<K> keyClass;

  public HttpKeyValueStateStoreService(KeyValueStateStoreService<K> service, Class<K> keyClass) {
    this.service = service;
    this.keyClass = keyClass;
  }

  @Get("/")
  public HttpResponse info() {
    return HttpResponse.ofJson(service.info());
  }

  @Get("/{key}") //TODO replace with context and obtain at runtime.
  public HttpResponse checkKey(@Param("key") String key) {
    if (keyClass == String.class) {
      return HttpResponse.ofJson(service.checkKey((K) key));
    } else if (keyClass == Integer.class) {
      final Integer intKey = Integer.parseInt(key);
      return HttpResponse.ofJson(service.checkKey((K) intKey));
    } else if (keyClass == Short.class) {
      final Short shortKey = Short.parseShort(key);
      return HttpResponse.ofJson(service.checkKey((K) shortKey));
    } else if (keyClass == Double.class) {
      final Double doubleKey = Double.parseDouble(key);
      return HttpResponse.ofJson(service.checkKey((K) doubleKey));
    } else if (keyClass == Float.class) {
      final Float floatKey = Float.parseFloat(key);
      return HttpResponse.ofJson(service.checkKey((K) floatKey));
    } else if (keyClass == Long.class) {
      final Long longKey = Long.parseLong(key);
      return HttpResponse.ofJson(service.checkKey((K) longKey));
    } else if (keyClass == UUID.class) {
      return HttpResponse.ofJson(service.checkKey((K) UUID.fromString(key)));
    } else if (keyClass == byte[].class) {
      return HttpResponse.ofJson(service.checkKey((K) key.getBytes(StandardCharsets.UTF_8)));
    } else if (keyClass == Bytes.class) {
      return HttpResponse.ofJson(
          service.checkKey((K) Bytes.wrap(key.getBytes(StandardCharsets.UTF_8))));
    } else if (keyClass == ByteBuffer.class) {
      return HttpResponse.ofJson(
          service.checkKey((K) ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8))));
    } else {
      return HttpResponse.of(HttpStatus.CONFLICT, MediaType.ANY_TEXT_TYPE,
          "Key %s (type: %s) is not supported".formatted(key, keyClass.getName()));
    }
  }
}
