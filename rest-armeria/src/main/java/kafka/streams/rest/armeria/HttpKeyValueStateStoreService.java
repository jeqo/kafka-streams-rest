package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import kafka.streams.rest.core.KeyValueStateStoreService;

public class HttpKeyValueStateStoreService<K> {
  final KeyValueStateStoreService<K> service;
  final Class<K> keyClass;

  public static HttpKeyValueStateStoreService ofStringKey(KeyValueStateStoreService<String> service) {
    return new HttpKeyValueStateStoreService(service, String.class);
  }

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
    if (keyClass != String.class) return HttpResponse.ofJson(service.checkKey((K) key));
    else return HttpResponse.of(HttpStatus.CONFLICT, MediaType.ANY_TEXT_TYPE, "Key is not string");
  }
}
