package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import kafka.streams.rest.core.KeyValueStateStoreService;

public class HttpKeyValueStateStoreService {

  final KeyValueStateStoreService<?, ?> service;

  public HttpKeyValueStateStoreService(KeyValueStateStoreService<?, ?> service) {
    this.service = service;
  }

  @Get("/{key}")
  public HttpResponse getValue(@Param("key") String name) {
    return HttpResponse.ofJson(service.get(name));
  }
}
