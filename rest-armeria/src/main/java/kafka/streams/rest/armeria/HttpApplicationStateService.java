package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;
import kafka.streams.rest.core.ApplicationStateService;

public final class HttpApplicationStateService {

  final ApplicationStateService service;

  public HttpApplicationStateService(ApplicationStateService service) {
    this.service = service;
  }

  @Get("/status")
  public HttpResponse status() {
    return HttpResponse.ofJson(service.state().asJson());
  }

  @Post("/start")
  public HttpResponse start() {
    if (service.state().isRunningOrRebalancing()) {
      return HttpResponse.ofJson(HttpStatus.CONFLICT,
          new ErrorResponse("Application is already running.").asJson());
    } else {
      service.start();
      return HttpResponse.of(HttpStatus.OK);
    }
  }

  @Post("/stop")
  public HttpResponse stop() {
    if (!service.state().isRunningOrRebalancing()) {
      return HttpResponse.ofJson(HttpStatus.CONFLICT,
          new ErrorResponse("Application is not running.").asJson());
    } else {
      service.stop();
      return HttpResponse.of(HttpStatus.OK);
    }
  }
}
