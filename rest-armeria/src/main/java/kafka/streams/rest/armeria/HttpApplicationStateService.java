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

  @Get("/config")
  public HttpResponse config() {
    return HttpResponse.ofJson(service.config().asJson());
  }

  @Get("/topology")
  public HttpResponse topology() {
    return HttpResponse.of(service.topology().description());
  }

  @Post("/start")
  public HttpResponse start() {
    try {
      service.start();
      return HttpResponse.of(HttpStatus.OK);
    } catch (IllegalStateException e) {
      return HttpResponse.ofJson(HttpStatus.CONFLICT, new ErrorResponse(e.getMessage()).asJson());
    }
  }

  @Post("/stop")
  public HttpResponse stop() {
    try {
      service.stop();
      return HttpResponse.of(HttpStatus.OK);
    } catch (IllegalStateException e) {
      return HttpResponse.ofJson(HttpStatus.CONFLICT, new ErrorResponse(e.getMessage()).asJson());
    }
  }

  @Post("/restart")
  public HttpResponse restart() {
    try {
      service.restart();
      return HttpResponse.of(HttpStatus.OK);
    } catch (IllegalStateException e) {
      return HttpResponse.ofJson(HttpStatus.CONFLICT, new ErrorResponse(e.getMessage()).asJson());
    }
  }
}
