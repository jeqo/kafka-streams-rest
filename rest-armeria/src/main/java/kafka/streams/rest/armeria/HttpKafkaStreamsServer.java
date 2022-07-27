package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import kafka.streams.rest.core.internal.DefaultKeyValueStateStoreService;
import kafka.streams.rest.core.internal.DefaultSessionStateStoreService;
import kafka.streams.rest.core.internal.DefaultWindowStateStoreService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

/**
 * HTTP Server for Kafka Streams application.
 */
public final class HttpKafkaStreamsServer {

  final DefaultApplicationService applicationService;
  final Map<String, Class<?>> kvStoreNames;
  final Map<String, Class<?>> windowStoreNames;
  final Map<String, Class<?>> sessionStoreNames;

  final ServerBuilder serverBuilder;

  final boolean prometheusMetricsEnabled;

  Server server;

  public HttpKafkaStreamsServer(
    final Topology topology,
    final Properties streamsConfig,
    final int port,
    final boolean prometheusMetricsEnabled,
    final Map<String, Class<?>> kvStoreNames,
    final Map<String, Class<?>> windowStoreNames,
    final Map<String, Class<?>> sessionStoreNames
  ) {
    this.kvStoreNames = kvStoreNames;
    this.windowStoreNames = windowStoreNames;
    this.sessionStoreNames = sessionStoreNames;
    this.applicationService = new DefaultApplicationService(topology, streamsConfig);
    this.prometheusMetricsEnabled = prometheusMetricsEnabled;
    this.serverBuilder =
      Server
        .builder()
        .http(port)
        .annotatedService("/application", new HttpApplicationStateService(applicationService))
        .serviceUnder("/", DocService.builder().build());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public void startServerOnly() {
    server.start();
  }

  public void startApplicationAndServer() {
    applicationService.start();

    kvStoreNames.forEach((store, keyClass) ->
      serverBuilder.annotatedService(
        "/stores/key-value/" + store,
        new HttpKeyValueStateStoreService<>(
          new DefaultKeyValueStateStoreService<>(applicationService::kafkaStreams, store),
          keyClass
        )
      )
    );
    windowStoreNames.forEach((store, keyClass) ->
      serverBuilder.annotatedService(
        "/stores/window/" + store,
        new HttpWindowStateStoreService<>(
          new DefaultWindowStateStoreService<>(applicationService::kafkaStreams, store),
          keyClass
        )
      )
    );
    sessionStoreNames.forEach((store, keyClass) ->
      serverBuilder.annotatedService(
        "/stores/session/" + store,
        new HttpSessionStateStoreService<>(
          new DefaultSessionStateStoreService<>(applicationService::kafkaStreams, store),
          keyClass
        )
      )
    );
    if (prometheusMetricsEnabled) {
      var prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      Metrics.addRegistry(prometheusRegistry);
      // JVM metrics
      new JvmInfoMetrics().bindTo(prometheusRegistry);
      new JvmThreadMetrics().bindTo(prometheusRegistry);
      new JvmGcMetrics().bindTo(prometheusRegistry);
      new JvmMemoryMetrics().bindTo(prometheusRegistry);
      new JvmHeapPressureMetrics().bindTo(prometheusRegistry);
      // Kafka Streams metrics
      new KafkaStreamsMetrics(applicationService.kafkaStreams()).bindTo(prometheusRegistry);

      serverBuilder.service(
        "/metrics",
        (ctx, req) -> HttpResponse.of(prometheusRegistry.scrape(TextFormat.CONTENT_TYPE_OPENMETRICS_100))
      );
    }
    this.server = serverBuilder.build();

    server.start();
  }

  public KafkaStreams kafkaStreams() {
    return applicationService.kafkaStreams();
  }

  public void close() {
    applicationService.stop();
    server.stop();
  }

  public static class Builder {

    int port = 8000;
    Map<String, Class<?>> keyValueStoreNames = new LinkedHashMap<>();
    Map<String, Class<?>> windowStoreNames = new LinkedHashMap<>();
    Map<String, Class<?>> sessionStoreNames = new LinkedHashMap<>();

    boolean prometheusMetricsEnabled = true;

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder addServiceForKeyValueStore(String storeName) {
      this.keyValueStoreNames.put(storeName, String.class);
      return this;
    }

    public Builder addServiceForKeyValueStore(String storeName, Class<?> keyClass) {
      this.keyValueStoreNames.put(storeName, keyClass);
      return this;
    }

    public Builder addServiceForWindowStore(String storeName) {
      this.windowStoreNames.put(storeName, String.class);
      return this;
    }

    public Builder addServiceForWindowStore(String storeName, Class<?> keyClass) {
      this.windowStoreNames.put(storeName, keyClass);
      return this;
    }

    public Builder addServiceForSessionStore(String storeName) {
      this.sessionStoreNames.put(storeName, String.class);
      return this;
    }

    public Builder addServiceForSessionStore(String storeName, Class<?> keyClass) {
      this.sessionStoreNames.put(storeName, keyClass);
      return this;
    }

    public Builder prometheusMetricsEnabled(boolean enable) {
      this.prometheusMetricsEnabled = enable;
      return this;
    }

    public HttpKafkaStreamsServer build(Topology topology, Properties configs) {
      return new HttpKafkaStreamsServer(
        topology,
        configs,
        port,
        prometheusMetricsEnabled,
        keyValueStoreNames,
        windowStoreNames,
        sessionStoreNames
      );
    }
  }
}
