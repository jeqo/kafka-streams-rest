package kafka.streams.rest.armeria;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.streams.rest.core.internal.DefaultApplicationService;
import kafka.streams.rest.core.internal.DefaultKeyValueStateStoreService;
import org.apache.kafka.streams.Topology;

/**
 * HTTP Server for Kafka Streams application.
 */
public final class HttpKafkaStreamsServer {

  final DefaultApplicationService applicationService;
  final Map<String, Class<?>> kvStoreNames;

  final ServerBuilder serverBuilder;

  final boolean prometheusMetricsEnabled;

  Server server;

  public HttpKafkaStreamsServer(final Topology topology,
      final Properties streamsConfig,
      final int port,
      final boolean prometheusMetricsEnabled,
      final Map<String, Class<?>> kvStoreNames) {
    this.kvStoreNames = kvStoreNames;
    this.applicationService = new DefaultApplicationService(topology, streamsConfig);
    this.prometheusMetricsEnabled = prometheusMetricsEnabled;
    this.serverBuilder = Server.builder()
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
            new HttpKeyValueStateStoreService<>(new DefaultKeyValueStateStoreService<>(
                applicationService::kafkaStreams,
                store), keyClass))
    );
    if (prometheusMetricsEnabled) {
      var prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      Metrics.addRegistry(prometheusRegistry);
      new KafkaStreamsMetrics(applicationService.kafkaStreams()).bindTo(prometheusRegistry);

      serverBuilder.service(
          "/metrics",
          (ctx, req) ->
              HttpResponse.of(prometheusRegistry.scrape(TextFormat.CONTENT_TYPE_OPENMETRICS_100)));
    }
    this.server = serverBuilder.build();

    server.start();
  }

  public void close() {
    applicationService.stop();
    server.stop();
  }

  public static class Builder {

    int port = 8000;
    Map<String, Class<?>> keyValueStoreNames = new LinkedHashMap<>();

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

    public Builder prometheusMetricsEnabled(boolean enable) {
      this.prometheusMetricsEnabled = enable;
      return this;
    }

    public HttpKafkaStreamsServer build(Topology topology, Properties configs) {
      return new HttpKafkaStreamsServer(topology, configs,
          port,
          prometheusMetricsEnabled,
          keyValueStoreNames);
    }
  }
}
