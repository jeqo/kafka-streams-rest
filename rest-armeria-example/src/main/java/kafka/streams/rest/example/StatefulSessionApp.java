package kafka.streams.rest.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;

public class StatefulSessionApp {

  public static void main(String[] args) {
    var app = new StatefulSessionApp();
//    var kafkaStreams = new KafkaStreams(app.topology(), app.config());
//    kafkaStreams.start();
    HttpKafkaStreamsServer.newBuilder()
            .addServiceForSessionStore("session-table")
            .port(8002)
            .build(app.topology(), app.config())
            .startApplicationAndServer();
  }

  private Properties config() {
    var props = new Properties();
    try (final var inputStream = new FileInputStream("streams.properties")) {
      props.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return props;
  }

  Topology topology() {
    var builder = new StreamsBuilder();
//    builder.table("input",
//            Consumed.with(Serdes.String(), Serdes.String()),
//            Materialized.as(Stores.persistentKeyValueStore("input-table")))
//        .toStream()
    builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1)))
        .reduce((value1, value2) -> value2, Materialized.as("session-table"))
        .toStream()
        .selectKey((key, value) -> key.key())
        .to("output", Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
