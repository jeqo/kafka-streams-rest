package kafka.streams.rest.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public class StatefulKeyValueApp {

  public static void main(String[] args) {
    var app = new StatefulKeyValueApp();
//    var kafkaStreams = new KafkaStreams(app.topology(), app.config());
//    kafkaStreams.start();
    HttpKafkaStreamsServer.newBuilder()
            .addServiceForKeyValueStore("input-table")
            .port(8001)
            .build(app.topology(), app.config())
            .startApplicationAndServer();
  }

  private Properties config() {
    var props = new Properties();
    try (final var inputStream = new FileInputStream("rest-example/src/main/resources/streams.properties")) {
      props.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return props;
  }

  Topology topology() {
    var builder = new StreamsBuilder();
    builder.table("input",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentKeyValueStore("input-table")))
        .toStream()
        .to("output", Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
