package kafka.streams.rest.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class StatelessApp {

  public static void main(String[] args) {
    var app = new StatelessApp();
    var kafkaStreams = new KafkaStreams(app.topology(), app.config());
    kafkaStreams.start();
    var server = new HttpKafkaStreamsServer(app.topology(), app.config());
    server.startServerAndApplication();
  }

  private Properties config() {
    var props = new Properties();
    try (final var inputStream = new FileInputStream("src/main/resources/streams.properties")) {
      props.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return props;
  }

  Topology topology() {
    var builder = new StreamsBuilder();
    builder.stream("input").to("output");
    return builder.build();
  }
}
