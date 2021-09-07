package kafka.streams.rest.example;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class StatelessApp {

  public static void main(String[] args) {
    var app = new StatelessApp();
    var kafkaStreams = new KafkaStreams(app.topology(), app.config());
    kafkaStreams.start();
  }

  private Properties config() {
    var props = new Properties();
    return props;
  }

  Topology topology() {
    var builder = new StreamsBuilder();
    return builder.build();
  }
}
