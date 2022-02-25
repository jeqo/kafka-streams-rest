package kafka.streams.rest.example;

import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class IntKeyStatefulApp {

  public static void main(String[] args) {
    var app = new IntKeyStatefulApp();
//    var kafkaStreams = new KafkaStreams(app.topology(), app.config());
//    kafkaStreams.start();
    HttpKafkaStreamsServer.newBuilder()
            .addServiceForKeyValueStore("int-input-table", Integer.class)
            .port(8002)
            .build(app.topology(), app.config())
            .startApplicationAndServer();
  }

  private Properties config() {
    var props = new Properties();
    try (final var inputStream = new FileInputStream("rest-example/src/main/resources/int-streams.properties")) {
      props.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return props;
  }

  Topology topology() {
    var builder = new StreamsBuilder();
    builder.table("input-int",
            Consumed.with(Serdes.Integer(), Serdes.String()),
            Materialized.as(Stores.persistentKeyValueStore("int-input-table")))
        .toStream()
        .to("output-int", Produced.with(Serdes.Integer(), Serdes.String()));
    return builder.build();
  }

  static class TestProducer {
    public static void main(String[] args) {
      var p = new TestProducer();
      var prod = new KafkaProducer<>(p.config(), new IntegerSerializer(), new StringSerializer());
      prod.send(new ProducerRecord<>("input-int", 1, "test"));
      prod.flush();
      prod.close();
    }

    private Properties config() {
      var props = new Properties();
      try (final var inputStream = new FileInputStream("rest-example/src/main/resources/int-streams.properties")) {
        props.load(inputStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return props;
    }
  }
}
