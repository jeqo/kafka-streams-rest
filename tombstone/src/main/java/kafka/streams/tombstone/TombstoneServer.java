package kafka.streams.tombstone;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import kafka.streams.rest.armeria.HttpKafkaStreamsServer;

/**
 */
public class TombstoneServer {

  public static void main(String[] args) {
    var tombstone = new TombstoneServer();
    var props = tombstone.config();
    var config = TombstoneConfig.load(props);
    var topology = new TombstoneTopology(
        config.maxAge(),
        config.scanFrequency(),
        config.sourceTopic()
    );
    var server = new HttpKafkaStreamsServer(topology.get(), props, 8080, Map.of());
    server.startApplicationAndServer();
  }

  private Properties config() {
    var props = new Properties();
    try (final var inputStream = new FileInputStream("tombstone/src/main/resources/streams.properties")) {
      props.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return props;
  }
}
