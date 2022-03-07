package kafka.streams.tombstone;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;

import kafka.streams.rest.armeria.HttpKafkaStreamsServer;

/**
 * HTTP Server for Tombstone service.
 */
public class TombstoneServer {

  public static void main(String[] args) {
    var tombstone = new TombstoneServer();
    var props = tombstone.config();
    var config = TombstoneConfig.load(props);
    var topology = new TombstoneTopology(
        config.maxAge(),
        Optional.of(config.scanFrequency()),
        config.sourceTopic()
    );
    final var server = HttpKafkaStreamsServer.newBuilder()
        .port(8080)
        .addServiceForKeyValueStore(config.sourceTopic(), ByteBuffer.class)
        .build(topology.sessionBased(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    server.startApplicationAndServer();
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
}
