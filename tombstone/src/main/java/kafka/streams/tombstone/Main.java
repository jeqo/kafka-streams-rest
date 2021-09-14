package kafka.streams.tombstone;

import java.util.Properties;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;

/**
 */
public class Main {

  public static void main(String[] args) {
    var props = new Properties();
    //TODO load props
    var config = TombstoneConfig.load(props);
    var topology = new TombstoneTopology(config.maxAge(), config.scanFrequency(), config.sourceTopic());
    var server = new HttpKafkaStreamsServer(topology.get(), props);
    server.startApplicationAndServer();
  }
}
