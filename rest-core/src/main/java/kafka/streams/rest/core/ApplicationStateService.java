package kafka.streams.rest.core;

public interface ApplicationStateService {

  // Queries
  ApplicationState state();
  ApplicationConfiguration config();
  ApplicationTopology topology();

  // Commands
  void start();
  void stop();
  void restart();
}
