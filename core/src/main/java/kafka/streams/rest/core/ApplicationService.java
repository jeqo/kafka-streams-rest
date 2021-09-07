package kafka.streams.rest.core;

public interface ApplicationService {

  // Queries
  ApplicationState state();

  // Commands
  void start();
  void stop();
}
