package kafka.streams.rest.core;

import com.fasterxml.jackson.databind.JsonNode;

public interface ApplicationStateService {

  // Queries
  ApplicationState state();

  // Commands
  void start();
  void stop();
}
