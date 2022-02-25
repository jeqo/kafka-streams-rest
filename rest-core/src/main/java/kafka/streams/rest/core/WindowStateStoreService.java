package kafka.streams.rest.core;

import java.time.Instant;

public interface WindowStateStoreService<K> {

  StateStoreInfo info();

  /**
   * Check if the key is found on the state store.
   *
   * @return whether key is found or not.
   */
  WindowFoundResponse windowFound(K key, Instant timeFrom, Instant timeTo);

  WindowsFoundResponse windowsFound(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo);
}
