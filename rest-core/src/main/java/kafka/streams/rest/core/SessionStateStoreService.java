package kafka.streams.rest.core;

public interface SessionStateStoreService<K> {

  StateStoreInfo info();

  /**
   * Check if the key is found on the state store.
   *
   * @return whether key is found or not.
   */
  SessionFoundResponse sessionFound(K key);

  SessionsFoundResponse sessionsFound(K keyFrom, K keyTo);
}
