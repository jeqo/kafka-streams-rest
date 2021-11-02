package kafka.streams.rest.core;

public interface KeyValueStateStoreService<K> {

  /**
   * @return Key Value store information
   */
  KeyValueStateStoreInfo info();

  /**
   * Check if the key is found on the state store.
   *
   * @return whether key is found or not.
   */
  KeyFoundResponse keyFound(K key);
}
