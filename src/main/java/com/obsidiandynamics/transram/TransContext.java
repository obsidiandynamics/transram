package com.obsidiandynamics.transram;

public interface TransContext <K, V extends DeepCloneable<V>> extends AutoCloseable {
  enum State {
    OPEN, ROLLED_BACK, COMMITTED
  }

  V read(K key) throws ConcurrentModeFailure;

  void insert(K key, V value) throws ConcurrentModeFailure;

  void update(K key, V value) throws ConcurrentModeFailure;

  void delete(K key) throws ConcurrentModeFailure;

  int size() throws ConcurrentModeFailure;

  void rollback();

  State getState();

  long getVersion();

  @Override
  void close() throws ConcurrentModeFailure;
}
