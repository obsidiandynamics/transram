package com.obsidiandynamics.transram;

import java.util.*;
import java.util.function.*;

public interface TransContext <K, V extends DeepCloneable<V>> {
  enum State {
    OPEN, ROLLED_BACK, COMMITTED
  }

  Set<K> keys(Predicate<K> predicate) throws ConcurrentModeFailure;

  V read(K key) throws ConcurrentModeFailure;

  void insert(K key, V value) throws ConcurrentModeFailure;

  void update(K key, V value) throws ConcurrentModeFailure;

  void delete(K key) throws ConcurrentModeFailure;

  int size() throws ConcurrentModeFailure;

  void rollback();

  State getState();

  long getVersion();

  void commit() throws ConcurrentModeFailure;
}
