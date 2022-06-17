package com.obsidiandynamics.transram;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

final class ThreadedContext<K, V extends DeepCloneable<V>> implements TransContext<K, V> {
  private final TransContext<K, V> delegate;

  private final ExecutorService executor;

  ThreadedContext(TransContext<K, V> delegate, ExecutorService executor) {
    this.delegate = delegate;
    this.executor = executor;
  }

  @FunctionalInterface
  private interface ConcurrentOperation<T> {
    T invoke() throws ConcurrentModeFailure;
  }

  @FunctionalInterface
  private interface ConcurrentVoidOperation {
    void invoke() throws ConcurrentModeFailure;
  }

  static final class RuntimeExecutionException extends RuntimeException {
    RuntimeExecutionException(Throwable cause) { super(cause); }
  }

  static final class RuntimeInterruptedException extends RuntimeException {
    RuntimeInterruptedException(String m) { super(m); }
  }

  private void executeVoid(ConcurrentVoidOperation op) throws ConcurrentModeFailure {
    execute(() -> {
      op.invoke();
      return null;
    });
  }

  private <T> T execute(ConcurrentOperation<T> op) throws ConcurrentModeFailure {
    final var future = executor.submit(op::invoke);
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw new RuntimeInterruptedException(e.getMessage());
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof ConcurrentModeFailure) {
        throw (ConcurrentModeFailure) cause;
      } else {
        throw new RuntimeExecutionException(e.getCause());
      }
    }
  }

  @Override
  public Set<K> keys(Predicate<K> predicate) throws ConcurrentModeFailure {
    return execute(() -> delegate.keys(predicate));
  }

  @Override
  public V read(K key) throws ConcurrentModeFailure {
    return execute(() -> delegate.read(key));
  }

  @Override
  public void insert(K key, V value) throws ConcurrentModeFailure {
    executeVoid(() -> delegate.insert(key, value));
  }

  @Override
  public void update(K key, V value) throws ConcurrentModeFailure {
    executeVoid(() -> delegate.update(key, value));
  }

  @Override
  public void delete(K key) throws ConcurrentModeFailure {
    executeVoid(() -> delegate.delete(key));
  }

  @Override
  public int size() throws ConcurrentModeFailure {
    return execute(delegate::size);
  }

  @Override
  public void commit() throws ConcurrentModeFailure {
    executeVoid(delegate::commit);
  }

  @Override
  public void rollback() {
    try {
      executeVoid(delegate::rollback);
    } catch (ConcurrentModeFailure e) {
      throw new RuntimeExecutionException(e);
    }
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public long getVersion() {
    return delegate.getVersion();
  }
}
