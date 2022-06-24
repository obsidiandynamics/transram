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

  private ContextFuture<Void> submit(ConcurrentVoidOperation op) {
    return submit(() -> {
      op.invoke();
      return null;
    });
  }

  private <T> ContextFuture<T> submit(ConcurrentOperation<T> op) {
    return new ContextFuture<>(CompletableFuture.supplyAsync(() -> {
      try {
        return op.invoke();
      } catch (ConcurrentModeFailure e) {
        throw new CompletionException(e);
      }
    }, executor));
  }

  static final class ContextFuture<T> {
    private final CompletableFuture<T> completable;

    ContextFuture(CompletableFuture<T> completable) { this.completable = completable; }

    T get() throws ConcurrentModeFailure {
      try {
        return completable.get();
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

    CompletableFuture<T> completable() {
      return completable;
    }
  }

  @Override
  public Set<K> keys(Predicate<K> predicate) throws ConcurrentModeFailure {
    return submit(() -> delegate.keys(predicate)).get();
  }

  @Override
  public V read(K key) throws ConcurrentModeFailure {
    return submit(() -> delegate.read(key)).get();
  }

  @Override
  public void insert(K key, V value) throws ConcurrentModeFailure {
    submit(() -> delegate.insert(key, value)).get();
  }

  @Override
  public void update(K key, V value) throws ConcurrentModeFailure {
    submit(() -> delegate.update(key, value)).get();
  }

  @Override
  public void delete(K key) throws ConcurrentModeFailure {
    submit(() -> delegate.delete(key)).get();
  }

  @Override
  public int size() throws ConcurrentModeFailure {
    return submit(delegate::size).get();
  }

  @Override
  public void commit() throws ConcurrentModeFailure {
    submit(delegate::commit).get();
  }

  public ContextFuture<Void> commitAsync() {
    return submit(delegate::commit);
  }

  @Override
  public void rollback() {
    try {
      submit(delegate::rollback).get();
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
