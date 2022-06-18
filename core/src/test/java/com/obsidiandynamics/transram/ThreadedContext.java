package com.obsidiandynamics.transram;

import org.assertj.core.internal.*;

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

  private ContextFuture<Void> executeVoid(ConcurrentVoidOperation op) {
    return execute(() -> {
      op.invoke();
      return null;
    });
  }

  private <T> ContextFuture<T> execute(ConcurrentOperation<T> op) {
    return new ContextFuture<>(executor.submit(op::invoke));
  }

  static class ContextFuture<T> {
    private final Future<T> future;

    ContextFuture(Future<T> future) { this.future = future; }

    T get() throws ConcurrentModeFailure {
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

    static void awaitAll(Collection<ContextFuture<?>> futures) {
      final var barrier = new CyclicBarrier(futures.size());
      for (var future : futures) {
        new Thread(() -> {
          try {
            future.get();
          } catch (ConcurrentModeFailure ignored) {}
          try {
            barrier.await();
          } catch (InterruptedException | BrokenBarrierException ignored) {}
        }).start();
      }
    }
  }

  @Override
  public Set<K> keys(Predicate<K> predicate) throws ConcurrentModeFailure {
    return execute(() -> delegate.keys(predicate)).get();
  }

  @Override
  public V read(K key) throws ConcurrentModeFailure {
    return execute(() -> delegate.read(key)).get();
  }

  @Override
  public void insert(K key, V value) throws ConcurrentModeFailure {
    executeVoid(() -> delegate.insert(key, value)).get();
  }

  @Override
  public void update(K key, V value) throws ConcurrentModeFailure {
    executeVoid(() -> delegate.update(key, value)).get();
  }

  @Override
  public void delete(K key) throws ConcurrentModeFailure {
    executeVoid(() -> delegate.delete(key)).get();
  }

  @Override
  public int size() throws ConcurrentModeFailure {
    return execute(delegate::size).get();
  }

  @Override
  public void commit() throws ConcurrentModeFailure {
    executeVoid(delegate::commit).get();
  }

  public ContextFuture<Void> commitFuture() {
    return executeVoid(delegate::commit);
  }

  @Override
  public void rollback() {
    try {
      executeVoid(delegate::rollback).get();
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
