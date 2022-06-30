package com.obsidiandynamics.transram.mutex;

import java.util.concurrent.*;

class ThreadedMutex implements Mutex {
  protected final Mutex delegate;

  private final ExecutorService executor;

  ThreadedMutex(Mutex delegate, ExecutorService executor) {
    this.delegate = delegate;
    this.executor = executor;
  }

  @FunctionalInterface
  protected interface InterruptibleOperation {
    boolean invoke() throws InterruptedException;
  }

  @FunctionalInterface
  protected interface InterruptibleVoidOperation {
    void invoke() throws InterruptedException;
  }

  static final class RuntimeExecutionException extends RuntimeException {
    RuntimeExecutionException(Throwable cause) { super(cause); }
  }

  static final class RuntimeInterruptedException extends RuntimeException {
    RuntimeInterruptedException(String m) { super(m); }
  }

  static final class MutexFuture {
    private final CompletableFuture<Boolean> completable;

    MutexFuture(CompletableFuture<Boolean> completable) { this.completable = completable; }

    boolean get() throws InterruptedException {
      try {
        return completable.get();
      } catch (InterruptedException e) {
        throw new RuntimeInterruptedException(e.getMessage());
      } catch (ExecutionException e) {
        final var cause = e.getCause();
        if (cause instanceof InterruptedException) {
          throw (InterruptedException) cause;
        } else {
          throw new RuntimeExecutionException(e.getCause());
        }
      }
    }

    void getUninterruptibly() {
      try {
        get();
      } catch (InterruptedException e) {
        throw new RuntimeInterruptedException(e.getMessage());
      }
    }

    CompletableFuture<Boolean> completable() {
      return completable;
    }
  }

  protected MutexFuture submit(InterruptibleVoidOperation op) {
    return submit(() -> {
      op.invoke();
      return true;
    });
  }

  protected MutexFuture submit(InterruptibleOperation op) {
    return new MutexFuture(CompletableFuture.supplyAsync(() -> {
      try {
        return op.invoke();
      } catch (InterruptedException e) {
        throw new CompletionException(e);
      }
    }, executor));
  }

  @Override
  public boolean tryReadAcquire(long timeoutMs) throws InterruptedException {
    return tryReadAcquireAsync(timeoutMs).get();
  }

  public MutexFuture tryReadAcquireAsync(long timeoutMs) {
    return submit(() -> delegate.tryReadAcquire(timeoutMs));
  }

  @Override
  public void readRelease() {
    submit(delegate::readRelease).getUninterruptibly();
  }

  @Override
  public boolean tryWriteAcquire(long timeoutMs) throws InterruptedException {
    return tryWriteAcquireAsync(timeoutMs).get();
  }

  public MutexFuture tryWriteAcquireAsync(long timeoutMs) {
    return submit(() -> delegate.tryWriteAcquire(timeoutMs));
  }

  @Override
  public void writeRelease() {
    submit(delegate::writeRelease).getUninterruptibly();
  }

  @Override
  public void downgrade() {
    submit(delegate::downgrade).getUninterruptibly();
  }
}
