package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.lock.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RunSrmlWorkload {
  private static final SrmlMap.Options OPTIONS = new SrmlMap.Options() {{
    lockStripes = 1024;
    lockFactory = UnfairUpgradeableLock::new;
  }};

  private static final int NUM_ACCOUNTS = 10_000;

  private static final int INITIAL_BALANCE = 100;

  private static final int MAX_XFER_AMOUNT = 100;

  private static final int NUM_XFER_THREADS = 16;

  private static final int NUM_XFERS_PER_THREAD = 10_000;

  private static final boolean LOG = false;

  private static class State {
    final TransMap<Integer, Account> map = new SrmlMap<>(OPTIONS);
    final AtomicLong cmFailures = new AtomicLong();
  }

  private enum Opcode {
    READ_WRITE {
      @Override
      void operate(State state, SplittableRandom rnd) {
        Enclose.over(state.map)
            .onFailure(__ -> {
              state.cmFailures.incrementAndGet();
              //                System.out.format("thread=%d, op=%d, fromAccountId=%d, toAccountId=%d, amount=%d: %s\n", threadNo, opNo, fromAccountId, toAccountId, amount, e.getMessage());
            })
            .transact(ctx -> {
              final var fromAccountId = (int) (rnd.nextDouble() * NUM_ACCOUNTS);
              final var toAccountId = (int) (rnd.nextDouble() * NUM_ACCOUNTS);
              final var amount = 1 + (int) (rnd.nextDouble() * (MAX_XFER_AMOUNT - 1));
              if (toAccountId == fromAccountId) {
                ctx.rollback();
                return;
              }
//              if (LOG) System.out.format("thread=%d, op=%d, fromAccountId=%d, toAccountId=%d, amount=%d\n", threadNo, opNo, fromAccountId, toAccountId, amount);
              final var fromAccount = ctx.read(fromAccountId);
              final var toAccount = ctx.read(toAccountId);

              Assert.that(fromAccount != null, () -> String.format("Account (from) %d is null", fromAccountId));
              final var newFromBalance = fromAccount.getBalance() - amount;
              if (newFromBalance < 0) {
                ctx.rollback();
                return;
              }

              fromAccount.setBalance(newFromBalance);
              Assert.that(toAccount != null, () -> String.format("Account (to) %d is null", toAccountId));
              toAccount.setBalance(toAccount.getBalance() + amount);

              ctx.write(fromAccountId, fromAccount);
              ctx.write(toAccountId, toAccount);
            });
      }
    };

    abstract void operate(State state, SplittableRandom rnd);
  }

  private static double[] PROFILE = {1.0};

  public static void main(String[] args) throws InterruptedException {
    System.out.println("SRML workload");

    final var state = new State();

    // initialise bank accounts
    for (var i = 0; i < NUM_ACCOUNTS; i++) {
      final var accountId = i;
      Enclose.over(state.map).transact(ctx -> {
        ctx.write(accountId, new Account(accountId, INITIAL_BALANCE));
      });
    }

    final var workload = new Workload(PROFILE);

    final var latch = new CountDownLatch(NUM_XFER_THREADS);
    final var startTime = System.currentTimeMillis();
    for (var i = 0; i < NUM_XFER_THREADS; i++) {
      final var threadNo = i;
      new Thread(() -> {
        final var rnd = new SplittableRandom();
        for (var j = 0; j < NUM_XFERS_PER_THREAD; j++) {
          final var opNo = j;
          Enclose.over(state.map)
              .onFailure(__ -> {
                state.cmFailures.incrementAndGet();
//                System.out.format("thread=%d, op=%d, fromAccountId=%d, toAccountId=%d, amount=%d: %s\n", threadNo, opNo, fromAccountId, toAccountId, amount, e.getMessage());
              })
              .transact(ctx -> {
                final var fromAccountId = (int) (rnd.nextDouble() * NUM_ACCOUNTS);
                final var toAccountId = (int) (rnd.nextDouble() * NUM_ACCOUNTS);
                final var amount = 1 + (int) (rnd.nextDouble() * (MAX_XFER_AMOUNT - 1));
                if (toAccountId == fromAccountId) {
                  ctx.rollback();
                  return;
                }
                if (LOG) System.out.format("thread=%d, op=%d, fromAccountId=%d, toAccountId=%d, amount=%d\n", threadNo, opNo, fromAccountId, toAccountId, amount);
                final var fromAccount = ctx.read(fromAccountId);
                final var toAccount = ctx.read(toAccountId);

                Assert.that(fromAccount != null, () -> String.format("Account (from) %d is null", fromAccountId));
                final var newFromBalance = fromAccount.getBalance() - amount;
                if (newFromBalance < 0) {
                  ctx.rollback();
                  return;
                }

                fromAccount.setBalance(newFromBalance);
                Assert.that(toAccount != null, () -> String.format("Account (to) %d is null", toAccountId));
                toAccount.setBalance(toAccount.getBalance() + amount);

                ctx.write(fromAccountId, fromAccount);
                ctx.write(toAccountId, toAccount);
              });
        }
        latch.countDown();
      }, "xfer_thread_" + i).start();
    }

    latch.await();
    final var took = System.currentTimeMillis() - startTime;
    if (LOG) dumpMap(state.map);
    checkMapSum(state.map);
    final var ops = NUM_XFER_THREADS * NUM_XFERS_PER_THREAD;
    System.out.format("Took %,d s, %,3.0f op/s, %,d CM failures\n", took / 1000, 1000f * ops / took, state.cmFailures.get());
  }

  private static void dumpMap(TransMap<?, Account> map) {
    for (var entry : map.dirtyView().entrySet()) {
      System.out.format("%10s:%s\n", entry.getKey(), entry.getValue());
    }
  }

  private static void checkMapSum(TransMap<?, Account> map) {
    final var sum = (Long) map.dirtyView().values().stream().map(Versioned::getValue).mapToLong(Account::getBalance).sum();
    final var expectedSum = NUM_ACCOUNTS * INITIAL_BALANCE;
    Assert.that(expectedSum == sum, () -> String.format("Expected: %d, actual: %d", expectedSum, sum));
  }
}
