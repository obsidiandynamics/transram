package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.lock.*;
import com.obsidiandynamics.transram.util.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RunSs2plWorkload {
  private static final Ss2plMap.Options OPTIONS = new Ss2plMap.Options() {{
    lockStripes = 1024;
    lockFactory = UnfairUpgradeableLock::new;
    lockTimeoutMs = 0;
  }};

  private static final int NUM_ACCOUNTS = 10_000;

  private static final int INITIAL_BALANCE = 100;

  private static final int MAX_XFER_AMOUNT = 100;

  private static final int NUM_XFER_THREADS = 16;

  private static final int NUM_XFERS_PER_THREAD = 100_000;

  private static final boolean LOG = false;

  private static class State {
    final TransMap<Integer, Account> map = new Ss2plMap<Integer, Account>(OPTIONS);
    final AtomicLong cmFailures = new AtomicLong();
  }

  private enum Opcode {
    SCAN,
    XFER,
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println("SS2PL workload");

    final var map = new Ss2plMap<Integer, Account>(OPTIONS);

    // initialise bank accounts
    for (var i = 0; i < NUM_ACCOUNTS; i++) {
      final var accountId = i;
      Enclose.over(map).transact(ctx -> {
        ctx.write(accountId, new Account(accountId, INITIAL_BALANCE));
      });
    }

    final var latch = new CountDownLatch(NUM_XFER_THREADS);
    final var cmFailures = new AtomicLong();
    final var startTime = System.currentTimeMillis();
    for (var i = 0; i < NUM_XFER_THREADS; i++) {
      final var threadNo = i;
      new Thread(() -> {
        for (var j = 0; j < NUM_XFERS_PER_THREAD; j++) {
          while (true) {
            final var fromAccountId = (int) (Math.random() * NUM_ACCOUNTS);
            final var toAccountId = (int) (Math.random() * NUM_ACCOUNTS);
            final var amount = 1 + (int) (Math.random() * (MAX_XFER_AMOUNT - 1));
            if (toAccountId == fromAccountId) {
              continue;
            }

            try (var txn = map.transact()) {
              final var fromAccount = txn.read(fromAccountId);
              final var toAccount = txn.read(toAccountId);
              if (LOG) System.out.format("thread=%d, op=%d, fromAccountId=%d, toAccountId=%d, amount=%d\n", threadNo, j, fromAccountId, toAccountId, amount);

              assert fromAccount != null;
              final var newFromBalance = fromAccount.getBalance() - amount;
              if (newFromBalance < 0) {
                txn.rollback();
                continue;
              }

              fromAccount.setBalance(newFromBalance);
              assert toAccount != null;
              toAccount.setBalance(toAccount.getBalance() + amount);

              if (fromAccountId < toAccountId) {
                txn.write(fromAccountId, fromAccount);
                txn.write(toAccountId, toAccount);
              } else {
                txn.write(toAccountId, toAccount);
                txn.write(fromAccountId, fromAccount);
              }
              break;
            } catch (ConcurrentModeFailure e) {
              cmFailures.incrementAndGet();
              System.out.format("thread=%d, op=%d, fromAccountId=%d, toAccountId=%d, amount=%d: %s\n", threadNo, j, fromAccountId, toAccountId, amount, e.getMessage());
            }
          }
        }
        latch.countDown();
      }, "xfer_thread_" + i).start();
    }

    latch.await();
    final var took = System.currentTimeMillis() - startTime;
    if (LOG) dumpMap(map);
    checkMapSum(map);
    final var ops = NUM_XFER_THREADS * NUM_XFERS_PER_THREAD;
    System.out.format("Took %,d s, %,3.0f op/s, %,d CM failures\n", took / 1000, 1000f * ops / took, cmFailures.get());
  }

  private static void dumpMap(Ss2plMap<?, Account> map) {
    for (var entry : map.dirtyView().entrySet()) {
      System.out.format("%10s:%s\n", entry.getKey(), entry.getValue());
    }
  }

  private static void checkMapSum(Ss2plMap<?, Account> map) {
    final var sum = (Long) map.dirtyView().values().stream().map(Versioned::getValue).mapToLong(Account::getBalance).sum();
    final var expectedSum = NUM_ACCOUNTS * INITIAL_BALANCE;
    Assert.that(expectedSum == sum, () -> String.format("Expected: %d, actual: %d", expectedSum, sum));
  }
}
