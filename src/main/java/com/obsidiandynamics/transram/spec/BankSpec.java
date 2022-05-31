package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Enclose.Region.*;
import com.obsidiandynamics.transram.run.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;

public final class BankSpec implements Spec<BankSpec.BankState, Integer, Account> {
  public static class BankOptions {
    public int thinkTimeMs = -1;
    public int numAccounts;
    public int initialBalance;
    public int maxXferAmount;
    public int scanAccounts;
    public boolean log = false;

    void validate() {
      Assert.that(thinkTimeMs >= 0);
      Assert.that(numAccounts > 0);
      Assert.that(initialBalance > 0);
      Assert.that(maxXferAmount > 0);
      Assert.that(scanAccounts > 0);
    }
  }

  static final class BankState {
    final TransMap<Integer, Account> map;

    BankState(TransMap<Integer, Account> map) {
      this.map = map;
    }
  }

  private static final BankOptions DEF_OPTIONS = new BankOptions() {{
    thinkTimeMs = 0; numAccounts = 1_000; initialBalance = 100; maxXferAmount = 100; scanAccounts = 100;
  }};

  private static final double[][] DEF_PROFILES = {
      {0.1, 0.0, 0.6, 0.3},
      {0.5, 0.0, 0.3, 0.2},
      {0.9, 0.0, 0.06, 0.04}
  };

  final double[][] profiles;

  final BankOptions options;

  public BankSpec() {
    this(DEF_OPTIONS, DEF_PROFILES);
  }

  public BankSpec(BankOptions options, double[][] profiles) {
    this.options = options; this.profiles = profiles;
  }

  @Override
  public String[] getOperationNames() {
    return Arrays.stream(Opcode.values()).map(Opcode::name).toArray(String[]::new);
  }

  @Override
  public double[][] getProfiles() {
    return profiles;
  }

  @Override
  public BankState instantiateState(TransMap<Integer, Account> map) {
    // initialise bank accounts
    for (var i = 0; i < options.numAccounts; i++) {
      final var accountId = i; Enclose.over(map).transact(ctx -> {
        final var existingAccount = ctx.read(accountId);
        Assert.that(existingAccount == null, () -> String.format("Found existing account %d (%s)", accountId, existingAccount));
        ctx.write(accountId, new Account(accountId, options.initialBalance)); return Action.COMMIT;
      });
    } return new BankState(map);
  }

  private enum Opcode {
    SNAPSHOT_READ {
      @Override
      void operate(BankState state, Failures failures, SplittableRandom rng, BankOptions options) {
        final var firstAccountId = (int) (rng.nextDouble() * options.numAccounts);
        Enclose.over(state.map).onFailure(failures::increment).transact(ctx -> {
          for (var i = 0; i < options.scanAccounts; i++) {
            final var accountId = i + firstAccountId; ctx.read(accountId % options.numAccounts);
          } think(options.thinkTimeMs); return Action.ROLLBACK;
        });
      }
    },

    READ_ONLY {
      @Override
      void operate(BankState state, Failures failures, SplittableRandom rng, BankOptions options) {
        final var firstAccountId = (int) (rng.nextDouble() * options.numAccounts);
        Enclose.over(state.map).onFailure(failures::increment).transact(ctx -> {
          for (var i = 0; i < options.scanAccounts; i++) {
            final var accountId = i + firstAccountId; ctx.read(accountId % options.numAccounts);
          } think(options.thinkTimeMs); return Action.COMMIT;
        });
      }
    },

    XFER {
      @Override
      void operate(BankState state, Failures failures, SplittableRandom rng, BankOptions options) {
        Enclose.over(state.map).onFailure(failures::increment).transact(ctx -> {
          final var fromAccountId = (int) (rng.nextDouble() * options.numAccounts);
          final var toAccountId = (int) (rng.nextDouble() * options.numAccounts);
          final var amount = 1 + (int) (rng.nextDouble() * (options.maxXferAmount - 1));
          if (toAccountId == fromAccountId) {
            return Action.ROLLBACK_AND_RESET;
          } if (options.log)
            System.out.format("%s, fromAccountId=%d, toAccountId=%d, amount=%d\n", Thread.currentThread().getName(), fromAccountId, toAccountId, amount);
          final var fromAccount = ctx.read(fromAccountId); if (fromAccount == null) {
            return Action.ROLLBACK_AND_RESET;
          } final var toAccount = ctx.read(toAccountId); if (toAccount == null) {
            return Action.ROLLBACK_AND_RESET;
          }

          final var newFromBalance = fromAccount.getBalance() - amount; if (newFromBalance < 0) {
            return Action.ROLLBACK_AND_RESET;
          }

          fromAccount.setBalance(newFromBalance); toAccount.setBalance(toAccount.getBalance() + amount);

          ctx.write(fromAccountId, fromAccount); ctx.write(toAccountId, toAccount); think(options.thinkTimeMs);
          return Action.COMMIT;
        });
      }
    },

    SPLIT_MERGE {
      @Override
      void operate(BankState state, Failures failures, SplittableRandom rng, BankOptions options) {
        Enclose.over(state.map).onFailure(failures::increment).transact(ctx -> {
          final var accountAId = (int) (rng.nextDouble() * options.numAccounts);
          final var accountBId = (int) (rng.nextDouble() * options.numAccounts); if (accountAId == accountBId) {
            return Action.ROLLBACK_AND_RESET;
          } final var accountA = ctx.read(accountAId); final var accountB = ctx.read(accountBId);
          if (accountA == null && accountB == null) {
            return Action.ROLLBACK_AND_RESET;
          } else if (accountA == null) {
            if (accountB.getBalance() == 0) {
              return Action.ROLLBACK_AND_RESET;
            } final var xferAmount = 1 + (int) (rng.nextDouble() * (accountB.getBalance() - 1));
            final var newAccountA = new Account(accountAId, xferAmount); ctx.write(accountAId, newAccountA);
            accountB.setBalance(accountB.getBalance() - xferAmount); ctx.write(accountBId, accountB);
          } else if (accountB == null) {
            if (accountA.getBalance() == 0) {
              return Action.ROLLBACK_AND_RESET;
            } final var xferAmount = 1 + (int) (rng.nextDouble() * (accountA.getBalance() - 1));
            final var newAccountB = new Account(accountBId, xferAmount); ctx.write(accountBId, newAccountB);
            accountA.setBalance(accountA.getBalance() - xferAmount); ctx.write(accountAId, accountA);
          } else if (rng.nextDouble() > 0.5) {
            accountA.setBalance(accountA.getBalance() + accountB.getBalance()); ctx.write(accountAId, accountA);
            ctx.write(accountBId, null);
          } else {
            accountB.setBalance(accountA.getBalance() + accountB.getBalance()); ctx.write(accountBId, accountB);
            ctx.write(accountAId, null);
          } return Action.COMMIT;
        });
      }
    };

    abstract void operate(BankState state, Failures failures, SplittableRandom rng, BankOptions options);

    private static void think(long time) {
      if (time > 0) {
        try {
          Thread.sleep(time);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void evaluate(int ordinal, BankState state, Failures failures, SplittableRandom rng) {
    Opcode.values()[ordinal].operate(state, failures, rng, options);
  }

  @Override
  public void validateState(BankState state) {
    if (options.log) {
      dumpMap(state.map);
    }
    checkMapSum(state.map, options);
  }

  private static void dumpMap(TransMap<?, ?> map) {
    for (var entry : map.debug().dirtyView().entrySet()) {
      System.out.format("%10s:%s\n", entry.getKey(), entry.getValue());
    }
  }

  private static void checkMapSum(TransMap<?, Account> map, BankOptions options) {
    final var values = map.debug().dirtyView().values();
    final var sum = values.stream().filter(Versioned::hasValue).map(Versioned::getValue).mapToLong(Account::getBalance).sum();
    final var expectedSum = options.numAccounts * options.initialBalance;
    Assert.that(expectedSum == sum, () -> String.format("Expected: %d, actual: %d", expectedSum, sum));
    final var min = values.stream().filter(Versioned::hasValue).map(Versioned::getValue).mapToLong(Account::getBalance).min().orElseThrow();
    Assert.that(min >= 0, () -> String.format("Minimum balance is %d", min));
  }
}
