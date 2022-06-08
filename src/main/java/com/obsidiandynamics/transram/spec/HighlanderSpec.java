package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Enclose.Region.*;
import com.obsidiandynamics.transram.spec.HighlanderSpec.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;
import java.util.function.*;

public final class HighlanderSpec implements Spec<State, BiKey, Void> {
  public static class Options {
    public int numPrefixes;
    public int numSuffixes;
    public int scanPrefixes;

    void validate() {
      Assert.that(numPrefixes > 0);
      Assert.that(numSuffixes > 0);
      Assert.that(scanPrefixes > 0);
    }
  }

  private static final Options DEF_OPTIONS = new Options() {{
    numPrefixes = 100;
    numSuffixes = 10;
    scanPrefixes = 10;
  }};

  public static final double[][] DEF_PROFILES = {
      {0.1, 0.9},
      {0.5, 0.5},
      {0.9, 0.1}
  };

  static final class State {
    final TransMap<BiKey, Void> map;

    State(TransMap<BiKey, Void> map) {
      this.map = map;
    }
  }

  private final Options options;

  private final double[][] profiles;

  public HighlanderSpec() {
    this(DEF_OPTIONS, DEF_PROFILES);
  }

  public HighlanderSpec(Options options, double[][] profiles) {
    options.validate();
    this.options = options;
    this.profiles = profiles;
  }

  private enum Operation {
    SNAPSHOT_READ {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, Options options) {
        final var firstPrefix = (int) (rng.nextDouble() * options.numPrefixes);
        Enclose.over(state.map).onFailure(failures::increment).transact(ctx -> {
          for (var i = 0; i < options.scanPrefixes; i++) {
            final var prefix = (i + firstPrefix) % options.numPrefixes;
            final var keys = ctx.keys(wherePrefixIs(prefix));
            Assert.that(keys.size() <= 1, () -> String.format("Too many keys for prefix %d: %d", prefix, keys.size()));
          }
          return Action.ROLLBACK;
        });
      }
    },

    INS_DEL {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, Options options) {
        final var prefix = (int) (rng.nextDouble() * options.numPrefixes);
        Enclose.over(state.map).onFailure(failures::increment).transact(ctx -> {
          final var keys = ctx.keys(wherePrefixIs(prefix));
          Assert.that(keys.size() <= 1, () -> String.format("Too many keys for prefix %d: %d", prefix, keys.size()));

          if (keys.isEmpty()) {
            final var suffix = (int) (rng.nextDouble() * options.numSuffixes);
            ctx.insert(new BiKey(prefix, suffix), Void.instance());
          } else {
            final var key = keys.iterator().next();
            ctx.delete(key);
          }
          return Action.COMMIT;
        });
      }
    };

    abstract void operate(State state, Failures failures, SplittableRandom rng, Options options);
  }

  @Override
  public String[] getOperationNames() {
    return Arrays.stream(Operation.values()).map(Operation::name).toArray(String[]::new);
  }

  @Override
  public double[][] getProfiles() {
    return profiles;
  }

  @Override
  public State instantiate(TransMap<BiKey, Void> map) {
    return new State(map);
  }

  private static Predicate<BiKey> wherePrefixIs(int prefix) {
    return key -> key.getPrefix() == prefix;
  }

  @Override
  public void verify(State state) {
    Enclose.over(state.map).transact(ctx -> {
      var liveKeys = 0;
      for (var prefix = 0; prefix < options.numPrefixes; prefix++) {
        final var keys = ctx.keys(wherePrefixIs(prefix));
        liveKeys += keys.size();
        final var _prefix = prefix;
        Assert.that(keys.size() <= 1, () -> String.format("Too many keys for prefix %d: %d", _prefix, keys.size()));
        for (var key : keys) {
          final var value = ctx.read(key);
          Assert.that(value != null, () -> "Null value for key " + key);
        }
      }

      final var size = ctx.size();
      final var _liveKeys = liveKeys;
      Assert.that(size == liveKeys, () -> String.format("Expected %d entries, got %d", _liveKeys, size));

      return Action.ROLLBACK;
    });
  }

  @Override
  public void evaluate(int ordinal, State state, Failures failures, SplittableRandom rng) {
    Operation.values()[ordinal].operate(state, failures, rng, options);
  }
}
