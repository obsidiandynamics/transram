package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Transact.Region.*;
import com.obsidiandynamics.transram.spec.HighlanderSpec.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;

public final class HighlanderSpec implements Spec<State, BiKey, Nil> {
  public static class Options {
    public int numHighlanders;
    public int numClonesPerHighlander;
    public int scanPrefixes;

    void validate() {
      Assert.that(numHighlanders > 0);
      Assert.that(numClonesPerHighlander > 0);
      Assert.that(scanPrefixes > 0);
    }
  }

  private static final Options DEF_OPTIONS = new Options() {{
    numHighlanders = 100;
    numClonesPerHighlander = 10;
    scanPrefixes = 10;
  }};

  public static final double[][] DEF_PROFILES = {
      {0.1, 0.9},
      {0.5, 0.5},
      {0.9, 0.1}
  };

  static final class State {
    final TransMap<BiKey, Nil> map;

    State(TransMap<BiKey, Nil> map) {
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
        final var firstHighlanderId = (int) (rng.nextDouble() * options.numHighlanders);
        Transact.over(state.map).withFailureHandler(failures::increment).run(ctx -> {
          for (var i = 0; i < options.scanPrefixes; i++) {
            final var highlanderId = (i + firstHighlanderId) % options.numHighlanders;
            final var keys = ctx.keys(BiKey.whereFirstIs(highlanderId));
            Assert.that(keys.size() <= 1, () -> String.format("Too many keys for highlander %d: %d", highlanderId, keys.size()));
          }
          return Action.ROLLBACK;
        });
      }
    },

    INS_DEL {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, Options options) {
        final var highlanderId = (int) (rng.nextDouble() * options.numHighlanders);
        Transact.over(state.map).withFailureHandler(failures::increment).run(ctx -> {
          final var keys = ctx.keys(BiKey.whereFirstIs(highlanderId));
          Assert.that(keys.size() <= 1, () -> String.format("Too many keys for highlander %d: %d", highlanderId, keys.size()));

          if (keys.isEmpty()) {
            final var cloneId = (int) (rng.nextDouble() * options.numClonesPerHighlander);
            ctx.insert(new BiKey(highlanderId, cloneId), Nil.instance());
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
  public State instantiate(TransMap<BiKey, Nil> map) {
    return new State(map);
  }

  @Override
  public void verify(State state) {
    Transact.over(state.map).run(ctx -> {
      var liveKeys = 0;
      for (var highlanderId = 0; highlanderId < options.numHighlanders; highlanderId++) {
        final var keys = ctx.keys(BiKey.whereFirstIs(highlanderId));
        liveKeys += keys.size();
        final var _highlanderId = highlanderId;
        Assert.that(keys.size() <= 1, () -> String.format("Too many keys for highlander %d: %d", _highlanderId, keys.size()));
        for (var key : keys) {
          final var highlander = ctx.read(key);
          Assert.that(highlander != null, () -> "Null value for key " + key);
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
