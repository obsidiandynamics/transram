package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.TransMap;
import com.obsidiandynamics.transram.Transact;
import com.obsidiandynamics.transram.Transact.Region.Action;
import com.obsidiandynamics.transram.spec.HospitalSpec.State;
import com.obsidiandynamics.transram.util.Assert;

import java.util.Arrays;
import java.util.SplittableRandom;

public final class HospitalSpec implements Spec<State, BiKey, Void> {
  public static class Options {
    public int numHospitals;
    public int numDocsPerHospital;
    public int scanHospitals;

    void validate() {
      Assert.that(numHospitals > 0);
      Assert.that(numDocsPerHospital > 0);
      Assert.that(scanHospitals > 0);
    }
  }

  private static final Options DEF_OPTIONS = new Options() {{
    numHospitals = 100;
    numDocsPerHospital = 10;
    scanHospitals = 10;
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

  public HospitalSpec() {
    this(DEF_OPTIONS, DEF_PROFILES);
  }

  public HospitalSpec(Options options, double[][] profiles) {
    options.validate();
    this.options = options;
    this.profiles = profiles;
  }

  private enum Operation {
    SNAPSHOT_READ {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, Options options) {
        final var firstPrefix = (int) (rng.nextDouble() * options.numHospitals);
        Transact.over(state.map).withFailureHandler(failures::increment).run(ctx -> {
          for (var i = 0; i < options.scanHospitals; i++) {
            final var prefix = (i + firstPrefix) % options.numHospitals;
            final var keys = ctx.keys(BiKey.whereFirstIs(prefix));
            Assert.that(keys.size() <= 1, () -> String.format("Too many keys for prefix %d: %d", prefix, keys.size()));
          }
          return Action.ROLLBACK;
        });
      }
    },

    INS_DEL {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, Options options) {
        final var prefix = (int) (rng.nextDouble() * options.numHospitals);
        Transact.over(state.map).withFailureHandler(failures::increment).run(ctx -> {
          final var keys = ctx.keys(BiKey.whereFirstIs(prefix));
          Assert.that(keys.size() <= 1, () -> String.format("Too many keys for prefix %d: %d", prefix, keys.size()));

          if (keys.isEmpty()) {
            final var suffix = (int) (rng.nextDouble() * options.numDocsPerHospital);
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

  @Override
  public void verify(State state) {
    Transact.over(state.map).run(ctx -> {
      var liveKeys = 0;
      for (var prefix = 0; prefix < options.numHospitals; prefix++) {
        final var keys = ctx.keys(BiKey.whereFirstIs(prefix));
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
