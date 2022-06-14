package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Transact.Region.*;
import com.obsidiandynamics.transram.spec.HospitalSpec.*;
import com.obsidiandynamics.transram.util.*;

import java.util.*;

public final class HospitalSpec implements Spec<State, BiKey, Doctor> {
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
      {0.0, 1.0},
      {0.1, 0.9},
      {0.5, 0.5},
      {0.9, 0.1}
  };

  static final class State {
    final TransMap<BiKey, Doctor> map;

    State(TransMap<BiKey, Doctor> map) {
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
        final var firstHospitalId = (int) (rng.nextDouble() * options.numHospitals);
        Transact.over(state.map).withFailureHandler(failures::increment).run(ctx -> {
          for (var i = 0; i < options.scanHospitals; i++) {
            final var hospitalId = (i + firstHospitalId) % options.numHospitals;
            final var keys = ctx.keys(BiKey.whereFirstIs(hospitalId));
            Assert.that(keys.size() == options.numDocsPerHospital, () -> String.format("Incorrect number of keys for hospital %d: %d", hospitalId, keys.size()));
          }
          return Action.ROLLBACK;
        });
      }
    },

    CHANGE_ROSTER {
      @Override
      void operate(State state, Failures failures, SplittableRandom rng, Options options) {
        final var hospitalId = (int) (rng.nextDouble() * options.numHospitals);
        Transact.over(state.map).withFailureHandler(failures::increment).run(ctx -> {
          // pick a random doctor and check if he's rostered
          final var doctorId1 = (int) (rng.nextDouble() * options.numDocsPerHospital);
          final var doctor1Key = new BiKey(hospitalId, doctorId1);
          final var doctor1 = ctx.read(doctor1Key);
          if (!doctor1.isRostered()) {
            doctor1.setRostered(true);
            return Action.COMMIT;
          }

          // continue with a 2nd doctor only if the 1st was rostered

          // pick another doctor id that differs from the doctor that was just checked
          var doctorId2 = doctorId1;
          do  {
            doctorId2 = (int) (rng.nextDouble() * options.numDocsPerHospital);
          } while (doctorId2 == doctorId1);

          // change the 2nd doctor's roster status
          final var doctor2Key = new BiKey(hospitalId, doctorId2);
          final var doctor2 = ctx.read(doctor2Key);
          doctor2.setRostered(!doctor2.isRostered());
          ctx.update(doctor2Key, doctor2);

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
  public State instantiate(TransMap<BiKey, Doctor> map) {
    final var state = new State(map);
    Transact.over(state.map).run(ctx -> {
      for (var hospitalId = 0; hospitalId < options.numHospitals; hospitalId++) {
        for (var doctorId = 0; doctorId < options.numDocsPerHospital; doctorId++) {
          ctx.insert(new BiKey(hospitalId, doctorId), new Doctor(true));
        }
      }
      return Action.COMMIT;
    });
    return state;
  }

  @Override
  public void verify(State state) {
    Transact.over(state.map).run(ctx -> {
      var liveKeys = 0;
      for (var hospitalId = 0; hospitalId < options.numHospitals; hospitalId++) {
        final var keys = ctx.keys(BiKey.whereFirstIs(hospitalId));
        liveKeys += keys.size();
        final var _hospitalId = hospitalId;
        Assert.that(keys.size() == options.numDocsPerHospital, () -> String.format("Incorrect number of keys for hospital %d: %d", _hospitalId, keys.size()));
        var numRostered = 0;
        for (var key : keys) {
          final var doctor = ctx.read(key);
          Assert.that(doctor != null, () -> "Null value for key " + key);
          if (doctor.isRostered()) {
            numRostered++;
          }
        }

        final var _numRostered = numRostered;
        Assert.that(_numRostered >= 1, () -> String.format("Too few rostered doctors for hospital %d: %d", _hospitalId, _numRostered));
      }

      final var size = ctx.size();
      final var _liveKeys = liveKeys;
      Assert.that(size == liveKeys, () -> String.format("Expected %d entries, got %d", _liveKeys, size));

      return Action.ROLLBACK;
    });
    //Diagnostics.dumpMap(state.map);
  }

  @Override
  public void evaluate(int ordinal, State state, Failures failures, SplittableRandom rng) {
    Operation.values()[ordinal].operate(state, failures, rng, options);
  }
}
