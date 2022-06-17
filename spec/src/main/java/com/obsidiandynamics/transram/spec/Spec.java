package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

import java.util.*;

public interface Spec<S, K, V extends DeepCloneable<V>> {
  String[] getOperationNames();

  double[][] getProfiles();

  S instantiate(TransMap<K, V> map);

  void evaluate(int ordinal, S state, Failures failures, SplittableRandom rng);

  void verify(S state);
}
