package com.obsidiandynamics.transram;

import java.util.*;

public interface Debug<K, V extends DeepCloneable<V>> {
  Map<K, Versioned<V>> dirtyView();

  int numRefs();
}
