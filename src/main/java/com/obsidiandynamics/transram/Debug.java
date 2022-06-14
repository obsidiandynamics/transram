package com.obsidiandynamics.transram;

import java.util.*;

public interface Debug<K, V extends DeepCloneable<V>> {
  Map<K, GenericVersioned<V>> dirtyView();

  int numRefs();

  long getVersion();
}
