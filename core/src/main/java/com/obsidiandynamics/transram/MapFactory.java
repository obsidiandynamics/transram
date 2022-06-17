package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.*;

public interface MapFactory {
  <K, V extends DeepCloneable<V>> TransMap<K, V> instantiate();
}
