package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

import java.util.*;

public interface MapFactory {
  <K, V extends DeepCloneable<V>> TransMap<K, V> instantiate();
}
