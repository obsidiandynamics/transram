package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.Transact.Region.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

final class SrmlMapTest {
  @Test
  void testFactory() {
    final var map = SrmlMap.factory(new Options()).instantiate();
    assertThat(map).isNotNull();
  }

  @Test
  void testDebug() {
    final var map = new SrmlMap<Integer, StringBox>(new Options());
    assertThat(map.debug().dirtyView()).isEmpty();
    assertThat(map.debug().numRefs()).isEqualTo(1);
    assertThat(map.debug().getVersion()).isEqualTo(0);

    Transact.over(map).run(ctx -> {
      ctx.insert(0, StringBox.of("zero_v0"));
      return Action.COMMIT;
    });
    assertThat(map.debug().dirtyView()).containsExactlyInAnyOrderEntriesOf(Map.of(0, new GenericVersioned<>(1, StringBox.of("zero_v0"))));
    // insertion also updates the size object, creating an extra revision
    assertThat(map.debug().numRefs()).isEqualTo(3);
    assertThat(map.debug().getVersion()).isEqualTo(1);

    Transact.over(map).run(ctx -> {
      ctx.update(0, StringBox.of("zero_v1"));
      return Action.COMMIT;
    });
    assertThat(map.debug().dirtyView()).containsExactlyInAnyOrderEntriesOf(Map.of(0, new GenericVersioned<>(2, StringBox.of("zero_v1"))));
    assertThat(map.debug().numRefs()).isEqualTo(4);
    assertThat(map.debug().getVersion()).isEqualTo(2);


    Transact.over(map).run(ctx -> {
      ctx.insert(1, StringBox.of("one_v0"));
      return Action.COMMIT;
    });
    assertThat(map.debug().dirtyView()).containsExactlyInAnyOrderEntriesOf(Map.of(0, new GenericVersioned<>(2, StringBox.of("zero_v1")),
                                                                                  1, new GenericVersioned<>(3, StringBox.of("one_v0"))));
    assertThat(map.debug().numRefs()).isEqualTo(6);
    assertThat(map.debug().getVersion()).isEqualTo(3);
  }
}