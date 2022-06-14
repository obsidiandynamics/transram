package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.SrmlMap.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

public class SrmlContextTest {
  private static <K, V extends DeepCloneable<V>> TransMap<K, V> newMap() {
    return new SrmlMap<>(new Options());
  }

  @Test
  public void testEmptyMapAppearsEmpty() throws ConcurrentModeFailure {
    final var map = newMap();
    final var ctx = map.transact();
    assertThat(ctx.size()).isEqualTo(0);
    assertThat(ctx.keys(__ -> true)).isEmpty();
  }

  @Test
  public void testInsertThenUpdateThenDeleteOfNonExistent() throws ConcurrentModeFailure {
    final var map = SrmlContextTest.<Integer, StringBox>newMap();
    {
      final var ctx = map.transact();
      ctx.insert(0, StringBox.of("zero_v1"));
      final var valueAfterInsert = ctx.read(0);
      assertThat(valueAfterInsert).isEqualTo(StringBox.of("zero_v1"));
      assertThat(ctx.size()).isEqualTo(1);

      valueAfterInsert.setValue("zero_v2");
      ctx.update(0, valueAfterInsert);
      final var valueAfterUpdate = ctx.read(0);
      assertThat(valueAfterUpdate).isEqualTo(StringBox.of("zero_v2"));
      assertThat(ctx.size()).isEqualTo(1);

      ctx.delete(0);
      final var valueAfterDelete = ctx.read(0);
      assertThat(valueAfterDelete).isNull();
      assertThat(ctx.size()).isEqualTo(0);
      ctx.commit();
    }
    {
      final var ctx = map.transact();
      assertThat(ctx.read(0)).isNull();
      assertThat(ctx.size()).isEqualTo(0);
    }
  }

  @Test
  public void testDeleteThenInsertOfExisting() throws ConcurrentModeFailure {
    final var map = SrmlContextTest.<Integer, StringBox>newMap();
    {
      final var ctx = map.transact();
      ctx.insert(0, StringBox.of("zero_v1"));
      ctx.commit();
    }
    {
      final var ctx = map.transact();
      assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
      assertThat(ctx.size()).isEqualTo(1);

      ctx.delete(0);
      assertThat(ctx.read(0)).isNull();
      assertThat(ctx.size()).isEqualTo(0);

      ctx.insert(0, StringBox.of("zero_v3"));
      assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v3"));
      assertThat(ctx.size()).isEqualTo(1);
      ctx.commit();
    }
    {
      final var ctx = map.transact();
      assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v3"));
      assertThat(ctx.size()).isEqualTo(1);
    }
  }
}
