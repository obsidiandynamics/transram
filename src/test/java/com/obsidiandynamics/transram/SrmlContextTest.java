package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.SrmlMap.*;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

public final class SrmlContextTest extends AbstractContextTest {
  <K, V extends DeepCloneable<V>> TransMap<K, V> newMap() {
    return new SrmlMap<>(new Options());
  }

  @Nested
  class AntidependencyTests {
    @Test
    void testAntidependencyFailureOnRead() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0")); // snapshot read

      ctx1.commit();
      assertThat(catchThrowable(ctx2::commit)).isExactlyInstanceOf(AntidependencyFailure.class);
    }

    @Test
    void testAntidependencyFailureOnReadAndWrite() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      assertThat(ctx2.read(0)).isEqualTo(StringBox.of("zero_v0")); // snapshot read
      ctx2.update(0, StringBox.of("zero_v2"));

      ctx1.commit();
      assertThat(catchThrowable(ctx2::commit)).isExactlyInstanceOf(AntidependencyFailure.class);

      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v1"));
      }
    }

    @Test
    void testBlindWriteInCommitmentOrder() throws ConcurrentModeFailure {
      final var map = SrmlContextTest.this.<Integer, StringBox>newMap();
      {
        final var ctx = map.transact();
        ctx.insert(0, StringBox.of("zero_v0"));
        ctx.commit();
      }

      final var ctx1 = map.transact();
      ctx1.update(0, StringBox.of("zero_v1"));
      final var ctx2 = map.transact();
      ctx2.update(0, StringBox.of("zero_v2"));

      ctx1.commit();
      ctx2.commit();

      {
        final var ctx = map.transact();
        assertThat(ctx.read(0)).isEqualTo(StringBox.of("zero_v2"));
      }
    }
  }
}
