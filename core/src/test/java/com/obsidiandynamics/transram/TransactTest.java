package com.obsidiandynamics.transram;

import com.obsidiandynamics.transram.TransContext.*;
import com.obsidiandynamics.transram.Transact.*;
import com.obsidiandynamics.transram.Transact.Region.*;
import com.obsidiandynamics.transram.util.*;
import org.assertj.core.api.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.concurrent.atomic.*;
import java.util.function.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

final class TransactTest {
  private interface TestTransMap extends TransMap<Integer, Nil> {}

  private interface TestTransContext extends TransContext<Integer, Nil> {}

  private interface TestFailureHandler extends Consumer<ConcurrentModeFailure> {}

  @Test
  void testCommit_successOnFirstAttempt() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var failureHandler = Mockito.mock(TestFailureHandler.class);

    final var completed = Transact.over(map)
        .withFailureHandler(failureHandler)
        .run(ctx -> Action.COMMIT);

    assertThat(completed).isSameAs(context);
    verify(map, times(1)).transact();
    verify(context, times(1)).commit();
    verify(context, never()).rollback();
    verify(failureHandler, never()).accept(any());
  }

  @Test
  void testCommit_successOnSecondAttempt() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var failureHandler = Mockito.mock(TestFailureHandler.class);
    final var error = Mockito.mock(ConcurrentModeFailure.class);
    final var runs = new AtomicInteger();
    doAnswer(__ -> {
      if (runs.incrementAndGet() == 2) {
        return null;
      } else {
        throw error;
      }
    }).when(context).commit();

    final var completed = Transact.over(map)
        .withFailureHandler(failureHandler)
        .run(ctx -> Action.COMMIT);

    assertThat(runs.get()).isEqualTo(2);
    assertThat(completed).isSameAs(context);
    verify(map, times(2)).transact();
    verify(context, times(2)).commit();
    verify(context, never()).rollback();
    verify(failureHandler, times(1)).accept(eq(error));
  }

  @Test
  void testCommit_alreadyCommitted() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var completed = Transact.over(map)
        .run(ctx -> {
          ctx.commit();
          when(ctx.getState()).thenReturn(State.COMMITTED);
          return Action.COMMIT;
        });

    assertThat(completed).isSameAs(context);
    verify(map, times(1)).transact();
    verify(context, times(1)).commit();
    verify(context, never()).rollback();
  }

  @Test
  void testRollback_normal() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var failureHandler = Mockito.mock(TestFailureHandler.class);

    final var completed = Transact.over(map)
        .withFailureHandler(failureHandler)
        .run(ctx -> Action.ROLLBACK);

    assertThat(completed).isSameAs(context);
    verify(map, times(1)).transact();
    verify(context, never()).commit();
    verify(context, times(1)).rollback();
    verify(failureHandler, never()).accept(any());
  }

  @Test
  void testRollback_alreadyRolledBack() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var failureHandler = Mockito.mock(TestFailureHandler.class);

    final var completed = Transact.over(map)
        .withFailureHandler(failureHandler)
        .run(ctx -> {
          ctx.rollback();
          when(ctx.getState()).thenReturn(State.ROLLED_BACK);
          return Action.ROLLBACK;
        });

    assertThat(completed).isSameAs(context);
    verify(map, times(1)).transact();
    verify(context, never()).commit();
    verify(context, times(1)).rollback();
    verify(failureHandler, never()).accept(any());
  }

  @Test
  void testRollbackAndReset_normal() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var failureHandler = Mockito.mock(TestFailureHandler.class);

    final var runs = new AtomicInteger();
    final var completed = Transact.over(map)
        .withFailureHandler(failureHandler)
        .run(ctx -> {
          if (runs.incrementAndGet() == 2) {
            return Action.COMMIT;
          } else {
            return Action.ROLLBACK_AND_RESET;
          }
        });

    assertThat(completed).isSameAs(context);
    assertThat(runs.get()).isEqualTo(2);
    verify(map, times(2)).transact();
    verify(context, times(1)).commit();
    verify(context, times(1)).rollback();
    verify(failureHandler, never()).accept(any());
  }

  @Test
  void testRollbackAndReset_alreadyRolledBack() throws ConcurrentModeFailure {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    final var failureHandler = Mockito.mock(TestFailureHandler.class);

    final var runs = new AtomicInteger();
    final var completed = Transact.over(map)
        .withFailureHandler(failureHandler)
        .run(ctx -> {
          if (runs.incrementAndGet() == 2) {
            return Action.COMMIT;
          } else {
            ctx.rollback();
            when(ctx.getState()).thenReturn(State.ROLLED_BACK);
            return Action.ROLLBACK_AND_RESET;
          }
        });

    assertThat(completed).isSameAs(context);
    assertThat(runs.get()).isEqualTo(2);
    verify(map, times(2)).transact();
    verify(context, times(1)).commit();
    verify(context, times(1)).rollback();
    verify(failureHandler, never()).accept(any());
  }

  @Test
  void testInterrupted() {
    final var map = Mockito.mock(TestTransMap.class);
    final var context = Mockito.mock(TestTransContext.class);
    when(map.transact()).thenReturn(context);

    Thread.currentThread().interrupt();
    assertThat(catchRuntimeException(() -> {
      Transact.over(map)
          .run(ctx -> {
            throw new AntidependencyFailure("");
          });
    })).isInstanceOf(RuntimeInterruptedException.class);
  }
}