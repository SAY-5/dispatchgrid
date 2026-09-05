package io.dispatchgrid.matching;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.model.TripEventType;
import io.dispatchgrid.common.shard.TripRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class LifecycleServiceTest {
  static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  InMemoryDriverIndex index = new InMemoryDriverIndex();
  TripRepository trips = mock(TripRepository.class);
  MatchStats stats = new MatchStats(new SimpleMeterRegistry(), Clock.systemUTC());
  LifecycleService service = new LifecycleService(trips, index, stats);

  private void claimed(String driver, String ride) {
    index.add(driver, 1, 47.6, -122.3);
    index.claim(1, driver, ride, Duration.ofMinutes(1));
  }

  @Test
  void completionFreesTheDriverOnceTheRowHasMoved() {
    claimed("d1", "r1");
    when(trips.markCompleted("r1", 1, "d1", T0)).thenReturn(true);
    service.handle(new TripEvent("r1", 1, "d1", TripEventType.COMPLETED, T0));
    assertThat(index.claimedBy(1, "d1")).isNull();
    assertThat(stats.snapshot()).containsEntry("completed", 1L);
  }

  @Test
  void completionByTheWrongDriverKeepsTheClaim() {
    claimed("d1", "r1");
    when(trips.markCompleted("r1", 1, "d2", T0)).thenReturn(false);
    service.handle(new TripEvent("r1", 1, "d2", TripEventType.COMPLETED, T0));
    assertThat(index.claimedBy(1, "d1")).isEqualTo("r1");
    assertThat(stats.snapshot())
        .containsEntry("completed", 0L)
        .containsEntry("lifecycleIgnored", 1L);
  }

  @Test
  void cancellationFreesOnlyTheCancelledRidesDriver() {
    claimed("d1", "r1");
    claimed("d2", "r2");
    service.handle(new TripEvent("r1", 1, "d1", TripEventType.CANCELLED, T0));
    service.handle(new TripEvent("r3", 1, null, TripEventType.CANCELLED, T0));
    assertThat(index.claimedBy(1, "d1")).isNull();
    assertThat(index.claimedBy(1, "d2")).isEqualTo("r2");
    assertThat(stats.snapshot()).containsEntry("cancelled", 2L);
  }
}
