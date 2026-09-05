package io.dispatchgrid.matching.surge;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class PricingControllerTest {
  static final Instant NOW = Instant.parse("2026-03-01T12:00:00Z");

  final SurgeTracker surge =
      new SurgeTracker(
          new SurgeProperties(1000, Duration.ofSeconds(60), Duration.ofSeconds(15), 0.5, 3.0, 2),
          new SimpleMeterRegistry(),
          Clock.fixed(NOW, ZoneOffset.UTC));
  final MockMvc mvc = MockMvcBuilders.standaloneSetup(new PricingController(surge)).build();

  @Test
  void reportsCityGridHottestCellFirst() throws Exception {
    surge.recordDriver(
        new DriverPosition("d1", 1, 47.6062, -122.3321, DriverStatus.AVAILABLE, NOW));
    for (int i = 0; i < 3; i++) {
      surge.recordRequest(1, 47.6062, -122.3321);
    }
    surge.recordDriver(new DriverPosition("d2", 1, 47.70, -122.20, DriverStatus.AVAILABLE, NOW));
    mvc.perform(get("/pricing/1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.cityId").value(1))
        .andExpect(jsonPath("$.maxMultiplier").value(2.0))
        .andExpect(jsonPath("$.surgingCells").value(1))
        .andExpect(jsonPath("$.cells.length()").value(2))
        .andExpect(jsonPath("$.cells[0].demand").value(3))
        .andExpect(jsonPath("$.cells[0].supply").value(1))
        .andExpect(jsonPath("$.cells[0].multiplier").value(2.0))
        .andExpect(jsonPath("$.cells[1].multiplier").value(1.0));
  }

  @Test
  void quotesOnePointAndUnknownCityIsFlat() throws Exception {
    mvc.perform(get("/pricing/7/quote").param("lat", "1").param("lng", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.multiplier").value(1.0));
    mvc.perform(get("/pricing/7"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.maxMultiplier").value(1.0))
        .andExpect(jsonPath("$.cells").isEmpty());
  }
}
