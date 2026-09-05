package io.dispatchgrid.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.model.TripEventType;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.redis.NearbyDriver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class DriverControllerTest {
  static final Instant NOW = Instant.parse("2026-03-01T12:00:00Z");

  DriverIndex index = mock(DriverIndex.class);

  @SuppressWarnings("unchecked")
  KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);

  DriverConfig.Props props = new DriverConfig.Props(Duration.ofSeconds(15), 5);
  MockMvc mvc;

  @BeforeEach
  void setUp() {
    mvc =
        MockMvcBuilders.standaloneSetup(
                new DriverController(index, kafka, props, Clock.fixed(NOW, ZoneOffset.UTC)))
            .build();
  }

  @Test
  void storesPositionWithHeartbeatTtlAndPublishesEvent() throws Exception {
    mvc.perform(
            post("/drivers/d-1/position")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"cityId\":2,\"lat\":47.6,\"lng\":-122.3}"))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.status").value("AVAILABLE"))
        .andExpect(jsonPath("$.expiresInMs").value(15000));

    ArgumentCaptor<DriverPosition> pos = ArgumentCaptor.forClass(DriverPosition.class);
    verify(index).upsert(pos.capture(), eq(Duration.ofSeconds(15)));
    assertThat(pos.getValue().driverId()).isEqualTo("d-1");
    assertThat(pos.getValue().status()).isEqualTo(DriverStatus.AVAILABLE);
    assertThat(pos.getValue().reportedAt()).isEqualTo(NOW);
    verify(kafka).send(eq(Topics.DRIVER_POSITIONS), eq("2"), eq(pos.getValue()));
  }

  @Test
  void rejectsOutOfRangeCoordinates() throws Exception {
    mvc.perform(
            post("/drivers/d-1/position")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"cityId\":2,\"lat\":91,\"lng\":-122.3}"))
        .andExpect(status().isBadRequest());
    verify(index, never()).upsert(any(), any());
  }

  @Test
  void nearbyCapsLimitAndPassesRadius() throws Exception {
    when(index.nearby(2, 47.6, -122.3, 1500, 5))
        .thenReturn(List.of(new NearbyDriver("a", 12.5), new NearbyDriver("b", 40)));
    mvc.perform(
            get("/drivers/nearby")
                .param("city", "2")
                .param("lat", "47.6")
                .param("lng", "-122.3")
                .param("radius", "1500")
                .param("limit", "50"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].driverId").value("a"))
        .andExpect(jsonPath("$[1].distanceMeters").value(40.0));
  }

  @Test
  void tripCompletionIsPublishedOnTheLifecycleTopicKeyedByCity() throws Exception {
    mvc.perform(
            post("/drivers/d-1/trips/ride-9/complete")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"cityId\":2}"))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.status").value("COMPLETING"));
    verify(kafka)
        .send(
            Topics.RIDE_LIFECYCLE,
            "2",
            new TripEvent("ride-9", 2, "d-1", TripEventType.COMPLETED, NOW));
  }
}
