package io.dispatchgrid.rider;

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
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.TripStatus;
import io.dispatchgrid.common.shard.CityShardRouter;
import io.dispatchgrid.common.shard.TripRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class RideControllerTest {
  static final Instant NOW = Instant.parse("2026-03-01T12:00:00Z");

  TripRepository trips = mock(TripRepository.class);

  @SuppressWarnings("unchecked")
  KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);

  CityShardRouter router =
      new CityShardRouter(
          List.<DataSource>of(new SimpleDriverDataSource(), new SimpleDriverDataSource()));
  MockMvc mvc;

  @BeforeEach
  void setUp() {
    RideController controller =
        new RideController(trips, router, kafka, Clock.fixed(NOW, ZoneOffset.UTC));
    mvc = MockMvcBuilders.standaloneSetup(controller).build();
  }

  @Test
  void createsTripInCityShardAndPublishesKeyedByCity() throws Exception {
    mvc.perform(
            post("/rides")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {"riderId":"r1","cityId":1,"pickupLat":30.27,"pickupLng":-97.74,
                     "dropoffLat":30.28,"dropoffLng":-97.75}
                    """))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.cityId").value(1))
        .andExpect(jsonPath("$.shard").value(1))
        .andExpect(jsonPath("$.status").value("REQUESTED"))
        .andExpect(jsonPath("$.rideId").isNotEmpty());

    ArgumentCaptor<RideRequest> saved = ArgumentCaptor.forClass(RideRequest.class);
    verify(trips).insertRequested(saved.capture());
    assertThat(saved.getValue().requestedAt()).isEqualTo(NOW);
    assertThat(saved.getValue().cityId()).isEqualTo(1);
    verify(kafka).send(eq(Topics.RIDE_REQUESTS), eq("1"), eq(saved.getValue()));
  }

  @Test
  void rejectsInvalidBodyWithoutTouchingStores() throws Exception {
    mvc.perform(
            post("/rides")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"riderId\":\"\",\"cityId\":1,\"pickupLat\":95,\"pickupLng\":0}"))
        .andExpect(status().isBadRequest());
    verify(trips, never()).insertRequested(any());
    verify(kafka, never()).send(any(), any(), any());
  }

  @Test
  void readsFromTheCityShardWhenCityIsGiven() throws Exception {
    var trip =
        new TripRepository.Trip(
            "abc", "r1", 2, TripStatus.MATCHED, 1, 2, 3, 4, "d9", 40, 1000, NOW, NOW, 0);
    when(trips.find(2, "abc")).thenReturn(Optional.of(trip));
    mvc.perform(get("/rides/abc").param("city", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.driverId").value("d9"))
        .andExpect(jsonPath("$.shard").value(0));
    verify(trips, never()).findAnywhere(any());
  }

  @Test
  void returns404ForUnknownRide() throws Exception {
    when(trips.findAnywhere("nope")).thenReturn(Optional.empty());
    mvc.perform(get("/rides/nope")).andExpect(status().isNotFound());
  }
}
