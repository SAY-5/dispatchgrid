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
import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.model.TripEventType;
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
        new RideController(trips, router, kafka, 8.0, Clock.fixed(NOW, ZoneOffset.UTC));
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

  private static TripRepository.Trip trip(TripStatus status, String driverId) {
    return new TripRepository.Trip(
        "abc", "r1", 2, status, 1, 2, 3, 4, driverId, 40, 1000, 800, 1.0, NOW, NOW, null, null, 0);
  }

  @Test
  void readsFromTheCityShardWhenCityIsGiven() throws Exception {
    when(trips.find(2, "abc")).thenReturn(Optional.of(trip(TripStatus.MATCHED, "d9")));
    mvc.perform(get("/rides/abc").param("city", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.driverId").value("d9"))
        .andExpect(jsonPath("$.shard").value(0))
        .andExpect(jsonPath("$.pickupEtaSeconds").value(100));
    verify(trips, never()).findAnywhere(any());
  }

  @Test
  void etaIsOnlyGivenWhileADriverIsOnTheWay() throws Exception {
    when(trips.find(2, "abc")).thenReturn(Optional.of(trip(TripStatus.REQUESTED, null)));
    mvc.perform(get("/rides/abc").param("city", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("REQUESTED"))
        .andExpect(jsonPath("$.pickupEtaSeconds").value(org.hamcrest.Matchers.nullValue()));
  }

  @Test
  void timelineListsTheShardsEventsOldestFirst() throws Exception {
    when(trips.findAnywhere("abc")).thenReturn(Optional.of(trip(TripStatus.MATCHED, "d9")));
    when(trips.events(2, "abc"))
        .thenReturn(
            List.of(
                new TripRepository.Event(1, "ride.requested", null, NOW),
                new TripRepository.Event(2, "ride.retry", null, NOW.plusSeconds(5)),
                new TripRepository.Event(3, "ride.matched", null, NOW.plusSeconds(9))));
    mvc.perform(get("/rides/abc/timeline"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("MATCHED"))
        .andExpect(jsonPath("$.events.length()").value(3))
        .andExpect(jsonPath("$.events[1].type").value("ride.retry"))
        .andExpect(jsonPath("$.events[2].type").value("ride.matched"));
  }

  @Test
  void returns404ForUnknownRide() throws Exception {
    when(trips.findAnywhere("nope")).thenReturn(Optional.empty());
    mvc.perform(get("/rides/nope")).andExpect(status().isNotFound());
  }

  @Test
  void cancelMovesTheRowAndTellsTheMatcherWhichDriverToFree() throws Exception {
    var matched = trip(TripStatus.MATCHED, "d9");
    when(trips.findAnywhere("abc")).thenReturn(Optional.of(matched));
    when(trips.markCancelled("abc", 2, NOW)).thenReturn(Optional.of(matched));
    mvc.perform(post("/rides/abc/cancel"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("CANCELLED"))
        .andExpect(jsonPath("$.previousStatus").value("MATCHED"))
        .andExpect(jsonPath("$.driverId").value("d9"));
    verify(kafka)
        .send(
            eq(Topics.RIDE_LIFECYCLE),
            eq("2"),
            eq(new TripEvent("abc", 2, "d9", TripEventType.CANCELLED, NOW)));
  }

  @Test
  void cancelOfAFinishedRideIs409AndPublishesNothing() throws Exception {
    when(trips.find(2, "abc")).thenReturn(Optional.of(trip(TripStatus.COMPLETED, "d9")));
    when(trips.markCancelled("abc", 2, NOW)).thenReturn(Optional.empty());
    mvc.perform(post("/rides/abc/cancel").param("city", "2"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value("COMPLETED"));
    verify(kafka, never()).send(any(), any(), any());
  }
}
