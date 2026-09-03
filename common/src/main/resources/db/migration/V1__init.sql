CREATE TABLE trips (
  ride_id            CHAR(36)      NOT NULL,
  rider_id           VARCHAR(64)   NOT NULL,
  city_id            INT           NOT NULL,
  status             VARCHAR(16)   NOT NULL,
  pickup_lat         DOUBLE        NOT NULL,
  pickup_lng         DOUBLE        NOT NULL,
  dropoff_lat        DOUBLE        NOT NULL,
  dropoff_lng        DOUBLE        NOT NULL,
  driver_id          VARCHAR(64)   NULL,
  match_latency_ms   INT           NULL,
  search_radius_m    INT           NULL,
  requested_at       TIMESTAMP(3)  NOT NULL,
  matched_at         TIMESTAMP(3)  NULL,
  updated_at         TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (ride_id),
  KEY idx_trips_city_status_requested (city_id, status, requested_at),
  KEY idx_trips_driver (driver_id),
  KEY idx_trips_rider (rider_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE drivers (
  driver_id          VARCHAR(64)   NOT NULL,
  city_id            INT           NOT NULL,
  total_matches      INT           NOT NULL DEFAULT 0,
  last_matched_at    TIMESTAMP(3)  NULL,
  last_ride_id       CHAR(36)      NULL,
  PRIMARY KEY (driver_id),
  KEY idx_drivers_city (city_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ride_events (
  id                 BIGINT        NOT NULL AUTO_INCREMENT,
  ride_id            CHAR(36)      NOT NULL,
  city_id            INT           NOT NULL,
  event_type         VARCHAR(32)   NOT NULL,
  payload            JSON          NULL,
  created_at         TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_ride_events_ride (ride_id, created_at),
  KEY idx_ride_events_city_type (city_id, event_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
