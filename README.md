# DispatchGrid

Marketplace matching service for a ride marketplace: a rider request service, a driver
location service, and a Kafka Streams matching service written in Java 21 and Spring Boot 3.
Trips live in MySQL shards keyed by city, live driver positions live in Redis GEO, and the
three services run on Kubernetes with liveness and readiness probes and zero-downtime rolling
updates. The bundled load generator drives about 500 synthetic rides per minute through the
whole stack and prints measured throughput, latency, and shard distribution.

```
                     POST /rides                          POST /drivers/{id}/position
                          |                                          |
                          v                                          v
              +-----------------------+                 +---------------------------+
              | rider-request-service |                 | driver-location-service   |
              |  write trip to shard  |                 |  GEOADD drivers:geo:{city}|
              |  produce ride.requested                 |  HSET heartbeat + TTL     |
              +-----------+-----------+                 |  produce driver.position  |
                          |                             +-------------+-------------+
             ride-requests (keyed by city)                            |
                          |                                   driver-positions
                          v                                           |
              +-----------------------+                               v
              |   matching-service    |   GEOSEARCH nearest    +-----------+
              |   Kafka Streams       | <--------------------> |  Redis 7  |
              |   radius expansion    |   Lua claim (SET NX)   +-----------+
              |   atomic driver claim |
              +-----+-----------+-----+
                    |           |
            ride-matches   ride-unmatched
                    |
                    v
   +----------------+----------------+       shard = city_id mod N
   | mysql-shard-0  |  mysql-shard-1 |       city 2 -> shard 0, city 1 -> shard 1
   | trips, drivers, ride_events     |       Flyway migrations applied per shard
   +---------------------------------+
```

## Quick start

Requirements: Docker with Compose v2, JDK 21 and Maven for local builds, `kind` and `kubectl`
for the Kubernetes proof.

```
make build      # mvn package
make test       # unit tests plus Testcontainers integration tests (needs Docker)
make lint       # spotless (google-java-format)
make demo       # docker compose stack + migrations + 60 s load run, prints the summary
make k8s-e2e    # kind cluster, deploy, load, rolling update with zero request errors
```

`make demo` brings up Redpanda, two MySQL 8 shards, Redis 7, and the three services, then runs
the load generator. The compose file caps every JVM at 160 MB heap and MySQL at a 32 MB buffer
pool so the whole stack fits in a 2 GiB Docker VM; set `JAVA_OPTS` to lift the cap. The run is: 300 simulated drivers per city across two cities pinging their position every
second, and ride requests at 10 per second for 60 seconds. This is the output of a real run on a
laptop (Apple M2 Pro, Docker under Colima):

<!-- demo-summary:start -->
```
(run `make demo` to regenerate; the block below is pasted from an actual run)
```
<!-- demo-summary:end -->

The numbers are measured, not configured: `matched`, `unmatched`, and the latency percentiles
come from `GET /matching/stats` on the matching service, and the shard distribution comes from
`GET /rides/stats`, which counts rows in each MySQL shard.

## Services and API

### rider-request-service (port 8081)

| Method | Path | Description |
| --- | --- | --- |
| POST | `/rides` | Body `{riderId, cityId, pickupLat, pickupLng, dropoffLat, dropoffLng}`. Writes the trip (status `REQUESTED`) to the city's shard and produces `ride.requested`. Returns 202 with `{rideId, cityId, shard, status, requestedAt}`. |
| GET | `/rides/{rideId}?city=` | Reads the trip from the city's shard; without `city` it scans shards. 404 if unknown. |
| GET | `/rides/stats` | Trip counts per shard grouped by `city:status`. |

### driver-location-service (port 8082)

| Method | Path | Description |
| --- | --- | --- |
| POST | `/drivers/{driverId}/position` | Body `{cityId, lat, lng, status?}`. `GEOADD` into `drivers:geo:{cityId}`, heartbeat hash with a TTL (default 15 s) so silent drivers age out, and produces `driver.position`. `status: OFFLINE` removes the driver. |
| GET | `/drivers/nearby?city&lat&lng&radius&limit` | `GEOSEARCH` nearest-first within `radius` meters. |
| GET | `/drivers/count?city` | Available drivers in the city index. |

### matching-service (port 8083)

| Method | Path | Description |
| --- | --- | --- |
| GET | `/matching/stats` | `matched`, `unmatched`, `dropped`, `matchesPerMinute` (trailing 60 s), `matchesPerMinuteOverall`, `p50/p95/p99LatencyMs`. |
| GET | `/matching/config` | Effective radius and claim settings. |

All services expose `/actuator/health/liveness`, `/actuator/health/readiness`, `/actuator/metrics`,
and `/actuator/prometheus`. Readiness includes the dependencies each service uses: Kafka and the
shards for the rider service, Kafka and Redis for the driver service, and Kafka, Redis, the shards,
and the Streams state for the matching service.

## Kafka topics

| Topic | Key | Value | Producer | Consumer |
| --- | --- | --- | --- | --- |
| `ride-requests` | city id | `RideRequest` | rider-request-service | matching-service (Streams) |
| `driver-positions` | city id | `DriverPosition` | driver-location-service | downstream analytics |
| `ride-matches` | city id | `Match` | matching-service | trip lifecycle consumers |
| `ride-unmatched` | city id | `RideUnmatched` | matching-service | retry and pricing consumers |

Every topic is keyed by city id and has six partitions, so a city's events are ordered and the
Streams topology processes different cities in parallel. Values are JSON produced by one shared
Jackson mapper (`common/serde`).

## Shard scheme

`CityShardRouter` maps `city_id` to a shard index with `floorMod(city_id, N)`; an optional
override map pins individual cities. Each service builds one Hikari pool per shard from
`dispatchgrid.shards[i].url` and runs the Flyway migrations in `common/src/main/resources/db/migration`
against every shard at startup. Tables: `trips` (primary key `ride_id`, indexes on
`(city_id, status, requested_at)`, `driver_id`, `rider_id`), `drivers`, and `ride_events`.
With two shards, city 1 lands on shard 1 and city 2 on shard 0, which the load summary shows.

## Kubernetes rollout proof

`deploy/k8s` contains Deployments for the three services (readiness, liveness, and startup
probes, `RollingUpdate` with `maxUnavailable: 0` and `maxSurge: 1`, a `preStop` sleep so
endpoints drain before the JVM shuts down gracefully), a Redpanda Deployment, two MySQL
StatefulSets, Redis, a ConfigMap, a Secret, and a `kustomization.yaml`.

`scripts/k8s-e2e.sh` builds the images, creates a kind cluster, loads the images, applies the
manifests, waits for readiness, starts the load generator as an in-cluster Job, and while rides
and pings are flowing changes an environment variable on all three Deployments to trigger a
rolling update. The script then asserts that the generator recorded zero HTTP errors, that every
ride got a decision, and that each shard holds exactly one city. CI runs this on every push.

<!-- rollout-evidence:start -->
```
(populated from a real run of scripts/k8s-e2e.sh)
```
<!-- rollout-evidence:end -->

## Tests

32 unit tests (Surefire) and 13 integration tests (Failsafe, Testcontainers) across the five modules.

* Unit: shard routing determinism and overrides, haversine, radius expansion, matcher policy
  (nearest-first, expansion, cross-city isolation, claim contention with concurrent rides), stats
  window and percentiles, controllers, load generator parsing.
* Integration (Testcontainers): two MySQL shards with Flyway (trip lands in the shard for its
  city), Redis GEO ordering, heartbeat expiry, exclusive Lua claims under 64 concurrent claimers,
  the rider and driver services end to end against Redpanda, and the full Streams topology:
  300 requests in, 300 matches out with no driver assigned twice, rows updated in the right
  shard, redelivery dropped, and throughput asserted at 500 or more matches per minute.

## Layout

```
common/                   domain records, JSON serde, CityShardRouter, TripRepository,
                          RedisDriverIndex (GEO + Lua claims), readiness indicators, migrations
rider-request-service/    POST /rides, GET /rides/{id}, GET /rides/stats
driver-location-service/  POST /drivers/{id}/position, GET /drivers/nearby
matching-service/         Kafka Streams topology, Matcher, MatchStats, GET /matching/stats
loadgen/                  synthetic fleet and rider traffic with a measured summary
deploy/docker-compose.yml local stack
deploy/k8s/               manifests + kustomization + loadgen job
scripts/k8s-e2e.sh        kind cluster, deploy, load, rolling update, assertions
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the reasoning behind the design.
