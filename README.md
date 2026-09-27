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
make k8s-e2e    # kind cluster, deploy, 180 s load, measured coverage of all rolling updates
```

`make demo` brings up Redpanda, two MySQL 8 shards, Redis 7, and the three services, then runs
the load generator. The compose file caps every JVM at 160 MB heap and MySQL at a 32 MB buffer
pool so the whole stack fits in a 2 GiB Docker VM; set `JAVA_OPTS` to lift the cap. The run is: 300 simulated drivers per city across two cities pinging their position every
second, and ride requests at 10 per second for 60 seconds. The output of the run captured at
commit ae5dba8 on 2026-09-03 looked like this; the summary printed today also carries retry,
skipped and durable decision fields, in the format the rollout evidence below shows:

<!-- demo-summary:start -->
```
== dispatchgrid load summary ==
drivers             600 (300 per city), pings ok=37200 errors=0
rides submitted     603, http errors=0, by shard {shard-0=301, shard-1=302}
matched             603
unmatched           0
matches per minute  603 over the 60 s run (matching-service trailing 60 s window: 603)
match latency       p50=14 ms  p95=53 ms  p99=271 ms
shard distribution  shard-0: city 2 -> 301 trips | shard-1: city 1 -> 302 trips
```
<!-- demo-summary:end -->

The block above is the output of `make demo` on a 6 CPU Colima VM: every number comes from live service responses (the load generator counts its own requests and reads `GET /matching/stats`), nothing is hardcoded.

The numbers are measured, not configured: `matched`, `unmatched`, and the latency percentiles
come from `GET /matching/stats` on the matching service, and the shard distribution comes from
`GET /rides/stats`, which counts rows in each MySQL shard.

## Services and API

### rider-request-service (port 8081)

| Method | Path | Description |
| --- | --- | --- |
| POST | `/rides` | Body `{riderId, cityId, pickupLat, pickupLng, dropoffLat, dropoffLng}`. Writes the trip (status `REQUESTED`) to the city's shard and produces `ride.requested`. Returns 202 with `{rideId, cityId, shard, status, requestedAt}`. |
| GET | `/rides/{rideId}?city=` | The trip row plus `pickupEtaSeconds` while a driver is on the way (straight-line `driverDistanceMeters` over `rider.pickup-speed-mps`, 8 m/s by default). Without `city` it scans shards. 404 if unknown. |
| GET | `/rides/{rideId}/timeline?city=` | Every `ride_events` row for the trip, oldest first: `ride.requested`, `ride.retry`, `ride.matched`, `ride.unmatched`, `ride.cancelled`, `ride.completed`, each with its payload. |
| POST | `/rides/{rideId}/cancel?city=` | Cancels a `REQUESTED` or `MATCHED` trip and produces `ride.cancelled` so the matcher frees the driver. 409 with the current status once the trip is final. |
| GET | `/rides/stats` | Trip counts per shard grouped by `city:status`. |

### driver-location-service (port 8082)

| Method | Path | Description |
| --- | --- | --- |
| POST | `/drivers/{driverId}/position` | Body `{cityId, lat, lng, status?}`. `GEOADD` into `drivers:geo:{cityId}`, heartbeat hash with a TTL (default 15 s) so silent drivers age out, and produces `driver.position`. `status: OFFLINE` removes the driver. |
| POST | `/drivers/{driverId}/trips/{rideId}/complete` | Body `{cityId}`. Produces `ride.completed`; the matcher moves the row to `COMPLETED` (only for the matched driver) and puts the driver straight back in the pool instead of waiting for the claim TTL. |
| GET | `/drivers/nearby?city&lat&lng&radius&limit` | `GEOSEARCH` nearest-first within `radius` meters. |
| GET | `/drivers/count?city` | Available drivers in the city index. |

### matching-service (port 8083)

| Method | Path | Description |
| --- | --- | --- |
| GET | `/matching/stats` | `matched`, `unmatched`, `dropped`, `retries`, `completed`, `cancelled`, `lifecycleIgnored`, `matchesPerMinute` (trailing 60 s), `matchesPerMinuteOverall`, `p50/p95/p99LatencyMs`. |
| GET | `/matching/config` | Effective radius, claim, and retry settings. |
| GET | `/pricing/{city}` | Surge grid for the city: `maxMultiplier`, `surgingCells`, and every cell with `demand`, `supply`, `ratio`, `multiplier`, hottest first. |
| GET | `/pricing/{city}/quote?lat&lng` | Multiplier for the cell containing a pickup point. |

All services expose `/actuator/health/liveness`, `/actuator/health/readiness`, `/actuator/metrics`,
and `/actuator/prometheus`. Readiness includes the dependencies each service uses: Kafka and the
shards for the rider service, Kafka and Redis for the driver service, and Kafka, Redis, the shards,
and the Streams state for the matching service.

## Kafka topics

| Topic | Key | Value | Producer | Consumer |
| --- | --- | --- | --- | --- |
| `ride-requests` | city id | `RideRequest` | rider-request-service | matching-service (Streams) |
| `driver-positions` | city id | `DriverPosition` | driver-location-service | matching-service (surge supply) |
| `ride-matches` | city id | `Match` | matching-service | trip lifecycle consumers |
| `ride-unmatched` | city id | `RideUnmatched` (with `attempts`) | matching-service | pricing and alerting consumers |
| `ride-lifecycle` | city id | `TripEvent` | rider-request-service (cancel), driver-location-service (complete) | matching-service (claim release) |

Every topic is keyed by city id and has six partitions, so a city's events are ordered and the
Streams topology processes different cities in parallel. Values are JSON produced by one shared
Jackson mapper (`common/serde`). The matcher also owns one changelogged state store,
`pending-retries`, where a ride that found no free driver waits for its next attempt
(`matching.retry`: 3 attempts, 5 s base backoff growing linearly, 1 s tick); a ride is only
published to `ride-unmatched` after the last attempt.

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
manifests, waits for readiness, and starts a 180-second in-cluster load Job (the Compose demo
still defaults to 60 seconds). It rolls the three Deployments sequentially, recording the UTC
epoch-millisecond interval from before each update until that Deployment is ready.
`scripts/verify-rollout-evidence.py` then checks those intervals against the generator's actual
active-load timestamps and one-second samples of successful rides and pings. CI runs this on
pull requests, main pushes, and manual dispatch; green deployment status alone is not proof.

Coverage requires all three complete rollout windows to be inside the measured load window,
with two seconds of margin at either end, no sample/progress gap over five seconds, and at least
90% of the configured successful request rate across each rollout (allowing scheduler jitter).
Any HTTP error or skipped send fails the verdict. Every submitted ride must have a durable
decision, at least 90% must match, and both shards must each contain exactly one city.
The runner and kind containers share the kernel's epoch clock; this timestamp comparison is
not a clock-synchronization guarantee for a remote multi-node cluster. The coverage claim is
sampled, not a claim about sub-second outages or zero transport retries; retries remain reported.

**Historical evidence, not complete rollout coverage:** the block below is retained verbatim
from run 36201543211, commit `20ce8c4`, on 2026-09-25. Its old `PASS` verdict was insufficient:
the 96-second rollout exceeded the 60-second load run, and the checker did not verify overlap
for each service. It must not be cited as proof that all three replacements occurred under
load. A passing hosted run of the strengthened gate is still required before that claim is
made. The latency line is the reservoir of whichever matching pod answered the stats read.

<!-- rollout-evidence:start -->
```
$ ./scripts/k8s-e2e.sh    # GitHub Actions ubuntu-latest (2 vCPU), run 36201543211, commit 20ce8c4, 2026-09-25
[23:40:47] rolling update: ROLLOUT_MARKER=rollout-1790379647 on rider-request-service driver-location-service matching-service (maxUnavailable=0, maxSurge=1)
deployment "rider-request-service" successfully rolled out
deployment "driver-location-service" successfully rolled out
deployment "matching-service" successfully rolled out
[23:42:23] rolling update finished in 96s

== rolling update evidence ==
rollout duration        96s, overlapping the 60s load run
ride requests           606 submitted, 0 http errors
driver position pings   37200 ok, 0 http errors
driver ping retries     0 (idempotent upsert retried once on transport failure)
ride retries            0 (retried once on transport failure; a stored first attempt would show as a trip row beyond submissions)
sends skipped           0 rides, 0 driver pings (in-flight bound reached; skipped and counted, not queued, not http errors)
rides decided           606 of 606 trip rows, 606 matched, 0 still requested
matching counters       303 matched / 0 unmatched (in process, per pod, reset by the rolling update)
matches per minute      303 (run), 303 (trailing window, answering pod only)
match latency           p50=19ms p95=123ms p99=273ms (answering pod reservoir)
trips by shard          {"shard-0": {"2": 303}, "shard-1": {"1": 303}}
RESULT: PASS (zero request errors across the rolling update)
```
<!-- rollout-evidence:end -->

The strengthened gate prints `COVERAGE: PASS` only after measured coverage and load validity
checks pass, followed by `RESULT: PASS`. Missing telemetry, a too-short load run, skipped sends,
or failed outcome checks instead produce `RESULT: FAIL` and a nonzero exit. Set
`DURATION_SECONDS` between 1 and 300 to tune a run; a longer configured duration does not bypass
the gate. The Job remains bounded by a 600-second deadline, including its 180-second settle
window. `ROLL_AFTER_SECONDS` defaults to 10 and must be below the load duration.

The `loadgen-summary` CI artifact includes `loadgen-summary.json` (timestamps and samples),
`rollout-windows.jsonl` (one completed interval per service), `rollout-evidence.txt` (verdict),
and `loadgen.log`. To recheck an artifact independently, run:

```sh
python3 scripts/verify-rollout-evidence.py loadgen-summary.json rollout-windows.jsonl
python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

## Tests

55 unit tests (Surefire) and 14 integration tests (Failsafe, Testcontainers) across the five modules.

* Unit: shard routing determinism and overrides, haversine, grid cells, pickup ETA, radius expansion, matcher
  policy (nearest-first, expansion, cross-city isolation, claim contention with concurrent rides,
  surge stamped from the pickup cell), surge tracker (window decay, supply TTL, clamping, minimum
  demand), pricing endpoints, lifecycle (completion frees the claim only after the row moved,
  cancellation frees only that ride's driver), the retry path through the real topology under
  `TopologyTestDriver` with a mocked wall clock (a ride waits for a driver that arrives later, a
  ride gives up after the third attempt), stats window and percentiles, controllers (including
  the ETA only while MATCHED and the timeline order), load generator parsing.
* Integration (Testcontainers): two MySQL shards with Flyway (trip lands in the shard for its
  city), Redis GEO ordering, heartbeat expiry, exclusive Lua claims under 64 concurrent claimers,
  the rider and driver services end to end against Redpanda, and the full Streams topology:
  300 requests in, 300 matches out with no driver assigned twice, rows updated in the right
  shard with a surge multiplier, the pricing grid populated, redelivery dropped, a completion and
  a cancellation returning their drivers to the index while an impostor's completion is ignored,
  a ride in an empty city matched on retry once a driver appears and another reported unmatched
  after three attempts, and throughput asserted at 500 or more matches per minute. The shard test also drives the trip
  state machine (complete only from MATCHED by the matched driver, cancel only while not final)
  and reads the timeline back with its payloads.

## Releases

| Version | Feature |
| --- | --- |
| [v1.0.0](https://github.com/SAY-5/dispatchgrid/releases/tag/v1.0.0) | baseline platform: rider request, driver location, and Kafka Streams matching with atomic Redis claims, page-growth-then-widen candidate search, city-keyed MySQL shards, and zero-downtime rolling updates on Kubernetes |
| [v2.0.0](https://github.com/SAY-5/dispatchgrid/releases/tag/v2.0.0) | per-city, per-cell surge pricing in the matching service from trailing-window demand and TTL-decayed supply, a clamped multiplier stamped on every match and served by `GET /pricing/{city}`, with two matching replicas under static membership |
| [v3.0.0](https://github.com/SAY-5/dispatchgrid/releases/tag/v3.0.0) | trip lifecycle over the `ride-lifecycle` topic: cancel and complete, the conditional row update only the matched driver may make, and the Redis claim released at once instead of at claim expiry |
| [v4.0.0](https://github.com/SAY-5/dispatchgrid/releases/tag/v4.0.0) | retries for a ride that finds no free driver: a changelogged `pending-retries` store with a wall-clock punctuator, a `ride.retry` row per pass, and `UNMATCHED` only after the last attempt |
| [v5.0.0](https://github.com/SAY-5/dispatchgrid/releases/tag/v5.0.0) | trip timeline and pickup ETA: `GET /rides/{rideId}/timeline` read from the shard that owns the trip, and `pickupEtaSeconds` from the driver distance the match now persists |
| [v5.1.0](https://github.com/SAY-5/dispatchgrid/releases/tag/v5.1.0) | load generator and rolling update proof hardening: the generator at a 1Gi ceiling with bounded in-flight sends that skip and count past the bound, one retry per ride on a transport failure, a failed generator job reported with its termination reason, ride decisions asserted from the durable trip rows rather than per-pod counters, the three Deployments replaced one at a time, and a measured coverage gate (`scripts/verify-rollout-evidence.py` over a 180 s load run and one recorded rollout window per Deployment) that a green rollout status alone no longer satisfies |

Each version is an annotated git tag with a GitHub release; the release notes carry the detail
behind a row.

## Layout

```
common/                   domain records, JSON serde, CityShardRouter, TripRepository,
                          RedisDriverIndex (GEO + Lua claims), readiness indicators, migrations
rider-request-service/    POST /rides, POST /rides/{id}/cancel, GET /rides/{id}, GET /rides/{id}/timeline, GET /rides/stats
driver-location-service/  POST /drivers/{id}/position, POST /drivers/{id}/trips/{ride}/complete, GET /drivers/nearby
matching-service/         Kafka Streams topology, Matcher, SurgeTracker, GET /matching/stats, GET /pricing
loadgen/                  synthetic fleet and rider traffic with a measured summary
deploy/docker-compose.yml local stack
deploy/k8s/               manifests + kustomization + loadgen job
scripts/k8s-e2e.sh        kind cluster, deploy, load, rolling update, assertions
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the reasoning behind the design.
