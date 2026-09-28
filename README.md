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
make demo       # compose stack + migrations + 60 s load run, recorded into demo-out/
make k8s-e2e    # kind cluster, deploy, 180 s load, measured coverage of all rolling updates
```

`make demo` runs `scripts/compose-demo.sh`, which brings up Redpanda, two MySQL 8 shards, Redis 7,
and the three services, then runs the load generator once against them. The compose file caps the
heap of each service JVM at 160 MiB and of the load generator at 192 MiB, limits Redpanda to 384 MiB
and gives each MySQL shard a 32 MiB buffer pool; `JAVA_OPTS` replaces the three services' JVM
options, not the load generator's. With those caps the run below used 1541 MiB of memory across its
eight containers halfway through the load, inside a Docker VM of 6 CPU and 7.7 GiB: the line
introducing the block records the first figure, as `docker stats --no-stream` reported it, and the
summary's machine line the second. The run is: 300 simulated drivers per city across two cities
pinging their position every second, and ride requests at 10 per second for 60 seconds. The summary
opens with its own commit, clock window, machine and load average at either end of the load,
because every number below them moves with those.

<!-- demo-summary:start -->
`make demo` at commit ad50b78, 2026-09-28T00:41:36Z to 2026-09-28T00:43:58Z; host load average 7.19 before the run and 18.02 after (one minute averages on the machine that launched it); 1541 MiB across 8 containers in use halfway through the load, as `docker stats --no-stream` reported it:

```
== dispatchgrid load summary ==
run                 60 s at 10 rides/s, cities 1=austin, 2=seattle
commit              ad50b78
measured window     2026-09-28T00:42:56Z -> 2026-09-28T00:43:57Z
machine             Darwin arm64 10 CPU, Docker VM 6 CPU / 7.7 GiB; container linux/aarch64, 6 CPU, JDK 21.0.12.1
load average        4.40 at the start of the load, 16.26 at the end (kernel above)
drivers             600 (300 per city), pings ok=37633 errors=0 retries=0 skipped=0
rides submitted     610, http errors=0, retries=0, skipped=0, by shard {shard-0=305, shard-1=305}
in-flight bound     1200 pings, 100 rides; a send past the bound is skipped and counted, not queued, and is not an http error
decided (durable)   610 of 610 trip rows, 610 matched, 0 still requested
matching counters   matched=610 unmatched=0 retried=0 dropped=0 (in process, per pod, reset by a rollout)
matches per minute  610 over the 60 s run (matching-service trailing 60 s window: 600)
match latency       p50=17 ms  p95=467 ms  p99=689 ms
shard distribution  shard-0: city 2 -> 305 trips | shard-1: city 1 -> 305 trips
```
<!-- demo-summary:end -->

No part of the block is written by hand, and replacing it takes a run, not an edit. The generator
renders the text from stored fields alone: the counts of its own requests, the counters it reads
from the services, its configuration and its provenance. It prints the text, then those fields and
the text together on one `SUMMARY_JSON` line. The summary file it also writes stays inside its
container, which `--rm` removes, so `scripts/compose-demo.sh` saves that line as
`demo-out/loadgen-summary.json` and records the host-side facts the container cannot see in
`demo-out/demo-run.json`. `scripts/patch-demo-summary.py` rewrites the block from that pair: the
fenced text is the generator's summary exactly as it printed it, and the line above it comes from
the run record. The patcher refuses if the run did not complete or the generator exited nonzero, if
it ran on a modified tree or at a commit other than the one checked out, if the two artifacts name
different commits or machines, if the measured window does not lie inside the span the run record
gives, if any line of the text is not what the fields stored beside it render to, or if a field is
missing or still a placeholder:

```sh
make demo
make demo-down
python3 scripts/patch-demo-summary.py demo-out/loadgen-summary.json demo-out/demo-run.json
```

The outcome figures are measured, not configured: `matched`, `unmatched`, and the latency
percentiles come from `GET /matching/stats` on the matching service, and the durable decisions and
the shard distribution come from `GET /rides/stats`, which counts rows in each MySQL shard.

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
Any HTTP error fails the verdict, as does a skipped ride submission; skipped driver pings fail
it above one percent of the pings delivered and are reported either way, because that bound is
the generator refusing to queue a position write while a pod drains rather than a caller seeing
the update. Every submitted ride must have a durable
decision, at least 90% must match, and both shards must each contain exactly one city.
The runner and kind containers share the kernel's epoch clock; this timestamp comparison is
not a clock-synchronization guarantee for a remote multi-node cluster. The coverage claim is
sampled, not a claim about sub-second outages or zero transport retries; retries remain reported.

The block below is the output of the run named on its first line, the first hosted run to pass the
strengthened gate. Each of the three replacements is reported with its own interval and the
successful request rate measured across it. An earlier block quoted run 36201543211, whose verdict
was insufficient: its 96-second rollout outlasted its 60-second load run, so the last replacement
carried no load, and the checker did not verify overlap per service. Totals differ a little between
runs, and the latency line is the reservoir of whichever matching pod answered the last stats read.
That reservoir is in process and is reset by each replacement, so the pod that answers has usually
just taken over and its sample is weighted toward the rides that were waiting in the retry store
when it did: the same gate produced p95 798 ms in run 36282420129 and p95 40,030 ms here, with every
ride decided and matched in both. The retried counter beside it is how many rides took that path.

<!-- rollout-evidence:start -->
```
$ ./scripts/k8s-e2e.sh    # GitHub Actions ubuntu-latest (2 vCPU), run 36282821355, commit a0393fb, 2026-09-27
[00:36:12] rolling update: ROLLOUT_MARKER=rollout-1790469372 on rider-request-service driver-location-service matching-service (maxUnavailable=0, maxSurge=1)
deployment "rider-request-service" successfully rolled out
[00:36:43] rollout window: rider-request-service 1790469372038ms -> 1790469403731ms (UTC epoch)
deployment "driver-location-service" successfully rolled out
[00:37:21] rollout window: driver-location-service 1790469403747ms -> 1790469441633ms (UTC epoch)
deployment "matching-service" successfully rolled out
[00:38:06] rollout window: matching-service 1790469441648ms -> 1790469486111ms (UTC epoch)
[00:38:06] rolling update finished in 114s

== rolling update evidence ==
measured active load    2026-09-27T00:36:00.258+00:00 -> 2026-09-27T00:39:00.557+00:00
coverage policy         2s boundary margin; <=5s sampled progress gaps; >=90% target rate
rider-request-service: 2026-09-27T00:36:12.038+00:00 -> 2026-09-27T00:36:43.731+00:00; load-contained=True
  ridesSubmitted: 332 successes / 33.165s = 10.01/s (target 10/s)
  pingsOk: 20325 successes / 33.165s = 612.84/s (target 600/s)
driver-location-service: 2026-09-27T00:36:43.747+00:00 -> 2026-09-27T00:37:21.633+00:00; load-contained=True
  ridesSubmitted: 390 successes / 39.043s = 9.99/s (target 10/s)
  pingsOk: 23400 successes / 39.043s = 599.34/s (target 600/s)
matching-service: 2026-09-27T00:37:21.648+00:00 -> 2026-09-27T00:38:06.111+00:00; load-contained=True
  ridesSubmitted: 450 successes / 45.015s = 10.00/s (target 10/s)
  pingsOk: 27000 successes / 45.015s = 599.80/s (target 600/s)
ride requests           1804 submitted, 0 errors, 0 skipped
driver position pings   115200 ok, 0 errors, 0 skipped
transport retries       0 rides, 138 pings
matching counters       614 matched, 0 unmatched, 38 retried, 0 dropped (in process, per pod, reset by each replacement)
match latency           p50=36ms p95=40030ms p99=44736ms (reservoir of the pod that answered the last read; a ride with no free driver waits in the retry store, which is where the tail comes from)
durable decisions       1804/1804; matched=1804
trips by shard          {"shard-0": {"2": 902}, "shard-1": {"1": 902}}
COVERAGE: PASS (all three complete rollouts inside sustained sampled load)
RESULT: PASS (zero final request errors, no skipped ride, pings within the skip bound)
```
<!-- rollout-evidence:end -->

The strengthened gate prints `COVERAGE: PASS` only after measured coverage and load validity
checks pass, followed by `RESULT: PASS`. Missing telemetry, a too-short load run, a skipped ride,
pings skipped beyond the bound, or failed outcome checks instead produce `RESULT: FAIL` and a
nonzero exit. Set
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

`mvn -B verify` at commit c1ba2be ran 63 unit tests across the five modules under Surefire and 14
Testcontainers integration tests under Failsafe, all passing, and
`python3 -m unittest discover -s scripts -p 'test_*.py'` runs 32 cases over the two gates that guard
the measured figures in this file: 17 for the demo patcher and 15 for the rollout gate.

* Unit: shard routing determinism and overrides, haversine, grid cells, pickup ETA, radius expansion, matcher
  policy (nearest-first, expansion, cross-city isolation, claim contention with concurrent rides,
  surge stamped from the pickup cell), surge tracker (window decay, supply TTL, clamping, minimum
  demand), pricing endpoints, lifecycle (completion frees the claim only after the row moved,
  cancellation frees only that ride's driver), the retry path through the real topology under
  `TopologyTestDriver` with a mocked wall clock (a ride waits for a driver that arrives later, a
  ride gives up after the third attempt), stats window and percentiles, controllers (including
  the ETA only while MATCHED and the timeline order), load generator parsing, the provenance it
  records with a summary, and a summary text that the fields stored beside it render exactly.
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
scripts/compose-demo.sh   the compose demo, with the commit, machine, load average and memory recorded
scripts/k8s-e2e.sh        kind cluster, deploy, load, rolling update, assertions
scripts/patch-demo-summary.py  rewrites the demo block above from a run, or refuses
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the reasoning behind the design.
