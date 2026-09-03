# Architecture notes

## Why Kafka Streams for matching

A ride request is an event with a natural key (the city). Putting the matcher behind a topic
instead of a synchronous call from the rider service gives three things:

* Backpressure and replay. If the matcher is slow or restarting, requests queue in the log and are
  processed in order when it returns. The rider service stays fast because its critical path is
  one shard insert and one produce.
* Ordering per city. With the key set to `city_id`, all requests for a city go through one stream
  task, so the matcher sees them in arrival order while different cities are processed in
  parallel across partitions and threads (`num.stream.threads`).
* Branching as topology. The result is split into `ride-matches` and `ride-unmatched` with
  `split().branch(...)`; downstream consumers (notifications, pricing, retry) subscribe to the
  outcome they care about without the matcher knowing about them.

The topology is deliberately stateless (`flatMapValues`) because the state that matters, which
driver is free, is shared across instances and has to live in Redis anyway. Processing is
at-least-once; the trip row's `status = REQUESTED` guard makes the match write idempotent, and a
redelivered request whose claim succeeded is released and counted as `dropped`.

## City-keyed sharding and its tradeoffs

Trips are written to `shard = floorMod(city_id, N)`. Cities are a good shard key for this domain
because almost every query is scoped to a city: matching, dispatcher dashboards, and rider
history all filter by city first. Every service resolves the shard with the same pure function
(`CityShardRouter`), so there is no lookup service in the write path and no coordination between
instances.

Costs that come with it:

* Hot cities. A single very large city cannot be split across shards with a pure modulus. The
  router accepts an override map so an operator can pin a city to a dedicated shard; a real
  deployment would move to range or hash sharding of `(city_id, ride_id)` for the largest
  markets.
* Resharding. Changing `N` moves most cities. Overrides can stage a migration one city at a
  time (dual write, backfill, flip the override), which is simpler than rebalancing a hash ring
  because the unit of movement is a whole city.
* Cross-city reads. `GET /rides/{id}` without a city fans out to all shards. Callers that know
  the city pass it; the ride id could also carry the city in a prefix.

Flyway runs on every shard at startup. The migration set is the same for all shards, which keeps
the schema identical and makes adding a shard a config change.

## Redis GEO and atomic claims

Driver positions arrive about once a second per driver. Keeping them in MySQL would make the
hottest write path in the system a relational update; Redis GEO gives `GEOADD` and `GEOSEARCH`
in microseconds with distance ordering built in.

Per city there is one sorted set `drivers:geo:{city}` and per driver a hash
`driver:{city}:{id}` with `status`, `lat`, `lng`, and `reported_at` under a TTL. The hash is the
heartbeat: when it expires the driver is stale. Because members of a GEO set do not expire on
their own, the claim script removes a stale member the first time it is considered.

Claiming is the only place where two rides can conflict. The claim is one Lua script:

```
if EXISTS heartbeat == 0 then ZREM geo driver; DEL claim; return -1   (stale)
if SET claim rideId NX PX ttl then ZREM geo driver; return 1          (claimed)
return 0                                                              (taken)
```

`SET NX` guarantees a single winner even with many matcher threads and instances; removing the
driver from the searchable set in the same script means later searches do not keep returning a
busy driver. The claim TTL doubles as the simulated trip length in the demo: when it expires the
driver's next heartbeat puts it back in the set. `release` puts the driver back immediately using
the coordinates in its heartbeat hash.

The matcher searches nearest-first in expanding rings (1000 m, 2000 m, 4000 m, 8000 m by
default) and tries to claim each candidate in order. Candidates that were taken between the
search and the claim simply fall through to the next one, which is why the unit test with a
racy index still terminates with `all_candidates_taken` rather than looping.

## Health checks and rolling updates

Liveness and readiness are separate on purpose. Liveness is a plain JVM check; a broker or Redis
outage must not restart every pod in the fleet. Readiness includes the dependencies a service
needs to do useful work: the shards and Kafka for the rider service, Redis and Kafka for the
driver service, and additionally the Kafka Streams state for the matcher. A pod whose readiness
fails is pulled from the Service endpoints and gets traffic back when it recovers.

The Deployments use `RollingUpdate` with `maxUnavailable: 0` and `maxSurge: 1`: a new pod must
pass its readiness probe before an old one is terminated, so capacity never drops below the
declared replica count. Three details make this actually zero-error under load:

* `preStop: sleep 5` on every service container. Endpoint removal is asynchronous; the sleep
  gives kube-proxy time to stop routing new connections before the process receives `SIGTERM`.
* `server.shutdown: graceful` with a 20 s phase timeout, so in-flight HTTP requests finish and
  Kafka producers flush before the JVM exits.
* A startup probe with a generous budget so JVM warm-up does not trip the liveness probe.

`scripts/k8s-e2e.sh` proves this by changing an environment variable on all three Deployments
while the in-cluster load generator is submitting rides and pings, then failing the build if the
generator saw a single HTTP error or an undecided ride. The matching service runs one replica in
the demo (its Streams state directory is ephemeral); a surge pod joins the consumer group, the
group rebalances, and the old pod leaves, which shows up as a brief latency bump and no lost
requests.
