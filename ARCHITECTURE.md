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
generator saw a single HTTP error or an undecided ride. The matching service runs two replicas
with `num.standby.replicas: 1` and static membership (`group.instance.id` set to the pod name,
20 s session timeout): a restarted pod rejoins under its old id inside the session window, so
the group does not rebalance at all, and if it does the standby already holds the state. A
rolling update shows up as a brief latency bump and no lost requests.

## Surge pricing from per-cell supply and demand

Surge is a read-side signal computed inside the matching service from the two streams it already
sees. The city is cut into fixed-size grid cells (`GeoCell`, 1 km by default). Every ride request
adds a timestamp to its pickup cell's demand deque for a trailing 60 s window; every available
driver position places the driver in a cell until its next ping or a 15 s supply TTL. Both sides
decay on their own, so a cell falls back to 1.0 once the burst ages out or drivers move in.

The multiplier is `1 + slope * (demand / supply - 1)` clamped to `[1, max]`, and only applies
once a cell has at least `minDemand` requests so one ride in an empty cell does not surge. The
matcher stamps the pickup cell's multiplier on the `Match` at claim time, and the trip row keeps
it in `surge_multiplier`, so a later price dispute can be traced to the demand the rider actually
saw. `GET /pricing/{city}` exposes the grid; `dispatchgrid.surge.max` and
`dispatchgrid.surge.cells` are gauges per city.

Keeping the tracker in process is a deliberate tradeoff. Requests and positions are consumed by
two separate sub-topologies, so with more than one matching pod a city's demand partition and its
supply partition can be assigned to different instances, and each pod then prices from the half
it sees. The demo accepts that (the load generator spreads both across all pods and `/pricing`
is read behind the Service); the production version keeps the per-cell counters in a
changelogged state store keyed by city so a task owns both sides, or in Redis next to the claims.

## Trip lifecycle and who owns which side

A trip row has two owners. The rider service owns the rider's intent (`REQUESTED`, `CANCELLED`)
and the matching service owns the driver's side (`MATCHED`, `COMPLETED`, and the Redis claim). A
cancellation therefore moves the row in the rider service, synchronously, so the rider gets a
definitive answer, and then travels on `ride-lifecycle` to the matcher, which releases the claim.
A completion goes the other way: the driver service only publishes, because it has no shard
connection by design, and the matcher applies the conditional update (`status = MATCHED AND
driver_id = ?`) before it frees the driver. A completion from a driver that does not hold the
ride changes nothing and is counted as `lifecycleIgnored`.

The two races this creates are both resolved by the conditional updates that were already there.
A ride cancelled while the matcher is mid-search still gets claimed, but `markMatched` requires
`REQUESTED`, so the matcher releases the driver it just took and drops the outcome, the same path
that handles redelivery. A cancellation of a matched ride and the driver's own completion can
cross on the topic; whichever lands first moves the row, the other becomes a no-op, and the
driver is released exactly once because a release only succeeds for the ride that holds the
claim. Every transition appends to `ride_events`, so the trip's timeline is reconstructible from
one shard.

Releasing on completion is what makes the claim TTL a safety net rather than the trip length.
Before v3 the synthetic fleet cycled only because claims expired; now a driver returns to the
pool the moment the trip ends, and the TTL only catches drivers whose app never reported.

## Retrying instead of failing fast

The first version reported a ride unmatched the moment one search found nobody. That is the
wrong answer for a marketplace: supply changes every second, and a driver who completes a trip
two blocks away ten seconds later would have taken the ride. The matcher now keeps the request
and tries again. The request path is a Processor API node with a persistent key-value store,
`pending-retries`, keyed by ride id. A pass that finds nobody before the last attempt writes the
request into the store with a due time, appends `ride.retry` to the timeline, and emits nothing;
a wall-clock punctuator (every second) replays the entries that are due. Attempt n waits
`n * backoff`, so with the defaults a ride is tried at 0 s, 5 s, and 15 s before it is given up
on, and only then is the row moved to `UNMATCHED` and the event published with `attempts`.

The store is changelogged, which is what makes this safe to run on Kubernetes: the pending set
is replayed into the standby on the other matching pod, so a rolling update or a crash moves the
waiting rides with the task instead of losing them. Punctuation is wall-clock rather than
stream-time because the trigger is real time passing with no new records, which is exactly when
stream time stands still. The cost is one full scan of the (small) store per tick per task; a
time-ordered store would remove that scan if the pending set ever grew large.

The retry path reuses the matcher unchanged and keeps the trip row `REQUESTED` between attempts,
so a cancellation during the wait is handled by the same conditional update as before: the
replayed pass claims a driver, `markMatched` sees the row is no longer `REQUESTED`, releases the
claim, and drops the outcome.
