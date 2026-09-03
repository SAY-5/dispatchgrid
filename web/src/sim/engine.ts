import { MemoryDriverIndex } from "./driverIndex";
import { drawMatchLatencyMs, drawPollDelayMs } from "./latency";
import { DEFAULT_PROPS, Matcher, type Match, type MatchingProperties, type RideRequest, type RideUnmatched } from "./matcher";
import { Rng } from "./rng";
import { CityShardRouter } from "./shard";
import { MatchStats } from "./stats";
import { Consumer, TOPIC_DRIVER_POSITIONS, TOPIC_RIDE_MATCHES, TOPIC_RIDE_REQUESTS, TOPIC_RIDE_UNMATCHED, Topic } from "./topics";
import { CITIES, Fleet, RideSource, type City } from "./world";
import type { DriverPosition } from "./driverIndex";

/**
 * The whole stack in one deterministic engine driven by a simulated clock: the fleet pings
 * positions once a second into Redis GEO and the driver-positions topic, the ride source posts
 * requests at a fixed rate (shard insert + produce to ride-requests), and the Streams task polls
 * ride-requests, runs the Matcher, and branches into ride-matches or ride-unmatched.
 */
export interface EngineOptions {
  seed: number;
  cities: number;
  driversPerCity: number;
  ridesPerSecond: number;
  durationMs: number;
  heartbeatTtlMs: number;
  shards: number;
  props: MatchingProperties;
}

export const DEMO_OPTIONS: EngineOptions = {
  seed: 7,
  cities: 2,
  driversPerCity: 300,
  ridesPerSecond: 10,
  durationMs: 60_000,
  heartbeatTtlMs: 15_000,
  shards: 2,
  props: DEFAULT_PROPS,
};

export interface TripRow {
  rideId: string;
  cityId: number;
  status: "REQUESTED" | "MATCHED" | "UNMATCHED";
  driverId: string | null;
}

export interface EngineSnapshot {
  now: number;
  submitted: number;
  httpErrors: number;
  pingsOk: number;
  byShard: number[];
  byShardCity: Array<Record<number, number>>;
  matched: number;
  unmatched: number;
  dropped: number;
  matchesPerMinute: number;
  matchesPerMinuteOverall: number;
  p50: number;
  p95: number;
  p99: number;
  lag: number;
  topicTotals: Record<string, number>;
  partitionDepths: Record<string, number[]>;
  available: number[];
  claimed: number[];
  finished: boolean;
}

export class Engine {
  readonly opts: EngineOptions;
  readonly cities: City[];
  readonly router: CityShardRouter;
  readonly index: MemoryDriverIndex;
  readonly matcher: Matcher;
  readonly stats: MatchStats;
  readonly fleet: Fleet;
  readonly rides: RideSource;
  readonly rideRequests = new Topic<RideRequest>(TOPIC_RIDE_REQUESTS);
  readonly driverPositions = new Topic<DriverPosition>(TOPIC_DRIVER_POSITIONS);
  readonly rideMatches = new Topic<Match>(TOPIC_RIDE_MATCHES);
  readonly rideUnmatched = new Topic<RideUnmatched>(TOPIC_RIDE_UNMATCHED);
  readonly streamsConsumer: Consumer<RideRequest>;
  readonly shards: Map<string, TripRow>[];
  readonly recentMatches: Match[] = [];
  readonly recentUnmatched: RideUnmatched[] = [];
  private readonly rnd: Rng;
  private now = 0;
  private nextPingAt = 0;
  private nextRideAt = 0;
  private nextPollAt = 0;
  private submitted = 0;
  private pingsOk = 0;
  private httpErrors = 0;
  private ridesStopped = false;
  private lastMatchCounted = 0;
  private lastMinuteBucket = -1;
  readonly perSecondMatches: number[] = [];

  constructor(opts: EngineOptions = DEMO_OPTIONS) {
    this.opts = opts;
    this.cities = CITIES.slice(0, opts.cities);
    this.router = new CityShardRouter(opts.shards);
    this.index = new MemoryDriverIndex(() => this.now);
    this.matcher = new Matcher(this.index, opts.props, () => this.now);
    this.stats = new MatchStats(() => this.now);
    this.fleet = new Fleet(this.cities, opts.driversPerCity, opts.seed);
    this.rides = new RideSource(this.cities, opts.seed + 99);
    this.streamsConsumer = new Consumer(this.rideRequests);
    this.shards = Array.from({ length: opts.shards }, () => new Map());
    this.rnd = new Rng(opts.seed * 31 + 5);
    // Loadgen seeds the index with one synchronous round of pings before rides start.
    for (const d of this.fleet.drivers) this.ping(d.position(this.now));
  }

  get time(): number {
    return this.now;
  }

  get finished(): boolean {
    return this.ridesStopped && this.streamsConsumer.lag() === 0;
  }

  private ping(p: DriverPosition): void {
    this.index.upsert(p, this.opts.heartbeatTtlMs);
    this.driverPositions.append(String(p.cityId), p, this.now);
    this.pingsOk++;
  }

  /** rider-request-service POST /rides: write the trip to the city's shard, produce ride.requested. */
  private submitRide(): void {
    const r = this.rides.next(this.now);
    const shard = this.router.shardIndexFor(r.cityId);
    this.shards[shard].set(r.rideId, { rideId: r.rideId, cityId: r.cityId, status: "REQUESTED", driverId: null });
    this.rideRequests.append(String(r.cityId), r, this.now);
    this.submitted++;
  }

  /** matching-service MatchService.handle on one polled record. */
  private handle(r: RideRequest): void {
    let claimsTried = 0;
    const outcome = this.matcher.match(r, (t) => {
      if (t.kind === "claim") claimsTried++;
    });
    const shard = this.shards[this.router.shardIndexFor(r.cityId)];
    const row = shard.get(r.rideId);
    if (outcome.matched) {
      const m = outcome.match;
      if (!row || row.status !== "REQUESTED") {
        this.index.release(m.cityId, m.driverId, m.rideId);
        this.stats.recordDropped();
        return;
      }
      row.status = "MATCHED";
      row.driverId = m.driverId;
      const latency = drawMatchLatencyMs(this.rnd, claimsTried, this.now - r.requestedAt);
      m.matchLatencyMs = latency;
      this.stats.recordMatch(latency);
      this.rideMatches.append(String(m.cityId), m, this.now);
      this.recentMatches.push(m);
      if (this.recentMatches.length > 24) this.recentMatches.shift();
      return;
    }
    const u = outcome.unmatched;
    if (!row || row.status !== "REQUESTED") {
      this.stats.recordDropped();
      return;
    }
    row.status = "UNMATCHED";
    this.stats.recordUnmatched();
    this.rideUnmatched.append(String(u.cityId), u, this.now);
    this.recentUnmatched.push(u);
    if (this.recentUnmatched.length > 24) this.recentUnmatched.shift();
  }

  /** Advance simulated time by dt milliseconds, running every scheduled event in order. */
  tick(dtMs: number): void {
    const target = this.now + dtMs;
    while (this.now < target) {
      const next = Math.min(target, this.nextPingAt, this.ridesStopped ? Infinity : this.nextRideAt, this.nextPollAt);
      this.now = next;
      if (this.now >= this.nextPingAt) {
        for (const d of this.fleet.drivers) {
          d.step();
          this.ping(d.position(this.now));
        }
        this.nextPingAt += 1000;
      }
      if (!this.ridesStopped && this.now >= this.nextRideAt) {
        // The loadgen ticker fires at a fixed period; the last two fire during shutdown, which
        // is why a 60 s run at 10 rides/s reports 603 submissions.
        if (this.now > this.opts.durationMs + 250) {
          this.ridesStopped = true;
        } else {
          this.submitRide();
          this.nextRideAt += 1000 / this.opts.ridesPerSecond;
        }
      }
      if (this.now >= this.nextPollAt) {
        for (const rec of this.streamsConsumer.poll(this.now, 64)) this.handle(rec.value);
        this.nextPollAt = this.now + drawPollDelayMs(this.rnd);
      }
      const bucket = Math.floor(this.now / 1000);
      if (bucket !== this.lastMinuteBucket) {
        const snap = this.stats.snapshot();
        this.perSecondMatches.push(snap.matched - this.lastMatchCounted);
        this.lastMatchCounted = snap.matched;
        this.lastMinuteBucket = bucket;
        if (this.perSecondMatches.length > 90) this.perSecondMatches.shift();
      }
    }
  }

  snapshot(): EngineSnapshot {
    const s = this.stats.snapshot();
    const byShardCity: Array<Record<number, number>> = this.shards.map((m) => {
      const counts: Record<number, number> = {};
      for (const row of m.values()) counts[row.cityId] = (counts[row.cityId] ?? 0) + 1;
      return counts;
    });
    const depths: Record<string, number[]> = {};
    for (const t of [this.rideRequests, this.driverPositions, this.rideMatches, this.rideUnmatched]) {
      depths[t.name] = t.partitions.map((p) => p.length);
    }
    const available = this.cities.map((c) => this.index.size(c.id));
    const claimed = this.cities.map((c) => this.index.drivers(c.id).filter((d) => !d.available).length);
    return {
      now: this.now,
      submitted: this.submitted,
      httpErrors: this.httpErrors,
      pingsOk: this.pingsOk,
      byShard: this.shards.map((m) => m.size),
      byShardCity,
      matched: s.matched,
      unmatched: s.unmatched,
      dropped: s.dropped,
      matchesPerMinute: s.matchesPerMinute,
      matchesPerMinuteOverall: s.matchesPerMinuteOverall,
      p50: s.p50LatencyMs,
      p95: s.p95LatencyMs,
      p99: s.p99LatencyMs,
      lag: this.streamsConsumer.lag(),
      topicTotals: {
        [TOPIC_RIDE_REQUESTS]: this.rideRequests.total,
        [TOPIC_DRIVER_POSITIONS]: this.driverPositions.total,
        [TOPIC_RIDE_MATCHES]: this.rideMatches.total,
        [TOPIC_RIDE_UNMATCHED]: this.rideUnmatched.total,
      },
      partitionDepths: depths,
      available,
      claimed,
      finished: this.finished,
    };
  }

  /** Invariant used by the self check: a driver never holds two live claims. */
  doubleClaims(): number {
    let dupes = 0;
    for (const shard of this.shards) {
      const owners = new Map<string, string>();
      for (const row of shard.values()) {
        if (row.status !== "MATCHED" || !row.driverId) continue;
        const live = this.index.claimedBy(row.cityId, row.driverId);
        if (live && live !== row.rideId) {
          const seen = owners.get(row.driverId);
          if (seen && seen !== row.rideId) dupes++;
        }
        owners.set(row.driverId, row.rideId);
      }
    }
    return dupes;
  }
}
