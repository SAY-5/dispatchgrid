import { MemoryDriverIndex } from "./driverIndex";
import { haversineMeters, radii } from "./geo";
import { DEFAULT_PROPS, Matcher } from "./matcher";
import { CityShardRouter } from "./shard";
import { partitionFor } from "./topics";
import { Engine, DEMO_OPTIONS } from "./engine";
import { CITIES } from "./world";

/**
 * Console self check for the simulation port. Runs under tsx (npm run selfcheck) and in the
 * browser console via window.dispatchgridSelfCheck().
 */
export interface CheckResult {
  name: string;
  ok: boolean;
  detail: string;
}

function check(name: string, ok: boolean, detail: string): CheckResult {
  return { name, ok, detail };
}

export function runSelfCheck(): CheckResult[] {
  const out: CheckResult[] = [];

  // 1. Shard router: floorMod determinism, negative ids, overrides.
  const router = new CityShardRouter(2);
  const a = router.shardIndexFor(1);
  const b = router.shardIndexFor(2);
  const same = Array.from({ length: 50 }, () => router.shardIndexFor(1)).every((s) => s === a);
  out.push(check("shard router is a pure function", same && a === 1 && b === 0, `city 1 -> shard ${a}, city 2 -> shard ${b}`));
  out.push(check("shard router floorMod handles negatives", router.shardIndexFor(-1) === 1, `city -1 -> shard ${router.shardIndexFor(-1)}`));
  const pinned = new CityShardRouter(3, [[7, 0]]);
  out.push(check("shard router override pins a city", pinned.shardIndexFor(7) === 0 && pinned.shardIndexFor(8) === 2, "city 7 pinned to shard 0, city 8 -> shard 2"));
  let threw = false;
  try {
    new CityShardRouter(2, [[1, 5]]);
  } catch {
    threw = true;
  }
  out.push(check("shard router rejects an override past N", threw, "override to shard 5 of 2 throws"));

  // 2. Geo helpers.
  const d = haversineMeters(30.2672, -97.7431, 47.6062, -122.3321);
  out.push(check("haversine austin to seattle", Math.abs(d - 2_852_000) < 20_000, `${Math.round(d / 1000)} km`));
  out.push(check("radius expansion 1000, 2000, 4000, 8000", radii({ initialMeters: 1000, factor: 2, maxMeters: 8000 }).join(",") === "1000,2000,4000,8000", radii({ initialMeters: 1000, factor: 2, maxMeters: 8000 }).join(", ")));

  // 3. Kafka partitioning by city key is stable.
  const p1 = partitionFor("1");
  const p2 = partitionFor("2");
  out.push(check("murmur2 partition for city keys is deterministic", partitionFor("1") === p1 && partitionFor("2") === p2, `key "1" -> partition ${p1}, key "2" -> partition ${p2}`));

  // 4. Atomic claims: exactly one winner, taken for everyone else, stale after heartbeat expiry.
  let now = 0;
  const index = new MemoryDriverIndex(() => now);
  index.upsert({ driverId: "d-1-0", cityId: 1, lat: 30.2672, lng: -97.7431, status: "AVAILABLE", reportedAt: 0 }, 15_000);
  const first = index.claim(1, "d-1-0", "ride-a", 20_000);
  const second = index.claim(1, "d-1-0", "ride-b", 20_000);
  const third = index.claim(1, "d-1-0", "ride-c", 20_000);
  out.push(check("claim is exclusive (SET NX)", first === "CLAIMED" && second === "TAKEN" && third === "TAKEN", `${first}, ${second}, ${third}`));
  out.push(check("claimed driver leaves the searchable set", index.size(1) === 0, `size after claim = ${index.size(1)}`));
  out.push(check("release by a non owner is ignored", index.release(1, "d-1-0", "ride-b") === false && index.claimedBy(1, "d-1-0") === "ride-a", "ride-b cannot release ride-a's claim"));
  out.push(check("release by the owner returns the driver", index.release(1, "d-1-0", "ride-a") === true && index.size(1) === 1, `size after release = ${index.size(1)}`));
  now = 16_000;
  out.push(check("expired heartbeat reads as STALE", index.claim(1, "d-1-0", "ride-d", 20_000) === "STALE", "claim after 16 s without a ping"));

  // 5. Matcher: three nearest drivers all claimed, still matched via page growth inside the ring.
  now = 0;
  const idx2 = new MemoryDriverIndex(() => now);
  const c = CITIES[0];
  const props = { ...DEFAULT_PROPS, candidatesPerRadius: 3 };
  for (let i = 0; i < 12; i++) {
    const north = 40 + i * 25;
    idx2.upsert({ driverId: `d-1-${i}`, cityId: 1, lat: c.lat + north / 111_320, lng: c.lng, status: "AVAILABLE", reportedAt: 0 }, 15_000);
  }
  // Another matcher thread wins the three nearest in between GEOSEARCH and the claim.
  const racy = {
    upsert: idx2.upsert.bind(idx2),
    nearby: (cityId: number, lat: number, lng: number, r: number, limit: number) => {
      const hits = idx2.nearby(cityId, lat, lng, r, limit);
      for (const h of hits.slice(0, 3)) idx2.claim(cityId, h.driverId, "ride-other", 20_000);
      return hits;
    },
    claim: idx2.claim.bind(idx2),
    release: idx2.release.bind(idx2),
    claimedBy: idx2.claimedBy.bind(idx2),
    size: idx2.size.bind(idx2),
  };
  const kinds: string[] = [];
  const m = new Matcher(racy, props, () => now);
  const outcome = m.match(
    { rideId: "ride-x", riderId: "r", cityId: 1, pickupLat: c.lat, pickupLng: c.lng, dropoffLat: c.lat, dropoffLng: c.lng, requestedAt: 0 },
    (t) => kinds.push(t.kind === "search" ? `search@${t.radius}x${t.limit}` : t.kind === "grow" ? `grow->${t.limit}` : t.kind),
  );
  const grew = kinds.includes("grow->6");
  out.push(check("three nearest taken: page grows inside the ring and still matches", outcome.matched && grew && outcome.match.radiusMeters === 1000, kinds.filter((k) => k !== "claim").join(" ")));

  // 6. all_candidates_taken vs no_drivers_in_range.
  const idx3 = new MemoryDriverIndex(() => now);
  const empty = new Matcher(idx3, DEFAULT_PROPS, () => now).match({ rideId: "r1", riderId: "r", cityId: 1, pickupLat: c.lat, pickupLng: c.lng, dropoffLat: 0, dropoffLng: 0, requestedAt: 0 });
  idx3.upsert({ driverId: "d", cityId: 1, lat: c.lat, lng: c.lng, status: "AVAILABLE", reportedAt: 0 }, 15_000);
  const takenIndex = { ...racy, nearby: idx3.nearby.bind(idx3), claim: () => "TAKEN" as const, upsert: idx3.upsert.bind(idx3), size: idx3.size.bind(idx3), release: idx3.release.bind(idx3), claimedBy: idx3.claimedBy.bind(idx3) };
  const allTaken = new Matcher(takenIndex, DEFAULT_PROPS, () => now).match({ rideId: "r2", riderId: "r", cityId: 1, pickupLat: c.lat, pickupLng: c.lng, dropoffLat: 0, dropoffLng: 0, requestedAt: 0 });
  out.push(check("unmatched reasons", !empty.matched && empty.unmatched.reason === "no_drivers_in_range" && !allTaken.matched && allTaken.unmatched.reason === "all_candidates_taken", `${!empty.matched ? empty.unmatched.reason : "?"} / ${!allTaken.matched ? allTaken.unmatched.reason : "?"}`));

  // 7. Full run: 600 drivers, 60 s at 10 rides/s, every ride matched, no driver claimed twice.
  const engine = new Engine(DEMO_OPTIONS);
  engine.tick(66_000);
  const s = engine.snapshot();
  const holders = new Map<string, Set<string>>();
  for (const shard of engine.shards) {
    for (const row of shard.values()) {
      if (row.driverId) {
        const set = holders.get(row.driverId) ?? new Set();
        set.add(row.rideId);
        holders.set(row.driverId, set);
      }
    }
  }
  // Within any 20 s claim window a driver can only be matched once; check consecutive matches.
  const matchesByDriver = new Map<string, number[]>();
  for (const partition of engine.rideMatches.partitions) {
    for (const rec of partition) {
      const list = matchesByDriver.get(rec.value.driverId) ?? [];
      list.push(rec.timestamp);
      matchesByDriver.set(rec.value.driverId, list);
    }
  }
  let overlap = 0;
  for (const times of matchesByDriver.values()) {
    times.sort((x, y) => x - y);
    for (let i = 1; i < times.length; i++) if (times[i] - times[i - 1] < DEFAULT_PROPS.claimTtlMs) overlap++;
  }
  out.push(check("60 s run: 500+ rides submitted and every one decided", s.submitted >= 500 && s.matched + s.unmatched === s.submitted, `submitted ${s.submitted}, matched ${s.matched}, unmatched ${s.unmatched}, lag ${s.lag}`));
  out.push(check("no driver claimed by two rides inside one claim TTL", overlap === 0, `${matchesByDriver.size} distinct drivers matched, ${overlap} overlapping claims`));
  out.push(check("shard split by city", s.byShard[0] + s.byShard[1] === s.submitted && Object.keys(s.byShardCity[0]).join() === "2" && Object.keys(s.byShardCity[1]).join() === "1", `shard-0 ${JSON.stringify(s.byShardCity[0])} shard-1 ${JSON.stringify(s.byShardCity[1])}`));
  out.push(check("throughput near the measured run", s.matchesPerMinuteOverall >= 500, `${s.matchesPerMinuteOverall} matches/min overall, p50 ${s.p50} ms, p95 ${s.p95} ms, p99 ${s.p99} ms`));

  return out;
}

export function formatSelfCheck(results: CheckResult[]): string {
  const lines = results.map((r) => `${r.ok ? "PASS" : "FAIL"}  ${r.name}: ${r.detail}`);
  const failed = results.filter((r) => !r.ok).length;
  lines.push(failed === 0 ? `all ${results.length} checks passed` : `${failed} of ${results.length} checks FAILED`);
  return lines.join("\n");
}

declare const process: { argv?: string[]; exitCode?: number } | undefined;
if (typeof process !== "undefined" && process?.argv && process.argv[1] && /selfcheck/.test(process.argv[1])) {
  const results = runSelfCheck();
  console.log(formatSelfCheck(results));
  if (results.some((r) => !r.ok)) process.exitCode = 1;
}
