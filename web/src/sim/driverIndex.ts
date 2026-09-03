import { haversineMeters } from "./geo";

/**
 * In-memory port of common/redis/RedisDriverIndex. Per city: one searchable set of available
 * drivers (the GEO set), one heartbeat record per driver with a TTL, and one claim key per driver.
 * Each operation mirrors one Lua script, so claim, stale detection, and set membership never race.
 */
export type ClaimResult = "CLAIMED" | "TAKEN" | "STALE";

export interface NearbyDriver {
  driverId: string;
  distanceMeters: number;
}

export interface DriverPosition {
  driverId: string;
  cityId: number;
  lat: number;
  lng: number;
  status: "AVAILABLE" | "OFFLINE";
  reportedAt: number;
}

interface Heartbeat {
  lat: number;
  lng: number;
  status: string;
  reportedAt: number;
  expiresAt: number;
}

interface Claim {
  rideId: string;
  expiresAt: number;
}

interface CityIndex {
  geo: Set<string>;
  heartbeats: Map<string, Heartbeat>;
  claims: Map<string, Claim>;
}

export interface DriverIndex {
  upsert(p: DriverPosition, heartbeatTtlMs: number): number;
  nearby(cityId: number, lat: number, lng: number, radiusMeters: number, limit: number): NearbyDriver[];
  claim(cityId: number, driverId: string, rideId: string, claimTtlMs: number): ClaimResult;
  release(cityId: number, driverId: string, rideId: string): boolean;
  claimedBy(cityId: number, driverId: string): string | null;
  size(cityId: number): number;
}

export class MemoryDriverIndex implements DriverIndex {
  private readonly cities = new Map<number, CityIndex>();
  /** Counts of index operations, for the console and the pipeline view. */
  readonly ops = { geoadd: 0, geosearch: 0, claim: 0, release: 0 };

  constructor(private readonly clock: () => number) {}

  private city(cityId: number): CityIndex {
    let c = this.cities.get(cityId);
    if (!c) {
      c = { geo: new Set(), heartbeats: new Map(), claims: new Map() };
      this.cities.set(cityId, c);
    }
    return c;
  }

  private liveClaim(c: CityIndex, driverId: string, now: number): Claim | undefined {
    const claim = c.claims.get(driverId);
    if (!claim) return undefined;
    if (claim.expiresAt <= now) {
      c.claims.delete(driverId);
      return undefined;
    }
    return claim;
  }

  private liveHeartbeat(c: CityIndex, driverId: string, now: number): Heartbeat | undefined {
    const hb = c.heartbeats.get(driverId);
    if (!hb) return undefined;
    if (hb.expiresAt <= now) {
      c.heartbeats.delete(driverId);
      return undefined;
    }
    return hb;
  }

  /** UPSERT_LUA: heartbeat + TTL; a claimed driver keeps its heartbeat but stays out of the set. */
  upsert(p: DriverPosition, heartbeatTtlMs: number): number {
    const now = this.clock();
    const c = this.city(p.cityId);
    if (p.status === "OFFLINE") {
      c.geo.delete(p.driverId);
      c.heartbeats.delete(p.driverId);
      return 0;
    }
    c.heartbeats.set(p.driverId, {
      lat: p.lat,
      lng: p.lng,
      status: p.status,
      reportedAt: p.reportedAt,
      expiresAt: now + heartbeatTtlMs,
    });
    if (this.liveClaim(c, p.driverId, now)) {
      c.geo.delete(p.driverId);
      return 0;
    }
    c.geo.add(p.driverId);
    this.ops.geoadd++;
    return 1;
  }

  /** GEOSEARCH ... BYRADIUS ... ASC COUNT limit WITHDIST */
  nearby(cityId: number, lat: number, lng: number, radiusMeters: number, limit: number): NearbyDriver[] {
    this.ops.geosearch++;
    const now = this.clock();
    const c = this.city(cityId);
    const hits: NearbyDriver[] = [];
    for (const driverId of c.geo) {
      const hb = c.heartbeats.get(driverId);
      if (!hb) continue;
      const d = haversineMeters(lat, lng, hb.lat, hb.lng);
      if (d <= radiusMeters) hits.push({ driverId, distanceMeters: d });
    }
    hits.sort((a, b) => a.distanceMeters - b.distanceMeters || (a.driverId < b.driverId ? -1 : 1));
    void now;
    return hits.slice(0, limit);
  }

  /** CLAIM_LUA: stale -> -1, SET NX PX -> 1, else 0. A win removes the driver from the set. */
  claim(cityId: number, driverId: string, rideId: string, claimTtlMs: number): ClaimResult {
    this.ops.claim++;
    const now = this.clock();
    const c = this.city(cityId);
    if (!this.liveHeartbeat(c, driverId, now)) {
      c.geo.delete(driverId);
      c.claims.delete(driverId);
      return "STALE";
    }
    if (!this.liveClaim(c, driverId, now)) {
      c.claims.set(driverId, { rideId, expiresAt: now + claimTtlMs });
      c.geo.delete(driverId);
      return "CLAIMED";
    }
    return "TAKEN";
  }

  /** RELEASE_LUA: only the owning ride can release; the driver goes back at its last position. */
  release(cityId: number, driverId: string, rideId: string): boolean {
    this.ops.release++;
    const now = this.clock();
    const c = this.city(cityId);
    const claim = this.liveClaim(c, driverId, now);
    if (claim && claim.rideId === rideId) {
      c.claims.delete(driverId);
      const hb = this.liveHeartbeat(c, driverId, now);
      if (hb) c.geo.add(driverId);
      return true;
    }
    return false;
  }

  claimedBy(cityId: number, driverId: string): string | null {
    const c = this.city(cityId);
    return this.liveClaim(c, driverId, this.clock())?.rideId ?? null;
  }

  size(cityId: number): number {
    return this.city(cityId).geo.size;
  }

  /** Snapshot for drawing: every heartbeating driver with its availability. */
  drivers(cityId: number): Array<{ driverId: string; lat: number; lng: number; available: boolean; rideId: string | null }> {
    const now = this.clock();
    const c = this.city(cityId);
    const out: Array<{ driverId: string; lat: number; lng: number; available: boolean; rideId: string | null }> = [];
    for (const [driverId, hb] of c.heartbeats) {
      if (hb.expiresAt <= now) continue;
      const claim = this.liveClaim(c, driverId, now);
      out.push({ driverId, lat: hb.lat, lng: hb.lng, available: c.geo.has(driverId), rideId: claim?.rideId ?? null });
    }
    return out;
  }
}
