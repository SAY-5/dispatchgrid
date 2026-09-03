import { offset } from "./geo";
import { Rng } from "./rng";
import type { DriverPosition } from "./driverIndex";
import type { RideRequest } from "./matcher";

/** Port of loadgen City: ids are 1-based so the two demo cities land on different shards. */
export interface City {
  id: number;
  name: string;
  lat: number;
  lng: number;
}

export const CITIES: City[] = [
  { id: 1, name: "austin", lat: 30.2672, lng: -97.7431 },
  { id: 2, name: "seattle", lat: 47.6062, lng: -122.3321 },
  { id: 3, name: "denver", lat: 39.7392, lng: -104.9903 },
  { id: 4, name: "chicago", lat: 41.8781, lng: -87.6298 },
];

export const SPAWN_RADIUS_M = 5000;
export const FENCE_RADIUS_M = 6500;
export const PICKUP_RADIUS_M = 4000;

/** Port of loadgen Fleet.Driver: wanders at 8 to 15 m/s and turns around at the fence. */
export class Driver {
  north: number;
  east: number;
  heading: number;

  constructor(
    readonly id: string,
    readonly city: City,
    private readonly rnd: Rng,
  ) {
    const r = SPAWN_RADIUS_M * Math.sqrt(rnd.next());
    const a = rnd.next() * 2 * Math.PI;
    this.north = r * Math.cos(a);
    this.east = r * Math.sin(a);
    this.heading = rnd.next() * 2 * Math.PI;
  }

  step(): void {
    this.heading += (this.rnd.next() - 0.5) * 0.6;
    const speed = 8 + this.rnd.next() * 7;
    this.north += speed * Math.cos(this.heading);
    this.east += speed * Math.sin(this.heading);
    if (Math.hypot(this.north, this.east) > FENCE_RADIUS_M) this.heading += Math.PI;
  }

  position(reportedAt: number): DriverPosition {
    const [lat, lng] = offset(this.city.lat, this.city.lng, this.north, this.east);
    return { driverId: this.id, cityId: this.city.id, lat, lng, status: "AVAILABLE", reportedAt };
  }
}

export class Fleet {
  readonly drivers: Driver[] = [];

  constructor(cities: City[], perCity: number, seed: number) {
    for (const c of cities) {
      for (let i = 0; i < perCity; i++) {
        this.drivers.push(new Driver(`d-${c.id}-${i}`, c, new Rng(seed + c.id * 100_000 + i)));
      }
    }
  }

  get size(): number {
    return this.drivers.length;
  }
}

/** Port of loadgen Rides: round-robin across cities, pickup within 4 km of the centre. */
export class RideSource {
  private counter = 0;
  private readonly rnd: Rng;

  constructor(
    private readonly cities: City[],
    seed: number,
  ) {
    this.rnd = new Rng(seed);
  }

  next(requestedAt: number): RideRequest {
    const c = this.cities[this.counter % this.cities.length];
    const n = this.counter++;
    const r = PICKUP_RADIUS_M * Math.sqrt(this.rnd.next());
    const a = this.rnd.next() * 2 * Math.PI;
    const [pickupLat, pickupLng] = offset(c.lat, c.lng, r * Math.cos(a), r * Math.sin(a));
    const r2 = (PICKUP_RADIUS_M + 2000) * Math.sqrt(this.rnd.next());
    const a2 = this.rnd.next() * 2 * Math.PI;
    const [dropoffLat, dropoffLng] = offset(c.lat, c.lng, r2 * Math.cos(a2), r2 * Math.sin(a2));
    return {
      rideId: `ride-${n.toString(36).padStart(4, "0")}`,
      riderId: `r-${c.id}-${this.rnd.int(100_000)}`,
      cityId: c.id,
      pickupLat,
      pickupLng,
      dropoffLat,
      dropoffLng,
      requestedAt,
    };
  }
}
