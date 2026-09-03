/**
 * Port of common/shard/CityShardRouter: a city id maps to floorMod(cityId, N) unless an
 * override pins it. Pure function, so every service agrees without coordination.
 */
export function floorMod(a: number, n: number): number {
  return ((a % n) + n) % n;
}

export class CityShardRouter {
  readonly shardCount: number;
  private readonly overrides: ReadonlyMap<number, number>;

  constructor(shardCount: number, overrides: Iterable<[number, number]> = []) {
    if (shardCount < 1) throw new Error("at least one shard is required");
    const map = new Map<number, number>();
    for (const [city, shard] of overrides) {
      if (shard < 0 || shard >= shardCount) {
        throw new Error(`override for city ${city} points at missing shard ${shard}`);
      }
      map.set(city, shard);
    }
    this.shardCount = shardCount;
    this.overrides = map;
  }

  shardIndexFor(cityId: number): number {
    const pinned = this.overrides.get(cityId);
    return pinned !== undefined ? pinned : floorMod(cityId, this.shardCount);
  }

  shardName(cityId: number): string {
    return `shard-${this.shardIndexFor(cityId)}`;
  }
}
