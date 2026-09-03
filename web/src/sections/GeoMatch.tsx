import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type MouseEvent } from "react";
import { useReducedMotion } from "framer-motion";
import { MemoryDriverIndex } from "../sim/driverIndex";
import { offset, toLocalMeters } from "../sim/geo";
import { DEFAULT_PROPS, Matcher, type MatchTrace } from "../sim/matcher";
import { Rng } from "../sim/rng";
import { CITIES, Fleet } from "../sim/world";

const CITY = CITIES[0];
const SIZE = 720;
const HALF_M = 7200;
const PX_PER_M = SIZE / 2 / HALF_M;
const CONTENTION = [
  { label: "none", n: 0, hint: "the nearest driver wins" },
  { label: "3 rides", n: 3, hint: "three nearest already claimed" },
  { label: "25 rides", n: 25, hint: "first page of the 2 km ring taken, page grows" },
  { label: "60 rides", n: 60, hint: "grows to 64 then widens to 4 km" },
];

interface DriverDot {
  id: string;
  x: number;
  y: number;
  lat: number;
  lng: number;
}

function toPx(lat: number, lng: number): [number, number] {
  const [east, north] = toLocalMeters(CITY.lat, CITY.lng, lat, lng);
  return [SIZE / 2 + east * PX_PER_M, SIZE / 2 - north * PX_PER_M];
}

function fromPx(x: number, y: number): [number, number] {
  const east = (x - SIZE / 2) / PX_PER_M;
  const north = (SIZE / 2 - y) / PX_PER_M;
  return offset(CITY.lat, CITY.lng, north, east);
}

export function GeoMatch() {
  const reduce = useReducedMotion();
  const drivers = useMemo<DriverDot[]>(() => {
    const fleet = new Fleet([CITY], 300, 7);
    return fleet.drivers.map((d) => {
      const p = d.position(0);
      const [x, y] = toPx(p.lat, p.lng);
      return { id: d.id, x, y, lat: p.lat, lng: p.lng };
    });
  }, []);

  const [contention, setContention] = useState(1);
  const [pickup, setPickup] = useState<[number, number] | null>(null);
  const [trace, setTrace] = useState<MatchTrace[]>([]);
  const [takenByOthers, setTakenByOthers] = useState<Set<string>>(new Set());
  const [shown, setShown] = useState(0);
  const rndRef = useRef(new Rng(99));
  const svgRef = useRef<SVGSVGElement>(null);

  const runAt = (lat: number, lng: number) => {
    const index = new MemoryDriverIndex(() => 0);
    for (const d of drivers) index.upsert({ driverId: d.id, cityId: CITY.id, lat: d.lat, lng: d.lng, status: "AVAILABLE", reportedAt: 0 }, 15_000);
    // Other rides that raced ahead of this one hold the N nearest drivers.
    const racing = index.nearby(CITY.id, lat, lng, 20_000, CONTENTION[contention].n);
    const others = new Set<string>();
    racing.forEach((r, i) => {
      index.claim(CITY.id, r.driverId, `ride-other-${i}`, 20_000);
      others.add(r.driverId);
    });
    const events: MatchTrace[] = [];
    new Matcher(index, DEFAULT_PROPS, () => 0).match(
      { rideId: "ride-you", riderId: "you", cityId: CITY.id, pickupLat: lat, pickupLng: lng, dropoffLat: lat, dropoffLng: lng, requestedAt: 0 },
      (t) => events.push(t),
    );
    setTakenByOthers(others);
    setPickup([lat, lng]);
    setTrace(events);
    setShown(reduce ? events.length : 0);
  };

  useEffect(() => {
    if (shown >= trace.length) return;
    const next = trace[shown];
    const delay = next.kind === "claim" ? 140 : next.kind === "search" ? 420 : 520;
    const t = setTimeout(() => setShown((s) => s + 1), delay);
    return () => clearTimeout(t);
  }, [shown, trace]);

  const onMapClick = (e: MouseEvent<SVGSVGElement>) => {
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const x = ((e.clientX - rect.left) / rect.width) * SIZE;
    const y = ((e.clientY - rect.top) / rect.height) * SIZE;
    const [lat, lng] = fromPx(x, y);
    runAt(lat, lng);
  };

  const dropRandom = () => {
    const rnd = rndRef.current;
    const r = 3500 * Math.sqrt(rnd.next());
    const a = rnd.next() * Math.PI * 2;
    const [lat, lng] = offset(CITY.lat, CITY.lng, r * Math.cos(a), r * Math.sin(a));
    runAt(lat, lng);
  };

  const onKey = (e: KeyboardEvent<SVGSVGElement>) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      dropRandom();
    }
  };

  const visible = trace.slice(0, shown);
  const searches = visible.filter((t): t is Extract<MatchTrace, { kind: "search" }> => t.kind === "search");
  const currentRadius = searches.length ? searches[searches.length - 1].radius : null;
  const currentLimit = searches.length ? searches[searches.length - 1].limit : null;
  const claims = visible.filter((t): t is Extract<MatchTrace, { kind: "claim" }> => t.kind === "claim");
  const claimState = new Map<string, "CLAIMED" | "TAKEN" | "STALE">();
  for (const c of claims) claimState.set(c.driverId, c.result);
  const candidateOrder = new Map<string, number>();
  if (searches.length) searches[searches.length - 1].candidates.forEach((c, i) => candidateOrder.set(c.driverId, i + 1));
  const final = visible.find((t) => t.kind === "matched" || t.kind === "unmatched");
  const matched = final?.kind === "matched" ? final.match : null;
  const pickupPx = pickup ? toPx(pickup[0], pickup[1]) : null;
  const winner = matched ? drivers.find((d) => d.id === matched.driverId) : null;
  const done = shown >= trace.length && trace.length > 0;

  return (
    <section className="section geo" id="geo" aria-labelledby="geo-title">
      <div className="wrap">
        <span className="eyebrow">02 · Geo match</span>
        <h2 className="section-title" id="geo-title">
          Nearest first, one atomic claim per candidate.
        </h2>
        <p className="section-lede">
          <code>GEOSEARCH</code> returns the nearest drivers inside the ring, sorted ascending. The matcher tries to claim
          each one with the Lua script (<code>SET claim NX PX ttl</code>). A driver taken between the search and the claim
          simply falls through. When a full page of a ring was taken, the page doubles (20, 40, 64) before the radius widens
          (1 km, 2 km, 4 km, 8 km). Click the map to drop a pickup.
        </p>

        <div className="geo-grid">
          <div className="glass geo-map-wrap">
            <svg
              ref={svgRef}
              className="geo-map"
              viewBox={`0 0 ${SIZE} ${SIZE}`}
              onClick={onMapClick}
              onKeyDown={onKey}
              tabIndex={0}
              role="button"
              aria-label="Austin driver map. Click or press Enter to drop a pickup and run the matcher."
            >
              <defs>
                <radialGradient id="pickup-glow">
                  <stop offset="0" stopColor="rgba(223,241,255,0.55)" />
                  <stop offset="1" stopColor="rgba(223,241,255,0)" />
                </radialGradient>
                <clipPath id="map-clip">
                  <rect width={SIZE} height={SIZE} rx={16} />
                </clipPath>
              </defs>
              <g clipPath="url(#map-clip)">
                <rect width={SIZE} height={SIZE} fill="rgba(7,11,20,0.55)" />
                {Array.from({ length: 13 }, (_, i) => (
                  <line key={`v${i}`} x1={(i * SIZE) / 12} y1={0} x2={(i * SIZE) / 12} y2={SIZE} stroke="rgba(160,196,255,0.06)" />
                ))}
                {Array.from({ length: 13 }, (_, i) => (
                  <line key={`h${i}`} x1={0} y1={(i * SIZE) / 12} x2={SIZE} y2={(i * SIZE) / 12} stroke="rgba(160,196,255,0.06)" />
                ))}
                <circle cx={SIZE / 2} cy={SIZE / 2} r={6500 * PX_PER_M} fill="none" stroke="rgba(160,196,255,0.12)" strokeDasharray="4 8" />
                <text x={SIZE / 2} y={SIZE / 2 - 6500 * PX_PER_M - 8} textAnchor="middle" className="svg-mono" fill="var(--text-mute)">
                  fence 6.5 km
                </text>

                {pickupPx &&
                  [1000, 2000, 4000, 8000].map((r) => {
                    const reached = searches.some((s) => s.radius === r);
                    const current = currentRadius === r;
                    return (
                      <g key={r}>
                        <circle
                          cx={pickupPx[0]}
                          cy={pickupPx[1]}
                          r={r * PX_PER_M}
                          fill={current ? "rgba(76,141,255,0.06)" : "none"}
                          stroke={current ? "var(--sapphire)" : reached ? "rgba(255,122,154,0.5)" : "rgba(160,196,255,0.15)"}
                          strokeWidth={current ? 1.6 : 1}
                          strokeDasharray={reached ? undefined : "3 6"}
                        />
                        <text x={pickupPx[0] + r * PX_PER_M * 0.72} y={pickupPx[1] - r * PX_PER_M * 0.72} className="svg-mono" fill={current ? "var(--ice)" : "var(--text-mute)"}>
                          {r / 1000} km
                        </text>
                      </g>
                    );
                  })}

                {drivers.map((d) => {
                  const state = claimState.get(d.id);
                  const other = takenByOthers.has(d.id) && pickup !== null;
                  const order = candidateOrder.get(d.id);
                  const isWinner = matched?.driverId === d.id;
                  let fill = "var(--sapphire)";
                  let stroke = "none";
                  let r = 3.2;
                  if (other) {
                    fill = "rgba(7,11,20,0.8)";
                    stroke = "var(--taken)";
                  }
                  if (state === "TAKEN") {
                    r = 4.5;
                  }
                  if (isWinner) {
                    fill = "var(--ice)";
                    stroke = "var(--ok)";
                    r = 7;
                  }
                  return (
                    <g key={d.id}>
                      <circle cx={d.x} cy={d.y} r={r} fill={fill} stroke={stroke} strokeWidth={1.5} opacity={other && !state ? 0.7 : 1} />
                      {order !== undefined && !isWinner && (
                        <text x={d.x + 7} y={d.y - 6} className="svg-mono" fill={state === "TAKEN" ? "var(--taken)" : "var(--text-dim)"} fontSize={10}>
                          {order}
                        </text>
                      )}
                    </g>
                  );
                })}

                {pickupPx && winner && (
                  <line x1={pickupPx[0]} y1={pickupPx[1]} x2={winner.x} y2={winner.y} stroke="var(--ok)" strokeWidth={2} strokeDasharray="4 4" />
                )}
                {pickupPx && (
                  <g>
                    <circle cx={pickupPx[0]} cy={pickupPx[1]} r={34} fill="url(#pickup-glow)" />
                    <circle cx={pickupPx[0]} cy={pickupPx[1]} r={6} fill="var(--ice)" />
                    <circle cx={pickupPx[0]} cy={pickupPx[1]} r={11} fill="none" stroke="var(--ice)" strokeOpacity={0.7} />
                  </g>
                )}
              </g>
            </svg>
            <div className="geo-legend">
              <span className="chip">
                <span className="dot" /> available
              </span>
              <span className="chip">
                <span className="dot" style={{ background: "transparent", border: "1.5px solid var(--taken)", boxShadow: "none" }} /> claimed by another ride
              </span>
              <span className="chip">
                <span className="dot" style={{ background: "var(--ice)", boxShadow: "0 0 10px var(--ok)" }} /> yours
              </span>
              <span className="chip">numbers = GEOSEARCH order</span>
            </div>
          </div>

          <div className="geo-side">
            <div className="glass geo-controls">
              <p className="label-cap">Rides racing this one</p>
              <div className="segmented" role="radiogroup" aria-label="Contention level">
                {CONTENTION.map((c, i) => (
                  <button key={c.label} type="button" role="radio" aria-checked={contention === i} className={contention === i ? "on" : ""} onClick={() => setContention(i)}>
                    {c.label}
                  </button>
                ))}
              </div>
              <p className="dim">{CONTENTION[contention].hint}</p>
              <button className="btn" type="button" onClick={dropRandom}>
                Drop a pickup
              </button>
            </div>

            <div className="glass geo-trace" aria-live="polite">
              <p className="label-cap">Matcher trace</p>
              {trace.length === 0 && <p className="dim">No pickup yet. Click the map or press the button.</p>}
              <ol>
                {visible.map((t, i) => {
                  if (t.kind === "search") {
                    return (
                      <li key={i} className="t-search">
                        <span className="mono">GEOSEARCH</span> r={t.radius} m, limit {t.limit} → {t.candidates.length} candidates
                      </li>
                    );
                  }
                  if (t.kind === "claim") {
                    return (
                      <li key={i} className={`t-claim ${t.result.toLowerCase()}`}>
                        <span className="mono">claim</span> {t.driverId} at {Math.round(t.distanceMeters)} m → <b>{t.result}</b>
                      </li>
                    );
                  }
                  if (t.kind === "grow") {
                    return (
                      <li key={i} className="t-grow">
                        full page taken: grow candidates to {t.limit} inside {t.radius} m
                      </li>
                    );
                  }
                  if (t.kind === "widen") {
                    return (
                      <li key={i} className="t-widen">
                        ring exhausted: widen {t.from} m → {t.to} m
                      </li>
                    );
                  }
                  if (t.kind === "matched") {
                    return (
                      <li key={i} className="t-done ok">
                        matched {t.match.driverId} at {Math.round(t.match.distanceMeters)} m in the {t.match.radiusMeters} m ring
                      </li>
                    );
                  }
                  return (
                    <li key={i} className="t-done bad">
                      unmatched: {t.unmatched.reason}
                    </li>
                  );
                })}
              </ol>
              {done && (
                <p className="dim">
                  {claims.length} claim script{claims.length === 1 ? "" : "s"}, {searches.length} search{searches.length === 1 ? "" : "es"},{" "}
                  {currentLimit !== null ? `final page ${currentLimit}` : ""}
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
