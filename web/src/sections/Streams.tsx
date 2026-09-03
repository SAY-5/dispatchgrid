import { useCallback, useMemo, useRef, useState } from "react";
import { useInView } from "framer-motion";
import { useRunner } from "../components/useRunner";
import { DEMO_OPTIONS, Engine, type EngineSnapshot } from "../sim/engine";
import { partitionFor, PARTITIONS, TOPIC_DRIVER_POSITIONS, TOPIC_RIDE_MATCHES, TOPIC_RIDE_REQUESTS, TOPIC_RIDE_UNMATCHED } from "../sim/topics";

const SPEED = 4;
const W = 1100;
const H = 420;

function Node({ x, y, w, h, title, sub, tone = "var(--sapphire)" }: { x: number; y: number; w: number; h: number; title: string; sub?: string; tone?: string }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={14} fill="rgba(7,11,20,0.75)" stroke={tone} strokeOpacity={0.55} />
      <text x={x + 16} y={y + 26} className="svg-label" fill="var(--ice)">
        {title}
      </text>
      {sub && (
        <text x={x + 16} y={y + 46} className="svg-mono" fill="var(--text-dim)">
          {sub}
        </text>
      )}
    </g>
  );
}

function Flow({ d, tone = "var(--sapphire)", active = true, dur = "1.6s" }: { d: string; tone?: string; active?: boolean; dur?: string }) {
  return (
    <>
      <path d={d} fill="none" stroke={tone} strokeOpacity={0.22} strokeWidth={1.5} />
      {active && <path d={d} fill="none" stroke={tone} strokeOpacity={0.9} strokeWidth={2} strokeDasharray="6 18" className="flow" style={{ animationDuration: dur }} />}
    </>
  );
}

function Lanes({ x, y, depths, litA, litB, label }: { x: number; y: number; depths: number[]; litA: number; litB: number; label: string }) {
  const max = Math.max(1, ...depths);
  return (
    <g>
      <text x={x} y={y - 10} className="svg-mono" fill="var(--text-dim)">
        {label}
      </text>
      {Array.from({ length: PARTITIONS }, (_, p) => {
        const lit = p === litA || p === litB;
        const w = 160 * (depths[p] / max);
        return (
          <g key={p}>
            <rect x={x} y={y + p * 20} width={160} height={12} rx={6} fill="rgba(160,196,255,0.06)" />
            <rect x={x} y={y + p * 20} width={Math.max(lit ? 6 : 0, w)} height={12} rx={6} fill={p === litA ? "var(--city-a)" : "var(--city-b)"} opacity={lit ? 0.95 : 0.2} />
            <text x={x + 170} y={y + p * 20 + 10} className="svg-mono" fill={lit ? "var(--ice)" : "var(--text-mute)"}>
              p{p} {depths[p] > 0 ? depths[p] : ""}
            </text>
          </g>
        );
      })}
    </g>
  );
}

export function Streams() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { margin: "-15% 0px" });
  const engineRef = useRef<Engine>(new Engine(DEMO_OPTIONS));
  const [snap, setSnap] = useState<EngineSnapshot>(() => engineRef.current.snapshot());
  const [run, setRun] = useState(0);

  const step = useCallback((dt: number) => {
    const e = engineRef.current;
    if (!e.finished) e.tick(dt);
  }, []);
  const onFrame = useCallback(() => setSnap(engineRef.current.snapshot()), []);
  useRunner(inView && !snap.finished, SPEED, step, onFrame, 10);

  const replay = () => {
    engineRef.current = new Engine({ ...DEMO_OPTIONS, seed: DEMO_OPTIONS.seed + run + 1 });
    setSnap(engineRef.current.snapshot());
    setRun((r) => r + 1);
  };

  const pA = useMemo(() => partitionFor("1"), []);
  const pB = useMemo(() => partitionFor("2"), []);
  const live = !snap.finished && inView;
  const shard0 = snap.byShard[0];
  const shard1 = snap.byShard[1];
  const target = 303;

  return (
    <section className="section streams" id="streams" aria-labelledby="streams-title" ref={ref}>
      <div className="wrap">
        <span className="eyebrow">01 · Streams and shards</span>
        <h2 className="section-title" id="streams-title">
          Two topics in, two topics out, one shard per city.
        </h2>
        <p className="section-lede">
          The rider service writes the trip to its city's shard and produces <code>ride.requested</code>. The Streams
          task on that partition runs the matcher and branches the outcome. Drivers ping once a second: a{" "}
          <code>GEOADD</code> into <code>drivers:geo:{"{city}"}</code> plus a heartbeat hash with a 15 s TTL, and a
          record on <code>driver-positions</code>. This is the sim running at {SPEED}x.
        </p>

        <div className="glass pipeline">
          <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Animated pipeline of the DispatchGrid topics and services">
            <Node x={20} y={60} w={200} h={64} title="rider-request-service" sub={`POST /rides  ${snap.submitted}`} />
            <Node x={20} y={280} w={200} h={64} title="driver-location-service" sub={`pings ok ${snap.pingsOk}`} tone="var(--city-b)" />

            <Flow d="M 220 92 C 270 92, 280 92, 330 92" active={live} />
            <Lanes x={340} y={40} depths={snap.partitionDepths[TOPIC_RIDE_REQUESTS] ?? []} litA={pA} litB={pB} label="ride-requests" />

            <Flow d="M 220 312 C 270 312, 280 312, 330 312" tone="var(--city-b)" active={live} />
            <Lanes x={340} y={270} depths={snap.partitionDepths[TOPIC_DRIVER_POSITIONS] ?? []} litA={pA} litB={pB} label="driver-positions" />

            <Flow d="M 560 92 C 610 92, 620 92, 670 92" active={live} />
            <Node x={680} y={40} w={200} h={104} title="Streams matcher" sub={`lag ${snap.lag}  matched ${snap.matched}`} />
            <text x={696} y={120} className="svg-mono" fill="var(--text-dim)">
              GEOSEARCH → claim (SET NX)
            </text>

            <Flow d="M 560 312 C 610 312, 620 312, 670 312" tone="var(--city-b)" active={live} />
            <Node x={680} y={280} w={200} h={64} title="Redis GEO" sub={`available ${snap.available.join(" / ")}  claimed ${snap.claimed.join(" / ")}`} tone="var(--city-b)" />
            <Flow d="M 780 280 C 780 230, 780 190, 780 144" tone="var(--ice)" active={live} dur="1.1s" />

            <Flow d="M 880 80 C 930 80, 940 70, 990 70" tone="var(--ok)" active={live} />
            <Flow d="M 880 110 C 930 110, 940 140, 990 140" tone="var(--taken)" active={false} />
            <text x={1000} y={74} className="svg-mono" fill="var(--ok)">
              {TOPIC_RIDE_MATCHES} {snap.topicTotals[TOPIC_RIDE_MATCHES]}
            </text>
            <text x={1000} y={144} className="svg-mono" fill="var(--taken)">
              {TOPIC_RIDE_UNMATCHED} {snap.topicTotals[TOPIC_RIDE_UNMATCHED]}
            </text>
            <text x={20} y={400} className="svg-mono" fill="var(--text-mute)">
              t = {(snap.now / 1000).toFixed(1)} s · {snap.finished ? "run complete, every ride decided" : "rides at 10/s, pings at 1/s per driver"}
            </text>
          </svg>
        </div>

        <div className="shards">
          {[
            { name: "mysql-shard-0", city: 2, cityName: "seattle", count: shard0, rows: snap.byShardCity[0] },
            { name: "mysql-shard-1", city: 1, cityName: "austin", count: shard1, rows: snap.byShardCity[1] },
          ].map((s) => (
            <div className="glass shard" key={s.name}>
              <div className="shard-head">
                <h3>{s.name}</h3>
                <span className="chip">
                  city {s.city} · {s.cityName}
                </span>
              </div>
              <div className="shard-tank" role="meter" aria-valuemin={0} aria-valuemax={target} aria-valuenow={s.count} aria-label={`${s.name} trips`}>
                <div className="shard-fill" style={{ height: `${Math.min(100, (s.count / target) * 100)}%` }} />
                <div className="shard-rows" aria-hidden="true">
                  {Array.from({ length: 24 }, (_, i) => (
                    <span key={i} className={i < Math.round((s.count / target) * 24) ? "on" : ""} />
                  ))}
                </div>
              </div>
              <div className="shard-foot">
                <span className="mono big">{s.count}</span>
                <span className="mono dim">trips · {Object.keys(s.rows).length <= 1 ? "one city per shard" : "mixed"}</span>
              </div>
            </div>
          ))}
          <div className="glass shard-note">
            <p className="mono dim">shard = floorMod(city_id, 2)</p>
            <p>
              City 1 lands on shard 1 and city 2 on shard 0, so the demo run fills them 302 / 301. Every service resolves
              the shard with the same pure function; there is no lookup service on the write path and nothing to
              coordinate between instances.
            </p>
            <button className="btn ghost" onClick={replay} type="button">
              Replay with a new seed
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
