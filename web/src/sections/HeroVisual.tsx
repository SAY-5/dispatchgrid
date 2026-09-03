import { useMemo } from "react";
import { useReducedMotion } from "framer-motion";
import { Rng } from "../sim/rng";
import { partitionFor, PARTITIONS } from "../sim/topics";

/**
 * Two cities on the left, six ride-requests partitions in the middle, one Streams matcher on the
 * right. Each city's requests ride to the partition murmur2 picks for its key, which is why a
 * city's decisions stay in order while cities run in parallel.
 */
const W = 960;
const H = 520;
const CITY = [
  { id: 1, name: "austin", cx: 120, cy: 140, tone: "var(--city-a)" },
  { id: 2, name: "seattle", cx: 120, cy: 380, tone: "var(--city-b)" },
];
const LANE_X0 = 360;
const LANE_X1 = 600;
const MATCH_X = 760;
const MATCH_Y = H / 2;

function laneY(p: number): number {
  return 90 + (p * (H - 180)) / (PARTITIONS - 1);
}

export function HeroVisual() {
  const reduce = useReducedMotion();
  const dots = useMemo(() => {
    const rnd = new Rng(2024);
    return CITY.map((c) =>
      Array.from({ length: 46 }, () => {
        const r = 62 * Math.sqrt(rnd.next());
        const a = rnd.next() * Math.PI * 2;
        return { x: c.cx + r * Math.cos(a), y: c.cy + r * Math.sin(a), s: 1.2 + rnd.next() * 1.8 };
      }),
    );
  }, []);

  const routes = CITY.map((c) => {
    const p = partitionFor(String(c.id));
    const y = laneY(p);
    const inPath = `M ${c.cx + 70} ${c.cy} C ${c.cx + 170} ${c.cy}, ${LANE_X0 - 90} ${y}, ${LANE_X0} ${y} L ${LANE_X1} ${y}`;
    const outPath = `M ${LANE_X1} ${y} C ${LANE_X1 + 80} ${y}, ${MATCH_X - 90} ${MATCH_Y}, ${MATCH_X - 44} ${MATCH_Y}`;
    return { ...c, p, y, path: `${inPath} ${outPath.replace(/^M [^C]+/, "")}` };
  });

  return (
    <svg className="hero-visual" viewBox={`0 0 ${W} ${H}`} role="img" aria-labelledby="hero-visual-title">
      <title id="hero-visual-title">
        Ride requests from two cities flow into the ride-requests topic, keyed by city onto separate partitions, and
        into one Kafka Streams matcher.
      </title>
      <defs>
        <linearGradient id="lane" x1="0" x2="1">
          <stop offset="0" stopColor="rgba(160,196,255,0.05)" />
          <stop offset="1" stopColor="rgba(160,196,255,0.22)" />
        </linearGradient>
        <radialGradient id="matcher-glow">
          <stop offset="0" stopColor="rgba(76,141,255,0.6)" />
          <stop offset="1" stopColor="rgba(76,141,255,0)" />
        </radialGradient>
        <filter id="soft" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="3" />
        </filter>
      </defs>

      {/* partition lanes */}
      {Array.from({ length: PARTITIONS }, (_, p) => {
        const y = laneY(p);
        const active = routes.some((r) => r.p === p);
        return (
          <g key={p}>
            <rect x={LANE_X0} y={y - 9} width={LANE_X1 - LANE_X0} height={18} rx={9} fill="url(#lane)" opacity={active ? 1 : 0.45} />
            <text x={LANE_X0 - 14} y={y + 4} textAnchor="end" className="svg-mono" fill={active ? "var(--ice)" : "var(--text-mute)"}>
              p{p}
            </text>
          </g>
        );
      })}
      <text x={(LANE_X0 + LANE_X1) / 2} y={44} textAnchor="middle" className="svg-mono" fill="var(--text-dim)">
        ride-requests  (6 partitions, key = city_id)
      </text>

      {/* cities */}
      {CITY.map((c, i) => (
        <g key={c.id}>
          <circle cx={c.cx} cy={c.cy} r={74} fill="none" stroke={c.tone} strokeOpacity={0.25} strokeDasharray="3 6" />
          {dots[i].map((d, j) => (
            <circle key={j} cx={d.x} cy={d.y} r={d.s} fill={c.tone} opacity={0.75} />
          ))}
          <text x={c.cx} y={c.cy + 100} textAnchor="middle" className="svg-mono" fill="var(--text-dim)">
            city {c.id} · {c.name}
          </text>
        </g>
      ))}

      {/* routes */}
      {routes.map((r) => (
        <path key={r.id} id={`route-${r.id}`} d={r.path} fill="none" stroke={r.tone} strokeOpacity={0.35} strokeWidth={1.2} />
      ))}

      {/* matcher */}
      <circle cx={MATCH_X} cy={MATCH_Y} r={110} fill="url(#matcher-glow)" />
      <circle cx={MATCH_X} cy={MATCH_Y} r={44} fill="rgba(7,11,20,0.9)" stroke="var(--sapphire)" strokeWidth={1.5} />
      <circle cx={MATCH_X} cy={MATCH_Y} r={58} fill="none" stroke="var(--ice)" strokeOpacity={0.18} strokeDasharray="2 5">
        {!reduce && <animateTransform attributeName="transform" type="rotate" from={`0 ${MATCH_X} ${MATCH_Y}`} to={`360 ${MATCH_X} ${MATCH_Y}`} dur="24s" repeatCount="indefinite" />}
      </circle>
      <text x={MATCH_X} y={MATCH_Y - 2} textAnchor="middle" className="svg-label" fill="var(--ice)">
        Streams
      </text>
      <text x={MATCH_X} y={MATCH_Y + 14} textAnchor="middle" className="svg-mono" fill="var(--text-dim)">
        matcher
      </text>
      <path d={`M ${MATCH_X + 44} ${MATCH_Y} L ${W - 24} ${MATCH_Y - 40}`} stroke="var(--ok)" strokeOpacity={0.5} fill="none" />
      <path d={`M ${MATCH_X + 44} ${MATCH_Y} L ${W - 24} ${MATCH_Y + 40}`} stroke="var(--taken)" strokeOpacity={0.3} fill="none" />
      <text x={W - 20} y={MATCH_Y - 48} textAnchor="end" className="svg-mono" fill="var(--ok)">
        ride-matches
      </text>
      <text x={W - 20} y={MATCH_Y + 58} textAnchor="end" className="svg-mono" fill="var(--taken)">
        ride-unmatched
      </text>

      {/* particles */}
      {!reduce &&
        routes.map((r) =>
          Array.from({ length: 9 }, (_, k) => (
            <circle key={`${r.id}-${k}`} r={3.2} fill={r.tone}>
              <animateMotion dur="5.2s" repeatCount="indefinite" begin={`${-(k * 5.2) / 9}s`} calcMode="spline" keySplines="0.4 0 0.6 1" keyTimes="0;1">
                <mpath href={`#route-${r.id}`} />
              </animateMotion>
            </circle>
          )),
        )}
      {!reduce &&
        Array.from({ length: 6 }, (_, k) => (
          <circle key={`out-${k}`} r={2.6} fill="var(--ok)">
            <animateMotion dur="2.4s" repeatCount="indefinite" begin={`${-(k * 2.4) / 6}s`} path={`M ${MATCH_X + 44} ${MATCH_Y} L ${W - 24} ${MATCH_Y - 40}`} />
          </circle>
        ))}
    </svg>
  );
}
