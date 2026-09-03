import { useCallback, useRef, useState } from "react";
import { useRunner } from "../components/useRunner";
import { DEMO_OPTIONS, Engine, type EngineSnapshot } from "../sim/engine";

const SPEEDS = [
  { label: "1x", v: 1 },
  { label: "4x", v: 4 },
  { label: "12x", v: 12 },
  { label: "max", v: 60 },
];

const REAL = { matched: 603, unmatched: 0, perMin: 603, p50: 14, p95: 53, p99: 271, shard0: 301, shard1: 302 };

function Sparkline({ values }: { values: number[] }) {
  const w = 600;
  const h = 90;
  const max = Math.max(12, ...values);
  const n = Math.max(values.length, 60);
  const pts = values.map((v, i) => `${(i / (n - 1)) * w},${h - (v / max) * (h - 6) - 2}`);
  const path = pts.length > 1 ? `M ${pts.join(" L ")}` : "";
  return (
    <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" className="spark" role="img" aria-label="Matches per second over the run">
      <line x1={0} y1={h - (10 / max) * (h - 6) - 2} x2={w} y2={h - (10 / max) * (h - 6) - 2} stroke="rgba(223,241,255,0.18)" strokeDasharray="3 6" />
      {path && <path d={path} fill="none" stroke="var(--sapphire)" strokeWidth={2} vectorEffect="non-scaling-stroke" />}
      {path && <path d={`${path} L ${pts[pts.length - 1].split(",")[0]},${h} L 0,${h} Z`} fill="rgba(76,141,255,0.14)" />}
    </svg>
  );
}

export function LoadRun() {
  const engineRef = useRef<Engine | null>(null);
  const [snap, setSnap] = useState<EngineSnapshot | null>(null);
  const [running, setRunning] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [series, setSeries] = useState<number[]>([]);
  const [runs, setRuns] = useState(0);

  const step = useCallback((dt: number) => {
    const e = engineRef.current;
    if (!e) return;
    if (e.finished) {
      setRunning(false);
      return;
    }
    e.tick(dt);
  }, []);
  const onFrame = useCallback(() => {
    const e = engineRef.current;
    if (!e) return;
    setSnap(e.snapshot());
    setSeries([...e.perSecondMatches]);
  }, []);
  useRunner(running, SPEEDS[speed].v, step, onFrame, 10);

  const start = () => {
    engineRef.current = new Engine({ ...DEMO_OPTIONS, seed: DEMO_OPTIONS.seed + runs * 13 });
    setSnap(engineRef.current.snapshot());
    setSeries([]);
    setRuns((r) => r + 1);
    setRunning(true);
  };
  const pause = () => setRunning((r) => !r);

  const s = snap;
  const t = s ? Math.min(60, s.now / 1000) : 0;
  const finished = s?.finished ?? false;
  const pct = Math.min(100, (t / 60) * 100);
  const rate = s ? (t >= 60 ? s.matched : s.matchesPerMinute) : 0;

  return (
    <section className="section load" id="load" aria-labelledby="load-title">
      <div className="wrap">
        <span className="eyebrow">04 · Load run</span>
        <h2 className="section-title" id="load-title">
          Sixty seconds, ten rides a second, every one decided.
        </h2>
        <p className="section-lede">
          The same run the load generator does against the real stack: 300 drivers per city across two cities pinging
          every second, rides at 10 per second for 60 s, round robin across cities. The counters come from the ported
          <code>MatchStats</code> (trailing 60 s window, reservoir percentiles). Service time is synthetic, fitted to the
          measured run; everything else is the real policy.
        </p>

        <div className="glass load-panel">
          <div className="load-controls">
            <button className="btn" type="button" onClick={start}>
              {s ? "Restart run" : "Start the 60 s run"}
            </button>
            <button className="btn ghost" type="button" onClick={pause} disabled={!s || finished}>
              {running ? "Pause" : "Resume"}
            </button>
            <div className="segmented speed" role="radiogroup" aria-label="Simulation speed">
              {SPEEDS.map((sp, i) => (
                <button key={sp.label} type="button" role="radio" aria-checked={speed === i} className={speed === i ? "on" : ""} onClick={() => setSpeed(i)}>
                  {sp.label}
                </button>
              ))}
            </div>
            <span className="mono dim load-clock">
              t = {t.toFixed(1)} s{finished ? " · complete" : running ? "" : s ? " · paused" : ""}
            </span>
          </div>
          <div className="progress" role="progressbar" aria-valuemin={0} aria-valuemax={60} aria-valuenow={Math.round(t)} aria-label="Run progress">
            <div className="progress-fill" style={{ width: `${pct}%` }} />
          </div>

          <div className="load-grid">
            <div className="stat">
              <span className="label">matches / min</span>
              <span className="value">{rate}</span>
              <span className="target mono">real run {REAL.perMin}</span>
            </div>
            <div className="stat">
              <span className="label">matched / submitted</span>
              <span className="value">
                {s?.matched ?? 0} / {s?.submitted ?? 0}
              </span>
              <span className="target mono">real run {REAL.matched} / {REAL.matched}</span>
            </div>
            <div className="stat">
              <span className="label">unmatched</span>
              <span className={`value ${s && s.unmatched === 0 ? "ok" : ""}`}>{s?.unmatched ?? 0}</span>
              <span className="target mono">real run {REAL.unmatched}</span>
            </div>
            <div className="stat">
              <span className="label">p50 / p95 / p99</span>
              <span className="value small-value">
                {s?.p50 ?? 0} / {s?.p95 ?? 0} / {s?.p99 ?? 0} ms
              </span>
              <span className="target mono">
                real run {REAL.p50} / {REAL.p95} / {REAL.p99} ms
              </span>
            </div>
            <div className="stat">
              <span className="label">shard-0 / shard-1</span>
              <span className="value">
                {s?.byShard[0] ?? 0} / {s?.byShard[1] ?? 0}
              </span>
              <span className="target mono">
                real run {REAL.shard0} / {REAL.shard1}
              </span>
            </div>
            <div className="stat">
              <span className="label">streams lag · available drivers</span>
              <span className="value small-value">
                {s?.lag ?? 0} · {s ? s.available.join(" + ") : "300 + 300"}
              </span>
              <span className="target mono">claim ttl 20 s returns drivers</span>
            </div>
          </div>

          <div className="load-spark">
            <p className="label-cap">matches per second</p>
            <Sparkline values={series} />
          </div>

          <pre className="summary mono" aria-label="Load summary in the format the real load generator prints">
{`== dispatchgrid load summary (browser port) ==
drivers             ${s ? s.available.length * 300 : 600} (300 per city), pings ok=${s?.pingsOk ?? 0} errors=${s?.httpErrors ?? 0}
rides submitted     ${s?.submitted ?? 0}, http errors=${s?.httpErrors ?? 0}, by shard {shard-0=${s?.byShard[0] ?? 0}, shard-1=${s?.byShard[1] ?? 0}}
matched             ${s?.matched ?? 0}
unmatched           ${s?.unmatched ?? 0}
matches per minute  ${rate} over the ${t.toFixed(0)} s run (trailing 60 s window: ${s?.matchesPerMinute ?? 0})
match latency       p50=${s?.p50 ?? 0} ms  p95=${s?.p95 ?? 0} ms  p99=${s?.p99 ?? 0} ms
shard distribution  shard-0: city 2 -> ${s?.byShardCity[0]?.[2] ?? 0} trips | shard-1: city 1 -> ${s?.byShardCity[1]?.[1] ?? 0} trips`}
          </pre>
        </div>
      </div>
    </section>
  );
}
