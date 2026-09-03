import { useCallback, useRef, useState } from "react";
import { useInView } from "framer-motion";
import { useRunner } from "../components/useRunner";
import { RolloutSim, type Deployment, type Pod } from "../sim/rollout";

const SPECS = [
  { name: "rider-request-service", port: 8081, replicas: 2, rate: 10, what: "POST /rides" },
  { name: "driver-location-service", port: 8082, replicas: 2, rate: 600, what: "POST /drivers/{id}/position" },
  { name: "matching-service", port: 8083, replicas: 2, rate: 10, what: "GET /matching/stats + Streams task" },
];

interface View {
  now: number;
  deployments: Deployment[];
  events: { at: number; text: string }[];
  rolling: boolean;
  durationMs: number | null;
  errors: number;
  served: number;
  rps: number;
}

function phaseLabel(p: Pod): string {
  switch (p.phase) {
    case "Pending":
      return "Pending";
    case "Starting":
      return "startup probe";
    case "Ready":
      return "Ready";
    case "Terminating":
      return "preStop, draining";
    default:
      return "gone";
  }
}

export function Rollout() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { margin: "-15% 0px" });
  const simRef = useRef(new RolloutSim(11, SPECS));
  const accRef = useRef<number[]>(SPECS.map(() => 0));
  const servedWindow = useRef<{ at: number; n: number }[]>([]);
  const snapshot = (): View => {
    const s = simRef.current;
    const cutoff = s.now - 1000;
    servedWindow.current = servedWindow.current.filter((w) => w.at >= cutoff);
    return {
      now: s.now,
      deployments: s.deployments.map((d) => ({ ...d, pods: d.pods.map((p) => ({ ...p })) })),
      events: [...s.events].reverse().slice(0, 9),
      rolling: s.rolling,
      durationMs: s.durationMs,
      errors: s.totalErrors,
      served: s.totalServed,
      rps: servedWindow.current.reduce((n, w) => n + w.n, 0),
    };
  };
  const [view, setView] = useState<View>(snapshot);

  const step = useCallback((dt: number) => {
    const s = simRef.current;
    // Load runs the whole time: each service sees its request rate routed through its Service.
    SPECS.forEach((spec, i) => {
      accRef.current[i] += (spec.rate * dt) / 1000;
      let n = 0;
      while (accRef.current[i] >= 1) {
        s.route(s.deployments[i]);
        accRef.current[i] -= 1;
        n++;
      }
      if (n > 0) servedWindow.current.push({ at: s.now, n });
    });
    s.tick(dt);
  }, []);
  const onFrame = useCallback(() => setView(snapshot()), []);
  useRunner(inView, 1, step, onFrame, 8);

  const start = () => {
    simRef.current.startRollout();
    setView(snapshot());
  };
  const reset = () => {
    simRef.current = new RolloutSim(11 + view.served, SPECS);
    accRef.current = SPECS.map(() => 0);
    servedWindow.current = [];
    setView(snapshot());
  };

  const seconds = view.durationMs === null ? null : (view.durationMs / 1000).toFixed(1);
  const finished = view.durationMs !== null && !view.rolling;

  return (
    <section className="section rollout" id="rollout" aria-labelledby="rollout-title" ref={ref}>
      <div className="wrap">
        <span className="eyebrow">03 · Rolling update</span>
        <h2 className="section-title" id="rollout-title">
          Replace every pod while the load never notices.
        </h2>
        <p className="section-lede">
          <code>maxUnavailable: 0</code>, <code>maxSurge: 1</code>: a surge pod is created, it must pass its readiness
          probe (Kafka, Redis, the shards, and the Streams state all answering) before one old pod is terminated, and the
          old pod sleeps 10 s in <code>preStop</code> so endpoints drain before <code>SIGTERM</code>. Requests only ever
          route to Ready pods. The real <code>make k8s-e2e</code> run replaced all three services in 23 s with zero
          request errors.
        </p>

        <div className="rollout-bar glass">
          <div className="stat">
            <span className="label">request errors</span>
            <span className={`value ${view.errors === 0 ? "ok" : "bad"}`}>{view.errors}</span>
          </div>
          <div className="stat">
            <span className="label">requests served</span>
            <span className="value">{view.served.toLocaleString()}</span>
          </div>
          <div className="stat">
            <span className="label">req / s</span>
            <span className="value">{view.rps}</span>
          </div>
          <div className="stat">
            <span className="label">rollout timer</span>
            <span className="value">{seconds === null ? "0.0 s" : `${seconds} s`}</span>
          </div>
          <div className="rollout-actions">
            <button className="btn" type="button" onClick={start} disabled={view.rolling}>
              {view.rolling ? "Rolling out" : finished ? "Roll out again" : "Roll out"}
            </button>
            <button className="btn ghost" type="button" onClick={reset}>
              Reset
            </button>
          </div>
        </div>

        <div className="deployments">
          {view.deployments.map((d, i) => (
            <div className="glass deployment" key={d.name}>
              <div className="deployment-head">
                <div>
                  <h3>{d.name}</h3>
                  <p className="dim mono">
                    :{d.port} · {SPECS[i].what} · {SPECS[i].rate} req/s
                  </p>
                </div>
                <span className={`chip ${d.rolledOutAt !== null && !view.rolling ? "chip-ok" : ""}`}>
                  rev {d.revision} · {d.pods.filter((p) => p.phase === "Ready").length}/{d.replicas} ready
                </span>
              </div>
              <ul className="pods">
                {d.pods.map((p) => (
                  <li key={p.name} className={`pod ${p.phase.toLowerCase()} ${p.revision === d.revision ? "new" : "old"}`}>
                    <span className="pod-light" aria-hidden="true" />
                    <span className="pod-name mono">{p.name}</span>
                    <span className="pod-phase">{phaseLabel(p)}</span>
                    <span className="pod-served mono">{p.served.toLocaleString()} req</span>
                  </li>
                ))}
              </ul>
              <p className="dim small">
                {d.errors === 0 ? "0 errors" : `${d.errors} errors`} · {d.served.toLocaleString()} served
              </p>
            </div>
          ))}
        </div>

        <div className="glass rollout-log" aria-live="polite">
          <p className="label-cap">controller events</p>
          <ul>
            {view.events.length === 0 && <li className="dim">Press Roll out to change ROLLOUT_MARKER on all three Deployments.</li>}
            {view.events.map((e, i) => (
              <li key={`${e.at}-${i}`}>
                <span className="mono dim">{(e.at / 1000).toFixed(1)}s</span> {e.text}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}
