import { motion, useReducedMotion } from "framer-motion";
import { CountUp } from "../components/CountUp";
import { HeroVisual } from "./HeroVisual";

const STATS = [
  { value: 603, label: "matches / min", suffix: "" },
  { value: 603, label: "matched of 603", suffix: "" },
  { value: 14, label: "p50 match latency", suffix: " ms" },
  { value: 53, label: "p95 match latency", suffix: " ms" },
  { value: 0, label: "unmatched", suffix: "" },
];

export function Hero() {
  const reduce = useReducedMotion();
  const rise = (delay: number) =>
    reduce
      ? {}
      : { initial: { opacity: 0, y: 22 }, animate: { opacity: 1, y: 0 }, transition: { duration: 0.9, delay, ease: [0.22, 1, 0.36, 1] as const } };

  return (
    <section className="hero" id="top" aria-labelledby="hero-title">
      <div className="wrap hero-grid">
        <div className="hero-copy">
          <motion.span className="eyebrow" {...rise(0.05)}>
            Marketplace matching · browser port
          </motion.span>
          <motion.h1 id="hero-title" className="hero-title" {...rise(0.15)}>
            Six hundred rides a minute, <span className="ice">one owner per driver.</span>
          </motion.h1>
          <motion.p className="hero-lede" {...rise(0.3)}>
            Ride requests flow through city keyed Kafka topics into a Kafka Streams matcher. Redis GEO finds the nearest
            drivers and a Lua <code>SET NX</code> claim makes sure a driver is handed to exactly one ride. Trips land in
            city keyed MySQL shards, and the three services roll on Kubernetes with zero request errors under load.
            Every number below is from a measured run of the real stack; everything moving is the same policy, ported to
            TypeScript, running in this tab.
          </motion.p>
          <motion.div className="hero-actions" {...rise(0.4)}>
            <a className="btn" href="#load">
              Run the 60 s load
            </a>
            <a className="btn ghost" href="#geo">
              Drop a pickup
            </a>
          </motion.div>
          <motion.dl className="hero-stats" {...rise(0.5)}>
            {STATS.map((s) => (
              <div className="stat" key={s.label}>
                <dt className="label">{s.label}</dt>
                <dd className="value">
                  <CountUp value={s.value} suffix={s.suffix} duration={1.8} />
                </dd>
              </div>
            ))}
          </motion.dl>
        </div>
        <motion.div className="hero-stage glass" {...rise(0.25)}>
          <HeroVisual />
          <div className="hero-stage-caption">
            <span className="chip">
              <span className="dot" /> city 1 → partition 3
            </span>
            <span className="chip">
              <span className="dot" style={{ background: "var(--city-b)", boxShadow: "0 0 10px var(--city-b)" }} /> city 2 → partition 2
            </span>
            <span className="chip">shard = floorMod(city_id, 2)</span>
          </div>
        </motion.div>
      </div>
    </section>
  );
}
