import { useEffect, useRef } from "react";

/**
 * Calls step(dtMs) on every animation frame with dt scaled by speed, and onFrame at most
 * ~fps times per second so React state stays cheap. Frame timestamps come from rAF, so the
 * simulation never reads the wall clock during render.
 */
export function useRunner(running: boolean, speed: number, step: (dtMs: number) => void, onFrame: () => void, fps = 12) {
  const stepRef = useRef(step);
  const frameRef = useRef(onFrame);
  stepRef.current = step;
  frameRef.current = onFrame;

  useEffect(() => {
    if (!running) return;
    let raf = 0;
    let last: number | null = null;
    let acc = 0;
    const budget = 1000 / fps;
    const loop = (ts: number) => {
      if (last === null) last = ts;
      const dt = Math.min(250, ts - last);
      last = ts;
      stepRef.current(dt * speed);
      acc += dt;
      if (acc >= budget) {
        acc = 0;
        frameRef.current();
      }
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [running, speed, fps]);
}
