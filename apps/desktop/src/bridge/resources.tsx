// The live resource feed, plus the short history the sparklines need.
//
// Frames arrive from the shell every couple of seconds. The provider keeps the
// latest frame and a rolling window of per-workload samples so the monitor can
// draw a trend without asking the shell for one -- Docker has no history to
// give, so the history is the app's job.

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { bridge } from "./bridge";
import type { DaemonCapacity, ResourceFrame, ResourceSample } from "./bridge";

/** How many frames of history to keep per workload. At the default 2s cadence
 *  this is about two minutes -- enough to see a runner spike and settle, short
 *  enough that memory stays flat over a long session. */
const HISTORY = 60;

export interface Series {
  readonly cpu: readonly (number | null)[];
  readonly mem: readonly (number | null)[];
}

interface ResourcesValue {
  readonly frame: ResourceFrame | null;
  readonly samples: readonly ResourceSample[];
  readonly history: ReadonlyMap<string, Series>;
  readonly error: string | null;
  readonly capacity: DaemonCapacity | null;
  readonly paused: boolean;
  readonly intervalMs: number;
  setPaused(paused: boolean): void;
  setIntervalMs(ms: number): void;
  /** Aggregate across everything currently running, with the daemon's ceiling
   *  alongside it. A percentage with no denominator is not information. */
  readonly totals: {
    cpu: number | null;
    mem: number | null;
    running: number;
    total: number;
    /** `cpus * 100`, i.e. the number `cpu` can reach. Null when unknown. */
    cpuCeiling: number | null;
    memCeiling: number | null;
  };
}

const Ctx = createContext<ResourcesValue | null>(null);

const EMPTY: readonly ResourceSample[] = [];

function push(series: readonly (number | null)[], value: number | null): (number | null)[] {
  const next = [...series, value];
  return next.length > HISTORY ? next.slice(next.length - HISTORY) : next;
}

export function ResourcesProvider({ children }: { children: ReactNode }) {
  const [frame, setFrame] = useState<ResourceFrame | null>(null);
  const [history, setHistory] = useState<Map<string, Series>>(new Map());
  const [paused, setPaused] = useState(false);
  const [intervalMs, setIntervalMs] = useState(2000);
  const seen = useRef(new Set<string>());

  useEffect(() => {
    const off = bridge().resources.onFrame((next) => {
      setFrame(next);
      setHistory((prev) => {
        const out = new Map(prev);
        const present = new Set<string>();
        for (const s of next.samples) {
          present.add(s.name);
          seen.current.add(s.name);
          const cur = out.get(s.name) ?? { cpu: [], mem: [] };
          out.set(s.name, { cpu: push(cur.cpu, s.cpuPercent), mem: push(cur.mem, s.memBytes) });
        }
        // A workload that disappeared keeps its trace for a while, padded with
        // nulls, so a container that just exited does not vanish mid-chart --
        // the gap is the information.
        for (const [name, series] of out) {
          if (present.has(name)) continue;
          const padded = { cpu: push(series.cpu, null), mem: push(series.mem, null) };
          if (padded.cpu.every((v) => v === null)) out.delete(name);
          else out.set(name, padded);
        }
        return out;
      });
    });
    return off;
  }, []);

  useEffect(() => {
    if (paused) {
      void bridge().resources.stop();
      return;
    }
    void bridge().resources.start(intervalMs);
    return () => void bridge().resources.stop();
  }, [paused, intervalMs]);

  // A fresh `[]` each render would make every downstream memo recompute; the
  // empty case has to be one stable identity.
  const samples = useMemo(() => frame?.samples ?? EMPTY, [frame]);

  const totals = useMemo(() => {
    const running = samples.filter((s) => s.state === "running");
    const cpu = running.reduce<number | null>(
      (acc, s) => (s.cpuPercent === null ? acc : (acc ?? 0) + s.cpuPercent),
      null,
    );
    const mem = running.reduce<number | null>(
      (acc, s) => (s.memBytes === null ? acc : (acc ?? 0) + s.memBytes),
      null,
    );
    const cpus = frame?.capacity?.cpus ?? null;
    return {
      cpu,
      mem,
      running: running.length,
      total: samples.length,
      cpuCeiling: cpus ? cpus * 100 : null,
      memCeiling: frame?.capacity?.memBytes ?? null,
    };
  }, [samples, frame]);

  const value = useMemo<ResourcesValue>(
    () => ({
      frame,
      samples,
      history,
      error: frame?.error ?? null,
      capacity: frame?.capacity ?? null,
      paused,
      intervalMs,
      setPaused,
      setIntervalMs,
      totals,
    }),
    [frame, samples, history, paused, intervalMs, totals],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useResources(): ResourcesValue {
  const value = useContext(Ctx);
  if (!value) throw new Error("useResources must be used inside <ResourcesProvider>");
  return value;
}
