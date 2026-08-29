// The resource feed behind the Docker-style monitor.
//
// `docker stats` is the model operators already have for "what is this thing
// costing me right now", so the feed is shaped like it. It is not sourced only
// from Docker, though: the `skill` and `local` tiers are containers, the
// `cluster` tier is pods, and the platform API only knows pod *names*, not their
// CPU. Rather than paper over that, every sample carries its `origin` and leaves
// unknown metrics null, so the UI can say "not measurable at this tier" instead
// of drawing a zero.

import { cpus, totalmem } from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { BrowserWindow } from "electron";

import type {
  DaemonCapacity,
  PortBinding,
  ResourceFrame,
  ResourceSample,
} from "../shared/contract.js";
import { CH } from "../shared/contract.js";
import { searchPath } from "./cli.js";

const execFileAsync = promisify(execFile);

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

const DOCKER_ENV = () => ({ ...process.env, PATH: searchPath() });

/** Parse Docker's human byte strings ("45.2MiB", "1.2kB", "0B"). Docker mixes
 *  SI and binary units in the same output, so both prefixes are handled. */
export function parseBytes(text: string | undefined): number | null {
  if (!text) return null;
  const m = /^([\d.]+)\s*([KMGTP]?i?)B?$/i.exec(text.trim());
  if (!m) return null;
  const value = Number(m[1]);
  if (!Number.isFinite(value)) return null;
  const unit = m[2].toUpperCase();
  const binary = unit.endsWith("I");
  const base = binary ? 1024 : 1000;
  const exp = ({ "": 0, K: 1, M: 2, G: 3, T: 4, P: 5 } as Record<string, number>)[
    unit.replace("I", "")
  ] ?? 0;
  return value * Math.pow(base, exp);
}

export function parsePercent(text: string | undefined): number | null {
  if (!text) return null;
  const value = Number(text.replace("%", "").trim());
  return Number.isFinite(value) ? value : null;
}

/** Split Docker's "A / B" pair fields (MemUsage, NetIO, BlockIO). */
function parsePair(text: string | undefined): [number | null, number | null] {
  if (!text) return [null, null];
  const [a, b] = text.split("/");
  return [parseBytes(a), parseBytes(b)];
}

interface DockerStatRow {
  ID?: string;
  Name?: string;
  CPUPerc?: string;
  MemUsage?: string;
  NetIO?: string;
  BlockIO?: string;
  PIDs?: string;
}

interface DockerPsRow {
  ID?: string;
  Names?: string;
  Image?: string;
  State?: string;
  Status?: string;
  CreatedAt?: string;
  Labels?: string;
  Ports?: string;
}

/**
 * The healthcheck verdict inside `docker ps`'s `Status` string.
 *
 * Docker has no dedicated health column in `ps`; it appends the verdict in
 * parentheses -- `Up 2 minutes (healthy)`, `Up 3 seconds (health: starting)`,
 * `Up 5 minutes (unhealthy)` -- and appends nothing at all when the image
 * declares no healthcheck. That last case is the one worth being careful about:
 * it must read as "no opinion", not as "starting", or a stack made partly of
 * unchecked services would never finish coming up on screen.
 */
export function parseHealth(status: string | undefined): "healthy" | "unhealthy" | "starting" | null {
  if (!status) return null;
  if (/\(health:\s*starting\)/i.test(status)) return "starting";
  if (/\(unhealthy\)/i.test(status)) return "unhealthy";
  if (/\(healthy\)/i.test(status)) return "healthy";
  return null;
}

/**
 * The exit status inside a stopped container's `Status` (`Exited (0) 8 minutes
 * ago`). Null while it is running -- there is no code yet, and zero would be a
 * lie in the one direction that matters.
 */
export function parseExitCode(status: string | undefined): number | null {
  const m = /^Exited\s*\((\d+)\)/i.exec(status ?? "");
  return m ? Number(m[1]) : null;
}

/**
 * Parse Docker's port summary.
 *
 * The raw form is a comma-separated list that lists each binding once per
 * address family -- `0.0.0.0:28000->8000/tcp, [::]:28000->8000/tcp` is ONE
 * published port, not two -- and mixes in bare `5432/tcp` entries for ports the
 * image exposes without publishing. Both need collapsing, or the UI shows an
 * operator twice as many ports as exist.
 */
export function parsePorts(raw: string | undefined): PortBinding[] {
  if (!raw) return [];
  const seen = new Map<string, PortBinding>();
  for (const part of raw.split(",")) {
    const text = part.trim();
    if (!text) continue;
    // `<addr>:<host>-><container>/<proto>` or just `<container>/<proto>`.
    const published = /^(?:.*:)?(\d+)->(\d+)\/(\w+)$/.exec(text);
    const exposed = /^(\d+)\/(\w+)$/.exec(text);
    let binding: PortBinding | null = null;
    if (published) {
      binding = {
        host: Number(published[1]),
        container: Number(published[2]),
        proto: published[3],
      };
    } else if (exposed) {
      binding = { host: null, container: Number(exposed[1]), proto: exposed[2] };
    }
    if (!binding) continue;
    // Key on the binding itself, so the v4 and v6 rows for one port collapse.
    seen.set(`${binding.host}:${binding.container}/${binding.proto}`, binding);
  }
  return [...seen.values()].sort((a, b) => (a.host ?? a.container) - (b.host ?? b.container));
}

function parseJsonLines<T>(stdout: string): T[] {
  const out: T[] = [];
  for (const line of stdout.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      out.push(JSON.parse(trimmed) as T);
    } catch {
      // A malformed line is one bad row, not a broken frame.
    }
  }
  return out;
}

function labelsOf(raw: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const pair of (raw ?? "").split(",")) {
    const idx = pair.indexOf("=");
    if (idx > 0) out[pair.slice(0, idx)] = pair.slice(idx + 1);
  }
  return out;
}

/** What kind of workload this is. Drives grouping, color, and which drill-downs
 *  the UI offers -- a runner has logs and an owning agent, Postgres has neither. */
/**
 * Canonical roles, in priority order.
 *
 * The compose service name is NOT usable as a role. A compose project prefixes
 * its own services, so the api service is called `curie-api`, and any consumer
 * that matches on a bare `api` silently drops it -- which is exactly what
 * happened: the canvas filtered infrastructure with `role.startsWith("api")`,
 * dropped `curie-api`, `curie-worker` and `curie-dispatcher`, and then failed
 * every edge lookup because the ids it wanted (`infra:dispatcher`) did not
 * exist. Three orphan boxes and no edges.
 *
 * Order is load bearing twice over:
 *   - one-shot jobs come first, so `rustfs-init` is a job rather than being
 *     mistaken for the object store it initialises;
 *   - `langfuse` precedes `worker`, so `langfuse-worker` is Langfuse rather than
 *     the platform's own worker.
 */
const ROLE_PATTERNS: readonly (readonly [RegExp, string])[] = [
  [/(^|[-_])(init|migrate|perms|setup|bootstrap|seed)([-_]|$)/, "job"],
  [/langfuse/, "langfuse"],
  [/clickhouse/, "clickhouse"],
  [/postgres|pgbouncer/, "postgres"],
  [/valkey|redis/, "valkey"],
  [/rustfs|minio|objectstore/, "objectstore"],
  [/otel|collector/, "otel"],
  [/ollama/, "model"],
  [/(^|[-_])api([-_]|$)/, "api"],
  [/(^|[-_])worker([-_]|$)/, "worker"],
  [/(^|[-_])dispatcher([-_]|$)/, "dispatcher"],
  [/(^|[-_])ui([-_]|$)/, "ui"],
];

/** Map a container to a canonical role, the compose service it came from, and
 *  the agent that owns it when that is knowable. `role` drives colour, grouping
 *  and the canvas; `service` is the name a human recognises from the compose
 *  file, and is what the table shows. */
export function classify(
  name: string,
  labels: Record<string, string>,
): { role: string; agent?: string; service?: string } {
  const service = labels["com.docker.compose.service"];

  // The platform's own sandboxes say who they are for, on a label.
  //
  // These are `curie-thread-<hash>-<hash>` -- the name carries a thread hash and
  // nothing else -- so before this they matched no pattern and fell through to
  // role `other`, project `standalone`, agent unknown. The one container on the
  // machine that IS an agent doing work was the one the Resources tab could not
  // attribute, under a heading that says "what each agent is using up". The
  // label is authoritative and needs no name parsing, so it is checked first and
  // covers `skill up` runners too if they ever grow one.
  const labelled = labels["curietech.ai/agent"];
  if (labelled) return { role: "runner", agent: labelled };

  // A `skill up` runner is not a compose service; its name also carries the
  // agent it was booted for.
  if (name.startsWith("curie-runner")) {
    const suffix = name.replace(/^curie-runner-?/, "");
    return { role: "runner", agent: suffix && suffix !== "local" ? suffix : undefined };
  }

  // A sandbox with no agent label is still a sandbox, not an "other".
  if (labels["curietech.ai/managed-by"] || name.startsWith("curie-thread-")) {
    return { role: "runner" };
  }

  const haystack = `${service ?? ""} ${name}`.toLowerCase();
  for (const [pattern, role] of ROLE_PATTERNS) {
    if (pattern.test(haystack)) return service ? { role, service } : { role };
  }

  // No pattern matched. The service name is still more informative than
  // "other", so keep it as the role rather than throwing the information away.
  if (service) return { role: service, service };
  return { role: "other" };
}

/**
 * The daemon's CPU and memory ceiling, cached.
 *
 * `docker info` is a heavier call than `stats` and the answer effectively never
 * changes, so it is fetched once a minute rather than on every frame. Note this
 * is the *daemon's* capacity, not the host's: on Docker Desktop the VM gets a
 * slice of the machine, and the VM's slice is the number a container percentage
 * should be read against.
 */
let capacityCache: { at: number; value: DaemonCapacity } | null = null;
const CAPACITY_TTL_MS = 60_000;

export async function daemonCapacity(): Promise<DaemonCapacity | null> {
  if (capacityCache && Date.now() - capacityCache.at < CAPACITY_TTL_MS) {
    return capacityCache.value;
  }
  try {
    const { stdout } = await execFileAsync("docker", ["info", "--format", "{{json .}}"], {
      env: DOCKER_ENV(),
      timeout: 10_000,
      maxBuffer: 2 * 1024 * 1024,
    });
    const info = JSON.parse(stdout) as {
      NCPU?: number;
      MemTotal?: number;
      ServerVersion?: string;
    };
    const value: DaemonCapacity = {
      cpus: typeof info.NCPU === "number" && info.NCPU > 0 ? info.NCPU : null,
      memBytes: typeof info.MemTotal === "number" && info.MemTotal > 0 ? info.MemTotal : null,
      serverVersion: info.ServerVersion ?? null,
      hostCpus: cpus().length || null,
      hostMemBytes: totalmem() || null,
    };
    capacityCache = { at: Date.now(), value };
    return value;
  } catch {
    // A failed probe must not poison the cache: the next frame retries.
    return capacityCache?.value ?? null;
  }
}

export function resetCapacityCache(): void {
  capacityCache = null;
}

export async function dockerAvailable(): Promise<boolean> {
  try {
    await execFileAsync("docker", ["version", "--format", "{{.Server.Version}}"], {
      env: DOCKER_ENV(),
      timeout: 5000,
    });
    return true;
  } catch {
    return false;
  }
}

export async function sampleDocker(): Promise<{ samples: ResourceSample[]; error?: string }> {
  const at = Date.now();
  try {
    // `stats` for the live numbers, `ps` for the identity (image, labels,
    // uptime) `stats` does not carry. Both are one-shot: a long-lived
    // `docker stats` stream would keep a pipe open across app suspend/resume,
    // and the poll interval is the UI's refresh rate anyway.
    const [statsRes, psRes] = await Promise.all([
      execFileAsync("docker", ["stats", "--no-stream", "--format", "{{json .}}"], {
        env: DOCKER_ENV(),
        timeout: 15_000,
        maxBuffer: 4 * 1024 * 1024,
      }),
      execFileAsync("docker", ["ps", "--all", "--format", "{{json .}}"], {
        env: DOCKER_ENV(),
        timeout: 15_000,
        maxBuffer: 4 * 1024 * 1024,
      }),
    ]);

    const stats = new Map<string, DockerStatRow>();
    for (const row of parseJsonLines<DockerStatRow>(statsRes.stdout)) {
      if (row.Name) stats.set(row.Name, row);
    }

    const samples: ResourceSample[] = [];
    for (const row of parseJsonLines<DockerPsRow>(psRes.stdout)) {
      const name = row.Names?.split(",")[0] ?? row.ID ?? "unknown";
      const labels = labelsOf(row.Labels);
      const project = labels["com.docker.compose.project"] ?? "";
      const { role, agent, service } = classify(name, labels);
      // Only Curie's own workloads. Someone's unrelated Postgres is not this
      // app's business, and listing it would make the monitor untrustworthy.
      const mine =
        name.startsWith("curie") || /curie/.test(project) || /curie/.test(row.Image ?? "");
      if (!mine) continue;

      const stat = stats.get(name);
      const [memBytes, memLimitBytes] = parsePair(stat?.MemUsage);
      const [netRxBytes, netTxBytes] = parsePair(stat?.NetIO);
      const [blockReadBytes, blockWriteBytes] = parsePair(stat?.BlockIO);

      samples.push({
        id: row.ID ?? name,
        name,
        origin: "docker",
        agent,
        project: project || null,
        service: service ?? null,
        role,
        state: row.State ?? "unknown",
        health: parseHealth(row.Status),
        exitCode: parseExitCode(row.Status),
        // A stopped container has no stats row at all; null (rendered as a dash)
        // is the honest answer, not 0.
        cpuPercent: stat ? parsePercent(stat.CPUPerc) : null,
        memBytes,
        memLimitBytes,
        netRxBytes,
        netTxBytes,
        blockReadBytes,
        blockWriteBytes,
        pids: stat?.PIDs ? Number(stat.PIDs) || null : null,
        startedAt: row.CreatedAt ?? null,
        image: row.Image ?? null,
        ports: parsePorts(row.Ports),
        at,
      });
    }
    samples.sort((a, b) => a.role.localeCompare(b.role) || a.name.localeCompare(b.name));
    return { samples };
  } catch (err) {
    return { samples: [], error: (err as Error).message };
  }
}

/**
 * Whether the local stack's worker is pinned to the offline fake model.
 *
 * Cached like `daemonCapacity`, and for the same reason: `docker inspect` is a
 * heavier call than `ps` and the answer only changes when the stack is
 * recreated. `null` means "no worker to ask", which is not the same as "no" and
 * must not render as a priced figure.
 *
 * The value matters far out of proportion to its size. Langfuse prices
 * observations from token counts and a price row for the model name, and does
 * so whether or not a request ever left the machine -- so a stack on the fake
 * model reports real dollars for runs that cost nothing.
 */
/** The compose service that runs agents, and therefore the one whose model mode
 *  decides whether a cost figure means anything. */
const WORKER_CONTAINER = "curie-curie-worker-1";

let fakeModelCache: { at: number; value: boolean | null } | null = null;

async function workerFakeModel(): Promise<boolean | null> {
  if (fakeModelCache && Date.now() - fakeModelCache.at < 60_000) return fakeModelCache.value;
  let value: boolean | null;
  try {
    const { stdout } = await execFileAsync(
      "docker",
      ["inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", WORKER_CONTAINER],
      { timeout: 6000, maxBuffer: 1 << 20 },
    );
    // Absent, empty, `0` and `false` all mean live; anything else means pinned
    // on. Matching the CLI, which treats the variable as set-or-not rather than
    // parsing it strictly.
    const line = stdout.split("\n").find((l) => l.startsWith("CURIE_FAKE_MODEL="));
    const raw = line?.slice("CURIE_FAKE_MODEL=".length).trim() ?? "";
    value = raw !== "" && raw !== "0" && raw.toLowerCase() !== "false";
  } catch {
    // No such container, or no daemon. `null`, not `false`: the UI must not say
    // a cost is real because a lookup failed.
    value = null;
  }
  fakeModelCache = { at: Date.now(), value };
  return value;
}

export async function collect(): Promise<ResourceFrame> {
  const [{ samples, error }, capacity, fakeModel] = await Promise.all([
    sampleDocker(),
    daemonCapacity(),
    workerFakeModel(),
  ]);
  return { at: Date.now(), samples, capacity, error, fakeModel };
}

export function startFeed(win: BrowserWindow, intervalMs: number): void {
  stopFeed();
  const tick = async () => {
    // Skip rather than queue: a slow Docker daemon should make the monitor
    // update less often, not build a backlog of stale frames.
    if (inFlight || win.isDestroyed()) return;
    inFlight = true;
    try {
      const frame = await collect();
      if (!win.isDestroyed()) win.webContents.send(CH.resFrame, frame);
    } finally {
      inFlight = false;
    }
  };
  void tick();
  timer = setInterval(() => void tick(), Math.max(1000, intervalMs));
}

export function stopFeed(): void {
  if (timer) clearInterval(timer);
  timer = null;
}

export async function containerLogs(id: string, tailLines: number): Promise<string> {
  try {
    const { stdout, stderr } = await execFileAsync(
      "docker",
      ["logs", "--tail", String(tailLines), id],
      { env: DOCKER_ENV(), timeout: 15_000, maxBuffer: 8 * 1024 * 1024 },
    );
    // Container logs legitimately arrive on both streams; interleaving order is
    // lost either way, so present them together rather than dropping stderr.
    return [stdout, stderr].filter(Boolean).join("");
  } catch (err) {
    return `Could not read logs for ${id}: ${(err as Error).message}`;
  }
}
