// The one boundary between the Chromium renderer and the native shell.
//
// Everything the UI can do that a browser tab cannot -- run the `curie` binary,
// read Docker's stats stream, open a file picker, keep secrets off the page --
// crosses here and nowhere else. Keeping the surface this small is deliberate:
// it is the seam a different shell (Tauri, a CEF host, a headless test double)
// re-implements, so the renderer never learns which shell it is running in.
//
// Types only, plus channel-name constants. No Node imports: the renderer type
// checks against this file too.

/** A single `curie ...` invocation, described structurally rather than as a
 *  pre-joined string. The shell is what turns this into argv, so the renderer
 *  never builds a shell command and nothing is ever passed through a shell. */
export interface CliInvocation {
  /** Dotted manifest path, e.g. `local.deploy`. Resolved against the committed
   *  command manifest in the main process before anything is spawned. */
  readonly action: string;
  /** Positional values, in the manifest's declared order. */
  readonly positionals?: readonly string[];
  /** `--long` flag values keyed by the manifest's `long` name. `true` renders a
   *  bare flag, a string renders `--flag value`, `false`/undefined omit it. */
  readonly flags?: Readonly<Record<string, string | boolean | undefined>>;
  /** Working directory for the run (a plugin bundle dir, usually). */
  readonly cwd?: string;
  /** Ask the CLI for `--json` and parse the payload into `result`. */
  readonly json?: boolean;
}

/** What the shell resolved an invocation to, before it runs. The renderer shows
 *  this verbatim so the operator always sees the exact command being run --
 *  the UI is never a black box wrapped around the CLI. */
export interface ResolvedCommand {
  readonly argv: readonly string[];
  /** Display form, shell-quoted for copy/paste into a terminal. */
  readonly display: string;
  readonly cwd: string;
}

export type RunState = "pending" | "running" | "ok" | "failed" | "cancelled";

export interface RunChunk {
  readonly runId: string;
  readonly stream: "stdout" | "stderr";
  readonly text: string;
  /** Millis since the run started, for the timeline gutter. */
  readonly at: number;
}

export interface RunResult {
  readonly runId: string;
  readonly state: RunState;
  readonly exitCode: number | null;
  readonly durationMs: number;
  /** Parsed `--json` payload when `json` was requested and stdout parsed. */
  readonly result?: unknown;
  /** Set when `--json` was requested but stdout was not valid JSON. */
  readonly jsonError?: string;
}

export interface RunHandle {
  readonly runId: string;
  readonly command: ResolvedCommand;
}

/** A published or exposed port. `host` is null when the port is exposed by the
 *  image but not bound to the host, which is a different fact from "bound to
 *  port 0" and the UI renders it differently. */
export interface PortBinding {
  readonly host: number | null;
  readonly container: number;
  readonly proto: string;
}

/** What the Docker daemon has to give, which is the denominator every CPU and
 *  memory number in the UI should be read against.
 *
 *  Without it a summed CPU figure is meaningless: "121%" is alarming on a
 *  2-core machine and idle on a 12-core one. Docker Desktop gets this right by
 *  always showing usage over capacity, and so should this. */
export interface DaemonCapacity {
  /** What the DAEMON has, which on macOS and Windows is a VM's allocation and
   *  not the machine's. Containers cannot exceed it, so it is the right ceiling
   *  for a container total -- but showing it unlabelled next to a host-sized
   *  number invites the reader to think it is the machine. */
  readonly cpus: number | null;
  readonly memBytes: number | null;
  readonly serverVersion: string | null;
  /** What the machine has, so the UI can name the gap. Docker Desktop's memory
   *  allocation is a setting, so a limit well below the host is actionable
   *  rather than just surprising. */
  readonly hostCpus: number | null;
  readonly hostMemBytes: number | null;
}

/**
 * Where `curie local up` publishes the platform API on the host.
 *
 * The container listens on 8000 and `compose.dev.yaml` maps it to **28000**,
 * because a dev stack that squats on the obvious ports collides with everything
 * else on a developer's machine. This app defaulted to `localhost:8000`, which
 * nothing serves, so the app that starts the stack could not then talk to it:
 * every API-backed screen sat empty behind "not answering" while the stack was
 * completely healthy.
 *
 * It lives in the contract because BOTH sides need it and neither may own it.
 * The main process uses it as the stored default; Settings shows it as the hint
 * under the field. Those were two hardcoded copies, and the copy in Settings
 * went on telling people the wrong port after the default was fixed. The CLI's
 * `LOCAL_API_URL` (`cli/src/observability.rs`) is the source, and
 * `electron/store.test.ts` reads that file so the three cannot drift.
 */
export const LOCAL_API_URL = "http://localhost:28000";

/**
 * The key the local dev stack ships with, and the CLI's own default.
 *
 * `curie local deploy` and friends default `--api-key` to this
 * (`cli/src/main.rs`), which is why deploying from a terminal works with no
 * setup at all. This app sent no key, so it 401'd against the very stack it had
 * just started -- an app that starts a platform and then cannot read it is not
 * an app anybody would call easy.
 *
 * `localOnly` is the whole safety story and is enforced in `api.ts`: this is a
 * well-known development credential, so it is fine to assume against a loopback
 * address and unacceptable to send anywhere else. A stored key always wins --
 * this only fills the gap where none was ever set.
 */
export const LOCAL_API_KEY = "curie-dev-key";

/** Whether a base URL points at this machine, and so whether the dev key may be
 *  assumed. Parsed rather than string-matched: `http://localhost.evil.com` is
 *  not localhost and a `startsWith` check would send the key to it. */
export function isLoopback(baseUrl: string): boolean {
  try {
    const h = new URL(baseUrl).hostname.toLowerCase();
    return h === "localhost" || h === "127.0.0.1" || h === "::1" || h === "[::1]";
  } catch {
    return false;
  }
}

/** One row of the resource monitor. Shaped after `docker stats` because that is
 *  the mental model operators already have, but sourced from whichever tier is
 *  live: Docker for `skill`/`local`, the platform API's runner pods for
 *  `cluster`. `origin` says which, so the UI never implies a number is more
 *  authoritative than it is. */
export interface ResourceSample {
  readonly id: string;
  readonly name: string;
  readonly origin: "docker" | "kubernetes";
  /** Agent this workload belongs to, when it could be attributed. */
  readonly agent?: string;
  /** Compose project, from `com.docker.compose.project`. Null for a container
   *  started outside compose, such as a `curie skill up` runner. */
  readonly project: string | null;
  /** Compose service name, when this container is part of a project. */
  readonly service: string | null;
  /** `runner`, `api`, `worker`, `postgres`, ... -- drives grouping and color. */
  readonly role: string;
  readonly state: string;
  /** The container's healthcheck verdict, parsed out of `docker ps`'s `Status`
   *  (`Up 2 minutes (healthy)`). `null` when the image declares no healthcheck,
   *  which is a real and common answer -- not a failure and not "starting".
   *
   *  It is carried because it is the only honest measure of a stack coming up:
   *  `docker compose up --wait` waits on exactly this, so "ready" during a
   *  start is `running` plus a health verdict that is not `starting`. `state`
   *  alone flips to `running` the instant the process spawns and would draw a
   *  stack as up while every service was still booting. */
  readonly health: "healthy" | "unhealthy" | "starting" | null;
  /** Exit status of a container that has stopped, from `Exited (0) 8 minutes
   *  ago`. `null` while it is still running.
   *
   *  A compose stack is not all long-lived services: `curie-migrate`,
   *  `rustfs-init` and the two `*-perms` containers run once and exit 0, and
   *  that IS them succeeding. Without the code, "stopped" and "failed" are the
   *  same value and a healthy stack reports four broken services. */
  readonly exitCode: number | null;
  readonly cpuPercent: number | null;
  readonly memBytes: number | null;
  readonly memLimitBytes: number | null;
  readonly netRxBytes: number | null;
  readonly netTxBytes: number | null;
  readonly blockReadBytes: number | null;
  readonly blockWriteBytes: number | null;
  readonly pids: number | null;
  readonly startedAt: string | null;
  readonly image: string | null;
  readonly ports: readonly PortBinding[];
  readonly at: number;
}

export interface ResourceFrame {
  readonly at: number;
  readonly samples: readonly ResourceSample[];
  /** Null when the daemon could not be asked. The UI then omits the ceiling
   *  rather than inventing one. */
  readonly capacity: DaemonCapacity | null;
  /** Present when the source could not be reached; the UI degrades honestly
   *  rather than showing a frozen last-good frame as if it were live. */
  readonly error?: string;
  /** Whether the local stack's worker is pinned to the OFFLINE FAKE MODEL
   *  (`CURIE_FAKE_MODEL`). `null` when there is no worker to ask.
   *
   *  It is carried because a cost figure means nothing without it. Langfuse
   *  prices observations from token counts and a price row for the model name,
   *  and it does that whether or not a request was ever made -- so a stack on
   *  the fake model reports real dollars for runs that cost nothing. The
   *  Overview showed $0.04 of spend that had not happened. */
  readonly fakeModel: boolean | null;
}

/** The host OS. Spelled out rather than reusing `NodeJS.Platform`, because this
 *  file is type-checked by the renderer too, which has no Node types. */
export type Platform = "darwin" | "win32" | "linux" | "aix" | "freebsd" | "openbsd" | "sunos";

/** How the installed CLI differs from the manifest this app was built against.
 *  See `electron/ipc/manifest.ts` for why the two directions are not equally
 *  bad. Null when the comparison could not be made at all (no CLI, or its
 *  schema output could not be read). */
export interface ManifestDrift {
  readonly cliVersion: string | null;
  /** The CLI has these; the app does not offer them. */
  readonly missingFromApp: readonly string[];
  /** The app offers these; the CLI does not have them. */
  readonly missingFromCli: readonly string[];
}

/** What the shell knows about the machine it is on, refreshed on demand. */
export interface ShellEnvironment {
  readonly cliPath: string | null;
  readonly cliVersion: string | null;
  /** True when the resolved `curie` came from a source checkout, which is what
   *  gates the `dev` command namespace. */
  readonly sourceCheckout: boolean;
  readonly repoRoot: string | null;
  readonly dockerAvailable: boolean;
  readonly kubectlAvailable: boolean;
  readonly helmAvailable: boolean;
  readonly platform: Platform;
  /** Where a command runs when no bundle is open. Resolved in the shell
   *  (`CURIE_WORKSPACE` or the home directory), because the renderer cannot see
   *  either and must not print a directory it guessed. */
  readonly defaultCwd: string;
  readonly appVersion: string;
  readonly electronVersion: string;
  readonly chromeVersion: string;
  readonly drift: ManifestDrift | null;
}

/** A plugin bundle the operator has opened. The desktop app keeps a list of
 *  these the way an editor keeps recent projects; `curie skill *` and the
 *  `deploy` commands all run against one. */
export interface Workspace {
  readonly path: string;
  readonly name: string;
  /** Parsed `.claude-plugin/plugin.json`, when present. */
  readonly plugin?: { name?: string; version?: string; description?: string };
  readonly skills: readonly string[];
  readonly hasEvals: boolean;
  readonly hasMcp: boolean;
  readonly lastOpened: number;
}

/** Platform API request proxied through the shell. Going through main rather
 *  than `fetch` in the renderer is what lets the desktop app talk to an API on
 *  any host without CORS, and keeps the API key out of the page. */
import type { ThemeId } from "./themes.js";

/** What the operator asked for. "system" defers to the OS, which can only mean
 *  light or dark, so it resolves to one of the two base themes. */
export type ThemePreference = "system" | ThemeId;

/**
 * The preference and what it currently resolves to.
 *
 * Both are sent because they answer different questions: the control reflects
 * `preference` (so "System" stays selected), and the palette is driven by
 * `effective`. Deriving `effective` in the renderer would mean two places
 * deciding what the OS is currently doing, and they would disagree.
 */
export interface ThemeState {
  readonly preference: ThemePreference;
  /** The theme actually in force, which is what `data-theme` is set to. */
  readonly effective: ThemeId;
  /** Whether that theme wants a light or dark native window. Carried separately
   *  because `nativeTheme` only understands those two, and a theme id does not
   *  tell you which without the registry. */
  readonly appearance: "light" | "dark";
}

export interface ApiRequest {
  readonly method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  readonly path: string;
  readonly query?: Readonly<Record<string, string | number | boolean | undefined>>;
  readonly body?: unknown;
}

export interface ApiResponse<T = unknown> {
  readonly status: number;
  readonly ok: boolean;
  readonly body: T;
  readonly error?: string;
}

export interface ApiConnection {
  readonly baseUrl: string;
  /** Never the key itself -- only whether one is held, so the renderer can show
   *  connection state without the material ever entering the page. */
  readonly hasKey: boolean;
  /** WHAT is authorizing this console, which the two hosts answer differently
   *  and the UI has to be able to say out loud. In the shell it is the platform
   *  key the main process holds; in a browser tab it is a session cookie the
   *  page cannot read (ADR-0083). Undefined when nothing is authorizing it,
   *  which is not the same as unreachable. */
  readonly via?: "key" | "session";
  readonly reachable: boolean;
  readonly orgName?: string;
  readonly checkedAt: number;
}

/** The full preload surface, mirrored by `window.curie`. */
export interface CurieBridge {
  env(): Promise<ShellEnvironment>;

  cli: {
    run(inv: CliInvocation): Promise<RunHandle>;
    cancel(runId: string): Promise<void>;
    /** stdin for the interactive commands (`init`, `skill eval-init`). */
    write(runId: string, data: string): Promise<void>;
    onChunk(cb: (chunk: RunChunk) => void): () => void;
    onResult(cb: (result: RunResult) => void): () => void;
  };

  resources: {
    start(intervalMs: number): Promise<void>;
    stop(): Promise<void>;
    onFrame(cb: (frame: ResourceFrame) => void): () => void;
    /** Container/pod logs for the drill-down drawer. */
    logs(id: string, tailLines: number): Promise<string>;
  };

  /** Native pickers. A path is something the operator points at, not something
   *  they should have to transcribe; the renderer cannot open a file dialog and
   *  must not be given the filesystem, so it asks the shell. */
  dialog: {
    /** Absolute path, or `null` if the operator cancelled. */
    pick(opts: { kind: "file" | "directory"; title?: string }): Promise<string | null>;
    /** The absolute path behind a dropped `File`.
     *
     *  Electron removed `File.path` in 32, so a drop handler in the renderer
     *  gets a `File` with no way back to disk. `webUtils.getPathForFile` is the
     *  replacement and it lives in the preload, which is the only side that may
     *  hold it. */
    pathForFile(file: File): string | null;
  };

  workspace: {
    list(): Promise<readonly Workspace[]>;
    open(): Promise<Workspace | null>;
    add(path: string): Promise<Workspace | null>;
    forget(path: string): Promise<void>;
    /** Delete the bundle directory from disk, permanently, and forget it.
     *
     *  Guarded in the shell rather than trusted from the renderer: it refuses
     *  anything the app is not already tracking, anything that is not a bundle,
     *  and any directory that is itself a git repository. A stale list entry
     *  must not be able to erase a checkout. */
    delete(path: string): Promise<{ ok: true } | { ok: false; error: string }>;
    /** Scaffold a new agent from a template: the CLI writes the base, the
     *  supplied files are written over it, and the result is opened. */
    createAgent(opts: {
      parentDir: string;
      name: string;
      files: Record<string, string>;
    }): Promise<{ ok: true; workspace: Workspace } | { ok: false; error: string }>;
    /** Paths, relative to the bundle root, of the files a human edits. The
     *  walker lives in the shell because it needs the filesystem; what counts as
     *  worth showing is decided in the renderer. */
    files(root: string): Promise<readonly string[]>;
    readFile(root: string, relative: string): Promise<string>;
    writeFile(root: string, relative: string, contents: string): Promise<void>;
    revealInFileManager(path: string): Promise<void>;
  };

  api: {
    connection(): Promise<ApiConnection>;
    connect(baseUrl: string, apiKey: string | null): Promise<ApiConnection>;
    /** End this console's authorization and report what is left.
     *
     *  One verb, two meanings, because the hosts hold different credentials:
     *  the shell forgets the stored platform key, a browser tab revokes its
     *  session at the server so the cookie cannot be replayed. Both leave the
     *  console reachable and unauthorized, which is the state sign-in expects. */
    signOut(): Promise<ApiConnection>;
    request<T = unknown>(req: ApiRequest): Promise<ApiResponse<T>>;
  };

  secrets: {
    /** Names only. Values live in the CLI's private storage and never transit
     *  this bridge in either direction. */
    list(): Promise<readonly string[]>;
    set(name: string, value: string): Promise<void>;
    unset(name: string): Promise<void>;
  };

  graph: {
    load(): Promise<unknown>;
    save(doc: unknown): Promise<void>;
  };

  theme: {
    get(): Promise<ThemeState>;
    set(preference: ThemePreference): Promise<ThemeState>;
    /** Fires when the OS appearance changes under a "system" preference. */
    onChange(cb: (state: ThemeState) => void): () => void;
  };

  shell: {
    openExternal(url: string): Promise<void>;
    copy(text: string): Promise<void>;
  };
}

export const CH = {
  env: "curie:env",
  cliRun: "curie:cli:run",
  cliCancel: "curie:cli:cancel",
  cliWrite: "curie:cli:write",
  cliChunk: "curie:cli:chunk",
  cliResult: "curie:cli:result",
  resStart: "curie:res:start",
  resStop: "curie:res:stop",
  resFrame: "curie:res:frame",
  resLogs: "curie:res:logs",
  dialogPick: "curie:dialog:pick",
  wsList: "curie:ws:list",
  wsOpen: "curie:ws:open",
  wsAdd: "curie:ws:add",
  wsForget: "curie:ws:forget",
  wsDelete: "curie:ws:delete",
  wsCreate: "curie:ws:create",
  wsFiles: "curie:ws:files",
  wsRead: "curie:ws:read",
  wsWrite: "curie:ws:write",
  wsReveal: "curie:ws:reveal",
  apiConnection: "curie:api:connection",
  apiConnect: "curie:api:connect",
  apiSignOut: "curie:api:sign-out",
  apiRequest: "curie:api:request",
  secList: "curie:sec:list",
  secSet: "curie:sec:set",
  secUnset: "curie:sec:unset",
  graphLoad: "curie:graph:load",
  graphSave: "curie:graph:save",
  themeGet: "curie:theme:get",
  themeSet: "curie:theme:set",
  themeChanged: "curie:theme:changed",
  openExternal: "curie:shell:open",
  copy: "curie:shell:copy",
} as const;
