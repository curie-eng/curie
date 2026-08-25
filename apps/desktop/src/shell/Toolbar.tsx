// The toolbar: part of the content pane, not a title bar above the window.
//
// A separate full-width title strip with a border under it is a web header. The
// platform's version is a unified toolbar that belongs to the pane it controls,
// carries that view's title and its actions, and lets content scroll underneath
// it. It is also the window's drag region, since the window has no OS chrome.

import { useApp, type Route } from "../bridge/app";
import { useRuns } from "../bridge/runs";
import { F, LINE, M, R, S, T } from "../tokens";
import { Button, Kbd, Spinner } from "../primitives";

const TITLES: Record<Route, { title: string; subtitle: string }> = {
  overview: { title: "Overview", subtitle: "What is happening right now" },
  build: { title: "Build", subtitle: "Author this bundle, then run it up the ladder" },
  resources: { title: "Resources", subtitle: "What each agent is consuming" },
  canvas: { title: "Canvas", subtitle: "Agents, integrations, and the infra under them" },
  commands: { title: "Commands", subtitle: "Everything the CLI can do" },
  activity: { title: "Activity", subtitle: "What this app has run" },
  settings: { title: "Settings", subtitle: "Connection, secrets, and what this app is" },
};

export function Toolbar({ scrolled }: { scrolled: boolean }) {
  const app = useApp();
  const runs = useRuns();
  const meta = TITLES[app.route];
  const isMac = app.env?.platform === "darwin";

  return (
    <header
      className="drag"
      data-tauri-drag-region
      style={{
        flex: "none",
        height: M.titlebar,
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "0 16px",
        background: S.content,
        // The separator appears only once content has scrolled under the
        // toolbar, which is exactly how the platform handles it. A permanent
        // rule under the header is the giveaway.
        borderBottom: `1px solid ${scrolled ? LINE.separator : "transparent"}`,
        transition: "border-color 160ms ease",
        // Windows and Linux draw their own controls on the right; leave room.
        paddingRight: isMac ? 16 : 140,
        zIndex: 20,
      }}
    >
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={{ ...F.title }}>{meta.title}</div>
        <div style={{ ...F.footnote, color: T.tertiary, marginTop: -1 }}>{meta.subtitle}</div>
      </div>

      {runs.active.length ? (
        <button
          className="no-drag"
          onClick={() => {
            runs.focus(runs.active[0].id);
            runs.setDrawerOpen(true);
          }}
          title="Show the running command"
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 7,
            border: "none",
            background: "rgba(255,255,255,0.10)",
            borderRadius: R.pill,
            padding: "4px 11px",
            ...F.caption,
            color: T.secondary,
            cursor: "default",
          }}
        >
          <Spinner size={11} />
          {runs.active.length === 1
            ? runs.active[0].action.replace(/\./g, " ")
            : `${runs.active.length} running`}
        </button>
      ) : null}

      <ApiPill />

      <Button
        tone="default"
        size="md"
        onClick={() => app.setPaletteOpen(true)}
        title="Search every curie command"
        style={{ gap: 8 }}
      >
        Run a command
        <Kbd>{isMac ? "⌘K" : "^K"}</Kbd>
      </Button>
    </header>
  );
}

function ApiPill() {
  const app = useApp();
  const api = app.api;
  const state = !api ? "unknown" : api.reachable ? "ok" : api.baseUrl ? "down" : "unset";

  const label =
    state === "ok" ? (api?.orgName ?? "Connected") : state === "down" ? "API offline" : "No API";

  return (
    <button
      className="no-drag"
      onClick={() => app.navigate("settings")}
      title={
        state === "ok"
          ? `Connected to ${api?.baseUrl}`
          : state === "down"
            ? `Cannot reach ${api?.baseUrl}`
            : "No platform API configured"
      }
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        border: "none",
        background: "rgba(255,255,255,0.06)",
        borderRadius: R.pill,
        padding: "4px 10px",
        ...F.caption,
        color: T.secondary,
        cursor: "default",
      }}
    >
      <span
        style={{
          width: 6,
          height: 6,
          borderRadius: 999,
          flex: "none",
          background:
            state === "ok" ? "#32d74b" : state === "down" ? "#ff453a" : "rgba(235,235,245,0.3)",
        }}
      />
      {label}
    </button>
  );
}
