// The toolbar: part of the content pane, not a title bar above the window.
//
// A separate full-width title strip with a border under it is a web header. The
// platform's version is a unified toolbar that belongs to the pane it controls,
// carries that view's title and its actions, and lets content scroll underneath
// it. It is also the window's drag region, since the window has no OS chrome.

import { useApp, type Route } from "../bridge/app";
import { useRuns } from "../bridge/runs";
import { F, LINE, M, PANE_FADE, R, S, STATUS, T } from "../tokens";
import { Segmented, Spinner } from "../primitives";

const TITLES: Record<Route, { title: string; subtitle: string }> = {
  overview: { title: "Overview", subtitle: "What is happening right now" },
  build: { title: "Build", subtitle: "Author this bundle, then run it up the ladder" },
  tiers: { title: "Tiers", subtitle: "The same agent, on a laptop or on a cluster" },
  resources: { title: "Resources", subtitle: "What each agent is consuming" },
  canvas: { title: "Canvas", subtitle: "Agents, integrations, and the infra under them" },
  // Two panes of one tab, so they share a title and differ in the subtitle.
  commands: { title: "Commands", subtitle: "Everything the CLI can do" },
  activity: { title: "Commands", subtitle: "Every command this app has run" },
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
        // The same ramp as the pane it belongs to, from the same origin: a solid
        // strip over a translucent pane would read as a title bar stuck on top,
        // and a strip that did not fade would put a hard corner back at the top
        // of the seam the pane just softened.
        background: PANE_FADE,
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

      <PaneSwitch />

      {runs.active.length ? (
        <button
          className="no-drag"
          onClick={() => {
            runs.focus(runs.active[0].id);
            runs.setConsoleOpen(true);
          }}
          title="Show the running command"
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 7,
            border: "none",
            background: S.control,
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

    </header>
  );
}

/**
 * Reference or History, for the one tab that has both.
 *
 * The route IS the pane rather than local state in the view: the native menu,
 * the Overview's "All activity" button and the sidebar all deep-link straight to
 * one of them, and a pane kept in component state would be unreachable from any
 * of those. It lives in the toolbar because the toolbar owns this view's chrome,
 * and because the two panes want different frame padding -- Reference bleeds to
 * the pane edges, History is a padded document -- so a control rendered inside
 * either one would have to exist twice.
 */
function PaneSwitch() {
  const app = useApp();
  if (app.route !== "commands" && app.route !== "activity") return null;
  return (
    <span className="no-drag">
      <Segmented<"commands" | "activity">
        size="sm"
        value={app.route}
        onChange={(next) => app.navigate(next)}
        options={[
          { value: "commands", label: "Reference", title: "Every command the CLI has" },
          { value: "activity", label: "History", title: "What this app has run, with full output" },
        ]}
      />
    </span>
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
        background: S.subtle,
        borderRadius: R.pill,
        padding: "4px 10px",
        ...F.caption,
        // The label says which state this is ("API offline", "No API", or the org
        // name), so a coloured dot beside it only repeats the word. Colour the
        // word instead: a connected org reads as calm secondary text, and a
        // failure is the only thing that takes a warning colour.
        color: state === "down" ? STATUS.danger : state === "unset" ? T.quaternary : T.secondary,
        cursor: "default",
      }}
    >
      {label}
    </button>
  );
}
