// The toolbar: part of the content pane, not a title bar above the window.
//
// A separate full-width title strip with a border under it is a web header. The
// platform's version is a unified toolbar that belongs to the pane it controls,
// carries that view's title and its actions, and lets content scroll underneath
// it. It is also the window's drag region, since the window has no OS chrome.

import { useState } from "react";

import { useApp, type Route } from "../bridge/app";
import { useRuns } from "../bridge/runs";
import { F, LINE, M, PANE_FADE, R, S, STATUS, T } from "../tokens";
import { Glyph, PanelToggle, Segmented, Spinner } from "../primitives";
import { PROMPT } from "../primitives/glyphs";
import { SignIn } from "../views/SignIn";

const TITLES: Record<Route, { title: string; subtitle: string }> = {
  overview: { title: "Overview", subtitle: "What is happening right now" },
  build: { title: "Build", subtitle: "Make an agent, try it, then put it to work" },
  tiers: { title: "Where it runs", subtitle: "Just you, this computer, or your whole team" },
  observability: {
    title: "Activity",
    subtitle: "What your agents have done, and what it cost",
  },
  resources: { title: "Resources", subtitle: "What each agent is using up" },
  canvas: { title: "Canvas", subtitle: "Your agents, what they connect to, and what runs them" },
  // Two panes of one tab, so they share a title and differ in the subtitle.
  commands: { title: "Commands", subtitle: "Every command, for when you would rather type one" },
  activity: { title: "Commands", subtitle: "Everything this app has run" },
  settings: { title: "Settings", subtitle: "Connection, secrets, and what this app is" },
};

export function Toolbar({
  scrolled,
  railCollapsed,
  onToggleRail,
}: {
  scrolled: boolean;
  railCollapsed: boolean;
  onToggleRail(): void;
}) {
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
      {/* Leading, where every native window puts it, and outside the title
          block so the title still starts on the pane's own margin. */}
      <PanelToggle
        collapsed={railCollapsed}
        onToggle={onToggleRail}
        label="the sidebar"
        style={{ marginLeft: -4, marginRight: -2 }}
      />

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

      <ConsoleButton />
      <ApiPill />

    </header>
  );
}

/**
 * The console toggle. Always here, on the right of the toolbar.
 *
 * It was briefly conditional -- rendered only while the console was hidden, on
 * the argument that a button offering to show you something already on screen
 * is redundant chrome. That argument is wrong for this control and the symptom
 * said so: the console is usually visible, so usually there was no button in the
 * corner at all, which is indistinguishable from the dead end it was added to
 * fix. A control that is only there in the state you are not in cannot be found
 * by looking.
 *
 * So it is permanent and it is a toggle: pressed while the console is showing,
 * and it hides it; unpressed while hidden, and it brings it back with the cursor
 * in the prompt (the console focuses itself -- this button unmounts nothing now,
 * but the transition effect is still the reliable place for it).
 *
 * The glyph carries it alone. A prompt is about as legible as an icon gets --
 * it is what every terminal in the world puts in its own corner -- so the word
 * "Console" beside it was a caption on a picture of itself. `aria-label` and
 * `aria-pressed` carry the name and the state.
 *
 * Visible means the GLYPH is strong, not that the button is a coloured badge. A
 * filled accent disc was tried and read as a status light: the controls beside
 * it are pills reporting state, so a third round coloured thing joins that set
 * rather than standing out from it. Primary ink, no fill until it is pressed or
 * hovered, which is what the platform's own toolbar toggles do.
 */
function ConsoleButton() {
  const runs = useRuns();
  const [hover, setHover] = useState(false);
  const showing = !runs.consoleHidden;

  return (
    <button
      className="no-drag"
      onClick={() => {
        if (showing) return runs.setConsoleHidden(true);
        runs.setConsoleHidden(false);
        runs.setConsoleOpen(true);
      }}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      aria-label="Console"
      aria-pressed={showing}
      title={
        showing
          ? "Hide the console (⌘L focuses it)"
          : "Show the console and put the cursor in it (⌘L)"
      }
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: 27,
        height: 27,
        border: "none",
        background: hover ? S.controlHover : showing ? S.control : "transparent",
        borderRadius: R.control,
        padding: 0,
        color: showing ? T.primary : T.secondary,
        cursor: "default",
      }}
    >
      <Glyph d={PROMPT} size={16} />
    </button>
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
  const [signingIn, setSigningIn] = useState(false);
  // Reachable and authorized are different facts, and this pill used to collapse
  // them. In a browser tab the API answers every call with 401 until a console
  // session exists (ADR-0083), so "reachable" was true and the pill said
  // "Connected" over a screen with nothing on it. A green light above an empty
  // table is worse than a red one.
  const state = !api
    ? "unknown"
    : api.reachable && api.hasKey
      ? "ok"
      : api.reachable
        ? "unauthorized"
        : api.baseUrl
          ? "down"
          : "unset";

  const label =
    state === "ok"
      ? (api?.orgName ?? "Connected")
      : state === "unauthorized"
        ? "Sign in"
        : state === "down"
          ? "API offline"
          : "No API";

  return (
    <>
    {signingIn ? <SignIn onClose={() => setSigningIn(false)} /> : null}
    <button
      className="no-drag"
      // The pill is the only place that reports this state, so it should also be
      // the way out of it: unauthorized opens the exchange, everything else goes
      // to the connection settings.
      onClick={() => (state === "unauthorized" ? setSigningIn(true) : app.navigate("settings"))}
      title={
        state === "ok"
          ? `Connected to ${api?.baseUrl}`
          : state === "unauthorized"
            ? `${api?.baseUrl} is reachable but this console is not signed in`
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
        color:
          state === "down"
            ? STATUS.danger
            : state === "unauthorized"
              ? STATUS.warn
              : state === "unset"
                ? T.quaternary
                : T.secondary,
        cursor: "default",
      }}
    >
      {label}
    </button>
    </>
  );
}
