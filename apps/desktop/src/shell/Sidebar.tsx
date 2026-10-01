// The sidebar: translucent, full height, and the thing the window is organised
// around.
//
// This is the single strongest native cue available. A window split into a
// translucent source list and an opaque content pane is what Finder, Mail,
// Notes, Xcode and System Settings all look like; a flat coloured strip next to
// a flat coloured page is what a website looks like. The translucency is real --
// the shell gives the window vibrancy and this surface paints nothing, so the
// desktop actually shows through.
//
// It also carries the traffic lights, which is why the top has a reserved inset:
// on macOS the OS draws them over our content.

import type { ReactNode } from "react";
import { useState } from "react";

import { useApp, type Route } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { ACCENT, F, M, R, S, STATUS, T } from "../tokens";
import { Glyph as Icon, Spinner } from "../primitives";
import { PROMPT } from "../primitives/glyphs";
import { bytes, percent } from "../lib/format";

interface Item {
  readonly id: Route;
  readonly label: string;
  readonly hint: string;
  readonly icon: ReactNode;
}

/**
 * The rail, in the order the work happens: look at what is running, author a
 * bundle, see how it is wired, watch what it costs.
 *
 * Tiers is last on purpose. It is a reference for a concept -- the same verbs
 * against a bigger deployment -- rather than somewhere you operate, and sitting
 * third it read as a step in the flow that nobody could place. A row high in the
 * rail is a claim that you should start there.
 */
const ITEMS: readonly Item[] = [
  {
    id: "overview",
    label: "Overview",
    hint: "Health, agents, spend",
    icon: <Icon d="M2.2 8.6 8 3.2l5.8 5.4M4 7.6V13h8V7.6" />,
  },
  {
    id: "build",
    label: "Build",
    hint: "Make an agent and put it to work",
    icon: <Icon d="M3.4 12.6h9.2M5.4 10.2V5.6M8 10.2V3.4M10.6 10.2V7.2" />,
  },
  {
    id: "canvas",
    label: "Canvas",
    hint: "How your agents are wired together",
    icon: <Icon d="M3.2 3.6h3.4v3.2H3.2zM9.4 9.2h3.4v3.2H9.4zM6.6 5.2h2.2a2 2 0 0 1 2 2v2" />,
  },
  {
    id: "observability",
    label: "Activity",
    hint: "What your agents have done, and what it cost",
    icon: <Icon d="M2.4 11.6 6 6.8l2.8 2.6L13.6 4" />,
  },
  {
    id: "resources",
    label: "Resources",
    hint: "What is using CPU, memory and disk",
    icon: <Icon d="M2.6 12.6V7.4M6.2 12.6V3.4M9.8 12.6V8.8M13.4 12.6V5.6" />,
  },
  {
    id: "tiers",
    label: "Where it runs",
    hint: "Just you, this computer, or your team",
    // Three rungs, ascending: the ladder itself.
    icon: <Icon d="M3 12.6h3.2V9.4H3zM6.4 12.6h3.2V6.2H6.4zM9.8 12.6H13V3.4H9.8z" />,
  },
];

export function Sidebar({ collapsed = false }: { collapsed?: boolean }) {
  const app = useApp();
  const runs = useRuns();
  const res = useResources();

  return (
    <nav
      className="drag"
      data-tauri-drag-region
      style={{
        width: collapsed ? M.sidebarCollapsed : M.sidebar,
        flex: "none",
        // Animated, because the pane beside it moves too: a rail that jumps
        // between two widths reads as a re-layout, and one that slides reads as
        // the same rail in a different state.
        transition: "width 160ms cubic-bezier(0.32, 0.72, 0, 1)",
        overflow: "hidden",
        // Paints nothing: the window's vibrancy is the background.
        background: S.sidebar,
        display: "flex",
        flexDirection: "column",
        paddingTop: M.trafficLights - 24,
      }}
    >
      {/* No bundle picker here. The open bundle is the Build tab's subject and it
          carries the switcher; a second one in the window's chrome implied the
          bundle was global chrome, and left two places to change the same thing.
          The global ACTION still exists where a global action belongs: File ->
          Open Bundle (Cmd+O). */}
      <div style={{ padding: "14px 10px 0", display: "flex", flexDirection: "column", gap: 1 }}>
        {ITEMS.map((item) => (
          <NavItem
            key={item.id}
            item={item}
            collapsed={collapsed}
            active={app.route === item.id}
            onClick={() => app.navigate(item.id)}
            badge={
              item.id === "resources" && res.totals.running
                ? String(res.totals.running)
                : undefined
            }
          />
        ))}
      </div>

      <div style={{ flex: 1 }} />

      <MachineStatus collapsed={collapsed} />

      {/* Commands and Settings sit together at the foot, below the flex spacer.
          The rail above is where the work is: a bundle, the tiers it runs on,
          what is consuming the machine. Commands is where you go to look
          something up or to read back what ran -- both panes are ABOUT commands
          rather than places you operate -- and a tab like that sitting in the
          primary rail reads as somewhere you are supposed to start. It is not.

          Its badge is the number of commands running right now, which is why
          Activity does not need a rail slot of its own: the one signal it
          carried lives here. */}
      <div style={{ padding: "0 10px 10px", display: "flex", flexDirection: "column", gap: 1 }}>
        <NavItem
          item={{
            id: "commands",
            label: "Commands",
            hint: "Every command, where each one lives in this app, and what has run",
            icon: <Icon d={PROMPT} />,
          }}
          collapsed={collapsed}
          // Active for either pane: History is not a separate destination.
          active={app.route === "commands" || app.route === "activity"}
          onClick={() => app.navigate("commands")}
          badge={runs.active.length ? String(runs.active.length) : undefined}
          busy={runs.active.length > 0}
        />
        <NavItem
          item={{
            id: "settings",
            label: "Settings",
            hint: "Connection, secrets, about",
            icon: (
              <Icon d="M8 5.9A2.1 2.1 0 1 0 8 10.1 2.1 2.1 0 0 0 8 5.9M8 2.4v1.4M8 12.2v1.4M2.4 8h1.4M12.2 8h1.4M4.05 4.05l1 1M10.95 10.95l1 1M11.95 4.05l-1 1M5.05 10.95l-1 1" />
            ),
          }}
          collapsed={collapsed}
          active={app.route === "settings"}
          onClick={() => app.navigate("settings")}
        />
      </div>
    </nav>
  );
}

function NavItem({
  item,
  active,
  onClick,
  badge,
  busy,
  collapsed,
}: {
  item: Item;
  active: boolean;
  onClick(): void;
  badge?: string;
  busy?: boolean;
  collapsed?: boolean;
}) {
  const [hover, setHover] = useState(false);
  return (
    <button
      className="no-drag"
      onClick={onClick}
      // Collapsed, the tooltip is the only place the label exists, so it has to
      // carry the name as well as the hint. `aria-label` regardless: a button
      // whose only content is an icon is unlabelled to a screen reader.
      title={collapsed ? `${item.label} — ${item.hint}` : item.hint}
      aria-label={item.label}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 9,
        width: "100%",
        textAlign: "left",
        border: "none",
        // A rounded filled pill, inset from the sidebar edge. A full-bleed row
        // with a coloured left border is the web version of this.
        borderRadius: R.control,
        padding: collapsed ? "6px 0" : "5px 8px",
        justifyContent: collapsed ? "center" : undefined,
        // Collapsed, the badge floats rather than taking a column: in the flow
        // it pushed the icon off centre, and one row's glyph sitting left of
        // every other row's reads as a misalignment rather than as a count.
        position: collapsed ? "relative" : undefined,
        background: active ? S.controlHover : hover ? S.subtle : "transparent",
        color: active ? T.primary : T.secondary,
        fontSize: 13,
        fontWeight: active ? 600 : 500,
        letterSpacing: -0.08,
        cursor: "default",
        transition: "background 90ms ease",
      }}
    >
      <span style={{ color: active ? ACCENT : T.tertiary, display: "flex" }}>{item.icon}</span>
      {collapsed ? null : <span style={{ flex: 1 }}>{item.label}</span>}
      {busy ? <Spinner size={10} color={ACCENT} /> : null}
      {badge ? (
        <span
          style={{
            ...F.footnote,
            color: T.tertiary,
            fontVariantNumeric: "tabular-nums",
            ...(collapsed
              ? { position: "absolute" as const, top: 3, right: 7, lineHeight: 1 }
              : null),
          }}
        >
          {badge}
        </span>
      ) : null}
    </button>
  );
}

/** What this machine can actually do, at the foot of the sidebar.
 *
 *  Everything the desktop app adds over the web console depends on local tooling,
 *  so its absence has to be visible rather than showing up later as an
 *  inscrutable command failure. A compact block here rather than a full-width
 *  footer bar: a status strip spanning the window is a browser habit. */
function MachineStatus({ collapsed }: { collapsed?: boolean }) {
  const app = useApp();
  const res = useResources();
  const env = app.env;

  const tools: { name: string; ok: boolean | null; detail: string }[] = [
    {
      name: "curie",
      ok: env ? !!env.cliPath : null,
      detail: env?.cliPath ?? "not found on PATH — this app cannot run anything",
    },
    {
      name: "docker",
      ok: env ? env.dockerAvailable : null,
      detail: env?.dockerAvailable ? "reachable" : "not reachable — skill and local tiers need it",
    },
    {
      name: "kubectl",
      ok: env ? env.kubectlAvailable : null,
      detail: env?.kubectlAvailable ? "found" : "not found — the cluster tier is unavailable",
    },
    {
      name: "helm",
      ok: env ? env.helmAvailable : null,
      detail: env?.helmAvailable ? "found" : "not found — curie cluster up cannot run",
    },
  ];

  const drifted =
    !!env?.drift && (env.drift.missingFromApp.length > 0 || env.drift.missingFromCli.length > 0);

  // Collapsed there is no room for four tool names, and cramming them in one
  // per line would make a quiet corner into a column of text. The invariant
  // holds either way -- only absence gets ink -- so what survives the collapse
  // is the absence itself: a mark when something is missing, nothing when the
  // machine is fine. Expanding gets you the names back.
  if (collapsed) {
    const missing = tools.filter((t) => t.ok === false);
    if (!missing.length && !drifted) return null;
    return (
      <div className="no-drag" style={{ padding: "0 0 12px", textAlign: "center" }}>
        <button
          onClick={() => app.navigate("settings")}
          title={
            (missing.length ? missing.map((t) => `${t.name}: ${t.detail}`).join("\n") : "") +
            (drifted ? "\nThis app was built against a different version of Curie." : "")
          }
          aria-label="Something on this machine needs attention"
          style={{
            border: "none",
            background: "transparent",
            padding: 4,
            cursor: "default",
            color: missing.length ? STATUS.danger : STATUS.warn,
            ...F.footnote,
          }}
        >
          !
        </button>
      </div>
    );
  }

  return (
    <div className="no-drag" style={{ padding: "0 14px 12px" }}>
      {res.samples.length ? (
        <div
          style={{
            ...F.footnote,
            color: T.tertiary,
            marginBottom: 8,
            fontVariantNumeric: "tabular-nums",
          }}
        >
          {res.totals.running} running · {percent(res.totals.cpu, 0)} · {bytes(res.totals.mem)}
        </div>
      ) : null}

      {/* The tool names ARE the indicator.
          Four green dots in a row is the same picture whether you read it or not:
          when everything works it is four identical marks carrying nothing, and
          the one case that matters -- something missing -- looks like the others
          but a different hue. So only absence gets ink. A present tool is plain
          text, a missing one is struck through, and an unknown one is dimmed.
          Nothing here shouts while the machine is fine, which is what a monitor
          in a corner should do. It also survives colour blindness, because the
          state is in the glyphs and not only in the hue. */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: "3px 9px" }}>
        {tools.map((t) => (
          <span
            key={t.name}
            title={`${t.name}: ${t.detail}`}
            style={{
              ...F.footnote,
              color: t.ok === false ? STATUS.danger : t.ok === null ? T.quaternary : T.tertiary,
              textDecoration: t.ok === false ? "line-through" : undefined,
              textDecorationThickness: t.ok === false ? "1px" : undefined,
            }}
          >
            {t.name}
          </span>
        ))}
      </div>

      {drifted ? (
        <button
          onClick={() => app.navigate("settings")}
          title="This app was built against a different version of Curie than the one installed here. Open Settings to see what differs."
          style={{
            marginTop: 7,
            border: "none",
            background: "transparent",
            padding: 0,
            ...F.footnote,
            color: STATUS.warn,
            cursor: "default",
            display: "inline-flex",
            alignItems: "center",
            gap: 4,
          }}
        >
          version mismatch
        </button>
      ) : null}
    </div>
  );
}
