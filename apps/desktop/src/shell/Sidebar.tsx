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

import { BundleMenu } from "./BundleMenu";
import { useApp, type Route } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { ACCENT, F, M, R, S, STATUS, T, tint } from "../tokens";
import { Spinner } from "../primitives";
import { bytes, percent } from "../lib/format";

interface Item {
  readonly id: Route;
  readonly label: string;
  readonly hint: string;
  readonly icon: ReactNode;
}

/** Inline SVG rather than an icon font: nine glyphs do not justify a dependency,
 *  and these inherit `currentColor`, so the selected state is one rule. Drawn on
 *  a 16px grid with a 1.4 stroke to sit close to SF Symbols' weight. */
function Icon({ d, filled }: { d: string; filled?: boolean }) {
  return (
    <svg width={16} height={16} viewBox="0 0 16 16" aria-hidden style={{ flex: "none" }}>
      <path
        d={d}
        fill={filled ? "currentColor" : "none"}
        stroke="currentColor"
        strokeWidth={filled ? 0 : 1.4}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}

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
    hint: "Author the open bundle",
    icon: <Icon d="M3.4 12.6h9.2M5.4 10.2V5.6M8 10.2V3.4M10.6 10.2V7.2" />,
  },
  {
    id: "resources",
    label: "Resources",
    hint: "Live CPU, memory, I/O",
    icon: <Icon d="M2.6 12.6V7.4M6.2 12.6V3.4M9.8 12.6V8.8M13.4 12.6V5.6" />,
  },
  {
    id: "canvas",
    label: "Canvas",
    hint: "Agents, pipelines, infra",
    icon: <Icon d="M3.2 3.6h3.4v3.2H3.2zM9.4 9.2h3.4v3.2H9.4zM6.6 5.2h2.2a2 2 0 0 1 2 2v2" />,
  },
  {
    id: "commands",
    label: "Commands",
    hint: "Every curie command",
    icon: <Icon d="m3 4.6 3 3-3 3M8.4 11.4H13" />,
  },
  {
    id: "activity",
    label: "Activity",
    hint: "What this app has run",
    icon: <Icon d="M1.8 8h2.9l1.9-4.4L9.9 12.4l1.7-4.4h2.6" />,
  },
];

export function Sidebar() {
  const app = useApp();
  const runs = useRuns();
  const res = useResources();

  return (
    <nav
      className="drag"
      data-tauri-drag-region
      style={{
        width: M.sidebar,
        flex: "none",
        // Paints nothing: the window's vibrancy is the background.
        background: S.sidebar,
        display: "flex",
        flexDirection: "column",
        paddingTop: M.trafficLights - 24,
      }}
    >
      <WorkspacePicker />

      <div style={{ padding: "10px 10px 0", display: "flex", flexDirection: "column", gap: 1 }}>
        {ITEMS.map((item) => (
          <NavItem
            key={item.id}
            item={item}
            active={app.route === item.id}
            onClick={() => app.navigate(item.id)}
            badge={
              item.id === "resources" && res.totals.running
                ? String(res.totals.running)
                : item.id === "activity" && runs.active.length
                  ? String(runs.active.length)
                  : undefined
            }
            busy={item.id === "activity" && runs.active.length > 0}
          />
        ))}
      </div>

      <div style={{ flex: 1 }} />

      <MachineStatus />

      <div style={{ padding: "0 10px 10px" }}>
        <NavItem
          item={{
            id: "settings",
            label: "Settings",
            hint: "Connection, secrets, about",
            icon: (
              <Icon d="M8 5.9A2.1 2.1 0 1 0 8 10.1 2.1 2.1 0 0 0 8 5.9M8 2.4v1.4M8 12.2v1.4M2.4 8h1.4M12.2 8h1.4M4.05 4.05l1 1M10.95 10.95l1 1M11.95 4.05l-1 1M5.05 10.95l-1 1" />
            ),
          }}
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
}: {
  item: Item;
  active: boolean;
  onClick(): void;
  badge?: string;
  busy?: boolean;
}) {
  const [hover, setHover] = useState(false);
  return (
    <button
      className="no-drag"
      onClick={onClick}
      title={item.hint}
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
        padding: "5px 8px",
        background: active ? "rgba(255,255,255,0.13)" : hover ? "rgba(255,255,255,0.06)" : "transparent",
        color: active ? T.primary : T.secondary,
        fontSize: 13,
        fontWeight: active ? 600 : 500,
        letterSpacing: -0.08,
        cursor: "default",
        transition: "background 90ms ease",
      }}
    >
      <span style={{ color: active ? ACCENT : T.tertiary, display: "flex" }}>{item.icon}</span>
      <span style={{ flex: 1 }}>{item.label}</span>
      {busy ? <Spinner size={10} color={ACCENT} /> : null}
      {badge ? (
        <span style={{ ...F.footnote, color: T.tertiary, fontVariantNumeric: "tabular-nums" }}>
          {badge}
        </span>
      ) : null}
    </button>
  );
}

/** The open bundle. Every skill-tier command is parameterised by it, so it sits
 *  at the top of the sidebar the way a document title would. */
function WorkspacePicker() {
  const app = useApp();
  const [open, setOpen] = useState(false);

  return (
    <div style={{ padding: "0 10px", position: "relative" }}>
      <button
        className="no-drag"
        onClick={() => setOpen((v) => !v)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          width: "100%",
          border: "none",
          background: open ? "rgba(255,255,255,0.10)" : "transparent",
          borderRadius: R.control,
          padding: "6px 8px",
          cursor: "default",
          textAlign: "left",
        }}
      >
        <span
          aria-hidden
          style={{
            width: 22,
            height: 22,
            flex: "none",
            borderRadius: 5,
            background: app.workspace ? tint(ACCENT, 0.2) : "rgba(255,255,255,0.08)",
            color: app.workspace ? ACCENT : T.tertiary,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: 11,
            fontWeight: 700,
          }}
        >
          {app.workspace ? app.workspace.name.slice(0, 1).toUpperCase() : "—"}
        </span>
        <span style={{ flex: 1, minWidth: 0 }}>
          <span
            style={{
              ...F.headline,
              display: "block",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
              color: app.workspace ? T.primary : T.tertiary,
            }}
          >
            {app.workspace ? app.workspace.name : "No bundle"}
          </span>
          <span style={{ ...F.footnote, color: T.tertiary }}>
            {app.workspace
              ? `${app.workspace.skills.length} skill${app.workspace.skills.length === 1 ? "" : "s"}`
              : "Open one to begin"}
          </span>
        </span>
        <span style={{ color: T.tertiary, fontSize: 9 }}>⌃</span>
      </button>

      {open ? <BundleMenu panel={{ left: 10, right: 10 }} onClose={() => setOpen(false)} /> : null}
    </div>
  );
}

/** What this machine can actually do, at the foot of the sidebar.
 *
 *  Everything the desktop app adds over the web console depends on local tooling,
 *  so its absence has to be visible rather than showing up later as an
 *  inscrutable command failure. A compact block here rather than a full-width
 *  footer bar: a status strip spanning the window is a browser habit. */
function MachineStatus() {
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
          title="This app was generated from a different version of the CLI than the one installed."
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
          command surface drifted
        </button>
      ) : null}
    </div>
  );
}
