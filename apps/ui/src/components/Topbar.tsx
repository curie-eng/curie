import { C } from "../tokens";
import { useStore } from "../state/store";
import { useWired } from "../state/wired";
import type { Nav } from "../state/types";

const CRUMB: Record<Nav, string> = {
  overview: "Overview",
  agents: "Agents",
  "work-items": "Work items",
  evals: "Evals",
  observability: "Observability",
  versions: "Versions",
  connections: "Connections",
  settings: "Settings",
};

export function Topbar() {
  const { state, dispatch, envDev } = useStore();
  const wired = useWired();
  const orgName = wired.orgName;
  const isProd = state.env === "prod" && !envDev;

  const pill = (label: string, active: boolean, activeBg: string, activeColor: string, onClick: () => void) => (
    <button
      type="button"
      onClick={onClick}
      style={{
        fontSize: 12,
        fontFamily: C.mono,
        padding: "4px 12px",
        borderRadius: 20,
        cursor: "pointer",
        border: "none",
        background: active ? activeBg : "transparent",
        color: active ? activeColor : C.muted,
        fontWeight: active ? 600 : 400,
      }}
    >
      {label}
    </button>
  );

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 16,
        padding: "12px 36px",
        borderBottom: "1px solid " + C.border,
        background: C.page,
        position: "sticky",
        top: 0,
        zIndex: 400,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 14 }}>
        <span style={{ color: C.muted }}>{orgName}</span>
        <span style={{ color: C.disabled }}>/</span>
        <span style={{ color: C.text }}>{CRUMB[state.nav]}</span>
      </div>
      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 12 }}>
        <div
          style={{
            display: "flex",
            background: C.card,
            border: "1px solid " + C.border,
            borderRadius: 20,
            padding: 2,
          }}
        >
          {pill("PROD", isProd, "rgba(62,207,142,.15)", C.brand, () => dispatch({ type: "setEnv", env: "prod" }))}
          {pill("DEV", envDev, "rgba(191,135,0,.18)", C.warn, () => dispatch({ type: "setEnv", env: "dev" }))}
        </div>
      </div>
    </div>
  );
}

export function DevBanner() {
  return (
    <div
      style={{
        background: "rgba(191,135,0,.12)",
        borderBottom: "1px solid rgba(191,135,0,.3)",
        padding: "7px 36px",
        fontSize: 12.5,
        color: C.warn,
        fontFamily: C.mono,
      }}
    >
      Viewing dev environment — @curie-dev · branch dev
    </div>
  );
}
