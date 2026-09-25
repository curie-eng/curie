import { C } from "../tokens";
import { Dot } from "../primitives";
import { hoverBg } from "../lib/style";
import { useStore } from "../state/store";
import { useWired } from "../state/wired";
import type { Nav } from "../state/types";

const ITEMS: [Nav, string][] = [
  ["overview", "Overview"],
  ["agents", "Agents"],
  ["work-items", "Work items"],
  ["evals", "Evals"],
  ["observability", "Observability"],
  ["versions", "Versions"],
  ["connections", "Connections"],
  ["settings", "Settings"],
];

export function Sidebar() {
  const { state, dispatch } = useStore();
  const wired = useWired();
  // The only real count is the agent list; evals/connections have no backend
  // count yet, so no fictional badges leak.
  const badges: Partial<Record<Nav, string | number>> = {
    agents: wired.agents.length || undefined,
  };
  return (
    <div
      style={{
        width: 220,
        flexShrink: 0,
        background: C.sidebar,
        borderRight: "1px solid " + C.border,
        display: "flex",
        flexDirection: "column",
        padding: "16px 10px",
        position: "sticky",
        top: 0,
        alignSelf: "flex-start",
        height: "100vh",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 8px 16px" }}>
        <Dot color={C.brand} size={9} />
        <span style={{ fontSize: 16, fontWeight: 600, letterSpacing: -0.3 }}>Curie</span>
      </div>

      <button
        type="button"
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          width: "100%",
          textAlign: "left",
          background: C.card,
          border: "1px solid " + C.border,
          borderRadius: 8,
          padding: "8px 10px",
          cursor: "pointer",
          color: C.text,
          marginBottom: 14,
        }}
        {...hoverBg(C.card, C.hover)}
      >
        <div
          style={{
            width: 20,
            height: 20,
            borderRadius: 5,
            background: C.sel,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: 11,
            color: C.text2,
          }}
        >
          A
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, fontWeight: 500, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
            {wired.orgName}
          </div>
          <div style={{ fontSize: 11, color: C.muted }}>production</div>
        </div>
        <span style={{ color: C.muted, fontSize: 11 }}>⌄</span>
      </button>

      <nav style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        {ITEMS.map(([id, label]) => {
          const active = state.nav === id && !state.agentDetail;
          const badge = badges[id];
          return (
            <button
              key={id}
              type="button"
              onClick={() => dispatch({ type: "go", nav: id })}
              aria-current={active ? "page" : undefined}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                width: "100%",
                textAlign: "left",
                background: active ? C.hover : "transparent",
                border: "none",
                borderRadius: 7,
                padding: "7px 10px 7px 12px",
                cursor: "pointer",
                color: active ? C.text : C.text2,
                fontSize: 14,
                fontWeight: active ? 500 : 400,
                position: "relative",
                transition: "background .1s",
              }}
              {...(active ? {} : hoverBg("transparent", C.hover))}
            >
              {active ? (
                <span
                  style={{
                    position: "absolute",
                    left: 0,
                    top: 6,
                    bottom: 6,
                    width: 2.5,
                    borderRadius: 2,
                    background: C.brand,
                  }}
                />
              ) : null}
              <span style={{ flex: 1 }}>{label}</span>
              {badge ? (
                <span
                  style={{
                    fontSize: 11,
                    fontFamily: C.mono,
                    color: C.muted,
                    background: C.card,
                    border: "1px solid " + C.border,
                    borderRadius: 20,
                    padding: "0px 7px",
                    minWidth: 20,
                    textAlign: "center",
                  }}
                >
                  {badge}
                </span>
              ) : null}
            </button>
          );
        })}
      </nav>

      <div style={{ marginTop: "auto", padding: "8px", fontSize: 11, color: C.disabled, fontFamily: C.mono }}>
        v0 · open source
      </div>
    </div>
  );
}
