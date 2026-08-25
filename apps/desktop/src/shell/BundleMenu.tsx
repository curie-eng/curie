// The list of bundles this app knows about, as a menu.
//
// It lived inside the sidebar, which meant the one place you could switch bundles
// was the top-left corner -- including on the Build tab, whose entire subject is
// the open bundle. Extracted so both can show the same list: one menu, one set of
// actions, no second implementation to drift.
//
// The caller owns the anchor. It positions itself against the nearest positioned
// ancestor, so a caller wraps its trigger in `position: relative` and passes the
// edges it wants pinned.

import type { CSSProperties } from "react";

import { useApp } from "../bridge/app";
import { ACCENT, F, LINE, R, S, STATUS, T } from "../tokens";
import { Badge, Mono } from "../primitives";

export function BundleMenu({
  onClose,
  panel,
}: {
  onClose(): void;
  /** Where the panel sits relative to the trigger's positioned ancestor. */
  panel?: CSSProperties;
}) {
  const app = useApp();
  return (
    <>
      {/* A click anywhere else closes it, and nothing behind it is clickable
          while it is open. */}
      <div className="no-drag" onClick={onClose} style={{ position: "fixed", inset: 0, zIndex: 70 }} />
      <div
        className="no-drag rise"
        style={{
          position: "absolute",
          top: "calc(100% + 4px)",
          minWidth: 260,
          zIndex: 80,
          background: S.overlay,
          borderRadius: R.group,
          boxShadow: "0 16px 40px rgba(0,0,0,0.5)",
          overflow: "hidden",
          padding: 5,
          ...panel,
        }}
      >
        {app.workspaces.length === 0 ? (
          <div style={{ ...F.caption, color: T.tertiary, padding: "10px 9px" }}>
            No bundles yet. Open one, or scaffold a new bundle with{" "}
            <Mono style={{ fontSize: 11 }}>curie init</Mono>.
          </div>
        ) : (
          app.workspaces.map((w) => {
            const active = w.path === app.workspace?.path;
            return (
              <button
                key={w.path}
                onClick={() => {
                  app.selectWorkspace(w.path);
                  onClose();
                }}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  width: "100%",
                  border: "none",
                  background: active ? "rgba(255,255,255,0.12)" : "transparent",
                  borderRadius: R.control,
                  padding: "6px 8px",
                  textAlign: "left",
                  cursor: "default",
                }}
              >
                <span style={{ width: 12, color: ACCENT, fontSize: 11 }}>{active ? "✓" : ""}</span>
                <span style={{ flex: 1, minWidth: 0 }}>
                  <span style={{ ...F.body, display: "block" }}>{w.name}</span>
                  <Mono
                    style={{
                      fontSize: 10,
                      color: T.tertiary,
                      display: "block",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                      direction: "rtl",
                      textAlign: "left",
                    }}
                  >
                    {w.path}
                  </Mono>
                </span>
                {w.hasEvals ? <Badge color={STATUS.warn}>evals</Badge> : null}
              </button>
            );
          })
        )}
        <div style={{ height: 1, background: LINE.separator, margin: "5px 8px" }} />
        <button
          onClick={() => {
            void app.openWorkspace();
            onClose();
          }}
          style={menuAction}
        >
          Open bundle…
        </button>
        <button
          onClick={() => {
            app.navigate("commands", "init");
            onClose();
          }}
          style={menuAction}
        >
          Scaffold a new bundle…
        </button>
      </div>
    </>
  );
}

const menuAction: CSSProperties = {
  display: "block",
  width: "100%",
  border: "none",
  background: "transparent",
  borderRadius: 6,
  padding: "6px 8px",
  textAlign: "left",
  fontSize: 13,
  letterSpacing: -0.08,
  color: "rgba(235,235,245,0.62)",
  cursor: "default",
};
