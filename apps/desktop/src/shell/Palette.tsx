// The command palette.
//
// This is the keyboard path to all 80 commands, and the closest thing the app
// has to a prompt. Typing `local up` here should feel like typing it in a shell:
// same words, same order, one keystroke to run. The difference is that the
// palette then hands you the command's arguments as a form instead of making you
// remember them -- so it is faster than the shell for the commands you know and
// far faster for the ones you do not.

import { useEffect, useMemo, useRef, useState } from "react";

import { useApp } from "../bridge/app";
import { commands, search, type Command } from "../lib/manifest";
import { CommandForm } from "../views/CommandForm";
import { ACCENT, FONT, HUE, LINE, R, S, SHADOW, STATUS, T } from "../tokens";
import { Badge, Kbd, Mono } from "../primitives";

const TIER_COLOR: Record<string, string> = {
  skill: ACCENT,
  local: STATUS.info,
  cluster: HUE.violet,
  dev: STATUS.warn,
  author: HUE.teal,
  platform: T.tertiary,
};

export function Palette() {
  const app = useApp();
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const [chosen, setChosen] = useState<Command | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  const results = useMemo(() => search(query, 60), [query]);

  // A new search invalidates the highlight; clamp it rather than letting it
  // point past the end of a shorter result list.
  const safeIndex = Math.min(index, Math.max(0, results.length - 1));

  // Reset as the palette opens, during render rather than after it, so it never
  // flashes the previous search before clearing.
  const [wasOpen, setWasOpen] = useState(app.paletteOpen);
  if (app.paletteOpen !== wasOpen) {
    setWasOpen(app.paletteOpen);
    if (app.paletteOpen) {
      setQuery("");
      setIndex(0);
      setChosen(null);
    }
  }

  useEffect(() => {
    if (!app.paletteOpen) return;
    // A frame's delay so the input exists before focus lands on it.
    const id = requestAnimationFrame(() => inputRef.current?.focus());
    return () => cancelAnimationFrame(id);
  }, [app.paletteOpen]);

  // Keep the highlighted row in view when arrowing past the fold.
  useEffect(() => {
    listRef.current?.querySelector<HTMLElement>(`[data-i="${safeIndex}"]`)?.scrollIntoView({
      block: "nearest",
    });
  }, [safeIndex]);

  if (!app.paletteOpen) return null;

  const close = () => app.setPaletteOpen(false);

  return (
    <div
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) close();
      }}
      style={{
        position: "fixed",
        inset: 0,
        background: "#0009",
        zIndex: 300,
        display: "flex",
        justifyContent: "center",
        paddingTop: "9vh",
      }}
    >
      <div
        className="rise"
        style={{
          width: chosen ? 720 : 620,
          maxWidth: "94vw",
          maxHeight: "78vh",
          display: "flex",
          flexDirection: "column",
          background: S.raised,
          border: `1px solid ${LINE.border}`,
          borderRadius: R.sheet,
          boxShadow: SHADOW.sheet,
          overflow: "hidden",
        }}
      >
        {chosen ? (
          <>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "11px 14px",
                borderBottom: `1px solid ${LINE.separator}`,
              }}
            >
              <button
                onClick={() => setChosen(null)}
                style={{
                  background: "none",
                  border: "none",
                  color: T.tertiary,
                  cursor: "pointer",
                  fontSize: 12,
                }}
              >
                ‹ Back
              </button>
              <Mono style={{ flex: 1, fontWeight: 600 }}>curie {chosen.path.join(" ")}</Mono>
              <Badge color={TIER_COLOR[chosen.tier] ?? T.tertiary}>{chosen.tier}</Badge>
            </div>
            <div style={{ padding: 16, overflow: "auto" }}>
              <div style={{ fontSize: 12, color: T.secondary, marginBottom: 14, lineHeight: 1.55 }}>
                {chosen.about}
              </div>
              <CommandForm key={chosen.id} cmd={chosen} compact onRan={close} />
            </div>
          </>
        ) : (
          <>
            <input
              ref={inputRef}
              value={query}
              placeholder="Run a curie command…"
              spellCheck={false}
              onChange={(e) => {
                setQuery(e.target.value);
                // A new search starts at the top; the old highlight meant
                // something about a list that no longer exists.
                setIndex(0);
              }}
              onKeyDown={(e) => {
                if (e.key === "Escape") return close();
                if (e.key === "ArrowDown") {
                  e.preventDefault();
                  return setIndex((i) => Math.min(results.length - 1, i + 1));
                }
                if (e.key === "ArrowUp") {
                  e.preventDefault();
                  return setIndex((i) => Math.max(0, i - 1));
                }
                if (e.key === "Enter" && results[safeIndex]) {
                  e.preventDefault();
                  // Shift-Enter jumps to the full Commands view instead of
                  // opening the inline form, for when you want to stay a while.
                  if (e.shiftKey) {
                    close();
                    return app.navigate("commands", results[safeIndex].id);
                  }
                  return setChosen(results[safeIndex]);
                }
              }}
              style={{
                border: "none",
                borderBottom: `1px solid ${LINE.separator}`,
                background: "transparent",
                color: T.primary,
                padding: "14px 16px",
                fontSize: 15,
                outline: "none",
                fontFamily: FONT.mono,
              }}
            />
            <div ref={listRef} style={{ overflow: "auto", flex: 1 }}>
              {results.length === 0 ? (
                <div style={{ padding: "22px 16px", color: T.tertiary, fontSize: 12 }}>
                  No command matches “{query}”. The palette searches every command in{" "}
                  <Mono>curie schema</Mono>, including its help text.
                </div>
              ) : (
                results.map((cmd, i) => (
                  <button
                    key={cmd.id}
                    data-i={i}
                    onMouseEnter={() => setIndex(i)}
                    onClick={() => setChosen(cmd)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                      width: "100%",
                      textAlign: "left",
                      padding: "8px 16px",
                      border: "none",
                      background: i === safeIndex ? S.selected : "transparent",
                      cursor: "pointer",
                    }}
                  >
                    <Mono
                      style={{
                        color: i === safeIndex ? T.primary : T.secondary,
                        fontWeight: 600,
                        minWidth: 178,
                      }}
                    >
                      curie {cmd.path.join(" ")}
                    </Mono>
                    <span
                      style={{
                        flex: 1,
                        minWidth: 0,
                        fontSize: 11,
                        color: T.tertiary,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {cmd.about}
                    </span>
                    {cmd.risk === "destructive" ? <Badge color={STATUS.danger}>destructive</Badge> : null}
                    <Badge color={TIER_COLOR[cmd.tier] ?? T.tertiary}>{cmd.tier}</Badge>
                  </button>
                ))
              )}
            </div>
            <div
              style={{
                display: "flex",
                gap: 14,
                padding: "7px 16px",
                borderTop: `1px solid ${LINE.separator}`,
                fontSize: 11,
                color: T.tertiary,
              }}
            >
              <span>
                <Kbd>↑↓</Kbd> navigate
              </span>
              <span>
                <Kbd>↵</Kbd> fill in
              </span>
              <span>
                <Kbd>⇧↵</Kbd> open full view
              </span>
              <div style={{ flex: 1 }} />
              <span>
                {results.length} of {commands.length} commands
              </span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
