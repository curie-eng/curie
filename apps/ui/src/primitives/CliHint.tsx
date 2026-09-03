import { useState } from "react";
import { C } from "../tokens";
import { useStore } from "../state/store";
import type { WiredActionId } from "./parity";

// CliHint: a resting `>_` glyph that morphs in place into a copy button on
// hover / keyboard-focus (cross-fades to `⧉`, accent color), previews the exact
// command in a tooltip, and copies on a single click (glyph flips to a green
// `✓` and fires the "Copied" toast). Composes the same clipboard + toast
// affordance as CopyButton, in a self-contained inline control.
//
// Keyboard-accessible (it is a real <button>, so Enter/Space activate it); on
// touch, a tap copies directly (there is no hover step to gate on). The
// morph is driven by hover/focus state and a transient "copied" flag, all
// CSS transitions so it degrades gracefully.
//
// Honest no-equivalent state (epic #145): a wired action with no CLI verb yet
// passes `noCliEquivalent={<tracking issue url>}` instead of a `command`. That
// renders an amber `◇` glyph whose tooltip says there is no CLI equivalent and
// whose click opens the tracking issue in a new tab. Typed action IDs identify
// the registry entries for this mode, and the parity test verifies those IDs
// link to `PARITY_TRACKING_ISSUE`. The same test scans direct `cliCommand`
// calls across production source and checks their literal command mappings.

const COPIED_RESET_MS = 1200;

type CliHintProps =
  | {
      command?: string;
      label?: string;
      noCliEquivalent?: never;
      actionIds?: never;
    }
  | {
      command?: never;
      label?: string;
      noCliEquivalent: string;
      actionIds: readonly WiredActionId[];
    };

export function CliHint({
  command,
  label,
  noCliEquivalent,
}: CliHintProps) {
  const { dispatch } = useStore();
  const [active, setActive] = useState(false); // hover or keyboard focus
  const [copied, setCopied] = useState(false);

  // Amber "no CLI equivalent yet" affordance: link out to the tracking issue
  // rather than copy. Kept a real <button> for keyboard parity with the copy
  // mode; the click opens the issue in a new tab.
  if (noCliEquivalent !== undefined) {
    return (
      <button
        type="button"
        onClick={() => window.open(noCliEquivalent, "_blank", "noopener,noreferrer")}
        onMouseEnter={() => setActive(true)}
        onMouseLeave={() => setActive(false)}
        onFocus={() => setActive(true)}
        onBlur={() => setActive(false)}
        title="No CLI equivalent yet — open the tracking issue"
        aria-label="No CLI equivalent yet; open the tracking issue"
        data-no-cli="true"
        style={{
          background: "transparent",
          border: "none",
          color: C.warn,
          cursor: "pointer",
          fontFamily: C.mono,
          fontSize: 12,
          padding: "2px 4px",
          display: "inline-flex",
          alignItems: "center",
          gap: 5,
          opacity: active ? 1 : 0.85,
        }}
      >
        <span aria-hidden="true" style={{ color: C.warn, width: "1.4em", textAlign: "center" }}>
          ◇
        </span>
        {label ? <span>{label}</span> : null}
      </button>
    );
  }

  const cmd = command ?? "";

  function copy() {
    if (navigator.clipboard) {
      void navigator.clipboard.writeText(cmd).catch(() => {});
    }
    dispatch({ type: "toast", message: "Copied" });
    setCopied(true);
    window.setTimeout(() => setCopied(false), COPIED_RESET_MS);
  }

  // Resting `>_`; morphs to `⧉` on hover/focus; flips to `✓` right after copy.
  const glyph = copied ? "✓" : active ? "⧉" : ">_";
  const glyphColor = copied ? C.brand : active ? C.link : C.muted;

  return (
    <button
      type="button"
      onClick={copy}
      onMouseEnter={() => setActive(true)}
      onMouseLeave={() => setActive(false)}
      onFocus={() => setActive(true)}
      onBlur={() => setActive(false)}
      title={`$ ${cmd}`}
      aria-label={`Copy command: ${cmd}`}
      data-copied={copied ? "true" : "false"}
      style={{
        background: "transparent",
        border: "none",
        color: glyphColor,
        cursor: "pointer",
        fontFamily: C.mono,
        fontSize: 12,
        padding: "2px 4px",
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
      }}
    >
      <span
        aria-hidden="true"
        style={{
          color: glyphColor,
          width: "1.4em",
          textAlign: "center",
          transition: "color .15s ease",
        }}
      >
        {glyph}
      </span>
      {label ? <span>{label}</span> : null}
    </button>
  );
}
