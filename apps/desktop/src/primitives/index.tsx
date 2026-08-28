// The control vocabulary.
//
// Every primitive here exists to keep one habit out of the app: reaching for a
// bordered rectangle. The web answer to "these things belong together" is a card
// with a border; the platform answer is a *grouped list* -- one rounded
// container, hairline separators between rows, a small uppercase header above
// it. `Group` + `Row` is that, and it is what most of this app is built from.

import {
  useEffect,
  useRef,
  useState,
  type ButtonHTMLAttributes,
  type CSSProperties,
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
} from "react";

import { ACCENT, ACCENT_HOVER, F, FONT, KNOB, LINE, M, ON_ACCENT, R, S, SHADOW, STATUS, T, readable, tint } from "../tokens";

// --- text ------------------------------------------------------------------

export function Title({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return <div style={{ ...F.title, color: T.primary, ...style }}>{children}</div>;
}

export function Headline({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return <div style={{ ...F.headline, color: T.primary, ...style }}>{children}</div>;
}

export function Caption({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return <div style={{ ...F.caption, color: T.secondary, ...style }}>{children}</div>;
}

/** The small, wide-tracked, uppercase label that sits above a grouped list.
 *  Outside the group's rounded box, not inside it -- that placement is most of
 *  what makes a grouped list read as native. */
export function SectionHeader({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-end",
        justifyContent: "space-between",
        gap: 12,
        padding: "0 4px 6px",
      }}
    >
      <div style={{ ...F.section, color: T.tertiary }}>{children}</div>
      {right}
    </div>
  );
}

/** Monospace for things that are literally a command, path, digest, or id.
 *  Opts back into text selection, which the app disables globally. */
export function Mono({
  children,
  style,
  title,
  testId,
}: {
  children: ReactNode;
  style?: CSSProperties;
  title?: string;
  testId?: string;
}) {
  return (
    <span
      data-selectable
      data-testid={testId}
      title={title}
      style={{ fontFamily: FONT.mono, fontSize: 12, letterSpacing: -0.2, ...style }}
    >
      {children}
    </span>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <span
      style={{
        fontFamily: FONT.ui,
        fontSize: 11,
        minWidth: 16,
        display: "inline-flex",
        justifyContent: "center",
        padding: "1px 4px",
        borderRadius: 4,
        background: S.subtle,
        color: T.tertiary,
      }}
    >
      {children}
    </span>
  );
}

// --- surfaces --------------------------------------------------------------

/** A grouped list: the app's default container. Rounded, no outline, rows
 *  inside it separated by inset hairlines. */
export function Group({
  children,
  style,
  inset = true,
}: {
  children: ReactNode;
  style?: CSSProperties;
  /** `false` for a group that fills its column edge to edge. */
  inset?: boolean;
}) {
  return (
    <div
      style={{
        background: S.cardFill,
        // Glass: the fill is a thin film and this is what makes what shows
        // through read as seen THROUGH it. Both spellings -- Safari and older
        // Chromium still want the prefix, and Electron is Chromium.
        backdropFilter: S.cardBackdrop,
        WebkitBackdropFilter: S.cardBackdrop,
        borderRadius: inset ? R.group : 0,
        // `none` on dark; a hairline plus a faint lift on light, where a surface
        // alone does not separate a card from the pane behind it.
        boxShadow: SHADOW.card,
        overflow: "hidden",
        ...style,
      }}
    >
      {children}
    </div>
  );
}

/** One row of a grouped list. The separator is drawn on the row rather than
 *  between rows, and inset from the left, which is what stops a list from
 *  looking like a table with borders. */
export function Row({
  children,
  onClick,
  first,
  style,
  insetSeparator = 14,
  selected,
}: {
  children: ReactNode;
  onClick?: () => void;
  first?: boolean;
  style?: CSSProperties;
  insetSeparator?: number;
  selected?: boolean;
}) {
  const [hover, setHover] = useState(false);
  return (
    <div
      onClick={onClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        position: "relative",
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "8px 14px",
        minHeight: M.rowHeight,
        cursor: onClick ? "default" : undefined,
        background: selected ? S.selected : hover && onClick ? S.hover : "transparent",
        ...style,
      }}
    >
      {!first ? (
        <span
          aria-hidden
          style={{
            position: "absolute",
            top: 0,
            left: insetSeparator,
            right: 0,
            height: 1,
            background: LINE.separator,
          }}
        />
      ) : null}
      {children}
    </div>
  );
}

/** A recessed well: transcripts, command previews, log tails. Darker than the
 *  surface it sits on, which is how the platform signals "content, not chrome". */
export function Well({
  children,
  style,
  mono = true,
}: {
  children: ReactNode;
  style?: CSSProperties;
  mono?: boolean;
}) {
  return (
    <div
      style={{
        background: S.well,
        borderRadius: R.field,
        padding: "9px 11px",
        fontFamily: mono ? FONT.mono : undefined,
        fontSize: mono ? 12 : undefined,
        ...style,
      }}
    >
      {children}
    </div>
  );
}

/** A number worth reading at a glance. Rendered as a tile in a grid rather than
 *  a bordered card. */
/**
 * One figure in a `Stats` row.
 *
 * Deliberately paints no chrome of its own. Four numbers are one fact about the
 * system, not four unrelated ones, and the app's grouping rule is a single
 * rounded container with hairline separators -- not a card per item. As four
 * separate cards these read as four white slabs on a pale field with a small
 * number lost in the middle of each, which is most of what made the Overview
 * look unfinished.
 *
 * `first` suppresses the leading separator, the same way `Row` does it: the line
 * belongs to the cell that follows it, so a row cannot end with a stray edge.
 */
export function Stat({
  label,
  value,
  sub,
  accent,
  first,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  accent?: string;
  first?: boolean;
}) {
  return (
    <div
      style={{
        flex: 1,
        minWidth: 0,
        padding: "11px 14px",
        borderLeft: first ? undefined : `1px solid ${LINE.separator}`,
      }}
    >
      <div style={{ ...F.caption, color: T.tertiary, marginBottom: 3 }}>{label}</div>
      <div
        style={{
          fontSize: 22,
          fontWeight: 600,
          letterSpacing: -0.6,
          lineHeight: 1.15,
          color: accent ?? T.primary,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {value}
      </div>
      {sub ? (
        <div
          style={{
            ...F.footnote,
            color: T.tertiary,
            marginTop: 2,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {sub}
        </div>
      ) : null}
    </div>
  );
}

/** The container `Stat` cells belong in: one card, hairline-divided. */
export function Stats({ children }: { children: ReactNode }) {
  return <Group style={{ display: "flex", alignItems: "stretch" }}>{children}</Group>;
}

export type NoticeTone = "info" | "warn" | "error" | "success";

const TONE: Record<NoticeTone, string> = {
  info: STATUS.info,
  warn: STATUS.warn,
  error: STATUS.danger,
  success: ACCENT,
};

/** An inline alert. A tinted surface with a coloured glyph, not a bordered box
 *  with a coloured left rail -- the rail is a bootstrap-ism. */
export function Notice({
  tone = "info",
  title,
  children,
  action,
}: {
  tone?: NoticeTone;
  title?: ReactNode;
  children?: ReactNode;
  action?: ReactNode;
}) {
  const color = TONE[tone];
  const glyph = tone === "success" ? "✓" : tone === "info" ? "i" : "!";
  return (
    // A `Group`, so a notice is a card like everything else on the screen: the
    // same radius, the same hairline, the same lift. It used to be a flat
    // tinted band, which on a pane full of cards read as a stripe painted onto
    // the background rather than as a thing sitting on it. The tint replaces
    // the card's own fill rather than layering over it -- the tone IS this
    // card's surface -- and `Group`'s backdrop blur still carries the window's
    // vibrancy through it.
    <Group
      style={{
        display: "flex",
        gap: 10,
        // With a title the body runs to several lines and the glyph has to sit
        // on the FIRST of them, so the row aligns to the top. Without one there
        // is a single line of text next to a button half again as tall, and
        // top-aligning pinned the glyph and the sentence to the ceiling while
        // the button set the height -- which is the misalignment, not a stray
        // margin. One line centres.
        alignItems: title ? "flex-start" : "center",
        padding: "10px 12px",
        background: tint(color, 0.12),
      }}
    >
      <span
        aria-hidden
        style={{
          flex: "none",
          width: 16,
          height: 16,
          // Nudges the glyph onto the first text line; only meaningful when the
          // row is top-aligned.
          marginTop: title ? 1 : 0,
          borderRadius: 999,
          background: color,
          color: "#000",
          fontSize: 11,
          fontWeight: 700,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        {glyph}
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        {title ? <div style={{ ...F.headline, marginBottom: 2 }}>{title}</div> : null}
        {children ? <div style={{ ...F.callout, color: T.secondary }}>{children}</div> : null}
      </div>
      {action ? <div style={{ flex: "none" }}>{action}</div> : null}
    </Group>
  );
}

export function EmptyState({
  title,
  children,
  action,
  icon,
}: {
  title: string;
  children?: ReactNode;
  action?: ReactNode;
  icon?: ReactNode;
}) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        textAlign: "center",
        padding: "30px 24px",
        color: T.tertiary,
      }}
    >
      {icon ? <div style={{ marginBottom: 12, opacity: 0.5 }}>{icon}</div> : null}
      <div style={{ ...F.title, color: T.secondary, marginBottom: 6 }}>{title}</div>
      {children ? (
        <div style={{ ...F.callout, maxWidth: 420, lineHeight: 1.55 }}>{children}</div>
      ) : null}
      {action ? <div style={{ marginTop: 18 }}>{action}</div> : null}
    </div>
  );
}

// --- status ----------------------------------------------------------------

export function Badge({
  children,
  color = T.tertiary,
  filled,
  title,
}: {
  children: ReactNode;
  color?: string;
  filled?: boolean;
  title?: string;
}) {
  return (
    <span
      title={title}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        ...F.footnote,
        fontWeight: 500,
        padding: "2px 7px",
        borderRadius: R.pill,
        whiteSpace: "nowrap",
        background: filled ? tint(color, 0.18) : S.subtle,
        // `readable(color)`, not `color`: the label sits on an 18% tint of its
        // own hue, where the raw value has almost no contrast -- a saturated
        // blue on dark, a dark green on light. `readable` pulls it toward the
        // theme's ink, which is the right direction in both.
        color: filled ? readable(color) : T.secondary,
      }}
    >
      {children}
    </span>
  );
}

export function Dot({ color, pulse }: { color: string; pulse?: boolean }) {
  return (
    <span
      style={{
        width: 6,
        height: 6,
        borderRadius: 999,
        background: color,
        display: "inline-block",
        flex: "none",
        animation: pulse ? "curie-pulse 1.8s ease-in-out infinite" : undefined,
      }}
    />
  );
}

export function Spinner({ size = 13, color = T.tertiary }: { size?: number; color?: string }) {
  return (
    <span
      style={{
        width: size,
        height: size,
        display: "inline-block",
        borderRadius: 999,
        border: `2px solid ${tint(KNOB, 0.12)}`,
        borderTopColor: color,
        animation: "curie-spin 700ms linear infinite",
      }}
    />
  );
}

// --- buttons ---------------------------------------------------------------

/** Inline SVG rather than an icon font: a dozen glyphs do not justify a
 *  dependency, and these inherit `currentColor`, so a selected or accented state
 *  is one rule. Drawn on a 16px grid with a 1.4 stroke to sit close to SF
 *  Symbols' weight. */
export function Glyph({
  d,
  filled,
  size = 16,
}: {
  readonly d: string;
  readonly filled?: boolean;
  readonly size?: number;
}) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" aria-hidden style={{ flex: "none" }}>
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

export type ButtonTone = "default" | "primary" | "danger" | "plain";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  tone?: ButtonTone;
  size?: "sm" | "md";
  busy?: boolean;
  icon?: ReactNode;
}

/** A push button. Filled and slightly raised rather than outlined -- an
 *  outlined button on a dark surface is a web pattern; the platform's is a
 *  filled control with a hairline top highlight. */
export function Button({
  tone = "default",
  size = "md",
  busy,
  icon,
  children,
  disabled,
  style,
  ...rest
}: ButtonProps) {
  const [hover, setHover] = useState(false);
  const [active, setActive] = useState(false);
  const off = disabled || busy;

  const palette: Record<ButtonTone, CSSProperties> = {
    default: {
      background: hover && !off ? S.controlHover : S.control,
      color: T.primary,
      boxShadow: SHADOW.raised,
    },
    primary: {
      background: hover && !off ? ACCENT_HOVER : ACCENT,
      color: ON_ACCENT,
      fontWeight: 600,
    },
    danger: {
      background: hover && !off ? tint(STATUS.danger, 0.26) : tint(STATUS.danger, 0.16),
      color: STATUS.danger,
      fontWeight: 500,
    },
    plain: {
      background: hover && !off ? S.subtle : "transparent",
      color: T.secondary,
    },
  };

  return (
    <button
      {...rest}
      disabled={off}
      className="no-drag"
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => {
        setHover(false);
        setActive(false);
      }}
      onMouseDown={() => setActive(true)}
      onMouseUp={() => setActive(false)}
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 5,
        padding: size === "sm" ? "3px 9px" : "5px 12px",
        fontSize: size === "sm" ? 12 : 13,
        fontWeight: 500,
        letterSpacing: -0.08,
        border: "none",
        borderRadius: R.control,
        cursor: off ? "default" : "default",
        opacity: off ? 0.4 : 1,
        whiteSpace: "nowrap",
        transform: active && !off ? "scale(0.97)" : undefined,
        transition: "background 90ms ease, transform 60ms ease",
        ...palette[tone],
        ...style,
      }}
    >
      {busy ? <Spinner size={11} color={tone === "primary" ? ON_ACCENT : T.secondary} /> : icon}
      {children}
    </button>
  );
}

/** Copy-to-clipboard that confirms in place. Used wherever a command string is
 *  shown, because copying it out to a real terminal is a first-class path. */
export function CopyButton({
  text,
  label = "Copy",
  size = "sm",
}: {
  text: string;
  label?: string;
  size?: "sm" | "md";
}) {
  const [done, setDone] = useState(false);
  useEffect(() => {
    if (!done) return;
    const t = setTimeout(() => setDone(false), 1200);
    return () => clearTimeout(t);
  }, [done]);
  return (
    <Button
      size={size}
      tone="plain"
      onClick={() => {
        void navigator.clipboard?.writeText(text);
        setDone(true);
      }}
      style={done ? { color: ACCENT } : undefined}
    >
      {done ? "Copied" : label}
    </Button>
  );
}

// --- inputs ----------------------------------------------------------------

const FIELD_BASE: CSSProperties = {
  width: "100%",
  background: S.field,
  border: `1px solid ${LINE.border}`,
  borderRadius: R.field,
  color: T.primary,
  padding: "5px 8px",
  fontSize: 13,
  letterSpacing: -0.08,
  fontFamily: "inherit",
  outline: "none",
};

export function Input({
  invalid,
  style,
  ...rest
}: InputHTMLAttributes<HTMLInputElement> & { invalid?: boolean }) {
  return (
    <input
      {...rest}
      style={{ ...FIELD_BASE, borderColor: invalid ? STATUS.danger : LINE.border, ...style }}
    />
  );
}

export function Textarea({ style, ...rest }: TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      {...rest}
      style={{
        ...FIELD_BASE,
        fontFamily: FONT.mono,
        fontSize: 12,
        resize: "vertical",
        minHeight: 68,
        ...style,
      }}
    />
  );
}

export function Select({ style, children, ...rest }: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select
      {...rest}
      style={{
        ...FIELD_BASE,
        background: S.control,
        border: "none",
        boxShadow: SHADOW.raised,
        padding: "4px 8px",
        ...style,
      }}
    >
      {children}
    </select>
  );
}

/** The platform switch: a pill that slides, not a checkbox. */
export function Toggle({
  checked,
  onChange,
  label,
  hint,
}: {
  checked: boolean;
  onChange(next: boolean): void;
  label?: ReactNode;
  hint?: ReactNode;
}) {
  const control = (
    <span
      role="switch"
      aria-checked={checked}
      tabIndex={0}
      onClick={() => onChange(!checked)}
      onKeyDown={(e) => {
        if (e.key === " " || e.key === "Enter") {
          e.preventDefault();
          onChange(!checked);
        }
      }}
      style={{
        width: 34,
        height: 20,
        flex: "none",
        borderRadius: 999,
        background: checked ? ACCENT : S.controlHover,
        position: "relative",
        transition: "background 140ms ease",
      }}
    >
      <span
        style={{
          position: "absolute",
          top: 2,
          left: checked ? 16 : 2,
          width: 16,
          height: 16,
          borderRadius: 999,
          background: "#fff",
          boxShadow: SHADOW.knob,
          transition: "left 140ms cubic-bezier(0.22,1,0.36,1)",
        }}
      />
    </span>
  );

  if (!label) return control;

  return (
    <label style={{ display: "flex", alignItems: "flex-start", gap: 9 }}>
      {control}
      <span style={{ minWidth: 0 }}>
        <span style={{ ...F.body }}>{label}</span>
        {hint ? <div style={{ ...F.caption, color: T.tertiary, marginTop: 1 }}>{hint}</div> : null}
      </span>
    </label>
  );
}

/** A segmented control: the platform's answer to a small set of exclusive
 *  choices, and a much better fit than a row of buttons or a `<select>`. */
export function Segmented<V extends string>({
  options,
  value,
  onChange,
  size = "md",
}: {
  options: readonly { value: V; label: ReactNode; title?: string }[];
  value: V;
  onChange(next: V): void;
  size?: "sm" | "md";
}) {
  return (
    <div
      style={{
        display: "inline-flex",
        padding: 2,
        gap: 2,
        background: S.well,
        borderRadius: R.control + 2,
      }}
    >
      {options.map((o) => {
        const on = o.value === value;
        return (
          <button
            key={o.value}
            title={o.title}
            onClick={() => onChange(o.value)}
            style={{
              border: "none",
              borderRadius: R.control,
              padding: size === "sm" ? "2px 8px" : "3px 11px",
              fontSize: size === "sm" ? 11 : 12,
              fontWeight: on ? 600 : 500,
              letterSpacing: -0.05,
              color: on ? T.primary : T.secondary,
              background: on ? S.controlHover : "transparent",
              boxShadow: on ? SHADOW.raised : undefined,
              cursor: "default",
              transition: "background 90ms ease",
            }}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

export function Field({
  label,
  hint,
  required,
  error,
  children,
  right,
}: {
  label: ReactNode;
  hint?: ReactNode;
  required?: boolean;
  error?: string | null;
  children: ReactNode;
  right?: ReactNode;
}) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          gap: 8,
          marginBottom: 4,
        }}
      >
        <label style={{ ...F.headline, color: T.secondary }}>
          {label}
          {required ? <span style={{ color: STATUS.danger, marginLeft: 3 }}>*</span> : null}
        </label>
        {right}
      </div>
      {children}
      {hint ? (
        <div style={{ ...F.caption, color: T.tertiary, marginTop: 4, lineHeight: 1.5 }}>{hint}</div>
      ) : null}
      {error ? (
        <div style={{ ...F.caption, color: STATUS.danger, marginTop: 4 }}>{error}</div>
      ) : null}
    </div>
  );
}

// --- sheets ----------------------------------------------------------------

/** A sheet, not a "modal": it drops from the top of the window, is rounded only
 *  at the bottom, and dims what is behind it. That entrance is one of the
 *  strongest native cues available to a windowed app. */
export function Sheet({
  title,
  onClose,
  children,
  footer,
  width = 520,
}: {
  title: ReactNode;
  onClose(): void;
  children: ReactNode;
  footer?: ReactNode;
  width?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    ref.current?.focus();
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
      style={{
        position: "fixed",
        inset: 0,
        background: SHADOW.scrim,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 200,
        padding: 24,
      }}
    >
      <div
        ref={ref}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label={typeof title === "string" ? title : undefined}
        style={{
          width,
          maxWidth: "100%",
          maxHeight: "84vh",
          display: "flex",
          flexDirection: "column",
          // The same glass every card gets, so a sheet reads as this app's own
          // surface rather than as a system dialog dropped on top of it. The
          // blur is what keeps it legible: a translucent panel over a scrimmed
          // page would be muddy without it.
          background: S.cardFill,
          backdropFilter: S.cardBackdrop,
          WebkitBackdropFilter: S.cardBackdrop,
          borderRadius: R.sheet,
          boxShadow: SHADOW.sheet,
          animation: "curie-sheet 200ms cubic-bezier(0.22,1,0.36,1)",
        }}
      >
        <style>{`@keyframes curie-sheet { from { transform: scale(0.97); opacity: 0 } to { transform: none; opacity: 1 } }`}</style>
        <div
          style={{
            padding: "14px 18px 12px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 12,
          }}
        >
          <div style={{ ...F.title }}>{title}</div>
          <Button size="sm" tone="plain" onClick={onClose}>
            Close
          </Button>
        </div>
        <div style={{ padding: "0 18px 18px", overflow: "auto", flex: 1 }}>{children}</div>
        {footer ? (
          <div
            style={{
              padding: "12px 18px",
              borderTop: `1px solid ${LINE.separator}`,
              display: "flex",
              justifyContent: "flex-end",
              gap: 8,
            }}
          >
            {footer}
          </div>
        ) : null}
      </div>
    </div>
  );
}
