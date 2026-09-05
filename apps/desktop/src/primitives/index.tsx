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
  type KeyboardEvent as ReactKeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  type Ref,
  type CSSProperties,
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
} from "react";
import { createPortal } from "react-dom";

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
  panelRef,
}: {
  children: ReactNode;
  style?: CSSProperties;
  /** `false` for a group that fills its column edge to edge. */
  inset?: boolean;
  /** For a group whose own box has to be measured -- a resizable panel needs to
   *  know where its edge actually is before the first drag moves it. */
  panelRef?: Ref<HTMLDivElement>;
}) {
  return (
    <div
      ref={panelRef}
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
      // A hook for CSS that needs the ROW's state, not the child's: a control
      // revealed on row hover cannot express that from its own rules.
      data-row=""
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
  title,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  accent?: string;
  first?: boolean;
  /** Where the number came from, or the caveat on it. The cell shows a figure
   *  and a short caption; anything longer than a caption belongs here. */
  title?: string;
}) {
  return (
    <div
      title={title}
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

/**
 * A live marker for something that is up and staying up.
 *
 * The counterpart to `Spinner`, and they must not be swapped. A spinner is a
 * promise that something will finish, so one left on screen after the work is
 * done says the opposite of the truth -- the stack card wore a spinner while
 * the stack was already running, which read as "still trying". This finishes
 * nothing and is not trying to: the loop IS the message.
 *
 * It also satisfies the app's rule about status dots, which allows exactly one
 * use -- a live marker whose animation is the information -- rather than a
 * coloured dot standing in for a word.
 */
export function LiveRing({ color = ACCENT, size = 9 }: { color?: string; size?: number }) {
  return (
    <span
      aria-hidden
      style={{
        position: "relative",
        flex: "none",
        display: "inline-flex",
        width: size,
        height: size,
      }}
    >
      <span
        style={{
          position: "absolute",
          inset: 0,
          borderRadius: 999,
          background: color,
          animation: "curie-ping 2400ms cubic-bezier(0, 0, 0.2, 1) infinite",
        }}
      />
      <span
        style={{
          position: "relative",
          width: size,
          height: size,
          borderRadius: 999,
          background: color,
        }}
      />
    </span>
  );
}

/** One row of a `MenuButton`'s popover. `tone: "danger"` is the destructive
 *  ink; the confirmation still belongs to whatever the item opens. */
export interface MenuItem {
  readonly label: string;
  readonly onSelect: () => void;
  readonly tone?: "danger";
  readonly disabled?: boolean;
}

/**
 * The overflow menu on a row: three dots, and a popover.
 *
 * A row action used to be a bare glyph doing one thing, which only works while
 * there is exactly one thing. The kebab is the platform's answer to "this row
 * has actions" and it does not have to be redesigned to hold a second one --
 * pass another item.
 *
 * It renders through a PORTAL. Every list in this app lives inside a `Group`,
 * and `Group` sets `overflow: hidden` to keep its children inside its rounded
 * corners, so a popover positioned inside the row is clipped by its own
 * container. Portalling to the body and positioning from the trigger's measured
 * rect is what lets the menu escape the card it belongs to.
 *
 * It closes on outside press, Escape, scroll and resize. The last two matter
 * because the menu is positioned once from a rect: left open through a scroll it
 * would hang in the wrong place, pointing at a row that has moved.
 */
export function MenuButton({
  label,
  items,
  className,
}: {
  /** Names the button for assistive tech; there is no visible text. */
  readonly label: string;
  readonly items: readonly MenuItem[];
  readonly className?: string;
}) {
  const [at, setAt] = useState<{ top: number; right: number } | null>(null);
  const ref = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!at) return;
    const close = () => setAt(null);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    // `mousedown`, not `click`: closing on click would fire after the item's own
    // click and could dismiss before the selection is handled.
    window.addEventListener("mousedown", close);
    window.addEventListener("keydown", onKey);
    window.addEventListener("resize", close);
    window.addEventListener("scroll", close, true);
    return () => {
      window.removeEventListener("mousedown", close);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", close);
      window.removeEventListener("scroll", close, true);
    };
  }, [at]);

  const open = () => {
    const r = ref.current?.getBoundingClientRect();
    if (r) setAt({ top: Math.round(r.bottom + 4), right: Math.round(window.innerWidth - r.right) });
  };

  return (
    <>
      <button
        ref={ref}
        className={className}
        aria-label={label}
        aria-haspopup="menu"
        aria-expanded={!!at}
        title={label}
        onClick={(e) => {
          e.stopPropagation();
          if (at) setAt(null);
          else open();
        }}
        style={{
          flex: "none",
          border: "none",
          background: at ? S.control : "transparent",
          padding: 3,
          borderRadius: R.control,
          color: "inherit",
          cursor: "default",
          display: "inline-flex",
        }}
      >
        <svg width={16} height={16} viewBox="0 0 16 16" aria-hidden style={{ flex: "none" }}>
          <circle cx="8" cy="3.5" r="1.35" fill="currentColor" />
          <circle cx="8" cy="8" r="1.35" fill="currentColor" />
          <circle cx="8" cy="12.5" r="1.35" fill="currentColor" />
        </svg>
      </button>

      {at
        ? createPortal(
            <div
              role="menu"
              onMouseDown={(e) => e.stopPropagation()}
              style={{
                position: "fixed",
                top: at.top,
                right: at.right,
                minWidth: 168,
                padding: 4,
                borderRadius: R.group,
                // The same film a sheet gets, and for the same reason: a menu
                // floats over arbitrary content -- here directly over the
                // accent-green "New agent…" button -- so it has to be its own
                // surface or what is behind competes with its labels. Not
                // `cardFill`, which is glass because a card sits on the pane.
                background: S.sheetFill,
                backdropFilter: S.cardBackdrop,
                WebkitBackdropFilter: S.cardBackdrop,
                boxShadow: SHADOW.sheet,
                border: `1px solid ${LINE.separator}`,
                zIndex: 300,
                animation: "curie-rise 90ms ease-out",
              }}
            >
              {items.map((item) => (
                <MenuRow key={item.label} item={item} onDone={() => setAt(null)} />
              ))}
            </div>,
            document.body,
          )
        : null}
    </>
  );
}

function MenuRow({ item, onDone }: { readonly item: MenuItem; readonly onDone: () => void }) {
  const [hover, setHover] = useState(false);
  return (
    <button
      role="menuitem"
      disabled={item.disabled}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onClick={(e) => {
        e.stopPropagation();
        onDone();
        item.onSelect();
      }}
      style={{
        display: "block",
        width: "100%",
        textAlign: "left",
        border: "none",
        background: hover && !item.disabled ? S.control : "transparent",
        borderRadius: R.control,
        padding: "5px 9px",
        ...F.body,
        color: item.tone === "danger" ? STATUS.danger : T.primary,
        opacity: item.disabled ? 0.4 : 1,
        cursor: "default",
      }}
    >
      {item.label}
    </button>
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
    // Fills the width it is given. A `Field` is a labelled control, and inside a
    // flex row it was sizing to its content instead: on the theme picker that
    // left the field 858px wide in a 1222px row, with the difference dead. In a
    // block context these two properties do nothing, so this is safe wherever a
    // Field already looked right.
    <div style={{ marginBottom: 14, flex: 1, minWidth: 0 }}>
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
  bodyHeight,
}: {
  title: ReactNode;
  onClose(): void;
  children: ReactNode;
  footer?: ReactNode;
  width?: number;
  /** A fixed height for the body, for a sheet whose steps must not resize it.
   *  Any CSS length; cap it against the sheet's own `84vh` or a short window
   *  clips the body instead of scrolling it. */
  bodyHeight?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    // Focus the panel ONLY if nothing inside it already took focus. It focuses
    // itself so a sheet opened by mouse still has the keyboard, but an
    // `autoFocus`ed field inside commits before this effect runs, so an
    // unconditional call stole focus back out of the field the sheet exists to
    // have you fill in -- you had to click the box before you could type.
    const panel = ref.current;
    if (panel && !panel.contains(document.activeElement)) panel.focus();
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  // Portalled to the body, and that is not a detail. A sheet is declared
  // wherever it is used -- inside a view, inside `main` -- and `position: fixed`
  // only escapes to the viewport while no ancestor establishes a containing
  // block or a stacking context. `main` carries a `mask-image` (the fade into
  // the console), and a mask does exactly that: the overlay was trapped in
  // `main`'s stacking context, so the console -- a SIBLING of `main` -- painted
  // over the scrim. Opening a sheet dimmed the whole window except the console,
  // which read as the console being somehow still live.
  //
  // Fixing the symptom would have meant a z-index on the console, and the next
  // masked or transformed ancestor would have brought it back somewhere else. A
  // modal belongs at the top of the document, not wherever it happens to be
  // written.
  return createPortal(
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
        // Centred on the CONTENT PANE, not the window. The scrim still covers
        // everything -- a modal that leaves part of the window looking live is
        // lying about what you can click -- but the sidebar is 218px of
        // permanent chrome that never goes away, so the lit area is the frame
        // the eye measures against. Centred on the window, a sheet sits ~109px
        // left of where it looks like it should be, which reads as "not
        // centred" and was reported as exactly that. Padding rather than
        // `left`, so the scrim keeps its full width and only the CENTRING moves.
        paddingLeft: M.sidebar + 24,
      }}
    >
      <div
        ref={ref}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label={typeof title === "string" ? title : undefined}
        style={{
          // No focus ring on the panel itself. `tabIndex={-1}` makes it a
          // target for programmatic focus, not a control somebody tabbed to,
          // and the app's global `:focus-visible` rule was drawing a 2px accent
          // outline around the whole sheet as if it were one. The ring belongs
          // on the field inside that actually has focus.
          outline: "none",
          width,
          maxWidth: "100%",
          maxHeight: "84vh",
          display: "flex",
          flexDirection: "column",
          // `sheetFill`, not `cardFill` and not fully opaque. A CARD is glass
          // because it sits on the pane and the window's vibrancy carrying
          // through it is the point; on glass a SHEET let the page underneath
          // compete with its own text -- "No agents yet" reading through the New
          // agent title. Fully opaque fixed that and went too far the other way,
          // reading as a system dialog dropped on the app rather than part of
          // it. A thin film with the blur under it is the answer: the page is
          // felt behind the panel without being read through it.
          //
          // The blur is load bearing at this alpha, which is why it is back. All
          // of this was settled by somebody looking at a real display -- captures
          // do not composite native vibrancy, so they are the wrong instrument
          // in either direction.
          background: S.sheetFill,
          backdropFilter: S.cardBackdrop,
          WebkitBackdropFilter: S.cardBackdrop,
          borderRadius: R.sheet,
          // Clip to the radius. The body and the footer are square, and with
          // this visible their corners painted straight over the panel's own
          // rounded ones -- a card scrolled to the bottom of the body squared
          // off the sheet's bottom-left. A rounded container whose children are
          // not clipped only looks rounded while nothing reaches the edge.
          overflow: "hidden",
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
        {/* One scrolling box, and it is this one. A caller that wants a fixed
            body passes `bodyHeight` rather than wrapping the children in a
            second `overflow: auto` of its own -- that wrapper clips at ITS
            padding edge, which was zero, so every card inside had its shadow
            cut off at the left, right and bottom. This box already carries the
            sheet's 18px inset, which is the room those shadows need. */}
        <div
          style={{
            padding: "0 18px 18px",
            overflow: "auto",
            flex: bodyHeight ? "none" : 1,
            height: bodyHeight,
          }}
        >
          {children}
        </div>
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
    </div>,
    document.body,
  );
}

/**
 * A grab strip on a panel's top edge. Drag it to resize the panel.
 *
 * Pointer *capture* rather than window listeners, for one reason that matters
 * here: the app sets `user-select: none` everywhere except the few surfaces
 * that opt back in, and the console's scrollback is one of them. Without
 * capture, a drag that crosses the scrollback would select its text on the way
 * past -- so the gesture that exists to make the history easier to copy would
 * fight the copying. Capture routes every move to this element until release.
 *
 * `role="separator"` with a `valuenow` is the ARIA pattern for a splitter, and
 * it is also the only way this is reachable without a mouse: focus it and the
 * arrow keys move the edge, Home/End take it to its stops.
 */
export function ResizeHandle({
  value,
  min,
  max,
  step = 24,
  label,
  onChange,
  onCommit,
  onReset,
}: {
  value: number;
  min: number;
  max: number;
  step?: number;
  label: string;
  onChange(next: number): void;
  /** Called once at the end of a gesture. Persist here, not on every move. */
  onCommit?(next: number): void;
  /** Double-click. Omit to make double-click do nothing. */
  onReset?(): void;
}) {
  const [live, setLive] = useState(false);
  const [warm, setWarm] = useState(false);
  const drag = useRef<{ id: number; y: number; from: number } | null>(null);

  const clamp = (n: number) => Math.min(max, Math.max(min, Math.round(n)));

  // Up is taller. The handle sits on the panel's top edge, so the pointer and
  // the edge it is holding travel together -- anything else feels like the
  // panel is fighting the hand.
  const at = (clientY: number) =>
    drag.current ? clamp(drag.current.from + (drag.current.y - clientY)) : value;

  const stop = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (drag.current?.id !== e.pointerId) return;
    const next = at(e.clientY);
    drag.current = null;
    setLive(false);
    onCommit?.(next);
  };

  const key = (e: ReactKeyboardEvent<HTMLDivElement>) => {
    const next =
      e.key === "ArrowUp"
        ? clamp(value + step)
        : e.key === "ArrowDown"
          ? clamp(value - step)
          : e.key === "Home"
            ? max
            : e.key === "End"
              ? min
              : undefined;
    if (next === undefined) return;
    e.preventDefault();
    onChange(next);
    onCommit?.(next);
  };

  const lit = live || warm;

  return (
    <div
      // `no-drag` because this strip can land inside a region the window itself
      // is draggable by, and a resize that moved the whole window instead would
      // be indistinguishable from a bug.
      className="no-drag"
      role="separator"
      aria-orientation="horizontal"
      aria-label={label}
      aria-valuenow={value}
      aria-valuemin={min}
      aria-valuemax={max}
      tabIndex={0}
      onPointerDown={(e) => {
        if (e.button !== 0) return;
        e.preventDefault();
        // Optional call: capture is what keeps the drag off the scrollback's
        // text, but a browser (or a test environment) without it should still
        // get a working handle rather than an exception thrown out of the very
        // first event of the gesture.
        e.currentTarget.setPointerCapture?.(e.pointerId);
        drag.current = { id: e.pointerId, y: e.clientY, from: value };
        setLive(true);
      }}
      onPointerMove={(e) => {
        if (drag.current?.id === e.pointerId) onChange(at(e.clientY));
      }}
      onPointerUp={stop}
      onPointerCancel={stop}
      onLostPointerCapture={() => {
        if (drag.current) {
          drag.current = null;
          setLive(false);
        }
      }}
      onKeyDown={key}
      onDoubleClick={onReset}
      onMouseEnter={() => setWarm(true)}
      onMouseLeave={() => setWarm(false)}
      onFocus={() => setWarm(true)}
      onBlur={() => setWarm(false)}
      style={{
        flex: "none",
        height: 11,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        cursor: "ns-resize",
        // The pointer events are the whole gesture; letting the browser also
        // interpret the drag as a scroll would steal every other move.
        touchAction: "none",
        outline: "none",
      }}
    >
      <div
        style={{
          width: lit ? 46 : 30,
          height: 3,
          borderRadius: 2,
          // Faint at rest, because a panel nobody has told you is resizable
          // still needs to look it; solid under the hand, so the grab reads as
          // taken.
          background: live ? T.accent : lit ? LINE.strong : LINE.border,
          transition: "width 120ms ease, background 120ms ease",
        }}
      />
    </div>
  );
}

/**
 * Show or hide a panel that sits beside the content: the window's rail, the
 * Build tab's agent column.
 *
 * Two marks, not one, because the app has two of these on screen at once and a
 * control that looks identical in two places is a promise that it does the same
 * thing in both. It does not -- one is window chrome that narrows the whole
 * window, the other is a list inside a single view -- so each draws the thing it
 * actually controls:
 *
 *   - `sidebar` is the platform's rail mark: a window with its left column
 *     shaded while the rail is showing. It narrows to icons rather than leaving,
 *     which is why the frame stays put in both states and only the fill moves.
 *   - `bottom` is the same window with its bottom strip shaded, for the console.
 *     Same family on purpose: these two put a panel away without it leaving the
 *     window, and the shaded edge is which panel. The frame does the
 *     distinguishing rather than two unrelated glyphs.
 *   - `list` is the list itself, three bars, with a chevron for the direction it
 *     goes. Direction is honest here in a way it would not be for the rail: this
 *     panel LEAVES, so "away to the left" and "back from the left" is exactly
 *     what the two states are.
 *
 * `aria-pressed` carries the state to a screen reader in every case, so nothing
 * depends on telling the glyphs apart.
 */
export function PanelToggle({
  collapsed,
  onToggle,
  label,
  variant = "sidebar",
  title,
  style,
}: {
  collapsed: boolean;
  onToggle(): void;
  /** What the panel is, e.g. "agents". Used to build the title both ways. */
  label: string;
  /** Which panel this is. See the note above -- these must not look alike. */
  variant?: "sidebar" | "bottom" | "list";
  /** Overrides the generated `Hide the x` / `Show the x`, for a control with
   *  more to say -- where the panel went, and what brings it back. */
  title?: string;
  style?: CSSProperties;
}) {
  const [hover, setHover] = useState(false);
  return (
    <button
      className="no-drag"
      onClick={onToggle}
      aria-pressed={!collapsed}
      title={title ?? (collapsed ? `Show ${label}` : `Hide ${label}`)}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        border: "none",
        background: hover ? S.controlHover : "transparent",
        color: collapsed ? T.tertiary : T.secondary,
        borderRadius: R.control,
        width: 24,
        height: 22,
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 0,
        cursor: "default",
        transition: "background 90ms ease",
        ...style,
      }}
    >
      <svg
        width={15}
        height={15}
        viewBox="0 0 16 16"
        aria-hidden
        fill="none"
        stroke="currentColor"
        strokeWidth={1.4}
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        {variant !== "list" ? (
          <>
            <rect x={2.2} y={3.4} width={11.6} height={9.2} rx={1.8} />
            {/* Stops at the frame's inner edges: the round cap this glyph now
                shares with the rest of the icon set adds half a stroke at each
                end, and a divider drawn corner to corner overshot into them. */}
            <path d={variant === "bottom" ? "M2.9 9.6h10.2" : "M6.4 4.1v7.8"} />
            {/* The panel itself, shaded while it is showing. Inset evenly inside
                the frame and stopping at the divider: drawn flush to the frame it
                overshot the rounded corners, which at a 15px glyph reads as a
                smudge rather than as a shaded panel. */}
            {collapsed ? null : variant === "bottom" ? (
              // Inset past where the frame's 1.8 corner radius is still
              // curving, or the strip's square ends sit outside the curve.
              <rect x={4.1} y={10.3} width={7.8} height={1.4} fill="currentColor" stroke="none" opacity={0.55} />
            ) : (
              <rect x={3.4} y={4.4} width={3} height={7.2} fill="currentColor" stroke="none" opacity={0.55} />
            )}
          </>
        ) : (
          <>
            <path d="M6.8 4.6h6.6M6.8 8h6.6M6.8 11.4h6.6" />
            {/* Away to the left, or back from it. */}
            <path d={collapsed ? "M2.4 5.8 4.6 8l-2.2 2.2" : "M4.6 5.8 2.4 8l2.2 2.2"} />
          </>
        )}
      </svg>
    </button>
  );
}
