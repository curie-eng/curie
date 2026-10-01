// Charts, hand-drawn as SVG.
//
// There is no charting library here and that is deliberate: the app draws
// exactly three shapes -- a sparkline, a usage bar, and a stacked area -- and a
// general-purpose library would cost more bundle than the whole renderer while
// fighting the design tokens for control of the colors.
//
// The shared rule is how gaps are drawn. A null sample means "not measured",
// which is a different fact from zero, so the line breaks rather than dipping to
// the floor. A chart that dips to zero when a container stops is telling you a
// container idled when what actually happened is that it died.

import { useCallback, useEffect, useId, useRef, useState, type ReactNode } from "react";
import { ACCENT, FONT, LINE, S, STATUS, T } from "../tokens";


/**
 * Measure a container so a chart can fill it.
 *
 * The charts draw into an SVG with real pixel coordinates rather than a scaled
 * viewBox, because a viewBox stretched to fit would distort stroke widths and
 * text. That means they need a number, and in a resizable window the only honest
 * source of that number is the box they are in.
 */
export function useMeasuredWidth<T extends HTMLElement>(): [
  (node: T | null) => void,
  number,
] {
  const [width, setWidth] = useState(0);
  const observer = useRef<ResizeObserver | null>(null);

  const ref = useCallback((node: T | null) => {
    observer.current?.disconnect();
    if (!node) return;
    setWidth(node.clientWidth);
    observer.current = new ResizeObserver(([entry]) => {
      // `contentRect` excludes padding, which is what the chart should fill.
      setWidth(Math.max(0, Math.floor(entry.contentRect.width)));
    });
    observer.current.observe(node);
  }, []);

  useEffect(() => () => observer.current?.disconnect(), []);

  return [ref, width];
}

/** A chart that fills its parent's width. Renders nothing until it has been
 *  measured, so the first paint is never a wrong-sized chart that jumps. */
export function FitWidth({
  height,
  children,
}: {
  height: number;
  children: (width: number) => ReactNode;
}) {
  const [ref, width] = useMeasuredWidth<HTMLDivElement>();
  return (
    <div ref={ref} style={{ width: "100%", height }}>
      {width > 0 ? children(width) : null}
    </div>
  );
}

export function Sparkline({
  values,
  width = 120,
  height = 26,
  color = ACCENT,
  fill = true,
  /** Force the top of the scale; without it the line autoscales to its own max,
   *  which is right for CPU and wrong for comparing two containers. */
  max: forcedMax,
  ariaLabel,
}: {
  values: readonly (number | null)[];
  width?: number;
  height?: number;
  color?: string;
  fill?: boolean;
  max?: number;
  ariaLabel?: string;
}) {
  const gradientId = useId();
  const points = values.length ? values : [null];
  const observed = points.filter((v): v is number => v !== null && Number.isFinite(v));
  const max = forcedMax ?? Math.max(...observed, 0.0001);
  const step = points.length > 1 ? width / (points.length - 1) : width;
  const y = (v: number) => height - 2 - (Math.min(v, max) / max) * (height - 4);

  // Build one path per unbroken run so a gap is a gap.
  const segments: string[] = [];
  let current: string[] = [];
  points.forEach((v, i) => {
    if (v === null || !Number.isFinite(v)) {
      if (current.length > 1) segments.push(current.join(" "));
      current = [];
      return;
    }
    current.push(`${current.length ? "L" : "M"}${(i * step).toFixed(2)},${y(v).toFixed(2)}`);
  });
  if (current.length > 1) segments.push(current.join(" "));
  // A single measured point has no line to draw; a 1px cap keeps it visible.
  const singles = points
    .map((v, i) => ({ v, i }))
    .filter(
      ({ v, i }) =>
        v !== null &&
        Number.isFinite(v) &&
        (points[i - 1] === null || points[i - 1] === undefined) &&
        (points[i + 1] === null || points[i + 1] === undefined),
    );

  const areaPath =
    fill && segments.length
      ? `${segments[segments.length - 1]} L${(((points.length - 1) * step) || 0).toFixed(2)},${height} L0,${height} Z`
      : null;

  if (!observed.length) {
    return (
      <svg width={width} height={height} role="img" aria-label={ariaLabel ?? "no data"}>
        <line
          x1={0}
          y1={height / 2}
          x2={width}
          y2={height / 2}
          stroke={LINE.border}
          strokeDasharray="2 3"
        />
      </svg>
    );
  }

  return (
    <svg
      width={width}
      height={height}
      role="img"
      aria-label={ariaLabel ?? `sparkline, peak ${max.toFixed(1)}`}
      style={{ display: "block", overflow: "visible" }}
    >
      {areaPath ? (
        <>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.26} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <path d={areaPath} fill={`url(#${gradientId})`} />
        </>
      ) : null}
      {segments.map((d, i) => (
        <path
          key={i}
          d={d}
          fill="none"
          stroke={color}
          strokeWidth={1.4}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      ))}
      {singles.map(({ v, i }) => (
        <circle key={`p${i}`} cx={i * step} cy={y(v as number)} r={1.4} fill={color} />
      ))}
    </svg>
  );
}

/** A usage bar with an optional limit. When the limit is unknown the bar shows
 *  the value against the largest peer instead, and says so in its title -- an
 *  unbounded container is not at "100%" of anything. */
export function UsageBar({
  value,
  max,
  color = ACCENT,
  height = 5,
  title,
  warnAt = 0.85,
}: {
  value: number | null;
  max: number | null;
  color?: string;
  height?: number;
  title?: string;
  /**
   * Fraction of `max` above which the bar turns into a warning, or `null` for a
   * bar that never warns.
   *
   * `null` is not a style preference. A warning colour means "this is close to a
   * ceiling", which is only true when `max` IS a ceiling -- a memory limit, a
   * CPU count. When `max` is merely the largest value in a list, the leading row
   * is at 100% by definition and warning about it says only that the biggest
   * item is the biggest item. `RankedBars` passes `null` for exactly that
   * reason.
   */
  warnAt?: number | null;
}) {
  const ratio = value !== null && max && max > 0 ? Math.min(1, value / max) : null;
  const warn = warnAt !== null && ratio !== null && ratio > warnAt;
  return (
    <div
      title={title}
      style={{
        width: "100%",
        height,
        borderRadius: 999,
        background: S.selected,
        overflow: "hidden",
      }}
    >
      {ratio === null ? (
        <div style={{ width: "100%", height: "100%", background: `${LINE.border}` }} />
      ) : (
        <div
          style={{
            width: `${(ratio * 100).toFixed(1)}%`,
            height: "100%",
            background: warn ? STATUS.warn : color,
            transition: "width 240ms ease",
          }}
        />
      )}
    </div>
  );
}

export interface StackBand {
  readonly key: string;
  readonly color: string;
  readonly values: readonly (number | null)[];
}

/** Stacked area: total machine load, split by workload. The bands are drawn
 *  bottom-up in the order given so the colour order is stable frame to frame --
 *  a stack that re-sorts itself every tick is unreadable. */
export function StackedArea({
  bands,
  width,
  height,
  max: forcedMax,
  guides = [],
}: {
  bands: readonly StackBand[];
  width: number;
  height: number;
  max?: number;
  /** Horizontal reference lines, in data units. Drawn only when they land
   *  inside the current scale -- a guide pinned off the top of the chart is
   *  clutter, and one squashed onto the axis is a lie about the scale. */
  guides?: readonly { value: number; label: string }[];
}) {
  const length = Math.max(0, ...bands.map((b) => b.values.length));
  if (!length || !bands.length) {
    return (
      <svg width={width} height={height}>
        <line
          x1={0}
          y1={height - 1}
          x2={width}
          y2={height - 1}
          stroke={LINE.separator}
          strokeDasharray="3 3"
        />
      </svg>
    );
  }

  const totals = Array.from({ length }, (_, i) =>
    bands.reduce((sum, b) => sum + (b.values[i] ?? 0), 0),
  );
  const max = forcedMax ?? Math.max(...totals, 0.0001);
  const step = length > 1 ? width / (length - 1) : width;
  const y = (v: number) => height - (Math.min(v, max) / max) * height;

  const running = new Array<number>(length).fill(0);
  const paths = bands.map((band) => {
    const top: string[] = [];
    const bottom: string[] = [];
    for (let i = 0; i < length; i++) {
      const base = running[i];
      const value = band.values[i] ?? 0;
      top.push(`${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${y(base + value).toFixed(1)}`);
      bottom.unshift(`L${(i * step).toFixed(1)},${y(base).toFixed(1)}`);
      running[i] = base + value;
    }
    return { key: band.key, color: band.color, d: `${top.join(" ")} ${bottom.join(" ")} Z` };
  });

  return (
    <svg width={width} height={height} style={{ display: "block", overflow: "visible" }}>
      {paths.map((p) => (
        <path
          key={p.key}
          d={p.d}
          fill={p.color}
          fillOpacity={0.55}
          stroke={p.color}
          strokeWidth={0.6}
        />
      ))}
      {guides
        .filter((g) => g.value > 0 && g.value < max)
        .map((g) => (
          <g key={g.label}>
            <line
              x1={0}
              y1={y(g.value)}
              x2={width}
              y2={y(g.value)}
              stroke={LINE.strong}
              strokeWidth={1}
              strokeDasharray="3 4"
            />
            <text
              x={width - 2}
              y={y(g.value) - 4}
              textAnchor="end"
              fill={T.quaternary}
              fontSize={9}
              fontFamily={FONT.ui}
            >
              {g.label}
            </text>
          </g>
        ))}
    </svg>
  );
}

/** Horizontal ranked bars, for "which agent spent the most" style panels. */
export function RankedBars({
  rows,
  format,
}: {
  rows: readonly { label: string; value: number; color?: string }[];
  format(value: number): string;
}) {
  const max = Math.max(...rows.map((r) => r.value), 0.0001);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
      {rows.map((r) => (
        <div key={r.label} style={{ display: "grid", gridTemplateColumns: "1fr 62px", gap: 10 }}>
          <div style={{ minWidth: 0 }}>
            <div
              style={{
                fontSize: 11,
                color: T.secondary,
                marginBottom: 3,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {r.label}
            </div>
            <UsageBar
              value={r.value}
              max={max}
              color={r.color ?? ACCENT}
              height={4}
              warnAt={null}
            />
          </div>
          <div
            style={{
              fontSize: 11,
              color: T.secondary,
              fontFamily: FONT.mono,
              alignSelf: "end",
              textAlign: "right",
              fontVariantNumeric: "tabular-nums",
            }}
          >
            {format(r.value)}
          </div>
        </div>
      ))}
    </div>
  );
}
