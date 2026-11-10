import { useState, type ReactNode } from "react";
import { C } from "../../tokens";
import { Card, SectionTitle, Chip, Notice, CliHint, PARITY_TRACKING_ISSUE } from "../../primitives";
import { useEvalMatrix } from "../../api/hooks";
import type { EvalStatus, EvalModelSummary } from "../../api/client";
import { ComingSoon } from "./WiredStubs";

// The wired Evals view (#868): renders the eval matrix from GET /evals/matrix
// over the same-origin /api proxy. Rows = fixed eval cases, columns = the most
// recently exercised versions (newest first), cells = each case's outcome on
// that version. A per-model rollup (pass-rate, completion, cost) surfaces the
// matrix's model dimension below the grid.
//
// The matrix is filtered by SUITE (the real dimension on eval traces), so the
// view carries a suite selector defaulting to the platform default suite. A
// fresh workspace with no eval runs degrades honestly to an empty state rather
// than showing demo data (#542). There is no top-level CLI verb that just reads
// the matrix, so the header shows the honest amber no-equivalent glyph.

const DEFAULT_SUITE = "default";
const VERSION_COLUMNS = 5;

// Short, mono-friendly version label: a commit sha renders as its first 7 chars.
function shortVersion(version: string): string {
  return version.length > 7 ? version.slice(0, 7) : version;
}

// One matrix cell's glyph + color. `plumbing_ok` is deliberately amber, not
// green: the case ran but no grader judged it, so it carries no pass/fail
// signal and must never read as a green promotion gate.
function cellPresentation(status: EvalStatus): { glyph: string; color: string; label: string } {
  switch (status) {
    case "pass":
      return { glyph: "✓", color: C.success, label: "pass" };
    case "fail":
      return { glyph: "✕", color: C.destructive, label: "fail" };
    case "plumbing_ok":
      return { glyph: "◇", color: C.warn, label: "ran, not graded" };
    case "missing":
      return { glyph: "·", color: C.muted, label: "not run" };
  }
}

function StatusCell({ status }: { status: EvalStatus }) {
  const { glyph, color, label } = cellPresentation(status);
  return (
    <div
      data-testid="eval-cell"
      data-status={status}
      title={label}
      style={{ textAlign: "center", color, fontFamily: C.mono, fontSize: 14 }}
    >
      {glyph}
    </div>
  );
}

function formatPassRate(s: EvalModelSummary): string {
  if (s.total === 0) return "—";
  return `${Math.round((s.passed / s.total) * 100)}%`;
}

function Matrix({ suite }: { suite: string }) {
  const { data, loading, error } = useEvalMatrix(suite, VERSION_COLUMNS);

  if (error) {
    return (
      <ComingSoon
        title="The eval matrix is not available"
        body={`Could not load the matrix from the backend: ${error}`}
      />
    );
  }
  if (loading || !data) return <Notice padding="40px 20px">Loading the eval matrix…</Notice>;
  if (data.cases.length === 0 || data.versions.length === 0) {
    return (
      <ComingSoon
        title="No eval runs yet"
        body={`No graded eval traces for suite “${suite}”. Trigger an eval run or push to a connected git branch, and the case-by-version matrix lights up here.`}
      />
    );
  }

  // rows = cases, columns = versions. First column is the case id; the rest are
  // one per version, newest first (the order the API returns them in).
  const grid = `minmax(180px, 1.6fr) repeat(${data.versions.length}, minmax(72px, 1fr))`;
  // The API may omit the per-model rollup; an absent list renders no table.
  const modelSummaries = data.model_summaries ?? [];
  const headers: ReactNode[] = [
    <span key="case" style={{ fontSize: 12, color: C.muted }}>
      Case
    </span>,
    ...data.versions.map((v) => (
      <span
        key={v}
        title={v}
        style={{ fontFamily: C.mono, fontSize: 11.5, color: C.muted, textAlign: "center" }}
      >
        {shortVersion(v)}
      </span>
    )),
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <Card>
        <div style={{ overflowX: "auto" }}>
          <div style={{ minWidth: 520 }}>
            <div
              data-testid="matrix-header"
              style={{
                display: "grid",
                gridTemplateColumns: grid,
                gap: 12,
                padding: "0 0 12px",
                borderBottom: "1px solid " + C.border,
                alignItems: "center",
              }}
            >
              {headers.map((h, i) => (
                <div key={i} style={{ textAlign: i === 0 ? "left" : "center" }}>
                  {h}
                </div>
              ))}
            </div>
            {data.rows.map((row) => (
              <div
                key={row.case_id}
                data-testid="matrix-row"
                style={{
                  display: "grid",
                  gridTemplateColumns: grid,
                  gap: 12,
                  padding: "12px 0",
                  alignItems: "center",
                  borderBottom: "1px solid " + C.border,
                }}
              >
                <span
                  style={{
                    fontFamily: C.mono,
                    fontSize: 12.5,
                    color: C.text,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {row.case_id}
                </span>
                {row.cells.map((cell) => (
                  <StatusCell key={cell.version} status={cell.status} />
                ))}
              </div>
            ))}
          </div>
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 16, marginTop: 14, fontSize: 11.5, color: C.muted }}>
          {(["pass", "fail", "plumbing_ok", "missing"] as const).map((s) => {
            const { glyph, color, label } = cellPresentation(s);
            return (
              <span key={s} style={{ display: "inline-flex", alignItems: "center", gap: 5 }}>
                <span style={{ color, fontFamily: C.mono }}>{glyph}</span>
                {label}
              </span>
            );
          })}
        </div>
      </Card>

      {modelSummaries.length > 0 ? (
        <Card>
          <div style={{ fontSize: 13, fontWeight: 500, color: C.text, marginBottom: 4 }}>By model</div>
          <div style={{ fontSize: 12, color: C.muted, marginBottom: 14 }}>
            Pass-rate and cost per model across the shown versions. A model with runs but zero completed turns never
            produced an answer — distinct from a real 0%.
          </div>
          <ModelSummaryTable summaries={modelSummaries} />
        </Card>
      ) : null}
    </div>
  );
}

function ModelSummaryTable({ summaries }: { summaries: EvalModelSummary[] }) {
  const grid = "1.4fr 0.8fr 0.9fr 0.9fr 0.9fr";
  return (
    <div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: grid,
          gap: 12,
          padding: "0 0 10px",
          fontSize: 12,
          color: C.muted,
          borderBottom: "1px solid " + C.border,
        }}
      >
        {["Model", "Pass-rate", "Passed", "Completed", "Cost"].map((h) => (
          <div key={h}>{h}</div>
        ))}
      </div>
      {summaries.map((s, i) => {
        // completed and plumbing default to zero when the API omits them.
        const completed = s.completed ?? 0;
        const plumbing = s.plumbing ?? 0;
        const neverCompleted = s.total > 0 && completed === 0;
        return (
          <div
            key={s.model ?? `unlabelled-${i}`}
            data-testid="model-summary-row"
            style={{
              display: "grid",
              gridTemplateColumns: grid,
              gap: 12,
              padding: "12px 0",
              alignItems: "center",
              borderBottom: "1px solid " + C.border,
              fontSize: 13,
            }}
          >
            <span style={{ fontFamily: C.mono, fontSize: 12.5, color: C.text, display: "flex", alignItems: "center", gap: 8 }}>
              {s.model ?? "unlabelled"}
              {plumbing > 0 ? (
                <Chip color={C.warn} border="rgba(191,135,0,.4)">
                  {plumbing} not graded
                </Chip>
              ) : null}
            </span>
            <span style={{ color: neverCompleted ? C.warn : C.text2, fontFamily: C.mono, fontSize: 12.5 }}>
              {neverCompleted ? "never ran" : formatPassRate(s)}
            </span>
            <span style={{ color: C.text2, fontFamily: C.mono, fontSize: 12.5 }}>
              {s.passed}/{s.total}
            </span>
            <span style={{ color: C.text2, fontFamily: C.mono, fontSize: 12.5 }}>
              {completed}/{s.total}
            </span>
            <span style={{ color: C.text2, fontFamily: C.mono, fontSize: 12.5 }}>
              {s.cost_usd == null ? "—" : `$${s.cost_usd.toFixed(4)}`}
            </span>
          </div>
        );
      })}
    </div>
  );
}

export function WiredEvals() {
  const [suite, setSuite] = useState(DEFAULT_SUITE);
  const [applied, setApplied] = useState(DEFAULT_SUITE);

  const apply = () => {
    const trimmed = suite.trim();
    if (trimmed) setApplied(trimmed);
  };

  return (
    <div>
      <SectionTitle
        title="Evals"
        sub="Fixed test cases run against a version + model, on demand and on every PR. This is not live traffic (see Observability)."
        right={<CliHint noCliEquivalent={PARITY_TRACKING_ISSUE} actionIds={["eval-matrix"]} label="No CLI equivalent" />}
      />
      <form
        onSubmit={(e) => {
          e.preventDefault();
          apply();
        }}
        style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 18 }}
      >
        <label htmlFor="eval-suite" style={{ fontSize: 12.5, color: C.muted }}>
          Suite
        </label>
        <input
          id="eval-suite"
          data-testid="eval-suite-input"
          value={suite}
          onChange={(e) => setSuite(e.target.value)}
          spellCheck={false}
          style={{
            background: C.input,
            border: "1px solid " + C.borderStrong,
            borderRadius: 7,
            padding: "7px 10px",
            color: C.text,
            fontFamily: C.mono,
            fontSize: 13,
            minWidth: 200,
          }}
        />
      </form>
      <Matrix key={applied} suite={applied} />
    </div>
  );
}
