import { lazy } from "react";
import { SectionTitle, Tabs } from "../primitives";
import { useStore } from "../state/store";
import type { ObsTab } from "../state/types";

const RealTracesList = lazy(() =>
  import("./obs/RealTraces").then((m) => ({ default: m.RealTracesList })),
);
const RealTraceDetail = lazy(() =>
  import("./obs/RealTraces").then((m) => ({ default: m.RealTraceDetail })),
);
const RealMetrics = lazy(() =>
  import("./obs/RealMetrics").then((m) => ({ default: m.RealMetrics })),
);
const RealLogs = lazy(() =>
  import("./obs/RealLogs").then((m) => ({ default: m.RealLogs })),
);
const RealCost = lazy(() =>
  import("./obs/RealCost").then((m) => ({ default: m.RealCost })),
);
const RealApprovals = lazy(() =>
  import("./obs/RealApprovals").then((m) => ({ default: m.RealApprovals })),
);
const WiredUsage = lazy(() =>
  import("./wired/WiredStubs").then((m) => ({ default: m.WiredUsage })),
);

const TABS: [ObsTab, string][] = [
  ["traces", "Traces"],
  ["metrics", "Metrics"],
  ["logs", "Logs"],
  ["approvals", "Approvals"],
  ["usage", "Usage"],
  ["cost", "Cost"],
];

export function Observability() {
  const { state, dispatch } = useStore();

  const tab = state.obsTab;
  let content;
  switch (tab) {
    case "traces":
      content = state.traceOpen ? <RealTraceDetail /> : <RealTracesList />;
      break;
    case "metrics":
      content = <RealMetrics />;
      break;
    case "logs":
      content = <RealLogs />;
      break;
    case "usage":
      content = <WiredUsage />;
      break;
    case "cost":
      content = <RealCost />;
      break;
    case "approvals":
      content = <RealApprovals />;
      break;
  }

  return (
    <div>
      <SectionTitle
        title="Observability"
        sub="OpenTelemetry traces, Prometheus-style metrics, and Loki-style logs — on by default."
      />
      <Tabs
        tabs={TABS}
        active={tab}
        onSelect={(id) => dispatch({ type: "setObsTab", tab: id })}
      />
      {content}
    </div>
  );
}
