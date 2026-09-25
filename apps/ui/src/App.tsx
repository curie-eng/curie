import { lazy, Suspense } from "react";
import { C } from "./tokens";
import { useStore } from "./state/store";
import { Sidebar } from "./components/Sidebar";
import { Topbar, DevBanner } from "./components/Topbar";
import { ModalHost } from "./components/ModalHost";
import { Confetti } from "./components/Confetti";
import { Notice, Toast } from "./primitives";

import { WiredOverview } from "./views/wired/WiredOverview";

const Observability = lazy(() =>
  import("./views/Observability").then((m) => ({ default: m.Observability })),
);
const WiredAgents = lazy(() =>
  import("./views/wired/WiredAgents").then((m) => ({ default: m.WiredAgents })),
);
const WiredAgentDetail = lazy(() =>
  import("./views/wired/WiredAgentDetail").then((m) => ({ default: m.WiredAgentDetail })),
);
const WiredWorkItems = lazy(() =>
  import("./views/wired/WiredWorkItems").then((m) => ({ default: m.WiredWorkItems })),
);
const WiredConnections = lazy(() =>
  import("./views/wired/WiredStubs").then((m) => ({ default: m.WiredConnections })),
);
const WiredSettings = lazy(() =>
  import("./views/wired/WiredStubs").then((m) => ({ default: m.WiredSettings })),
);
const WiredEvals = lazy(() =>
  import("./views/wired/WiredEvals").then((m) => ({ default: m.WiredEvals })),
);
const WiredVersions = lazy(() =>
  import("./views/wired/WiredVersions").then((m) => ({ default: m.WiredVersions })),
);

// The console is always backed by the live API. Each nav renders its
// backend-driven view; views that are not wired yet render an honest
// "Coming Soon" stub rather than demo data. Overview stays eager so the
// landing path does not wait on a view chunk; every other view is a
// separate async chunk behind the single Suspense boundary below.
function Main() {
  const { state } = useStore();
  if (state.agentDetail) return <WiredAgentDetail />;
  switch (state.nav) {
    case "overview":
      return <WiredOverview />;
    case "agents":
      return <WiredAgents />;
    case "work-items":
      return <WiredWorkItems />;
    case "evals":
      return <WiredEvals />;
    case "observability":
      return <Observability />;
    case "versions":
      return <WiredVersions />;
    case "connections":
      return <WiredConnections />;
    case "settings":
      return <WiredSettings />;
    default:
      return <WiredOverview />;
  }
}

export function App() {
  const { envDev } = useStore();
  return (
    <div style={{ fontFamily: C.sans, color: C.text, minHeight: "100vh", background: C.page }}>
      <div style={{ display: "flex", minHeight: "100vh" }}>
        <Sidebar />
        <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}>
          <Topbar />
          {envDev ? <DevBanner /> : null}
          <div style={{ flex: 1, padding: "28px 36px", maxWidth: 1280, width: "100%", margin: "0 auto" }}>
            <Suspense fallback={<Notice>Loading…</Notice>}>
              <Main />
            </Suspense>
          </div>
        </div>
      </div>
      <ModalHost />
      <Toast />
      <Confetti />
    </div>
  );
}
