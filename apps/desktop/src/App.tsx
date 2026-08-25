// The window: a translucent source list on the left, an inset content pane on
// the right, and nothing that scrolls except the pane's body.
//
// The layout is the design. A web page is one scrolling column with a header
// stuck to the top of it; an app is a fixed frame whose panes scroll
// independently, and whose chrome never moves. Getting that right does more for
// "this is not a browser tab" than any amount of restyling inside the panes.

import { useEffect, useRef, useState } from "react";

import { AppProvider, useApp } from "./bridge/app";
import { ResourcesProvider } from "./bridge/resources";
import { RunsProvider, useRuns } from "./bridge/runs";
import { Sidebar } from "./shell/Sidebar";
import { Toolbar } from "./shell/Toolbar";
import { RunDrawer } from "./shell/RunDrawer";
import { Palette } from "./shell/Palette";
import { Overview } from "./views/Overview";
import { Resources } from "./views/Resources";
import { Canvas } from "./views/Canvas";
import { Commands } from "./views/Commands";
import { Activity } from "./views/Activity";
import { Settings } from "./views/Settings";
import { R, S } from "./tokens";

function View() {
  const { route } = useApp();
  switch (route) {
    case "resources":
      return <Resources />;
    case "canvas":
      return <Canvas />;
    case "commands":
      return <Commands />;
    case "activity":
      return <Activity />;
    case "settings":
      return <Settings />;
    default:
      return <Overview />;
  }
}

/** Global keys. Kept here so a shortcut behaves the same everywhere, and so a
 *  keystroke aimed at a text field is never stolen. */
function Keys() {
  const app = useApp();
  const runs = useRuns();

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const mod = e.metaKey || e.ctrlKey;
      const target = e.target as HTMLElement | null;
      const typing =
        target?.tagName === "INPUT" || target?.tagName === "TEXTAREA" || target?.isContentEditable;

      if (mod && e.key.toLowerCase() === "k") {
        e.preventDefault();
        return app.setPaletteOpen(true);
      }
      if (mod && e.key.toLowerCase() === "j") {
        e.preventDefault();
        return runs.setDrawerOpen(!runs.drawerOpen);
      }
      if (mod && /^[1-5]$/.test(e.key)) {
        e.preventDefault();
        const routes = ["overview", "resources", "canvas", "commands", "activity"] as const;
        return app.navigate(routes[Number(e.key) - 1]);
      }
      if (e.key === "Escape" && !typing && runs.drawerOpen) {
        return runs.setDrawerOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [app, runs]);

  return null;
}

function Frame() {
  const { route } = useApp();
  const [scrolled, setScrolled] = useState(false);
  const scroller = useRef<HTMLElement>(null);

  // Canvas and Commands manage their own scrolling and want the whole pane;
  // the rest are documents and get padding and a comfortable measure.
  const bleed = route === "canvas" || route === "commands";

  // Two halves of "start the new view at the top", each in the place it belongs.
  //
  // The toolbar's separator is state, so it is corrected during render: the new
  // view must never paint a frame carrying the old view's scrolled look.
  const [lastRoute, setLastRoute] = useState(route);
  if (route !== lastRoute) {
    setLastRoute(route);
    setScrolled(false);
  }

  // Scroll position is the DOM's, so resetting it is a side effect and belongs
  // in one. Reading a ref during render is not allowed, and would be wrong here
  // anyway -- the new children have not been committed yet.
  useEffect(() => {
    if (scroller.current) scroller.current.scrollTop = 0;
  }, [route]);

  return (
    <div style={{ display: "flex", height: "100vh", background: "transparent", overflow: "hidden" }}>
      <Sidebar />

      {/* The content pane. Opaque, so text stays readable over whatever is
          behind the window, and rounded on the left so the vibrancy shows at
          the corners rather than the pane meeting the sidebar in a hard seam. */}
      <div
        style={{
          flex: 1,
          minWidth: 0,
          display: "flex",
          flexDirection: "column",
          background: S.content,
          borderRadius: `${R.pane}px 0 0 ${R.pane}px`,
          overflow: "hidden",
        }}
      >
        <Toolbar scrolled={scrolled} />
        <main
          ref={scroller}
          onScroll={(e) => setScrolled(e.currentTarget.scrollTop > 4)}
          style={{
            flex: 1,
            minWidth: 0,
            overflow: bleed ? "hidden" : "auto",
            padding: bleed ? 16 : "18px 22px 32px",
          }}
        >
          <div style={{ maxWidth: bleed ? "none" : 1080, height: bleed ? "100%" : undefined }}>
            <View />
          </div>
        </main>
        <RunDrawer />
      </div>

      <Palette />
      <Keys />
    </div>
  );
}

export function App() {
  return (
    <AppProvider>
      <RunsProvider>
        <ResourcesProvider>
          <Frame />
        </ResourcesProvider>
      </RunsProvider>
    </AppProvider>
  );
}
