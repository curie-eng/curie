// The window: a translucent source list on the left, an inset content pane on
// the right, and nothing that scrolls except the pane's body.
//
// The layout is the design. A web page is one scrolling column with a header
// stuck to the top of it; an app is a fixed frame whose panes scroll
// independently, and whose chrome never moves. Getting that right does more for
// "this is not a browser tab" than any amount of restyling inside the panes.

import { useCallback, useEffect, useRef, useState } from "react";

import { AppProvider, useApp } from "./bridge/app";
import { ResourcesProvider } from "./bridge/resources";
import { RunsProvider, useRuns } from "./bridge/runs";
import { Sidebar } from "./shell/Sidebar";
import { Toolbar } from "./shell/Toolbar";
import { Console } from "./shell/Console";
import { Palette } from "./shell/Palette";
import { Overview } from "./views/Overview";
import { Build } from "./views/Build";
import { Tiers } from "./views/Tiers";
import { Resources } from "./views/Resources";
import { Observability } from "./views/Observability";
import { Canvas } from "./views/Canvas";
import { Commands } from "./views/Commands";
import { Activity } from "./views/Activity";
import { Settings } from "./views/Settings";
import { RunSheetHost } from "./views/Actions";
import { PANE_FADE } from "./tokens";
import { readBool, write as remember } from "./lib/uiState";

function View() {
  const { route } = useApp();
  switch (route) {
    case "build":
      return <Build />;
    case "tiers":
      return <Tiers />;
    case "observability":
      return <Observability />;
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
function Keys({ onToggleRail }: { onToggleRail(): void }) {
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
      if (mod && e.key.toLowerCase() === "b") {
        e.preventDefault();
        return onToggleRail();
      }
      if (mod && e.key.toLowerCase() === "j") {
        e.preventDefault();
        return runs.setConsoleOpen(!runs.consoleOpen);
      }
      // The console is always on screen, so this focuses its prompt rather than
      // opening anything. A console you have to reach for with the mouse is a
      // button with extra steps.
      if (mod && e.key.toLowerCase() === "l") {
        e.preventDefault();
        runs.setConsoleHidden(false);
        runs.setConsoleOpen(true);
        // The console may have been hidden a moment ago, so the input does not
        // exist until this render commits.
        requestAnimationFrame(() =>
          document.querySelector<HTMLInputElement>("[data-console-input]")?.focus(),
        );
        return;
      }
      if (mod && /^[1-6]$/.test(e.key)) {
        e.preventDefault();
        // Top to bottom as the sidebar draws them, Commands last: a number
        // that does not match the row it lands on is worse than no shortcut.
        // History has no number of its own -- it is a pane of Commands, reached
        // with the toolbar's switch or from the Go menu.
        const routes = [
          "overview",
          "build",
          "canvas",
          "resources",
          "observability",
          "tiers",
          "commands",
        ] as const;
        return app.navigate(routes[Number(e.key) - 1]);
      }
      if (e.key === "Escape" && !typing && runs.consoleOpen) {
        return runs.setConsoleOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [app, runs, onToggleRail]);

  return null;
}

/** The scroller's bottom band, as a mask: opaque until the last 28px, then out.
 *  Eased rather than linear for the same reason `PANE_FADE` is -- a slope that
 *  jumps from zero to constant on one pixel is what the eye reads as a line. */
const CONTENT_FADE =
  "linear-gradient(to bottom, #000 calc(100% - 28px), rgba(0,0,0,0.85) calc(100% - 20px), rgba(0,0,0,0.5) calc(100% - 11px), rgba(0,0,0,0.15) calc(100% - 4px), transparent 100%)";

/** The measure. Wide enough that a large window is used rather than framed by a
 *  dead band, narrow enough that a table never runs to arm's length. */
const CONTENT_MAX = 1440;

function Frame() {
  const { route } = useApp();
  const runs = useRuns();
  const [scrolled, setScrolled] = useState(false);
  const scroller = useRef<HTMLElement>(null);

  // Canvas and Commands manage their own scrolling and want the whole pane;
  // the rest are documents and get padding and a comfortable measure.
  const bleed = route === "canvas" || route === "commands";
  // One value for the pane's horizontal inset, so the console at the foot lines
  // up with the content above it by construction rather than by a number copied
  // into two files that then drift.
  const padX = bleed ? 16 : 22;
  // See the mask below: the ramp is only worth having when the console is there
  // to be faded into.
  const fade = route !== "canvas" && !runs.consoleHidden;

  // The rail, collapsed or not. A remembered UI position: the operator who
  // wants the width back should not have to ask for it at every launch.
  const [railCollapsed, setRailCollapsed] = useState(() => readBool("rail.collapsed", false));
  const toggleRail = useCallback(() => setRailCollapsed((prev) => !prev), []);
  // Written from an effect, not from inside the updater. A state updater has to
  // be pure -- React invokes it more than once, and in development invokes it
  // twice on purpose -- so a `localStorage.setItem` in there runs an unknown
  // number of times against an unknown `prev`, and what ends up on disk can
  // disagree with what is on screen. It did.
  useEffect(() => remember("rail.collapsed", railCollapsed), [railCollapsed]);

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
      <Sidebar collapsed={railCollapsed} />

      {/* The content pane. Translucent, like the sidebar -- the window's vibrancy
          carries through it, so the desktop reads faintly behind the whole app
          rather than only behind the source list. It is far less translucent
          than the sidebar (see `--s-content-fill`) because this is where the
          text is, and translucency is paid for in contrast.

          Square against the sidebar. The left corners used to be rounded, on the
          idea that letting the vibrancy through at the seam was softer than a
          hard edge. It is not: the sidebar is full height and the pane is full
          height, so rounding one of them cuts two notches out of an edge that
          runs the whole window, and the eye reads a notch as a mistake rather
          than as softness. The two surfaces already separate by value; the seam
          does not need a radius as well. */}
      <div
        style={{
          flex: 1,
          minWidth: 0,
          display: "flex",
          flexDirection: "column",
          // Eased in from the seam rather than starting at full strength on it.
          // The edge is square (a notch in a full-height join reads as a
          // mistake), but square does not have to mean abrupt: the first 40px
          // of the pane fade up from nothing, so the sidebar's vibrancy carries
          // a little way across and the two surfaces meet without a step.
          //
          // The toolbar paints the same ramp from the same origin. It is a child
          // of this pane at the same left edge, so any other value there would
          // put a hard corner back at the top of the seam.
          background: PANE_FADE,
          overflow: "hidden",
        }}
      >
        <Toolbar
          scrolled={scrolled}
          railCollapsed={railCollapsed}
          onToggleRail={toggleRail}
        />
        <main
          ref={scroller}
          onScroll={(e) => setScrolled(e.currentTarget.scrollTop > 4)}
          style={{
            flex: 1,
            minWidth: 0,
            overflow: bleed ? "hidden" : "auto",
            padding: bleed ? padX : `18px ${padX}px 32px`,
            // The scroller ends where the console's card begins, so a card
            // scrolled part-way is cut off flat against a rounded corner, and a
            // square edge butted onto a rounded one reads as a frame around the
            // console rather than as two cards. Insetting the console to put a
            // band of pane between them fixes the collision and costs more than
            // it is worth: the band is opaque, so it hides content the console
            // was not covering.
            //
            // Fading the scroller's own last band costs nothing. Content
            // dissolves as it reaches the edge instead of being guillotined, so
            // there is no square edge left to collide with. At rest nothing
            // fades -- the 32px bottom padding keeps the last card clear of the
            // ramp -- so this is only ever visible mid-scroll, which is exactly
            // when something is being cut.
            //
            // Applied on the full-bleed routes too, where the view scrolls
            // inside itself rather than here: the mask is on this box, so it
            // fades whatever reaches this edge either way, and the Commands
            // list has exactly the same collision. Canvas is the one exception
            // -- it is not a document meeting an edge, it is a graph, and a node
            // fading out for a reason that is really about layout would read as
            // state the node does not have.
            //
            // And only while there is a console to meet. The fade exists to stop
            // content being guillotined against the console's rounded top edge;
            // with the console dismissed the pane runs to the bottom of the
            // window and there is nothing there to collide with, so the ramp was
            // just softening the last line of the page for no reason.
            maskImage: fade ? CONTENT_FADE : undefined,
            WebkitMaskImage: fade ? CONTENT_FADE : undefined,
          }}
        >
          {/* Centred, and capped so prose never runs to a 2000px measure.
              Left-aligned it read as the whole app being shoved against the
              sidebar with a dead band on the right -- the wider the window, the
              worse, because every pixel of extra width went to the gap. Centred,
              a window wider than the cap grows the margins evenly and the
              content stays where the eye already is. */}
          <div
            style={{
              maxWidth: bleed ? "none" : CONTENT_MAX,
              marginInline: bleed ? undefined : "auto",
              width: "100%",
              height: bleed ? "100%" : undefined,
            }}
          >
            <View />
          </div>
        </main>
        <Console padX={padX} />
      </div>

      {/* One sheet host for the whole app: any control anywhere opens the
          generated form over the screen it was pressed on, rather than sending
          the operator to the Commands list to find it again. */}
      <RunSheetHost />
      <Palette />
      <Keys onToggleRail={toggleRail} />
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
