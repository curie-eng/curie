// Curie Desktop -- the native shell.
//
// This is Chromium with the browser thrown away. What the app keeps is the part
// that is genuinely worth having: a fast, predictable, identical renderer on
// macOS, Windows and Linux, which matters more here than usual because the two
// centerpiece surfaces are a live-updating resource monitor and a pannable
// graph canvas -- exactly the things that drift between WebKit and WebView2 if
// you let the host OS pick the engine.
//
// What it discards is everything that makes a browser a browser: no tabs, no
// omnibox, no history, no extensions, no profile sync, no translate, no
// autofill, no safe-browsing service, no print preview, no media router, no
// spellcheck service. Those are switched off below rather than merely unused,
// so they are not loaded, not networked, and not attack surface. The window can
// reach exactly one document -- the local renderer bundle -- and every outbound
// navigation or popup is refused and handed to the real browser instead.
//
// The result: one window, one renderer process, and a Node side whose entire
// job is the four things a web page cannot do -- run the `curie` binary, read
// Docker's stats, pick a directory, and hold an API key.

import { app, BrowserWindow, ipcMain, shell, clipboard, nativeTheme, Menu } from "electron";
import { join } from "node:path";

import {
  CH,
  type ApiRequest,
  type CliInvocation,
  type ThemePreference,
  type ThemeState,
} from "./shared/contract.js";
import { type ThemeId, themeInfo } from "./shared/themes.js";
import * as cli from "./ipc/cli.js";
import { compareToLive } from "./ipc/manifest.js";
import * as resources from "./ipc/resources.js";
import * as workspace from "./ipc/workspace.js";
import * as api from "./ipc/api.js";
import * as secrets from "./ipc/secrets.js";
import { prefs, update } from "./ipc/store.js";
import { buildMenu } from "./menu.js";
import { findRepoRoot } from "./ipc/repo.js";

// esbuild emits this file as CommonJS (Electron loads the preload as CJS
// regardless, so both outputs share one format), which is why this is
// `__dirname` rather than an `import.meta.url` dance.
declare const __dirname: string;
const here = __dirname;
const DEV_SERVER = process.env.VITE_DEV_SERVER_URL;

/** What this build calls itself. Dev says so, because it is normal to have the
 *  packaged app open at the same time and the two windows are near identical. */
const APP_NAME = DEV_SERVER ? "Curie (Dev)" : "Curie";

// In dev the running binary is Electron's own, so macOS labels the app "Electron"
// wherever it reads the bundle. With a packaged Curie.app open beside it -- the
// normal state while working on this -- the two are hard to tell apart, and it is
// genuinely easy to conclude a change did not land while looking at the snapshot.
//
// `setName` reaches the menu bar (menu.ts labels the first submenu with
// `app.getName()`), the About panel, and the window title. It does NOT reach the
// Dock or the app switcher: those read CFBundleName from the running bundle's
// Info.plist, which belongs to node_modules' Electron.app. Patching a copy of
// that bundle was tried and rejected -- it invalidates the nested Electron
// Framework signature and macOS kills the process on launch, and re-signing an
// Electron app correctly needs a real inside-out signing pass, which has no place
// in a dev loop. So the Dock still says "Electron"; the menu bar is the tell.
//
// `setName` also feeds the userData path, so the existing one is captured and put
// back afterwards: renaming the app must not look like every workspace and
// setting was lost.
if (DEV_SERVER) {
  const userData = app.getPath("userData");
  app.setName(APP_NAME);
  app.setPath("userData", userData);
}

// --- Chromium, trimmed -----------------------------------------------------
// Each of these is a browser subsystem this app has no use for. Disabling them
// at the command line keeps them from initializing at all, which is where the
// startup time and the background network chatter actually go.
app.commandLine.appendSwitch(
  "disable-features",
  [
    "Translate", // no page translation UI or backend
    "MediaRouter", // no Cast discovery on the local network
    "DialMediaRouteProvider",
    "OptimizationHints", // no model downloads from Google
    "AutofillServerCommunication", // no form data leaving the app
    "CalculateNativeWinOcclusion",
    "HardwareMediaKeyHandling",
    "SpareRendererForSitePerProcess", // one document; a spare renderer is pure RSS
    "WebRtcHideLocalIpsWithMdns",
  ].join(","),
);
app.commandLine.appendSwitch("disable-background-networking");
app.commandLine.appendSwitch("disable-component-update");
app.commandLine.appendSwitch("disable-domain-reliability");
// A local operator console is not a place to be composing text in another
// script; skipping the IME/spellcheck dictionary download is free.
app.commandLine.appendSwitch("disable-print-preview");

let win: BrowserWindow | null = null;

/**
 * The stored preference and what it resolves to right now.
 *
 * `shouldUseDarkColors` is Chromium's answer after `themeSource` is applied, so
 * it already accounts for "system" without this having to read the OS itself.
 */
function themeState(): ThemeState {
  const preference = prefs().theme;
  // "system" is the only preference the OS has an opinion about, and the OS only
  // says light or dark, so it resolves to whichever base theme matches. Any other
  // preference names a theme outright and the OS is irrelevant to it.
  const effective: ThemeId =
    preference === "system" ? (nativeTheme.shouldUseDarkColors ? "dark" : "light") : preference;
  return { preference, effective, appearance: themeInfo(effective)?.appearance ?? "dark" };
}

/** What `nativeTheme` should be told, which is not the same as the preference:
 *  picking Solarized Light has to put the window's own chrome in light too. */
function nativeSourceFor(preference: ThemePreference): "system" | "light" | "dark" {
  if (preference === "system") return "system";
  return themeInfo(preference)?.appearance ?? "dark";
}

function createWindow(): BrowserWindow {
  const w = new BrowserWindow({
    width: 1480,
    height: 940,
    minWidth: 1040,
    minHeight: 680,
    show: false,
    title: APP_NAME,
    // Transparent, because the window is given real translucency below and a
    // painted background would sit in front of it.
    backgroundColor: "#00000000",
    // Real macOS vibrancy behind the whole window. This is the single strongest
    // native cue a windowed app has: the desktop shows through the way it does
    // in Finder and Mail. The renderer decides how much reaches each surface --
    // the sidebar paints nothing at all, the content pane paints
    // `--s-content-fill`, which is its own colour at ~60% so the desktop reads
    // faintly behind the text without costing much contrast.
    ...(process.platform === "darwin"
      ? { vibrancy: "sidebar" as const, visualEffectState: "active" as const }
      : {}),
    // Windows 11 has its own material; older Windows and Linux fall back to the
    // renderer's solid colour, which `styles.css` supplies.
    ...(process.platform === "win32" ? { backgroundMaterial: "mica" as const } : {}),
    // The app draws its own chrome so the window reads as one surface rather
    // than a web page wearing an OS hat.
    titleBarStyle: process.platform === "darwin" ? "hiddenInset" : "hidden",
    titleBarOverlay:
      process.platform === "darwin"
        ? undefined
        : { color: "#00000000", symbolColor: "#9a9aa0", height: 52 },
    trafficLightPosition: { x: 16, y: 18 },
    webPreferences: {
      preload: join(here, "preload.cjs"),
      // The renderer is untrusted by construction: it runs in a sandbox, with
      // context isolation, with no Node. Everything privileged is an IPC call
      // to this file, which is a surface small enough to read in one sitting.
      sandbox: true,
      contextIsolation: true,
      nodeIntegration: false,
      webviewTag: false,
      spellcheck: false,
      // No <webview>, no popups, no remote content -- so nothing needs a
      // second renderer or a permission prompt.
      devTools: !app.isPackaged,
    },
  });

  w.once("ready-to-show", () => w.show());

  // index.html says <title>Curie</title>, and a page title replaces the window
  // title by default -- which would undo the dev rename the moment the renderer
  // loaded. The window's name is the shell's to decide, not the page's.
  w.on("page-title-updated", (e) => e.preventDefault());

  // A window created with `show: false` that never becomes ready is invisible
  // with no explanation -- the worst failure mode this app has, because there is
  // nowhere for the error to appear. Say what happened on stderr, and show the
  // window anyway so the failure is at least on screen.
  w.webContents.on("did-fail-load", (_e, code, description, url) => {
    console.error(`[curie] renderer failed to load (${code} ${description}): ${url}`);
    if (!w.isDestroyed()) w.show();
  });
  w.webContents.on("render-process-gone", (_e, details) => {
    console.error(`[curie] renderer process gone: ${details.reason}`);
  });
  // Electron 34 still uses the positional signature here; the typed-event form
  // arrives in a later major.
  w.webContents.on(
    "console-message",
    (_e: unknown, level: number, message: string, line: number, source: string) => {
      // 3 is "error". A renderer exception is otherwise invisible from the
      // terminal that launched the app.
      if (level >= 3) console.error(`[renderer] ${message} (${source}:${line})`);
    },
  );

  // A window that can navigate is a browser. This one cannot: every attempt to
  // leave the local bundle is refused, and external links go to the user's real
  // browser where they belong.
  w.webContents.on("will-navigate", (event, url) => {
    const allowed = DEV_SERVER ? url.startsWith(DEV_SERVER) : url.startsWith("file://");
    if (!allowed) {
      event.preventDefault();
      void shell.openExternal(url);
    }
  });
  w.webContents.setWindowOpenHandler(({ url }) => {
    void shell.openExternal(url);
    return { action: "deny" };
  });
  // Nothing in this app legitimately needs a camera, a microphone, or your
  // location, so no permission prompt should ever be reachable.
  w.webContents.session.setPermissionRequestHandler((_wc, _perm, cb) => cb(false));

  if (DEV_SERVER) void w.loadURL(DEV_SERVER);
  else void w.loadFile(join(here, "..", "dist", "index.html"));

  w.on("closed", () => {
    resources.stopFeed();
    win = null;
  });
  return w;
}

function requireWindow(): BrowserWindow {
  if (!win || win.isDestroyed()) throw new Error("no window");
  return win;
}

// --- IPC -------------------------------------------------------------------

function registerIpc(): void {
  ipcMain.handle(CH.env, async () => {
    const cliPath = cli.findCli();
    let cliVersion: string | null = null;
    let sourceCheckout = false;
    let repoRoot: string | null = null;
    let drift = null;

    if (cliPath) {
      const [version, devProbe, schema] = await Promise.all([
        cli.runOnce(["--version"], { timeoutMs: 8000 }),
        // `curie dev *` only exists from a source checkout, so whether the CLI
        // reports one decides which commands the UI can offer at all.
        cli.runOnce(["dev", "docs-lint", "--help"], { timeoutMs: 8000 }),
        // Ask the binary for its own command surface and compare it to the one
        // this app was generated from. The app can be pointed at a newer or
        // older CLI than it was built against, and either way the operator
        // should be told rather than discovering it through a broken button.
        cli.runOnce(["schema"], { timeoutMs: 10_000 }),
      ]);
      cliVersion = version.code === 0 ? version.stdout.trim() : null;
      sourceCheckout = devProbe.code === 0;
      // Found by walking up from the app's own location rather than read from
      // an environment variable nobody sets. See `findRepoRoot`.
      repoRoot = findRepoRoot(here);
      if (schema.code === 0) {
        try {
          drift = compareToLive(JSON.parse(schema.stdout), cliVersion);
        } catch {
          // Unparseable schema output is not worth failing the whole probe for;
          // the app simply reports no drift information.
        }
      }
    }

    return {
      cliPath,
      cliVersion,
      sourceCheckout,
      repoRoot,
      dockerAvailable: await resources.dockerAvailable(),
      kubectlAvailable: await which("kubectl"),
      helmAvailable: await which("helm"),
      platform: process.platform,
      defaultCwd: cli.defaultCwd(),
      appVersion: app.getVersion(),
      electronVersion: process.versions.electron,
      chromeVersion: process.versions.chrome,
      drift,
    };
  });

  ipcMain.handle(CH.cliRun, (_e, inv: CliInvocation) => cli.startRun(requireWindow(), inv));
  ipcMain.handle(CH.cliCancel, (_e, runId: string) => cli.cancelRun(runId));
  ipcMain.handle(CH.cliWrite, (_e, runId: string, data: string) => cli.writeToRun(runId, data));

  ipcMain.handle(CH.resStart, (_e, intervalMs: number) => {
    update({ resourceIntervalMs: intervalMs });
    resources.startFeed(requireWindow(), intervalMs);
  });
  ipcMain.handle(CH.resStop, () => resources.stopFeed());
  ipcMain.handle(CH.resLogs, (_e, id: string, tail: number) => resources.containerLogs(id, tail));

  ipcMain.handle(CH.wsList, () => workspace.list());
  ipcMain.handle(CH.wsOpen, () => workspace.open(requireWindow()));
  ipcMain.handle(CH.dialogPick, (_e, opts: { kind: "file" | "directory"; title?: string }) =>
    workspace.pick(requireWindow(), opts),
  );
  ipcMain.handle(CH.wsAdd, (_e, path: string) => workspace.add(path));
  ipcMain.handle(CH.wsCreate, (_e, opts: { parentDir: string; name: string; files: Record<string, string> }) =>
    workspace.createAgent(opts),
  );
  ipcMain.handle(CH.wsDelete, (_e, path: string) => workspace.remove(path));
  ipcMain.handle(CH.wsForget, (_e, path: string) => workspace.forget(path));
  ipcMain.handle(CH.wsFiles, (_e, root: string) => workspace.bundleFiles(root));
  ipcMain.handle(CH.wsRead, (_e, root: string, rel: string) => workspace.readFile(root, rel));
  ipcMain.handle(CH.wsWrite, (_e, root: string, rel: string, body: string) =>
    workspace.writeFile(root, rel, body),
  );
  ipcMain.handle(CH.wsReveal, (_e, path: string) => workspace.reveal(path));

  ipcMain.handle(CH.apiConnection, () => api.connection());
  ipcMain.handle(CH.apiConnect, (_e, base: string, key: string | null) => api.connect(base, key));
  ipcMain.handle(CH.apiSignOut, () => api.signOut());
  ipcMain.handle(CH.apiRequest, (_e, req: ApiRequest) => api.request(req));

  ipcMain.handle(CH.secList, () => secrets.list());
  ipcMain.handle(CH.secSet, (_e, name: string, value: string) => secrets.set(name, value));
  ipcMain.handle(CH.secUnset, (_e, name: string) => secrets.unset(name));

  ipcMain.handle(CH.themeGet, (): ThemeState => themeState());
  ipcMain.handle(CH.themeSet, (_e, preference: ThemePreference): ThemeState => {
    // Reject an id this build does not have rather than storing a preference
    // that would resolve to nothing on the next launch.
    if (preference !== "system" && !themeInfo(preference)) return themeState();
    update({ theme: preference });
    nativeTheme.themeSource = nativeSourceFor(preference);
    return themeState();
  });

  ipcMain.handle(CH.graphLoad, () => prefs().graph);
  ipcMain.handle(CH.graphSave, (_e, doc: unknown) => void update({ graph: doc }));

  ipcMain.handle(CH.openExternal, (_e, url: string) => {
    // Only ever hand the OS a web URL. A `file://` or custom scheme arriving
    // from the renderer would be a way to launch something local.
    if (!/^https?:\/\//i.test(url)) throw new Error("refusing to open a non-http URL");
    return shell.openExternal(url);
  });
  ipcMain.handle(CH.copy, (_e, text: string) => clipboard.writeText(text));
}

async function which(bin: string): Promise<boolean> {
  const { execFile } = await import("node:child_process");
  return new Promise((res) => {
    execFile(
      process.platform === "win32" ? "where" : "which",
      [bin],
      { env: { ...process.env, PATH: cli.searchPath() } },
      (err) => res(!err),
    );
  });
}

// --- Lifecycle -------------------------------------------------------------

// One window is the whole app; a second instance should raise the first rather
// than start a second resource feed against the same Docker daemon.
if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on("second-instance", () => {
    if (win) {
      if (win.isMinimized()) win.restore();
      win.focus();
    }
  });

  // The operator's preference drives Chromium AND the native window: vibrancy,
  // the traffic lights and any OS-drawn control follow `themeSource`, so setting
  // it is what makes a light window actually look native rather than a dark app
  // with pale colours in it.
  nativeTheme.themeSource = nativeSourceFor(prefs().theme);

  void app.whenReady().then(() => {
    registerIpc();
    win = createWindow();
    Menu.setApplicationMenu(buildMenu(() => win));
    // Only meaningful while the preference is "system", but harmless otherwise:
    // an explicit choice pins `shouldUseDarkColors`, so the state we send back
    // is unchanged and the renderer re-applies the same attribute.
    nativeTheme.on("updated", () => {
      if (win && !win.isDestroyed()) win.webContents.send(CH.themeChanged, themeState());
    });
    app.on("activate", () => {
      if (BrowserWindow.getAllWindows().length === 0) win = createWindow();
    });
  });

  app.on("window-all-closed", () => {
    if (process.platform !== "darwin") app.quit();
  });

  // A `curie local up` left running after the window closes would be a process
  // the operator can no longer see or stop from the UI. Wind them down.
  app.on("before-quit", () => {
    resources.stopFeed();
    cli.cancelAll();
  });
}
