// Settings: the connection, the secrets, and what this shell actually is.
//
// The secrets panel is deliberately thin. It lists names and can add or remove
// one, and that is all -- values go straight to `curie secrets` and are never
// read back, because a desktop app that could show you a secret is a second
// place secrets live.

import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";

import { useApp } from "../bridge/app";
import { isLoopback, LOCAL_API_KEY, LOCAL_API_URL } from "../../electron/shared/contract";
import { THEMES } from "../../electron/shared/themes";
import { surfacesById } from "../lib/surfaces";
import { Actions, DrivenBy, RunButton } from "./Actions";
import { bridge, hasShell } from "../bridge/bridge";
import { ACCENT, F, FONT, LINE, R, S, STATUS, T, tint } from "../tokens";
import { SignIn } from "./SignIn";
import { ThemePreview } from "./ThemePreview";
import {
  Badge,
  Button,
  Field,
  Group,
  Input,
  Mono,
  Notice,
  Row,
  SectionHeader,
  Segmented,
  Sheet,
} from "../primitives";

/**
 * A settings panel: a section header *outside* a grouped box.
 *
 * That placement is most of what makes a grouped list read as native rather than
 * as a card with a title bar. It exists as a component so no panel can get it
 * wrong by hand.
 */
function Panel({
  title,
  right,
  children,
}: {
  title: ReactNode;
  right?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section>
      <SectionHeader right={right}>{title}</SectionHeader>
      <Group style={{ padding: 14 }}>{children}</Group>
    </section>
  );
}

/**
 * Settings, in tabs.
 *
 * Nine panels in one column was a scroll, not a screen: everything was equally
 * far away and nothing said what belonged with what. Grouping them means the
 * question you arrived with ("where do I put the API key", "why does it think
 * curie is missing") lands on a tab rather than on a scrollbar.
 *
 * The control is `Segmented` and it sits INSIDE the view, not in the toolbar.
 * The Commands pane switch lives in the toolbar for two reasons that do not
 * apply here -- three things deep-link straight to History, and its two panes
 * want different frame padding -- and neither is worth exporting a route per
 * settings tab for. Horizontal, because a vertical rail beside a rail is two
 * navigation systems answering the same question.
 *
 * Which tab you were on is a UI position, so it lives in `localStorage` beside
 * the Build cursor and the agent-sheet tier, not in platform state.
 */
const TABS = [
  { value: "connection", label: "Connection", title: "The platform API, and the secrets it uses" },
  { value: "appearance", label: "Appearance", title: "Theme" },
  { value: "machine", label: "Machine", title: "What this app can see on this machine" },
  { value: "developer", label: "Developer", title: "Contributor scripts and things that print" },
  { value: "about", label: "About", title: "What this shell is" },
] as const;

type Tab = (typeof TABS)[number]["value"];

const TAB_KEY = "curie.settings.tab";

function storedTab(): Tab {
  try {
    const raw = localStorage.getItem(TAB_KEY);
    if (TABS.some((t) => t.value === raw)) return raw as Tab;
  } catch {
    // A disabled or full localStorage must not cost anyone the Settings screen.
  }
  return "connection";
}

export function Settings() {
  const [tab, setTab] = useState<Tab>(storedTab);

  const choose = (next: Tab) => {
    setTab(next);
    try {
      localStorage.setItem(TAB_KEY, next);
    } catch {
      // Same as above: remembering the tab is a nicety, not a requirement.
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div>
        <Segmented<Tab> options={TABS} value={tab} onChange={choose} />
      </div>

      {tab === "connection" ? (
        <>
          <AccessPanel />
          <ApiPanel />
          <SecretsPanel />
        </>
      ) : null}

      {tab === "appearance" ? <AppearancePanel /> : null}

      {tab === "machine" ? (
        <>
          <EnvironmentPanel />
          <CommandSurfacePanel />
          <MaintenancePanel />
        </>
      ) : null}

      {tab === "developer" ? (
        <>
          <DevPanel />
          <ReferencePanel />
        </>
      ) : null}

      {tab === "about" ? <AboutPanel /> : null}
    </div>
  );
}

/** A hovered theme has to be settled on before it is drawn.
 *
 *  Each row swaps the whole preview -- every surface, every colour -- so moving
 *  the pointer across the grid repainted it once per row it crossed. Seventeen
 *  themes deep that is a strobe, not a preview.
 *
 *  Leaving is held back for the same reason, and it is the bigger half. A row
 *  clears the preview as the pointer goes and the next row sets it as the
 *  pointer arrives, so a sweep did not run theme to theme: it bounced through
 *  the current theme between every pair, twice the flashing for the same
 *  distance travelled. Holding the clear lets the next row cancel it, so
 *  crossing the grid changes nothing until the pointer stops, and stopping
 *  outside it still returns to what you are wearing. */
function useSettledPreview(delay: number): [string | null, (id: string | null) => void] {
  const [preview, setPreview] = useState<string | null>(null);
  const pending = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(
    () => () => {
      if (pending.current !== undefined) clearTimeout(pending.current);
    },
    [],
  );

  const show = useCallback(
    (id: string | null) => {
      if (pending.current !== undefined) clearTimeout(pending.current);
      pending.current = setTimeout(() => {
        pending.current = undefined;
        setPreview(id);
      }, delay);
    },
    [delay],
  );

  return [preview, show];
}

function AppearancePanel() {
  const app = useApp();
  const theme = app.theme;
  const preference = theme?.preference ?? "system";
  // Long enough that crossing a row does not count as choosing it, short enough
  // that stopping on one still feels answered rather than loaded.
  const [preview, setPreview] = useSettledPreview(140);

  // Hovering shows that theme; otherwise the preview shows what is on. "System"
  // has no palette of its own, so it previews whichever the OS currently
  // resolves to -- which is the honest answer to "what will I get".
  const chosen = preference === "system" ? (theme?.effective ?? "dark") : preference;
  const shownTheme = preview ?? chosen;
  const shownLabel =
    THEMES.find((t) => t.id === shownTheme)?.label ??
    (shownTheme === "dark" ? "Curie Dark" : "Curie Light");

  return (
    <Panel
      title="Appearance"
      right={
        <span style={{ ...F.footnote, color: T.quaternary }}>
          {THEMES.length} themes
        </span>
      }
    >
      <Row first>
        <Field
          label="Theme"
          hint="System follows the OS, including when it switches at sunset. Anything else is absolute."
        >
          {/* Two columns where there is room for two. The list used to sit in a
              narrow band with the rest of the pane empty, and a swatch cannot
              answer the question you actually have, which is what the theme
              looks like applied. Hovering previews; the preview otherwise shows
              what you are wearing. */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(320px, 1fr) minmax(0, 1fr)",
              gap: 16,
              alignItems: "start",
            }}
          >
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <ThemeChoice
              id="system"
              label="System"
              swatch={null}
              active={preference === "system"}
              note={theme?.preference === "system" ? `currently ${theme.effective}` : undefined}
              onPick={() => app.setTheme("system")}
              onPreview={setPreview}
            />
            {/* A grid rather than a list: seventeen themes read as a palette to
                scan, and each one is recognisable from its own colours without
                being applied. */}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(168px, 1fr))",
                gap: 6,
              }}
            >
              {THEMES.map((t) => (
                <ThemeChoice
                  key={t.id}
                  id={t.id}
                  label={t.label}
                  swatch={t.swatch}
                  active={preference === t.id}
                  onPick={() => app.setTheme(t.id)}
                  onPreview={setPreview}
                />
              ))}
            </div>
          </div>

          <ThemePreview theme={shownTheme} label={shownLabel} />
          </div>
        </Field>
      </Row>
    </Panel>
  );
}

/** One theme, drawn in its own colours. */
function ThemeChoice({
  onPreview,
  id,
  label,
  swatch,
  active,
  note,
  onPick,
}: {
  id: string;
  label: string;
  swatch: readonly [string, string, string] | null;
  active: boolean;
  note?: string;
  onPick(): void;
  /** Hovering or focusing shows this theme in the preview beside the list, so
   *  the answer arrives before you commit to wearing it. */
  onPreview(theme: string | null): void;
}) {
  return (
    <button
      onClick={onPick}
      title={id}
      aria-pressed={active}
      onMouseEnter={() => onPreview(id === "system" ? null : id)}
      onMouseLeave={() => onPreview(null)}
      onFocus={() => onPreview(id === "system" ? null : id)}
      onBlur={() => onPreview(null)}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 9,
        width: "100%",
        textAlign: "left",
        border: `1px solid ${active ? ACCENT : LINE.border}`,
        background: active ? tint(ACCENT, 0.1) : S.control,
        borderRadius: R.control,
        padding: "6px 9px",
        cursor: "default",
        color: "inherit",
      }}
    >
      {swatch ? (
        // The theme's own content, raised and accent colours, so the swatch is
        // the palette rather than a label with a dot next to it.
        <span
          aria-hidden
          style={{
            display: "flex",
            flex: "none",
            width: 26,
            height: 18,
            borderRadius: 4,
            overflow: "hidden",
            border: `1px solid ${LINE.border}`,
          }}
        >
          <span style={{ flex: 1, background: swatch[0] }} />
          <span style={{ flex: 1, background: swatch[1] }} />
          <span style={{ flex: "none", width: 6, background: swatch[2] }} />
        </span>
      ) : (
        <span
          aria-hidden
          style={{
            flex: "none",
            width: 26,
            height: 18,
            borderRadius: 4,
            border: `1px dashed ${LINE.strong}`,
          }}
        />
      )}
      <span
        style={{
          ...F.body,
          minWidth: 0,
          flex: 1,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
          color: active ? T.primary : T.secondary,
        }}
      >
        {label}
      </span>
      {/* Beside the label, not beneath it. Stacked, the note gave this one row
          a second line, so System stood taller than every other theme and the
          list reflowed as the note came and went. */}
      {note ? (
        <span style={{ ...F.footnote, flex: "none", color: T.tertiary }}>{note}</span>
      ) : null}
    </button>
  );
}

/** What is authorizing this console, said out loud, with the way out of it.
 *
 *  The state existed and was invisible. Both hosts authorize every call, and
 *  nothing on any screen said so or offered a way to stop: the shell holds the
 *  platform key, a browser tab holds a session (ADR-0083), and the only hint
 *  either way was a pill that read "Connected". `revoke_console_session` was
 *  written months ago and reachable from nowhere, so a console could be signed
 *  in and never out. */
function AccessPanel() {
  const app = useApp();
  const api = app.api;
  const [signingIn, setSigningIn] = useState(false);
  const [busy, setBusy] = useState(false);

  const signOut = async () => {
    setBusy(true);
    try {
      await app.signOutApi();
      app.refreshAgents();
    } finally {
      setBusy(false);
    }
  };

  // Four states, and the difference between the middle two is the whole reason
  // this panel exists: an API that refuses this console looks identical to one
  // that is not there unless something says which.
  const state = !api ? "unknown" : api.via ? "in" : api.reachable ? "out" : "unreachable";

  return (
    <Panel
      title="Access"
      right={
        state === "in" ? (
          <Badge color={ACCENT} filled>
            signed in
          </Badge>
        ) : state === "out" ? (
          <Badge color={STATUS.warn} filled>
            signed out
          </Badge>
        ) : null
      }
    >
      {signingIn ? <SignIn onClose={() => setSigningIn(false)} /> : null}

      <div style={{ fontSize: 12, color: T.tertiary, marginBottom: 12, lineHeight: 1.6 }}>
        {state === "in" && api?.via === "key" ? (
          <>
            Authorized by the platform key, which the desktop shell holds and the page never
            receives. Signing out forgets it here; it does not revoke anything, because a key is
            not a session. On this computer the local stack still answers afterwards, using the
            key Curie ships with.
          </>
        ) : state === "in" ? (
          <>
            Signed in with a console session: a cookie this page cannot read, which expires on its
            own and can be revoked. Signing out revokes it at the API rather than only dropping it
            here, so a copy of the cookie stops working too.
          </>
        ) : state === "out" ? (
          <>
            The API is answering and refusing this console. Nothing on these screens will load
            until it has a session.
          </>
        ) : state === "unreachable" ? (
          <>Nothing is answering at {api?.baseUrl || "that address"}, so there is nothing to sign in to yet.</>
        ) : (
          <>Still checking.</>
        )}
      </div>

      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        {state === "in" ? (
          <Button tone="plain" busy={busy} onClick={() => void signOut()}>
            Sign out
          </Button>
        ) : null}
        {state === "out" ? (
          <Button tone="primary" onClick={() => setSigningIn(true)}>
            Sign in
          </Button>
        ) : null}
        <div style={{ flex: 1 }} />
        <span style={{ ...F.footnote, color: T.quaternary }}>
          One console, not one person: there are no per-user accounts yet, so this says how this
          window is authorized, not who you are.
        </span>
      </div>
    </Panel>
  );
}

function ApiPanel() {
  const app = useApp();
  const [baseUrl, setBaseUrl] = useState(app.api?.baseUrl ?? "");
  const [apiKey, setApiKey] = useState("");
  const [busy, setBusy] = useState(false);

  // The stored URL can arrive after this panel mounts. Adopt it during render
  // rather than in an effect, and only when it actually changed, so a URL being
  // typed is never yanked back to the stored one.
  const [lastKnownUrl, setLastKnownUrl] = useState(app.api?.baseUrl);
  if (app.api?.baseUrl !== lastKnownUrl) {
    setLastKnownUrl(app.api?.baseUrl);
    if (app.api?.baseUrl) setBaseUrl(app.api.baseUrl);
  }

  const connect = async () => {
    setBusy(true);
    try {
      // An empty key field means "leave the stored key alone", which is what
      // lets someone change the URL without re-pasting a key they cannot see.
      await app.connectApi(baseUrl, apiKey === "" ? null : apiKey);
      setApiKey("");
      app.refreshAgents();
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="Where your agents live"
      right={
        app.api ? (
          <Badge color={app.api.reachable ? ACCENT : STATUS.danger} filled>
            {app.api.reachable ? "reachable" : "unreachable"}
          </Badge>
        ) : null
      }
    >
      <div style={{ fontSize: 12, color: T.tertiary, marginBottom: 12, lineHeight: 1.6 }}>
        Agents, versions, memory, approvals and traces all come from here. Requests go through the
        native shell rather than the page, which is why this app can reach an API on any host — and
        why the key never enters the renderer.
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Field label="Base URL" hint={`Local stack default is ${LOCAL_API_URL}`}>
          <Input
            value={baseUrl}
            spellCheck={false}
            placeholder={LOCAL_API_URL}
            onChange={(e) => setBaseUrl(e.target.value)}
            style={{ fontFamily: FONT.mono }}
          />
        </Field>
        <Field
          label="API key"
          hint={
            app.api?.hasKey
              ? "A key is stored. Leave blank to keep it."
              : isLoopback(baseUrl)
                ? `Leave blank and Curie uses ${LOCAL_API_KEY}, the key it ships with for this computer.`
                : "This is not this computer, so it needs a key."
          }
        >
          <Input
            type="password"
            value={apiKey}
            autoComplete="off"
            placeholder={app.api?.hasKey ? "•••••••• (stored)" : ""}
            onChange={(e) => setApiKey(e.target.value)}
          />
        </Field>
      </div>

      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <Button tone="primary" busy={busy} onClick={() => void connect()}>
          Connect
        </Button>
        <Button onClick={() => app.refreshApi()}>Test again</Button>
        {app.api?.hasKey ? (
          <Button
            tone="plain"
            onClick={() => void app.connectApi(baseUrl, "")}
            title="Forget the stored API key"
          >
            Clear key
          </Button>
        ) : null}
        <div style={{ flex: 1 }} />
        {app.api?.reachable ? (
          <span style={{ fontSize: 11, color: T.tertiary }}>
            {app.agents.length} agent{app.agents.length === 1 ? "" : "s"} ·{" "}
            {app.api.orgName ?? "unnamed workspace"}
          </span>
        ) : null}
      </div>

      {app.api && !app.api.reachable && app.api.baseUrl ? (
        <div style={{ marginTop: 12 }}>
          <Notice tone="warn" title="Could not reach that API">
            Nothing answered at <Mono>{app.api.baseUrl}/config</Mono>. If you meant the local stack,
            bring it up with{" "}
            <RunButton id="local.up" tone="plain">
              curie local up
            </RunButton>
            .
          </Notice>
        </div>
      ) : null}
    </Panel>
  );
}

function SecretsPanel() {
  const [names, setNames] = useState<readonly string[]>([]);
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [nonce, setNonce] = useState(0);
  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const list = await bridge().secrets.list();
        if (cancelled) return;
        setNames(list);
        setError(null);
      } catch (err) {
        if (!cancelled) setError((err as Error).message);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [nonce]);

  return (
    <Panel
      title="Secrets"
      right={
        <Button size="sm" onClick={() => setAdding(true)} disabled={!hasShell()}>
          Add secret
        </Button>
      }
    >
      <div style={{ fontSize: 12, color: T.tertiary, marginBottom: 12, lineHeight: 1.6 }}>
        {surfacesById.get("settings.secrets")!.blurb} A value you type here is passed along in a
        way that keeps it out of anything another program on this computer could read, which is why
        this panel does the job itself instead of opening the usual form.
      </div>

      {error ? (
        <Notice tone="warn">{error}</Notice>
      ) : names.length === 0 ? (
        <div style={{ fontSize: 12, color: T.tertiary }}>No secrets saved.</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {names.map((name) => (
            <div
              key={name}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "6px 9px",
                borderRadius: R.control,
                background: S.well,
              }}
            >
              <Mono style={{ flex: 1, color: T.secondary }}>{name}</Mono>
              <span style={{ fontSize: 10, color: T.tertiary }}>value hidden</span>
              <Button
                size="sm"
                tone="plain"
                onClick={async () => {
                  await bridge().secrets.unset(name);
                  refresh();
                }}
              >
                Remove
              </Button>
            </div>
          ))}
        </div>
      )}

      <DrivenBy ids={["secrets.list", "secrets.set", "secrets.unset"]} />

      {adding ? (
        <AddSecret
          onClose={() => setAdding(false)}
          onSaved={() => {
            setAdding(false);
            refresh();
          }}
        />
      ) : null}
    </Panel>
  );
}

function AddSecret({ onClose, onSaved }: { onClose(): void; onSaved(): void }) {
  const [name, setName] = useState("");
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const valid = /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) && value.length > 0;

  const save = async () => {
    setBusy(true);
    setError(null);
    try {
      await bridge().secrets.set(name, value);
      onSaved();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Sheet
      title="Save a secret"
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button tone="primary" disabled={!valid} busy={busy} onClick={() => void save()}>
            Save
          </Button>
        </>
      }
    >
      <Field
        label="Name"
        hint="Upper snake case, like an environment variable: ANTHROPIC_API_KEY."
        error={name && !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? "Letters, digits and underscores only." : null}
      >
        <Input value={name} autoFocus spellCheck={false} onChange={(e) => setName(e.target.value.toUpperCase())} />
      </Field>
      <Field label="Value" hint="Written straight to Curie private storage. It is never read back.">
        <Input type="password" value={value} autoComplete="off" onChange={(e) => setValue(e.target.value)} />
      </Field>
      {error ? <Notice tone="error">{error}</Notice> : null}
    </Sheet>
  );
}

/** The app's command surface versus the CLI it is actually driving.
 *
 *  This app generates its whole UI from `cli/command-manifest.json` at build
 *  time, but it runs whatever `curie` is on PATH. When those disagree the app is
 *  either offering buttons that cannot work or hiding commands the CLI has --
 *  the second being exactly the "GUI is the lesser surface" failure this app is
 *  built to avoid. Neither is allowed to be silent. */
function CommandSurfacePanel() {
  const app = useApp();
  const drift = app.env?.drift;

  if (!app.env?.cliPath) return null;

  const ahead = drift?.missingFromApp ?? [];
  const behind = drift?.missingFromCli ?? [];
  const clean = drift && !ahead.length && !behind.length;

  return (
    <Panel
      title="Version check"
      right={
          <Badge color={clean ? ACCENT : drift ? STATUS.warn : T.tertiary} filled={!!drift}>
            {clean ? "in sync" : drift ? "drifted" : "not checked"}
          </Badge>
        }
    >

      <div style={{ fontSize: 12, color: T.tertiary, marginBottom: 12, lineHeight: 1.6 }}>
        This app is built against one version of Curie and drives whichever one is installed. It
        checks the two agree when it starts.{" "}
        {clean
          ? `Everything this app offers matches ${drift?.cliVersion ?? "the installed version"}.`
          : "Below is where they disagree. Updating Curie usually fixes it."}
      </div>

      {behind.length ? (
        <div style={{ marginBottom: 10 }}>
          <Notice
            tone="error"
            title={
              behind.length === 1
                ? "This app offers something the installed Curie does not have"
                : `This app offers ${behind.length} commands the installed CLI does not have`
            }
          >
            <Mono style={{ fontSize: 11 }}>{behind.join(", ")}</Mono>
            <div style={{ marginTop: 6 }}>
              Running {behind.length === 1 ? "it" : "them"} will fail. The app was built against a
              different version of Curie.
            </div>
          </Notice>
        </div>
      ) : null}

      {ahead.length ? (
        <Notice
          tone="warn"
          title={
            ahead.length === 1
              ? "The installed Curie can do something this app does not offer"
              : `The installed CLI has ${ahead.length} commands this app does not offer`
          }
        >
          <Mono style={{ fontSize: 11 }}>{ahead.join(", ")}</Mono>
          <div style={{ marginTop: 6 }}>
            Until the app is rebuilt, reach {ahead.length === 1 ? "it" : "them"} from a terminal.
            Fix with{" "}
            <Mono>pnpm gen:manifest</Mono> in <Mono>apps/desktop</Mono>, then rebuild.
          </div>
        </Notice>
      ) : null}
    </Panel>
  );
}

function EnvironmentPanel() {
  const app = useApp();
  const env = app.env;

  const rows: [string, string, boolean | null][] = env
    ? [
        ["curie", env.cliPath ?? "not found on PATH", !!env.cliPath],
        ["version", env.cliVersion ?? "unknown", env.cliVersion ? true : null],
        ["source checkout", env.sourceCheckout ? "yes — curie dev commands available" : "no", null],
        ["docker", env.dockerAvailable ? "reachable" : "not reachable", env.dockerAvailable],
        ["kubectl", env.kubectlAvailable ? "found" : "not found", env.kubectlAvailable],
        ["helm", env.helmAvailable ? "found" : "not found", env.helmAvailable],
      ]
    : [];

  return (
    <Panel
      title="What this app can find"
      right={
        <div style={{ display: "flex", gap: 6 }}>
          <RunButton id="doctor">Full diagnosis</RunButton>
          <Button size="sm" onClick={() => app.refreshEnv()}>
            Re-detect
          </Button>
        </div>
      }
    >
      {env ? (
        <div style={{ display: "grid", gap: 5, fontSize: 12 }}>
          {rows.map(([label, value, ok]) => (
            <div key={label} style={{ display: "grid", gridTemplateColumns: "140px 1fr", gap: 10 }}>
              <span style={{ color: T.tertiary }}>{label}</span>
              <Mono
                style={{
                  fontSize: 11,
                  color: ok === false ? STATUS.danger : T.secondary,
                  wordBreak: "break-all",
                }}
              >
                {value}
              </Mono>
            </div>
          ))}
        </div>
      ) : (
        <div style={{ fontSize: 12, color: T.tertiary }}>Detecting…</div>
      )}

      {env && !env.cliPath ? (
        <div style={{ marginTop: 12 }}>
          <Notice tone="error" title="Curie is not installed here">
            A GUI launch does not inherit your login shell&apos;s PATH. This app also looks in{" "}
            <Mono>~/.cargo/bin</Mono>, <Mono>~/.local/bin</Mono>, <Mono>/opt/homebrew/bin</Mono> and{" "}
            <Mono>/usr/local/bin</Mono>. If yours lives somewhere else, set{" "}
            <Mono>CURIE_CLI_PATH</Mono> and reopen the app.
          </Notice>
        </div>
      ) : null}
    </Panel>
  );
}

/** Setting the CLI up, updating it, and finding out what is wrong with it.
 *
 *  These have no natural home anywhere else: they act on the machine rather than
 *  on a bundle, a tier or an agent. Settings is where a person already goes to
 *  ask "what is this app pointed at", so it is where the commands that change
 *  that answer belong. */
function MaintenancePanel() {
  return (
    <Actions surface={surfacesById.get("settings.machine")!}>
      <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
        <Mono style={{ fontSize: 10 }}>curie interactive</Mono> is the CLI&apos;s own terminal UI and
        needs a real terminal, so it opens here as a reference rather than as something this app can
        launch — this window is the same thing with a mouse.
      </div>
    </Actions>
  );
}

/** The two commands that print something to read. Their output is the point, so
 *  they land in the transcript drawer and stay there. */
function ReferencePanel() {
  return <Actions surface={surfacesById.get("settings.reference")!} />;
}

/** The contributor checks, gated on an actual source checkout.
 *
 *  Sixteen commands that a released binary refuses outright. They are shown
 *  regardless -- hiding them would make this app quietly smaller than the CLI --
 *  but the group says up front when this machine cannot run them, which is the
 *  same treatment every other precondition gets. */
function DevPanel() {
  const app = useApp();
  const surface = surfacesById.get("settings.dev")!;
  return (
    <Actions
      surface={surface}
      right={
        <span style={{ ...F.footnote, color: T.quaternary }}>
          {app.env?.sourceCheckout ? (app.env.repoRoot ?? "source checkout") : "needs a checkout"}
        </span>
      }
    >
      <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
        Each of these is a script in the repo, run through <Mono style={{ fontSize: 10 }}>curie dev</Mono>{" "}
        so there is one entry point rather than a scatter of shell files. Output goes to the
        transcript drawer.
      </div>
    </Actions>
  );
}

function AboutPanel() {
  const app = useApp();
  const env = app.env;
  return (
    <Panel title="About">
      <div style={{ fontSize: 12, color: T.secondary, lineHeight: 1.7 }}>
        Curie Desktop is Chromium with the browser removed. It keeps the renderer — one engine that
        draws identically on macOS, Windows and Linux — and drops tabs, history, extensions, profile
        sync, translation, autofill, safe browsing, print preview and the media router. The window
        can load exactly one document, cannot navigate away from it, and grants no permissions;
        every link opens in your real browser instead.
        <br />
        <br />
        The renderer itself is an ordinary sandboxed web app with no Node access. Everything
        privileged — running <Mono>curie</Mono>, reading Docker, opening a directory, holding the API
        key — happens in the main process behind a fixed set of IPC calls.
      </div>
      {env ? (
        <div style={{ marginTop: 12, display: "flex", gap: 14, fontSize: 11, color: T.tertiary }}>
          <span>
            app <Mono>{env.appVersion}</Mono>
          </span>
          <span>
            electron <Mono>{env.electronVersion}</Mono>
          </span>
          <span>
            chromium <Mono>{env.chromeVersion}</Mono>
          </span>
          <span>
            platform <Mono>{env.platform}</Mono>
          </span>
        </div>
      ) : null}
      {!hasShell() ? (
        <div style={{ marginTop: 12 }}>
          <Notice tone="warn" title="Running without the app around it">
            This window is a plain web page, so anything needing the local machine is disabled. Start
            it with <Mono>pnpm dev</Mono> from <Mono>apps/desktop</Mono> to get the full app.
          </Notice>
        </div>
      ) : null}
    </Panel>
  );
}
