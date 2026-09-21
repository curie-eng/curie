// Build: the authoring half of the product.
//
// Curie builds and deploys agents, and until now this app only did the second
// half. It could *run* `curie init` and `curie skill up` through the generic
// command forms, but there was nowhere to actually author a bundle: no way to see
// what is in it, edit a SKILL.md, write eval cases, or find out why it is not
// ready to ship. The bridge could already read and write bundle files; nothing
// surfaced it.
//
// The view is organised around the loop the scaffolded AGENTS.md describes:
// boot the runner, edit behaviour and the eval contract, restart, grade, ship.
// So it is a workbench, not a form: what the bundle contains, what is wrong with
// it, the file you are editing, and the rungs of the ladder in order.
//
// Judgement about a bundle lives in `src/lib/bundle.ts` as pure functions with
// tests. This file renders those verdicts and owns the editing state.

import { useCallback, useEffect, useMemo, useState } from "react";

import { useApp } from "../bridge/app";
import { SlackPacks } from "./BuildPacks";
import { NewAgent } from "./NewAgent";
import { AgentSettings } from "./AgentSettings";
import { Deployment, DeployedDot } from "./Deployment";
import { Actions, RunButton } from "./Actions";
import { surfacesById } from "../lib/surfaces";
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { readBool, write as remember } from "../lib/uiState";
import { bridge, type Workspace } from "../bridge/bridge";
import {
  GROUP_LABEL,
  classifyFile,
  organise,
  parseEvalSuite,
  parsePlugin,
  parseSkill,
  readiness,
  validateForSave,
  verdict,
  type BundleFile,
  type Check,
  type Level,
  type PluginManifest,
  type SkillMeta,
} from "../lib/bundle";
import { ACCENT, F, FONT, LINE, M, S, STATUS, T, tint } from "../tokens";
import {
  Badge,
  Button,
  EmptyState,
  Field,
  Group,
  Input,
  MenuButton,
  Mono,
  Notice,
  Row,
  SectionHeader,
  Sheet,
  PanelToggle,
  Well,
} from "../primitives";

const LEVEL_COLOR: Record<Level, string> = {
  error: STATUS.danger,
  warn: STATUS.warn,
  info: T.tertiary,
};

export function Build() {
  const app = useApp();
  const ws = app.workspace;

  // The list can be put away. It is a switcher, and on a narrow window or a
  // long editing session it is 196px of column you are not using -- but it is
  // also the only thing that says how many agents there are, so the way back
  // has to be visible while it is gone. The toggle therefore lives in the
  // DETAIL column's header, which is on screen in both states, rather than on
  // the list's own header where it would vanish with the panel.
  const [collapsed, setCollapsed] = useState(() => readBool("build.agents.collapsed", false));
  const toggle = () => setCollapsed((prev) => !prev);
  // From an effect, because a state updater must be pure -- see the same note
  // in `App.tsx`, where writing from inside one put the stored value out of step
  // with the screen.
  useEffect(() => remember("build.agents.collapsed", collapsed), [collapsed]);

  // Master-detail, with the list on the left.
  //
  // Switching used to be a chevron on the bundle's own name in the header, which
  // hid the set of agents behind a click on the thing you had already chosen. A
  // standing list says how many there are and which one you are in without being
  // opened.
  //
  // It goes to the LEFT of the detail, not into the empty band on the right: that
  // band is not free space, it is the content pane's `maxWidth: 1080` cap, so a
  // list out there would sit outside the column every other view is measured
  // against. Inside the cap, list-then-detail is also the order these panes are
  // read in.
  return (
    <div style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
      {collapsed ? null : <AgentList />}
      <div style={{ flex: 1, minWidth: 0 }}>
        {/* A header the list column already has, so the two columns start on the
            same line. Without it the detail began 22px higher than the list and
            every section below inherited the offset. */}
        <SectionHeader>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 7, minHeight: 16 }}>
            <PanelToggle
              collapsed={collapsed}
              onToggle={toggle}
              label="the agent list"
              variant="list"
              style={{ width: 20, height: 16, marginLeft: -4 }}
            />
            {ws ? "Agent" : "No agent"}
            {/* Collapsed, the count is the only thing left saying the list is
                there at all. */}
            {collapsed && app.workspaces.length ? (
              <span style={{ ...F.footnote, color: T.quaternary, textTransform: "none" }}>
                {app.workspaces.length} in all
              </span>
            ) : null}
          </span>
        </SectionHeader>
        {/* Keyed on the path so switching resets every bit of editing state
            rather than carrying a half-typed SKILL.md across. */}
        {ws ? <Workbench key={ws.path} /> : <NoBundle first={!app.workspaces.length} />}

        {/* Scaffolding is not a property of the bundle you have open, so this
            group sits outside the workbench and is here either way. With
            nothing open it is the only thing to do; with something open it is
            how you start the next one. */}
        <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 14 }}>
          {/* The escape hatch, and deliberately at the foot of the page. The
              gallery above is how an agent gets made; this is the same work
              spelled as the commands, for somebody who would rather drive it
              that way or wants the line to paste into a terminal. Keeping it
              here is what lets everything above stop naming commands. */}
          <Actions surface={surfacesById.get("build.author")!} />
          {/* Last on the page. See `NotHere`. */}
          {ws ? <NotHere /> : null}
        </div>
      </div>
    </div>
  );
}

/** Same stroke weight and cap style as the rail's icons, so a button glyph here
 *  does not read as a different icon set. */
function Glyph({ d }: { d: string }) {
  return (
    <svg width={13} height={13} viewBox="0 0 16 16" aria-hidden style={{ flex: "none" }}>
      <path d={d} fill="none" stroke="currentColor" strokeWidth={1.6} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

/** The agents this app knows about, and the way to add one. */
function AgentList() {
  const app = useApp();
  const active = app.workspace?.path ?? null;
  const [pendingDelete, setPendingDelete] = useState<Workspace | null>(null);
  const [creating, setCreating] = useState(false);

  return (
    <section style={{ width: 168, flex: "none" }}>
      {/* The count says this is a collection with a size, not a fixed pair of
          labels. */}
      <SectionHeader
        right={
          app.workspaces.length ? (
            <span style={{ ...F.footnote, color: T.quaternary }}>{app.workspaces.length}</span>
          ) : null
        }
      >
        <span style={{ display: "inline-flex", alignItems: "center", minHeight: 16 }}>Agents</span>
      </SectionHeader>
      {/* One container for the whole column: the list scrolls inside it, the
          actions are pinned to its foot.

          The actions used to sit OUTSIDE the group, so the column had no outer
          boundary and nothing said where the list ended. It simply ran on, and a
          long list would have pushed the buttons away down the page rather than
          scrolling. A bounded panel with a footer says both things at once: this
          is one module, and growth happens inside it. */}
      <Group style={{ display: "flex", flexDirection: "column" }}>
        {/* Bounded, so a twentieth agent scrolls here instead of stretching the
            column. `minHeight: 0` because a flex child will otherwise refuse to
            shrink below its content and the overflow never engages. */}
        <div style={{ maxHeight: 264, overflowY: "auto", minHeight: 0 }}>
        {app.workspaces.length === 0 ? (
          <div style={{ padding: "12px 14px", ...F.footnote, color: T.tertiary }}>
            None yet.
          </div>
        ) : (
          app.workspaces.map((w, i) => {
            const on = w.path === active;
            return (
              <Row
                key={w.path}
                first={i === 0}
                selected={on}
                onClick={() => app.selectWorkspace(w.path)}
              >
                {/* Two things make a row read as one of several interchangeable
                    items rather than a line of text: it carries its own mark, and
                    the selected one is marked at the edge as well as tinted. A
                    background tint alone is easy to miss and says nothing about
                    what the row IS. */}
                {on ? (
                  <span
                    aria-hidden
                    style={{
                      position: "absolute",
                      left: 0,
                      top: 0,
                      bottom: 0,
                      width: 3,
                      background: ACCENT,
                      borderRadius: "0 2px 2px 0",
                    }}
                  />
                ) : null}
                <span
                  aria-hidden
                  style={{
                    width: 22,
                    height: 22,
                    flex: "none",
                    borderRadius: 6,
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    background: on ? tint(ACCENT, 0.18) : S.control,
                    color: on ? ACCENT : T.tertiary,
                    ...F.caption,
                    fontWeight: 600,
                  }}
                >
                  {(w.plugin?.name ?? w.name).slice(0, 1).toUpperCase()}
                </span>
                <div style={{ flex: 1, minWidth: 0 }} title={w.path}>
                  {/* Identity and state on the first line, facts on the second.
                      The `live` pill used to sit beside the facts, which in a
                      168px column left "1 skill · evals" about seventy pixels
                      and clipped the half of it that is not the word "skill".
                      Up here it costs nothing: a name is short and already
                      ellipsises, and "squawk · live" is the pair somebody scans
                      this list for anyway. */}
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <span
                      style={{
                        ...F.body,
                        color: on ? T.primary : T.secondary,
                        minWidth: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {w.plugin?.name ?? w.name}
                    </span>
                    <DeployedDot bundleName={w.plugin?.name ?? w.name} />
                  </div>
                  {/* One line, clipped rather than wrapped. Wrapping doubled the
                      row's height for one agent and not the next, and a list
                      whose rows differ in height for a reason nobody can see
                      reads as broken rather than as informative. */}
                  <div
                    style={{
                      ...F.footnote,
                      color: T.quaternary,
                      marginTop: 1,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {w.skills.length} skill{w.skills.length === 1 ? "" : "s"}
                    {w.hasEvals ? " · evals" : ""}
                  </div>
                </div>
                {/* The row's actions, behind the platform's overflow affordance
                    rather than a bare glyph doing one thing -- a single-purpose
                    control only works while there is exactly one purpose, and
                    this list will grow more than one. Revealed on row hover or
                    focus, because a control standing beside every row is a
                    mis-click waiting to happen on a list you point at to SWITCH
                    agents far more often than to act on one. Opacity rather than
                    display, so it stays in the tab order; see `.row-delete` in
                    styles.css. */}
                <MenuButton
                  className="row-delete"
                  data-reveal
                  label={`Actions for ${w.plugin?.name ?? w.name}`}
                  items={[
                    {
                      label: "Delete…",
                      tone: "danger",
                      onSelect: () => setPendingDelete(w),
                    },
                  ]}
                />
              </Row>
            );
          })
        )}
        </div>

      {/* Two actions with a clear ranking. Authoring a new agent is what this
          column is for, so it takes the accent; importing one that already exists
          on disk is the occasional case and takes the ordinary treatment.
          Neither is `plain`: a transparent button with dimmed text is
          indistinguishable from a disabled one, which is how the second action
          read before.

          Both keep the trailing ellipsis, which on this platform means "opens
          something you then complete" -- one lands on a form, the other on a
          directory chooser. */}
        <div
          style={{
            flex: "none",
            borderTop: `1px solid ${LINE.separator}`,
            padding: 8,
            display: "flex",
            flexDirection: "column",
            gap: 6,
          }}
        >
        {/* Opens the starting-point gallery, not the scaffolder. Running the
            scaffolder handed back an empty directory, which answers "how do I
            make one" with a blank page -- and the command it ran is not a thing
            anybody arrives wanting to know about. */}
        <Button
          size="sm"
          tone="primary"
          icon={<Glyph d="M8 3.5v9M3.5 8h9" />}
          onClick={() => setCreating(true)}
          style={{ width: "100%" }}
        >
          New agent…
        </Button>
        <Button
          size="sm"
          icon={<Glyph d="M8 2.5v6.8M5.4 6.9 8 9.5l2.6-2.6M3 10.8v1.7a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-1.7" />}
          onClick={() => void app.openWorkspace()}
          style={{ width: "100%" }}
          title="Import an agent that already exists on disk"
        >
          Import…
        </Button>
        </div>
      </Group>

      {creating ? <NewAgent onClose={() => setCreating(false)} /> : null}
      {pendingDelete ? (
        <DeleteBundle bundle={pendingDelete} onClose={() => setPendingDelete(null)} />
      ) : null}
    </section>
  );
}

/**
 * The delete confirmation.
 *
 * Typing the bundle's name, which is the gate every destructive command in this
 * app already uses -- muscle memory must not be able to carry somebody through a
 * deletion they did not mean. It is doing more work than usual here: a command
 * can be re-run, and a directory cannot be un-deleted.
 *
 * No trash, on purpose. An app that deletes into a holding pen has to grow a way
 * to see and empty that pen, and until it does the operator cannot tell whether
 * the thing is actually gone.
 *
 * The path is shown in full. The name says which agent; the path is the only
 * thing that says which COPY, and two checkouts of one bundle differ by nothing
 * else.
 */
function DeleteBundle({ bundle, onClose }: { bundle: Workspace; onClose(): void }) {
  const app = useApp();
  const name = bundle.plugin?.name ?? bundle.name;
  const [typed, setTyped] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const confirm = async () => {
    setBusy(true);
    const res = await app.deleteWorkspace(bundle.path);
    setBusy(false);
    if (res.ok) onClose();
    else setError(res.error);
  };

  return (
    <Sheet
      title="Delete this agent"
      onClose={onClose}
      width={520}
      footer={
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", width: "100%" }}>
          <Button tone="plain" onClick={onClose}>
            Cancel
          </Button>
          <Button tone="danger" busy={busy} disabled={typed !== name} onClick={() => void confirm()}>
            Delete permanently
          </Button>
        </div>
      }
    >
      <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.6, marginBottom: 12 }}>
        This removes the directory and everything in it. There is no undo, and it does not go to the
        Trash.
      </div>
      <Well style={{ padding: "8px 10px", marginBottom: 14 }} mono>
        {bundle.path}
      </Well>
      {error ? (
        <div style={{ marginBottom: 12 }}>
          <Notice tone="error" title="Not deleted">
            {error}
          </Notice>
        </div>
      ) : null}
      <Field
        label={
          <>
            Type <Mono style={{ color: T.primary }}>{name}</Mono> to confirm
          </>
        }
      >
        <Input
          value={typed}
          autoFocus
          spellCheck={false}
          autoComplete="off"
          onChange={(e) => setTyped(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && typed === name) void confirm();
          }}
        />
      </Field>
    </Sheet>
  );
}

/**
 * The detail pane with nothing open. Two cases, one component.
 *
 * It carries no create button in either. The column three inches to the left
 * lists every agent and holds both ways to get another, and a second pair here
 * was the same control twice -- which made the real one harder to find, not
 * easier. The whole Build interface stays on screen regardless: replacing it
 * with a single first-run panel did remove the duplicate buttons, and it also
 * removed the interface, which is not a trade anyone asked for.
 */
function NoBundle({ first }: { readonly first: boolean }) {
  return (
    <Group>
      <EmptyState title={first ? "No agents yet" : "Pick an agent"}>
        {first ? (
          <>
            Start one with <Mono>New agent…</Mono> in the Agents column on the left, or{" "}
            <Mono>Import…</Mono> one you already have. An agent is a folder holding what it should
            do, the outside tools it may use, and the examples that prove a change works. Once one
            is open, this is where you try it, score it and put it to work.
          </>
        ) : (
          <>
            Choose one from the list on the left, or start another with <Mono>New agent…</Mono> at
            the foot of it. An agent is a folder holding what it should do, the outside tools it may
            use, and the examples that prove a change works.
          </>
        )}
      </EmptyState>
    </Group>
  );
}

function Workbench() {
  const app = useApp();
  const runs = useRuns();
  const ws = app.workspace!;

  const [paths, setPaths] = useState<readonly string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [text, setText] = useState("");
  const [saved, setSaved] = useState("");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [nonce, setNonce] = useState(0);
  const reload = useCallback(() => setNonce((n) => n + 1), []);

  // Contents of the files the verdicts are computed from. Read once per refresh
  // rather than on every render: they are on disk, not in memory.
  const [manifestText, setManifestText] = useState<string | null>(null);
  const [evalsText, setEvalsText] = useState<string | null>(null);
  const [skillTexts, setSkillTexts] = useState<readonly string[]>([]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const list = await bridge().workspace.files(ws.path);
      if (cancelled) return;
      setPaths(list);

      const read = async (rel: string) => {
        try {
          return await bridge().workspace.readFile(ws.path, rel);
        } catch {
          return null;
        }
      };
      const [manifest, evals] = await Promise.all([
        read(".claude-plugin/plugin.json"),
        list.includes("evals/cases.json") ? read("evals/cases.json") : Promise.resolve(null),
      ]);
      const skills = await Promise.all(
        list.filter((p) => /^skills\/[^/]+\/SKILL\.md$/.test(p)).map((p) => read(p)),
      );
      if (cancelled) return;
      setManifestText(manifest);
      setEvalsText(evals);
      setSkillTexts(skills.filter((t): t is string => t !== null));
    })();
    return () => {
      cancelled = true;
    };
  }, [ws.path, nonce]);

  // Load the selected file's contents.
  useEffect(() => {
    if (!selected) return;
    let cancelled = false;
    void (async () => {
      try {
        const body = await bridge().workspace.readFile(ws.path, selected);
        if (cancelled) return;
        setText(body);
        setSaved(body);
        setLoadError(null);
        setSaveError(null);
      } catch (err) {
        if (!cancelled) setLoadError((err as Error).message);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [ws.path, selected, nonce]);

  const plugin = useMemo(
    () => (manifestText === null ? undefined : parsePlugin(manifestText)),
    [manifestText],
  );
  const evals = useMemo(
    () => (evalsText === null ? undefined : parseEvalSuite(evalsText)),
    [evalsText],
  );
  const skills = useMemo<SkillMeta[]>(() => skillTexts.map(parseSkill), [skillTexts]);
  const checks = useMemo(
    () => readiness(ws, { plugin, evals, skills }),
    [ws, plugin, evals, skills],
  );
  const groups = useMemo(() => organise(paths), [paths]);
  const dirty = text !== saved;

  const save = async () => {
    if (!selected) return;
    // Refuse to write a contract file that would not parse. The CLI would reject
    // it later with less context, and a broken plugin.json makes the bundle
    // unloadable at every tier.
    const problem = validateForSave(selected, text);
    if (problem) {
      setSaveError(problem);
      return;
    }
    setBusy(true);
    setSaveError(null);
    try {
      await bridge().workspace.writeFile(ws.path, selected, text);
      setSaved(text);
      reload();
    } catch (err) {
      setSaveError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <Header checks={checks} plugin={plugin?.ok ? plugin.value : undefined} />
      {checks.length ? <Checklist checks={checks} /> : null}

      {/* Above the loop, because configuring the agent is what somebody does
          first and repeatedly, and running it is what they do after. It edits
          the same files the list below does, through the same write functions,
          so the two surfaces cannot disagree about what a file means. */}
      {/* Directly under the header, because "is this thing actually running"
          is the question somebody arrives with after deploying once, and the
          only answer Build gave was a readiness badge about the files. */}
      <Deployment bundleName={plugin?.ok ? (plugin.value.name ?? ws.name) : ws.name} />

      <AgentSettings
        ws={ws}
        plugin={plugin?.ok ? plugin.value : undefined}
        onSaved={reload}
      />

      <Ladder />

      <section>
        <SectionHeader
          right={
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              {dirty ? (
                <span style={{ ...F.footnote, color: STATUS.warn }}>unsaved changes</span>
              ) : null}
              <Button size="sm" tone="plain" onClick={reload} disabled={dirty}>
                Reload
              </Button>
              <Button
                size="sm"
                tone="plain"
                onClick={() => void bridge().workspace.revealInFileManager(ws.path)}
              >
                Reveal
              </Button>
            </div>
          }
        >
          Bundle files
        </SectionHeader>

        {/* `stretch`, not `start`: the file list is usually taller than the editor,
            and starting both at the top left the editor floating above a band of
            empty pane. Matching heights makes the row read as one object. */}
        <div style={{ display: "grid", gridTemplateColumns: "232px 1fr", gap: 14, alignItems: "stretch" }}>
          <Group>
            {groups.length === 0 ? (
              <div style={{ padding: 14, ...F.callout, color: T.tertiary }}>
                Nothing readable in this directory.
              </div>
            ) : (
              groups.map((g, gi) => (
                <div key={g.group}>
                  <div
                    style={{
                      padding: "7px 14px 5px",
                      ...F.footnote,
                      color: T.quaternary,
                      fontWeight: 600,
                      letterSpacing: 0.5,
                      textTransform: "uppercase",
                      borderTop: gi === 0 ? undefined : `1px solid ${LINE.separator}`,
                    }}
                  >
                    {GROUP_LABEL[g.group]}
                  </div>
                  {g.files.map((f) => (
                    <FileRow
                      key={f.path}
                      file={f}
                      active={f.path === selected}
                      dirty={dirty && f.path === selected}
                      onClick={() => setSelected(f.path)}
                    />
                  ))}
                </div>
              ))
            )}
          </Group>

          <Editor
            path={selected}
            text={text}
            dirty={dirty}
            busy={busy}
            loadError={loadError}
            saveError={saveError}
            onChange={setText}
            onSave={() => void save()}
            onRevert={() => {
              setText(saved);
              setSaveError(null);
            }}
          />
        </div>
      </section>

      <Evals suite={evals} onOpen={() => setSelected("evals/cases.json")} />

      <SlackPacks plugin={plugin?.ok ? plugin.value : undefined} />

      {runs.runs.length ? (
        <div style={{ ...F.footnote, color: T.quaternary }}>
          Every command this view runs is in Activity with its full output.
        </div>
      ) : null}
    </div>
  );
}

/** Identity and verdict. */
function Header({ checks, plugin }: { checks: readonly Check[]; plugin?: PluginManifest }) {
  const app = useApp();
  const ws = app.workspace!;
  const v = verdict(checks);
  const color = v.level === "ok" ? ACCENT : LEVEL_COLOR[v.level];

  return (
    <Group style={{ padding: 14 }}>
      <div style={{ display: "flex", alignItems: "flex-start", gap: 14 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: 8, flexWrap: "wrap" }}>
            {/* A label, not a control: switching is the list on the left. */}
            <span style={{ ...F.title }}>{plugin?.name ?? ws.name}</span>
            {plugin?.version ? <Badge>{plugin.version}</Badge> : null}
            <Badge color={color} filled>
              {v.text}
            </Badge>
          </div>
          {plugin?.description ? (
            <div style={{ ...F.callout, color: T.secondary, marginTop: 6, maxWidth: M.prose }}>
              {plugin.description}
            </div>
          ) : null}
          <Mono
            style={{ display: "block", marginTop: 6, fontSize: 11, color: T.quaternary }}
            title={ws.path}
          >
            {ws.path}
          </Mono>
        </div>
      </div>

      {/* What this agent is made of, which is otherwise only visible by opening
          its files one at a time. */}
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginTop: 12 }}>
        <Fact label="Things it can do" value={String(ws.skills.length)} detail={ws.skills.join(", ")} />
        <Fact
          label="Examples"
          value={ws.hasEvals ? "yes" : "none"}
          detail={ws.hasEvals ? "scored on every change" : "no way to prove a change works"}
        />
        <Fact
          label="Outside tools"
          value={ws.hasMcp ? "yes" : "none"}
          detail={ws.hasMcp ? "declared by this agent" : "uses only what Curie provides"}
        />
        {plugin?.secrets?.length ? (
          <Fact
            label="Secrets it needs"
            value={String(plugin.secrets.length)}
            detail={plugin.secrets.join(", ")}
          />
        ) : null}
        {plugin?.approvalGates?.length ? (
          <Fact
            label="Needs approval"
            value={String(plugin.approvalGates.length)}
            detail={plugin.approvalGates.join(", ")}
          />
        ) : null}
        {plugin?.triggerCount ? (
          <Fact label="Runs on its own" value={String(plugin.triggerCount)} detail="on a schedule, or when something happens" />
        ) : null}
      </div>
    </Group>
  );
}

function Fact({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div title={detail}>
      <div style={{ ...F.footnote, color: T.quaternary }}>{label}</div>
      <div style={{ ...F.headline, color: T.primary, marginTop: 1 }}>{value}</div>
    </div>
  );
}

/**
 * The parity ladder, in order, with the runner's live state.
 *
 * The rungs are the product's central idea, and the order matters: a SKILL.md
 * edit only reaches the runner after a restart, because `skill up` runs an
 * immutable snapshot taken at boot. That is the single most expensive thing to
 * learn the hard way, so the view says it rather than leaving it to the docs.
 */
function Ladder() {
  const res = useResources();
  const runnerUp = res.samples.some((s) => s.role === "runner" && s.state === "running");

  // Rendered from the placement map rather than a local list of rungs. The
  // buttons here and the ones on the Tiers view are the same declarations, so a
  // command cannot be quietly dropped from one of them.
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <Actions
        surface={surfacesById.get("build.loop")!}
        right={
          <span style={{ ...F.footnote, color: runnerUp ? ACCENT : T.quaternary }}>
            {runnerUp ? "runner live" : "no runner"}
          </span>
        }
      >
        <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
          A test copy is a snapshot taken when it started, so edits to what the agent should do
          reach it only after you restart it — and scoring without restarting scores the old
          version, with nothing on screen to say so. Starting a test copy again replaces it. Your
          examples are read fresh every time, so those never need a restart.
        </div>
      </Actions>

      <Actions surface={surfacesById.get("build.ship")!}>
        <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
          Deploying creates an immutable version and points the agent at it. What each tier can
          reach, and what it costs to start, is on the Tiers view.
        </div>
      </Actions>

    </div>
  );
}

/**
 * The verbs the ladder has further up and this tier does not.
 *
 * They stay in the app because "why can I not do X here" is a real question and
 * these commands exist precisely to print the answer -- hiding them would make
 * this the surface that silently has less than the CLI. But they are five
 * controls whose labels all begin "Why no", and sitting between "Ship it" and
 * the file list they were an explanation of absences interrupting the path from
 * authoring to deploying. Last on the page, and closed until asked: a question
 * about something that is not here is not answered by putting it in the way.
 */
function NotHere() {
  const [open, setOpen] = useState(false);
  return (
    <section>
      <SectionHeader
        right={
          <Button size="sm" tone="plain" onClick={() => setOpen((v) => !v)}>
            {open ? "Hide" : "Show"}
          </Button>
        }
      >
        Not at this tier
      </SectionHeader>
      {open ? (
        <Actions surface={surfacesById.get("build.not-here")!} />
      ) : (
        <Group style={{ padding: "10px 12px" }}>
          <div style={{ ...F.footnote, color: T.tertiary, lineHeight: 1.55 }}>
            Versions, memory and run history need the platform, and the skill tier does not have
            one. Each of those commands still exists here and will say so in its own words.
          </div>
        </Group>
      )}
    </section>
  );
}

function Checklist({ checks }: { checks: readonly Check[] }) {
  const [open, setOpen] = useState(true);
  const errors = checks.filter((c) => c.level === "error").length;

  return (
    <section>
      <SectionHeader
        right={
          <Button size="sm" tone="plain" onClick={() => setOpen((v) => !v)}>
            {open ? "Hide" : `Show (${checks.length})`}
          </Button>
        }
      >
        {errors ? "Problems" : "Worth a look"}
      </SectionHeader>
      {open ? (
        <Group>
          {checks.map((c, i) => (
            <Row key={c.id} first={i === 0}>
              <span
                aria-hidden
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: 999,
                  flex: "none",
                  alignSelf: "flex-start",
                  marginTop: 6,
                  background: LEVEL_COLOR[c.level],
                }}
              />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ ...F.headline }}>{c.title}</div>
                <div style={{ ...F.callout, color: T.tertiary, marginTop: 1 }}>{c.detail}</div>
              </div>
              {c.fix ? (
                <RunButton id={c.fix}>Fix</RunButton>
              ) : null}
            </Row>
          ))}
        </Group>
      ) : null}
    </section>
  );
}

function FileRow({
  file,
  active,
  dirty,
  onClick,
}: {
  file: BundleFile;
  active: boolean;
  dirty: boolean;
  onClick(): void;
}) {
  return (
    <button
      onClick={onClick}
      title={file.path}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 7,
        width: "100%",
        border: "none",
        background: active ? S.selected : "transparent",
        borderLeft: `2px solid ${active ? ACCENT : "transparent"}`,
        padding: "5px 12px",
        textAlign: "left",
        cursor: "default",
      }}
    >
      <Mono
        style={{
          flex: 1,
          fontSize: 11.5,
          color: active ? T.primary : T.secondary,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {file.label}
      </Mono>
      {dirty ? <span style={{ color: STATUS.warn, fontSize: 14, lineHeight: 1 }}>•</span> : null}
    </button>
  );
}

function Editor({
  path,
  text,
  dirty,
  busy,
  loadError,
  saveError,
  onChange,
  onSave,
  onRevert,
}: {
  path: string | null;
  text: string;
  dirty: boolean;
  busy: boolean;
  loadError: string | null;
  saveError: string | null;
  onChange(next: string): void;
  onSave(): void;
  onRevert(): void;
}) {
  if (!path) {
    return (
      <Group style={{ padding: 0 }}>
        <EmptyState title="Pick a file">
          Skills are prose and are the main thing you edit. The plugin manifest, the eval cases and
          deploy.yaml are contracts, so a save that would not parse is refused here rather than
          failing later in the CLI.
        </EmptyState>
      </Group>
    );
  }

  const file = classifyFile(path);

  return (
    <Group style={{ display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 9,
          padding: "9px 12px",
          borderBottom: `1px solid ${LINE.separator}`,
        }}
      >
        <Mono style={{ flex: 1, color: T.secondary, fontSize: 11.5 }}>{path}</Mono>
        {file.structured ? (
          <Badge color={STATUS.info}>contract</Badge>
        ) : (
          <Badge>prose</Badge>
        )}
        <Button size="sm" tone="plain" onClick={onRevert} disabled={!dirty}>
          Revert
        </Button>
        <Button size="sm" tone="primary" onClick={onSave} disabled={!dirty} busy={busy}>
          Save
        </Button>
      </div>

      {loadError ? (
        <div style={{ padding: 12 }}>
          <Notice tone="error" title="Could not read this file">
            {loadError}
          </Notice>
        </div>
      ) : (
        <>
          {saveError ? (
            <div style={{ padding: "12px 12px 0" }}>
              <Notice tone="error" title="Not saved: this would not parse">
                {saveError}
              </Notice>
            </div>
          ) : null}
          <textarea
            // Keyed on the path so switching files remounts the control. One
            // reused textarea keeps the previous file's scroll offset, which
            // lands you halfway down a long file you have just opened.
            key={path}
            value={text}
            spellCheck={false}
            onChange={(e) => onChange(e.target.value)}
            onKeyDown={(e) => {
              // The save shortcut people already have in their fingers.
              if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
                e.preventDefault();
                if (dirty) onSave();
              }
            }}
            style={{
              width: "100%",
              minHeight: 300,
              resize: "vertical",
              border: "none",
              outline: "none",
              background: S.well,
              color: T.primary,
              fontFamily: FONT.mono,
              fontSize: 12,
              lineHeight: 1.6,
              padding: 12,
              tabSize: 2,
            }}
          />
        </>
      )}
    </Group>
  );
}

/** The eval suite, read from the file rather than described. */
function Evals({
  suite,
  onOpen,
}: {
  suite: ReturnType<typeof parseEvalSuite> | undefined;
  onOpen(): void;
}) {
  if (!suite) {
    return (
      <section>
        <SectionHeader>Evals</SectionHeader>
        <Group style={{ padding: 14 }}>
          <div style={{ ...F.callout, color: T.tertiary, marginBottom: 10 }}>
            No <Mono>evals/cases.json</Mono>. Eval cases are the promotion gate and the one file that
            does not change across tiers, so a bundle without them is deployable but not
            falsifiable.
          </div>
          <RunButton id="skill.eval-init">Generate a starter suite</RunButton>
        </Group>
      </section>
    );
  }

  if (!suite.ok) {
    return (
      <section>
        <SectionHeader>Evals</SectionHeader>
        <Notice
          tone="error"
          title="evals/cases.json does not parse"
          action={
            <Button size="sm" onClick={onOpen}>
              Edit
            </Button>
          }
        >
          {suite.error}
        </Notice>
      </section>
    );
  }

  const cases = suite.value.cases;
  return (
    <section>
      <SectionHeader
        right={
          <div style={{ display: "flex", gap: 6 }}>
            <Button size="sm" tone="plain" onClick={onOpen}>
              Edit
            </Button>
            <RunButton id="skill.eval">Run</RunButton>
          </div>
        }
      >
        Evals · {suite.value.name} · {cases.length} case{cases.length === 1 ? "" : "s"}
      </SectionHeader>
      <Group>
        {cases.map((c, i) => (
          <Row key={c.id} first={i === 0}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <Mono style={{ fontSize: 11.5, color: T.primary }}>{c.id}</Mono>
              <div
                style={{
                  ...F.callout,
                  color: T.tertiary,
                  marginTop: 2,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
                title={c.input}
              >
                {c.input}
              </div>
            </div>
            <Badge color={STATUS.info}>{c.grader.kind}</Badge>
            <Well style={{ padding: "2px 7px", maxWidth: 220, overflow: "hidden" }} mono>
              <span
                style={{
                  display: "block",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  fontSize: 11,
                  color: T.secondary,
                }}
                title={c.grader.expected}
              >
                {c.grader.expected}
              </span>
            </Well>
            {c.expect_status === "awaiting-approval" ? (
              <Badge color={STATUS.warn}>expects gate</Badge>
            ) : null}
            {c.shared_history ? <Badge color={tint(STATUS.info, 1)}>chained</Badge> : null}
          </Row>
        ))}
      </Group>
      <div style={{ ...F.footnote, color: T.quaternary, marginTop: 8, lineHeight: 1.55 }}>
        Grading is a real-credential concept. Under <Mono style={{ fontSize: 10 }}>--fake-model</Mono>{" "}
        the run reports plumbing only: it proves the turn completed and grades nothing, so it is not
        the promotion gate.
      </div>
    </section>
  );
}
