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
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { bridge } from "../bridge/bridge";
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
import { ACCENT, F, FONT, LINE, S, STATUS, T, tint } from "../tokens";
import {
  Badge,
  Button,
  EmptyState,
  Group,
  Mono,
  Notice,
  Row,
  SectionHeader,
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
      <AgentList />
      <div style={{ flex: 1, minWidth: 0 }}>
        {/* Keyed on the path so switching resets every bit of editing state
            rather than carrying a half-typed SKILL.md across. */}
        {ws ? <Workbench key={ws.path} /> : <NoBundle />}
      </div>
    </div>
  );
}

/** The agents this app knows about, and the way to add one. */
function AgentList() {
  const app = useApp();
  const active = app.workspace?.path ?? null;

  return (
    <section style={{ width: 196, flex: "none" }}>
      <SectionHeader>Agents</SectionHeader>
      <Group>
        {app.workspaces.length === 0 ? (
          <div style={{ padding: "12px 14px", ...F.footnote, color: T.tertiary }}>
            None yet.
          </div>
        ) : (
          app.workspaces.map((w, i) => (
            <Row
              key={w.path}
              first={i === 0}
              selected={w.path === active}
              onClick={() => app.selectWorkspace(w.path)}
            >
              <div style={{ minWidth: 0 }} title={w.path}>
                <div
                  style={{
                    ...F.body,
                    color: w.path === active ? T.primary : T.secondary,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {w.plugin?.name ?? w.name}
                </div>
                <div style={{ ...F.footnote, color: T.quaternary, marginTop: 1 }}>
                  {w.skills.length} skill{w.skills.length === 1 ? "" : "s"}
                  {w.hasEvals ? " · evals" : ""}
                </div>
              </div>
            </Row>
          ))
        )}
      </Group>

      <div style={{ display: "flex", flexDirection: "column", gap: 6, marginTop: 8 }}>
        <Button size="sm" onClick={() => app.navigate("commands", "init")}>
          + New Agent
        </Button>
        {/* Adding an agent that already exists on disk. Also File -> Open
            Bundle, but a menu-only action is not discoverable from here. */}
        <Button size="sm" tone="plain" onClick={() => void app.openWorkspace()}>
          Open existing…
        </Button>
      </div>
    </section>
  );
}

function NoBundle() {
  const app = useApp();
  return (
    <Group>
      <EmptyState
        title="No bundle open"
        action={
          <div style={{ display: "flex", gap: 8, justifyContent: "center" }}>
            <Button tone="primary" onClick={() => void app.openWorkspace()}>
              Open a bundle
            </Button>
            <Button onClick={() => app.navigate("commands", "init")}>Scaffold a new one</Button>
          </div>
        }
      >
        A bundle is a directory with <Mono>.claude-plugin/plugin.json</Mono> in it: skills, the MCP
        servers they call, and the eval cases that make a change falsifiable. Open one to edit it, or
        scaffold a new one with <Mono>curie init</Mono>.
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
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <Header checks={checks} plugin={plugin?.ok ? plugin.value : undefined} />
      <Ladder />
      {checks.length ? <Checklist checks={checks} /> : null}

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

        <div style={{ display: "grid", gridTemplateColumns: "232px 1fr", gap: 14, alignItems: "start" }}>
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
    <Group style={{ padding: 16 }}>
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
            <div style={{ ...F.callout, color: T.secondary, marginTop: 6, maxWidth: 720 }}>
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

      {/* What the bundle declares, which is otherwise only visible by reading
          plugin.json by hand. */}
      <div style={{ display: "flex", gap: 18, flexWrap: "wrap", marginTop: 14 }}>
        <Fact label="Skills" value={String(ws.skills.length)} detail={ws.skills.join(", ")} />
        <Fact
          label="Eval cases"
          value={ws.hasEvals ? "present" : "none"}
          detail={ws.hasEvals ? "evals/cases.json" : "not falsifiable yet"}
        />
        <Fact label="MCP" value={ws.hasMcp ? "declared" : "none"} detail=".mcp.json" />
        {plugin?.secrets?.length ? (
          <Fact
            label="Secrets"
            value={String(plugin.secrets.length)}
            detail={plugin.secrets.join(", ")}
          />
        ) : null}
        {plugin?.approvalGates?.length ? (
          <Fact
            label="Approval gates"
            value={String(plugin.approvalGates.length)}
            detail={plugin.approvalGates.join(", ")}
          />
        ) : null}
        {plugin?.triggerCount ? (
          <Fact label="Triggers" value={String(plugin.triggerCount)} detail="cron or webhook" />
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
  const app = useApp();
  const res = useResources();
  const runnerUp = res.samples.some((s) => s.role === "runner" && s.state === "running");

  const rungs: { id: string; label: string; hint: string }[] = [
    { id: "skill.check", label: "Check", hint: "Do the MCP servers load, offline?" },
    {
      id: runnerUp ? "skill.down" : "skill.up",
      label: runnerUp ? "Stop runner" : "Boot runner",
      hint: runnerUp ? "A runner is live" : "One container, straight from this directory",
    },
    { id: "skill.message", label: "Message", hint: "Send a synthetic event and read the reply" },
    { id: "skill.eval", label: "Grade", hint: "Run evals/cases.json through the runner" },
    { id: "local.deploy", label: "Deploy local", hint: "Push to the local platform" },
    { id: "cluster.deploy", label: "Deploy cluster", hint: "Push to Kubernetes" },
  ];

  return (
    <section>
      <SectionHeader
        right={
          <span style={{ ...F.footnote, color: runnerUp ? ACCENT : T.quaternary }}>
            {runnerUp ? "runner live" : "no runner"}
          </span>
        }
      >
        The loop
      </SectionHeader>
      <Group style={{ padding: 12 }}>
        <div style={{ display: "flex", gap: 7, flexWrap: "wrap", alignItems: "center" }}>
          {rungs.map((r, i) => (
            <span key={r.id} style={{ display: "inline-flex", alignItems: "center", gap: 7 }}>
              {i > 0 ? <span style={{ color: T.quaternary, fontSize: 10 }}>›</span> : null}
              <Button
                size="sm"
                tone={r.id === "skill.down" ? "danger" : i === 0 ? "primary" : "default"}
                title={r.hint}
                onClick={() => app.navigate("commands", r.id)}
              >
                {r.label}
              </Button>
            </span>
          ))}
        </div>
        <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
          A runner executes an immutable snapshot taken at <Mono style={{ fontSize: 10 }}>skill up</Mono>,
          so a SKILL.md edit reaches it only after a restart. Grading without that restart grades
          the pre-edit bundle with nothing to say it is stale.{" "}
          <Mono style={{ fontSize: 10 }}>skill up --replace</Mono> is the restart.
          {" "}evals/cases.json is read live from source, so the contract needs no restart.
        </div>
      </Group>
    </section>
  );
}

function Checklist({ checks }: { checks: readonly Check[] }) {
  const app = useApp();
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
                <Button size="sm" onClick={() => app.navigate("commands", c.fix!)}>
                  Fix
                </Button>
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
              minHeight: 420,
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
  const app = useApp();

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
          <Button size="sm" onClick={() => app.navigate("commands", "skill.eval-init")}>
            Generate a starter suite
          </Button>
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
            <Button size="sm" onClick={() => app.navigate("commands", "skill.eval")}>
              Run
            </Button>
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
