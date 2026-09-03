// Slack behavior packs, on the Build screen.
//
// Packs are the per-agent, opt-in Slack layer (docs/behavior-packs.md): rotating
// "working..." captions, capability tips, canned replies to a bare "hi" or "what
// can you do", a hub button so a reply is never a dead end. Six of them, stored
// as JSON on the agent row.
//
// Two things about this surface are worth knowing before reading the code.
//
// **The CLI cannot do this.** There is no `curie` verb for packs; the only
// surface is `GET|PUT /agents/{id}/behavior-packs`. Every other view in this app
// is built to be no worse than the CLI. This one has no CLI to be no worse than,
// so it is the first place outside the web console where a pack can be authored.
//
// **Packs live on a deployed agent, not in the bundle.** `plugin.json` has no
// pack field and `packages/plugin-format` would reject one; a pack is written
// against an agent that already exists. That is a real tension with a screen
// whose subject is files on disk, and it is resolved by saying so rather than by
// pretending: the section names its scope, targets an agent explicitly, and
// offers to draft packs FROM the bundle's own facts -- its description and its
// starter prompts, which are already "things you can ask me". Drafting is the
// part that belongs to Build; the write is to the agent.
//
// The preview is not a mock-up. `src/lib/packs.ts` mirrors the worker's matcher
// and sampler, and `electron/packs-parity.test.ts` runs both implementations
// over the same corpus and fails when they disagree.

import { useCallback, useEffect, useMemo, useState } from "react";
import { channelLabel, primaryChannel } from "../lib/channels";

import { useApp, type AgentSummary } from "../bridge/app";
import { bridge } from "../bridge/bridge";
import type { PluginManifest } from "../lib/bundle";
import { DASH } from "../lib/format";
import {
  DEFAULT_MAX_BYTES,
  EMPTY_PACKS,
  PACK_KINDS,
  SETTING_KINDS,
  byteSize,
  caption,
  enabledPacks,
  inertPacks,
  isInert,
  matchGreeting,
  matchHelp,
  packIssues,
  parsePacks,
  proposeFromBundle,
  samePacks,
  type BehaviorPacks,
  type IssueLevel,
  type PackId,
  type PackKind,
  type Setting,
} from "../lib/packs";
import { ACCENT, F, FONT, KNOB, LINE, M, S, STATUS, T, tint } from "../tokens";
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
  Select,
  Textarea,
  Toggle,
  Well,
} from "../primitives";

/** The worker's `status_text` default. Slack prefixes the caption with the app
 *  name, so the author supplies only the rest -- which is why the default reads
 *  as a sentence fragment. */
const DEFAULT_STATUS = "is working on your request...";

const ISSUE_COLOR: Record<IssueLevel, string> = {
  error: STATUS.danger,
  warn: STATUS.warn,
  info: T.tertiary,
};

/**
 * Where the operator last was in this screen, so returning to the tab does not
 * throw away their place.
 *
 * localStorage rather than the main process's prefs file: this is a UI cursor,
 * not platform state, and `sticky` (the values remembered across command forms)
 * already establishes that the renderer keeps its own position here. Adding an
 * IPC channel for a cursor would be a bigger surface than the thing deserves.
 *
 * Cleared when the operator goes back to the list, because "I was looking at the
 * list" is a place too.
 */
const CURSOR_KEY = "curie.desktop.packsAgent";

function readCursor(): string | null {
  try {
    return localStorage.getItem(CURSOR_KEY);
  } catch {
    return null;
  }
}

function writeCursor(id: string | null): void {
  try {
    if (id) localStorage.setItem(CURSOR_KEY, id);
    else localStorage.removeItem(CURSOR_KEY);
  } catch {
    // A cursor is a convenience; losing it must never break the screen.
  }
}

export function SlackPacks({ plugin }: { plugin?: PluginManifest }) {
  const app = useApp();
  const agents = app.agents;

  // Read once, on mount: after that the operator's clicks are the truth.
  const [cursor, setCursor] = useState<string | null>(() => readCursor());

  // A remembered agent the platform no longer has resolves to no selection, so a
  // deleted agent lands you on the list instead of on a screen pointing at
  // nothing. `cursor` is deliberately NOT cleared -- if the agent comes back (a
  // reachable API after a blip, say) the place comes back with it.
  const open = cursor && agents.some((a) => a.id === cursor) ? cursor : null;

  const choose = (id: string | null) => {
    setCursor(id);
    writeCursor(id);
  };

  return (
    <section>
      <SectionHeader>Slack behavior packs</SectionHeader>
      <Scope />

      {!app.api?.reachable ? (
        <Notice
          tone="warn"
          title="No platform API"
          action={
            <Button size="sm" tone="plain" onClick={() => app.navigate("settings")}>
              Connect
            </Button>
          }
        >
          Packs are stored on an agent row, so they are read and written through the platform API.
          Point this app at one in Settings.
        </Notice>
      ) : app.agentsError ? (
        <Notice tone="error" title="Cannot list agents">
          {app.agentsError}
        </Notice>
      ) : agents.length === 0 ? (
        <Notice tone="info" title="No agents deployed yet">
          A pack is written against an agent that already exists. Run this bundle up the ladder
          first; the rungs above end at <Mono>skill up</Mono> or <Mono>cluster deploy</Mono>.
        </Notice>
      ) : open ? (
        // Keyed by agent: switching agents remounts, which resets the draft, the
        // errors and the dirty flag together. An effect that reset them by hand
        // would have to get all four right on every path.
        <AgentPacks
          key={open}
          agentId={open}
          agents={agents}
          onBack={() => choose(null)}
          plugin={plugin}
        />
      ) : (
        <AgentList agents={agents} onOpen={choose} />
      )}
    </section>
  );
}

/**
 * The agents you can write packs to, as an inventory.
 *
 * Shown even when there is exactly one, which is the point: opening straight
 * into a single agent's editor reads as "this is THE agent" rather than "this is
 * one of your agents", and hides the fact that the screen is per-agent at all.
 * A list of one still answers "what can I configure, and what state is it in".
 *
 * Each row carries that state, which is why this is a list and not a menu of
 * names: an operator wants to see which agents have packs configured, and which
 * only look configured, without opening each one.
 */
function AgentList({
  agents,
  onOpen,
}: {
  agents: readonly AgentSummary[];
  onOpen: (id: string) => void;
}) {
  // `undefined` = not read yet, `null` = the read failed. Distinguished because
  // "unknown" and "broken" are different things to show, and neither is "off".
  const [packs, setPacks] = useState<Readonly<Record<string, BehaviorPacks | null>>>({});

  // Keyed on the ids, not the array: `agents` is refreshed on a timer and gets a
  // new identity each poll, which would refetch every agent's packs forever.
  const key = agents.map((a) => a.id).join(",");
  useEffect(() => {
    const ids = key ? key.split(",") : [];
    if (!ids.length) return;
    let cancelled = false;
    void (async () => {
      const entries = await Promise.all(
        ids.map(async (id) => {
          const res = await bridge().api.request<unknown>({
            method: "GET",
            path: `/agents/${id}/behavior-packs`,
          });
          return [id, res.ok ? parsePacks(res.body) : null] as const;
        }),
      );
      if (cancelled) return;
      setPacks(Object.fromEntries(entries));
    })();
    return () => {
      cancelled = true;
    };
  }, [key]);

  return (
    <Group>
      {agents.map((agent, i) => (
        <AgentRow
          key={agent.id}
          agent={agent}
          packs={packs[agent.id]}
          first={i === 0}
          onOpen={() => onOpen(agent.id)}
        />
      ))}
    </Group>
  );
}

function AgentRow({
  agent,
  packs,
  first,
  onOpen,
}: {
  agent: AgentSummary;
  packs: BehaviorPacks | null | undefined;
  first?: boolean;
  onOpen: () => void;
}) {
  const on = packs ? enabledPacks(packs) : null;
  const dead = packs ? inertPacks(packs) : [];
  const where = channelLabel(agent);

  return (
    <Row first={first} onClick={onOpen}>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ ...F.body, color: T.primary }}>{agent.name}</div>
        <div style={{ ...F.footnote, color: T.tertiary, marginTop: 1 }}>
          {agent.model ?? "platform default model"}
          {where ? ` · ${where}` : ""}
        </div>
      </div>

      <div style={{ display: "flex", gap: 6, alignItems: "center", flex: "none" }}>
        {!where ? (
          <Badge color={STATUS.warn} title="Nothing renders a pack until a surface is bound.">
            no surface
          </Badge>
        ) : null}
        {dead.length ? (
          <Badge
            color={STATUS.warn}
            title={`On but unusable: ${dead.join(", ")}. Open the agent to see why.`}
          >
            {dead.length} will not fire
          </Badge>
        ) : null}
        <span style={{ ...F.footnote, color: T.tertiary, minWidth: 74, textAlign: "right" }}>
          {packs === undefined
            ? DASH
            : packs === null
              ? "unreadable"
              : on && on.length
                ? `${on.length} of ${PACK_KINDS.length} on`
                : "no packs"}
        </span>
        <span style={{ color: T.quaternary, fontFamily: FONT.mono, fontSize: 12 }}>›</span>
      </div>
    </Row>
  );
}

function AgentPacks({
  agentId,
  agents,
  onBack,
  plugin,
}: {
  agentId: string;
  agents: readonly AgentSummary[];
  onBack: () => void;
  plugin?: PluginManifest;
}) {
  const [server, setServer] = useState<BehaviorPacks | null>(null);
  const [draft, setDraft] = useState<BehaviorPacks>(EMPTY_PACKS);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const res = await bridge().api.request<unknown>({
        method: "GET",
        path: `/agents/${agentId}/behavior-packs`,
      });
      if (cancelled) return;
      if (!res.ok) {
        setLoadError(res.error ?? "could not read this agent's packs");
        return;
      }
      // Total by contract on both sides: the API returns the all-off default for
      // a NULL column, and parsePacks tolerates a blob it does not recognise
      // rather than refusing to open an agent the platform runs fine.
      const packs = parsePacks(res.body);
      setServer(packs);
      setDraft(packs);
    })();
    return () => {
      cancelled = true;
    };
  }, [agentId]);

  const dirty = server !== null && !samePacks(server, draft);
  const issues = useMemo(() => packIssues(draft), [draft]);
  const size = byteSize(draft);
  const on = enabledPacks(draft);
  const agent = agents.find((a) => a.id === agentId);

  const save = useCallback(async () => {
    setSaving(true);
    setSaveError(null);
    const res = await bridge().api.request<unknown>({
      method: "PUT",
      path: `/agents/${agentId}/behavior-packs`,
      body: draft,
    });
    setSaving(false);
    if (!res.ok) {
      setSaveError(res.error ?? "the write was rejected");
      return;
    }
    // Trust the response over the draft: the API is what stored it.
    const stored = parsePacks(res.body);
    setServer(stored);
    setDraft(stored);
  }, [agentId, draft]);

  return (
    <>
      <Group style={{ marginBottom: 12 }}>
        <Row first>
          <Button size="sm" tone="plain" onClick={onBack}>
            {"\u2039 All agents"}
          </Button>
          <div style={{ flex: 1, minWidth: 0, marginLeft: 4 }}>
            <div style={{ ...F.headline, color: T.primary }}>{agent?.name ?? agentId}</div>
            <div style={{ ...F.footnote, color: T.tertiary }}>
              {agent?.model ?? "platform default model"}
            </div>
          </div>
          {server === null && !loadError ? (
            <span style={{ ...F.footnote, color: T.quaternary }}>reading...</span>
          ) : (
            <>
              <span style={{ ...F.footnote, color: T.tertiary }}>
                {on.length ? `${on.length} of ${PACK_KINDS.length} on` : "all packs off"}
              </span>
              <span
                style={{
                  ...F.footnote,
                  color: size > DEFAULT_MAX_BYTES ? STATUS.danger : T.quaternary,
                }}
                title={`The API caps a pack write at ${DEFAULT_MAX_BYTES} bytes by default.`}
              >
                {size} B
              </span>
            </>
          )}
        </Row>

        {server ? (
          <Row>
            {plugin ? (
              <Button
                size="sm"
                tone="plain"
                title="Draft greeting, help and tips from this bundle's description and starter prompts"
                onClick={() =>
                  setDraft((d) =>
                    proposeFromBundle(
                      {
                        name: plugin.name,
                        description: plugin.description,
                        starterPrompts: plugin.starterPrompts,
                      },
                      d,
                    ),
                  )
                }
              >
                Draft from this bundle
              </Button>
            ) : null}
            <div style={{ flex: 1 }} />
            {dirty ? <span style={{ ...F.footnote, color: STATUS.warn }}>unsaved</span> : null}
            <Button size="sm" tone="plain" disabled={!dirty} onClick={() => setDraft(server)}>
              Revert
            </Button>
            <Button
              size="sm"
              tone="primary"
              disabled={!dirty}
              busy={saving}
              onClick={() => void save()}
            >
              Save to agent
            </Button>
          </Row>
        ) : null}

        {agent ? <Binding agent={agent} /> : null}
      </Group>

      {loadError ? (
        <Notice tone="error" title="Cannot read this agent's packs">
          {loadError}
        </Notice>
      ) : null}
      {saveError ? (
        <Notice tone="error" title="The write was rejected">
          {saveError}
        </Notice>
      ) : null}

      {server ? (
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {issues.length ? <Issues issues={issues} /> : null}

          {PACK_KINDS.map((kind) => (
            <PackCard
              key={kind.id}
              kind={kind}
              packs={draft}
              inert={isInert(draft, kind.id)}
              onChange={setDraft}
            />
          ))}

          <TryIt packs={draft} />
        </div>
      ) : null}
    </>
  );
}

/** Says what this section is, because a reader who has just been editing files
 *  will otherwise assume these are more files. */
function Scope() {
  return (
    <div style={{ ...F.footnote, color: T.tertiary, padding: "0 4px 8px", maxWidth: M.prose }}>
      Opt-in Slack touches applied around a turn: a load caption, a tip, canned replies to a bare
      greeting or help request, a hub button. They are stored on the agent, not in the bundle, so
      they are saved to a deployed agent and take effect on its next message. The CLI has no verb
      for them.
    </div>
  );
}

/** Packs are a Slack layer, so an agent bound to something else is worth naming
 *  rather than letting the author configure a caption nothing will render. */
function Binding({ agent }: { agent: AgentSummary }) {
  const kind = primaryChannel(agent)?.kind;
  if (!kind)
    return (
      <Row>
        <div style={{ ...F.footnote, color: STATUS.warn }}>
          This agent has no surface bound, so nothing renders a pack yet. Add one with{" "}
          <Mono>agent channels --add</Mono>.
        </div>
      </Row>
    );
  if (kind.toLowerCase() !== "slack")
    return (
      <Row>
        <div style={{ ...F.footnote, color: T.tertiary }}>
          This agent is bound to <Mono>{kind}</Mono>. Packs are rendered by the Slack adapter; the
          greeting and help replies still send, but the load caption and hub button are Slack
          affordances.
        </div>
      </Row>
    );
  return null;
}

function Issues({ issues }: { issues: ReturnType<typeof packIssues> }) {
  const worst = issues.some((i) => i.level === "error")
    ? "error"
    : issues.some((i) => i.level === "warn")
      ? "warn"
      : "info";
  return (
    <Group>
      <Row first>
        <div style={{ ...F.caption, color: ISSUE_COLOR[worst as IssueLevel], fontWeight: 600 }}>
          {issues.filter((i) => i.level === "error").length
            ? "Some of these packs are on and will not fire"
            : issues.some((i) => i.level === "warn")
              ? "Worth a look"
              : "Notes"}
        </div>
      </Row>
      {issues.map((issue, i) => (
        <Row key={`${issue.pack}-${i}`}>
          <div style={{ display: "flex", gap: 8, alignItems: "baseline" }}>
            <Badge color={ISSUE_COLOR[issue.level]}>{issue.pack}</Badge>
            <div style={{ ...F.footnote, color: T.secondary }}>{issue.message}</div>
          </div>
        </Row>
      ))}
    </Group>
  );
}

// --- one pack ----------------------------------------------------------------

function PackCard({
  kind,
  packs,
  inert,
  onChange,
}: {
  kind: PackKind;
  packs: BehaviorPacks;
  inert: boolean;
  onChange: (next: (prev: BehaviorPacks) => BehaviorPacks) => void;
}) {
  const pack = packs[kind.id];
  const setEnabled = (enabled: boolean) =>
    onChange((d) => ({ ...d, [kind.id]: { ...d[kind.id], enabled } }) as BehaviorPacks);

  return (
    <Group>
      <Row first>
        <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <Toggle
              checked={pack.enabled}
              onChange={setEnabled}
              label={kind.title}
              hint={
                <>
                  {kind.what} <span style={{ color: T.quaternary }}>{kind.surface}</span>
                </>
              }
            />
          </div>
          <div style={{ display: "flex", gap: 6, flex: "none", paddingTop: 2 }}>
            {!kind.live ? (
              <Badge color={T.quaternary} title="The platform stores and validates this pack, but no runtime reads it yet.">
                no runtime yet
              </Badge>
            ) : null}
            {inert ? (
              <Badge color={STATUS.warn} title="This pack is on but has nothing the runtime can use.">
                does nothing
              </Badge>
            ) : null}
          </div>
        </div>
      </Row>

      {pack.enabled ? <div style={{ padding: "12px 14px 14px" }}>{body(kind.id, packs, onChange)}</div> : null}
    </Group>
  );
}

function body(
  id: PackId,
  packs: BehaviorPacks,
  onChange: (next: (prev: BehaviorPacks) => BehaviorPacks) => void,
) {
  switch (id) {
    case "load":
      return (
        <StringList
          label="Load lines"
          hint="One is picked per message. Slack puts the app name in front, so write the rest: 'is crunching the numbers...'."
          values={packs.load.lines}
          placeholder="is crunching the numbers..."
          onChange={(lines) => onChange((d) => ({ ...d, load: { ...d.load, lines } }))}
        />
      );
    case "tips":
      return (
        <StringList
          label="Tips"
          hint="What the agent can do, not what it is doing. Shown after the load line as 'Tip: ...'."
          values={packs.tips.tips}
          placeholder="I can rank leaks by $"
          onChange={(tips) => onChange((d) => ({ ...d, tips: { ...d.tips, tips } }))}
        />
      );
    case "greeting":
    case "help": {
      const pack = packs[id];
      return (
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <StringList
            label="Trigger phrases"
            hint={
              id === "greeting"
                ? "Matched only when the message is the phrase alone, or the phrase plus filler ('hey there team'). 'hi show me the report' still reaches the model."
                : "Matched only on a bare help request. Anything with a real question in it reaches the model."
            }
            values={pack.phrases}
            placeholder={id === "greeting" ? "good morning" : "what can you do"}
            onChange={(phrases) => onChange((d) => ({ ...d, [id]: { ...d[id], phrases } }) as BehaviorPacks)}
          />
          <Field
            label="Reply"
            hint="Sent as-is, with no model call. Without a reply the pack never fires, however many phrases it has."
            error={pack.reply.trim() ? null : "required: an empty reply switches the pack off"}
          >
            <Textarea
              rows={5}
              value={pack.reply}
              placeholder={id === "greeting" ? "Hi! I triage alerts..." : "Ask me to..."}
              onChange={(e) =>
                onChange((d) => ({ ...d, [id]: { ...d[id], reply: e.target.value } }) as BehaviorPacks)
              }
            />
          </Field>
        </div>
      );
    }
    case "settings":
      return <SettingsEditor pack={packs.settings.settings} onChange={onChange} />;
    case "nav":
      return (
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          <Field label="Button label" hint="What the button says.">
            <Input
              value={packs.nav.hub_label}
              placeholder="Help"
              onChange={(e) => onChange((d) => ({ ...d, nav: { ...d.nav, hub_label: e.target.value } }))}
            />
          </Field>
          <Field label="Hub command" hint="The action id the button sends, which must reach the agent's home screen.">
            <Input
              value={packs.nav.hub_command}
              placeholder="hub"
              onChange={(e) => onChange((d) => ({ ...d, nav: { ...d.nav, hub_command: e.target.value } }))}
            />
          </Field>
        </div>
      );
  }
}

/** The list editor every pack needs: lines, tips, phrases, choices. */
function StringList({
  label,
  hint,
  values,
  placeholder,
  onChange,
}: {
  label: string;
  hint?: string;
  values: readonly string[];
  placeholder?: string;
  onChange: (next: string[]) => void;
}) {
  const [pending, setPending] = useState("");
  const add = () => {
    const v = pending.trim();
    if (!v) return;
    onChange([...values, v]);
    setPending("");
  };
  return (
    <Field label={label} hint={hint}>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {values.map((v, i) => (
          <div key={i} style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <Input
              value={v}
              style={{ flex: 1 }}
              onChange={(e) => onChange(values.map((x, j) => (j === i ? e.target.value : x)))}
            />
            <Button
              size="sm"
              tone="plain"
              title="Remove"
              onClick={() => onChange(values.filter((_, j) => j !== i))}
            >
              Remove
            </Button>
          </div>
        ))}
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <Input
            value={pending}
            placeholder={placeholder}
            style={{ flex: 1 }}
            onChange={(e) => setPending(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                add();
              }
            }}
          />
          <Button size="sm" tone="plain" disabled={!pending.trim()} onClick={add}>
            Add
          </Button>
        </div>
      </div>
    </Field>
  );
}

function SettingsEditor({
  pack,
  onChange,
}: {
  pack: readonly Setting[];
  onChange: (next: (prev: BehaviorPacks) => BehaviorPacks) => void;
}) {
  const set = (i: number, patch: Partial<Setting>) =>
    onChange((d) => ({
      ...d,
      settings: {
        ...d.settings,
        settings: d.settings.settings.map((s, j) => (j === i ? { ...s, ...patch } : s)),
      },
    }));
  const remove = (i: number) =>
    onChange((d) => ({
      ...d,
      settings: { ...d.settings, settings: d.settings.settings.filter((_, j) => j !== i) },
    }));
  const add = () =>
    onChange((d) => ({
      ...d,
      settings: {
        ...d.settings,
        settings: [
          ...d.settings.settings,
          { key: "", label: "", kind: "str", default: "", help: "", choices: [], applies_live: true },
        ],
      },
    }));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {pack.map((s, i) => (
        <div
          key={i}
          style={{
            border: `1px solid ${LINE.separator}`,
            borderRadius: 8,
            padding: 12,
            display: "flex",
            flexDirection: "column",
            gap: 10,
            background: S.well,
          }}
        >
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "flex-end" }}>
            <Field label="Key" hint="How the knob is referenced.">
              <Input value={s.key} placeholder="verbosity" onChange={(e) => set(i, { key: e.target.value })} />
            </Field>
            <Field label="Label">
              <Input value={s.label} placeholder="Verbosity" onChange={(e) => set(i, { label: e.target.value })} />
            </Field>
            <Field label="Kind">
              <Select value={s.kind} onChange={(e) => set(i, { kind: e.target.value })}>
                {SETTING_KINDS.map((k) => (
                  <option key={k} value={k}>
                    {k}
                  </option>
                ))}
                {SETTING_KINDS.includes(s.kind as never) ? null : <option value={s.kind}>{s.kind}</option>}
              </Select>
            </Field>
            <Field label="Default">
              <Input value={s.default} onChange={(e) => set(i, { default: e.target.value })} />
            </Field>
            <Button size="sm" tone="plain" onClick={() => remove(i)}>
              Remove
            </Button>
          </div>
          <Field label="Help" hint="Shown to whoever edits the knob.">
            <Input value={s.help} onChange={(e) => set(i, { help: e.target.value })} />
          </Field>
          {s.kind === "choice" ? (
            <StringList
              label="Choices"
              hint="A value outside this list is rejected."
              values={s.choices}
              placeholder="terse"
              onChange={(choices) => set(i, { choices })}
            />
          ) : null}
          <Toggle
            checked={s.applies_live}
            onChange={(applies_live) => set(i, { applies_live })}
            label="Applies live"
            hint="Off means a change only takes effect on the next restart."
          />
        </div>
      ))}
      <div>
        <Button size="sm" tone="plain" onClick={add}>
          Add a setting
        </Button>
      </div>
    </div>
  );
}

// --- the preview -------------------------------------------------------------

/** Three thread ids, to show that the sampler rotates rather than picking one
 *  line forever. Fixed values, so the preview does not change under the author
 *  while they are reading it. */
const SEEDS: readonly string[] = ["1712345678.000100", "1712345679.000200", "1712345680.000300"];

/**
 * What Slack would actually do with a message.
 *
 * This is the part that justifies mirroring the worker rather than describing it.
 * The matcher's rules are not guessable from the form: the phrase has to start
 * the utterance, only a fixed filler set may follow it, an empty reply switches
 * the pack off, and the greeting pack is tried before the help pack. An author
 * finds all of that out here instead of in a Slack channel.
 */
function TryIt({ packs }: { packs: BehaviorPacks }) {
  const [text, setText] = useState("hey there team");
  const greeting = matchGreeting(packs, text);
  const help = greeting ? null : matchHelp(packs, text);
  const reply = greeting ?? help;

  return (
    <Group>
      <Row first>
        <Field
          label="Try a message"
          hint="Run a message through the same matcher the worker uses, before anyone types it in Slack."
        >
          <Input value={text} onChange={(e) => setText(e.target.value)} placeholder="hi" />
        </Field>
      </Row>
      <Row>
        {reply ? (
          <div>
            <div style={{ ...F.footnote, color: ACCENT, marginBottom: 6 }}>
              Answered by the {greeting ? "greeting" : "help"} pack, with no model call.
            </div>
            <Well style={{ whiteSpace: "pre-wrap" }} mono={false}>
              {reply}
            </Well>
          </div>
        ) : (
          <div style={{ ...F.footnote, color: T.tertiary }}>
            No pack matches, so this reaches the model as a normal turn.
          </div>
        )}
      </Row>
      <Row>
        <div>
          <div style={{ ...F.footnote, color: T.quaternary, marginBottom: 6 }}>
            While a turn runs, Slack shows this caption after the app name. One per thread, rotating.
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {SEEDS.map((seed) => {
              const cap = caption(packs, seed, DEFAULT_STATUS);
              const fromPack = packs.load.enabled || packs.tips.enabled;
              return (
                <div
                  key={seed}
                  style={{
                    ...F.footnote,
                    color: cap === DEFAULT_STATUS && fromPack ? T.quaternary : T.secondary,
                    whiteSpace: "pre-wrap",
                    background: tint(KNOB, 0.04),
                    borderRadius: 6,
                    padding: "6px 8px",
                  }}
                >
                  {cap ?? "(no caption: the deployment's status text is blank)"}
                </div>
              );
            })}
          </div>
          {!packs.load.enabled && !packs.tips.enabled ? (
            <div style={{ ...F.footnote, color: T.quaternary, marginTop: 6 }}>
              That is the platform default. Turn on load lines or tips to replace it.
            </div>
          ) : null}
        </div>
      </Row>
    </Group>
  );
}
