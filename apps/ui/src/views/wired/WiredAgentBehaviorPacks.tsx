import { useCallback, useEffect, useMemo, useState } from "react";
import { C } from "../../tokens";
import { Button, Card, CliHint, Notice, PARITY_TRACKING_ISSUE } from "../../primitives";
import {
  getBehaviorPacks,
  putBehaviorPacks,
  ApiError,
  type BehaviorPacksConfig,
  type GreetingPack,
  type HelpPack,
  type LoadPack,
  type NavPack,
  type SettingConfig,
  type TipsPack,
} from "../../api/client";

// The wired "behavior packs" surface (#870): a small settings-style panel on the
// agent detail page that reads GET /agents/{id}/behavior-packs and writes the
// whole config back with PUT. Behavior packs are per-agent opt-in deterministic
// behaviors (rotating load lines, capability tips, a greeting/help short-circuit,
// a declarative editable-settings allowlist, and the no-dead-ends nav button).
//
// The API takes the FULL config on write (no partial patch), so this panel loads
// the current config, edits a working copy in place, and PUTs the whole object on
// Save. The settings pack is schema-only today (the per-user override store is a
// deferred runtime), so its declared knobs are shown read-only and round-tripped
// unchanged. No CLI verb reads/writes behavior_packs (cli/api-mirrors.json), so
// Save carries the honest amber CliHint gap (parity id behavior-packs-edit).

// A pack's edits are read by the worker at the agent's next bind, not mid-turn.
const APPLIES_NOTE =
  "Behavior packs are read at the agent's next bind (a fresh mention), not mid-conversation.";

// A textarea's text is kept as one entry per line while editing (empties and
// all), so multi-line typing works — a freshly-typed newline leaves a transient
// blank line the operator is about to fill. Blank/whitespace-only lines are only
// dropped at save time (cleanList), so the worker never renders a "" load line.
function toEditLines(text: string): string[] {
  return text.split("\n");
}

function cleanList(lines: string[]): string[] {
  return lines.map((l) => l.trim()).filter((l) => l.length > 0);
}

// The editor's working copy, with every pack and pack field present. The API
// schema declares all of them optional, so an absent one takes the declared
// default (off, empty). Declared settings knobs are kept verbatim: the settings
// pack is read-only here and must round-trip unchanged.
interface ResolvedPacks {
  load: Required<LoadPack>;
  tips: Required<TipsPack>;
  greeting: Required<GreetingPack>;
  help: Required<HelpPack>;
  settings: { enabled: boolean; settings: SettingConfig[] };
  nav: Required<NavPack>;
}

function resolvePacks(config: BehaviorPacksConfig): ResolvedPacks {
  const { load = {}, tips = {}, greeting = {}, help = {}, settings = {}, nav = {} } = config;
  return {
    load: { enabled: load.enabled ?? false, lines: load.lines ?? [] },
    tips: { enabled: tips.enabled ?? false, tips: tips.tips ?? [] },
    greeting: {
      enabled: greeting.enabled ?? false,
      phrases: greeting.phrases ?? [],
      reply: greeting.reply ?? "",
    },
    help: { enabled: help.enabled ?? false, phrases: help.phrases ?? [], reply: help.reply ?? "" },
    settings: { enabled: settings.enabled ?? false, settings: settings.settings ?? [] },
    nav: {
      enabled: nav.enabled ?? false,
      hub_label: nav.hub_label ?? "",
      hub_command: nav.hub_command ?? "",
    },
  };
}

// The exact object PUT on save: every list field trimmed + blank-stripped, the
// rest (nav strings, replies, the read-only settings pack) carried verbatim.
function cleanConfig(config: ResolvedPacks): BehaviorPacksConfig {
  return {
    ...config,
    load: { ...config.load, lines: cleanList(config.load.lines) },
    tips: { ...config.tips, tips: cleanList(config.tips.tips) },
    greeting: { ...config.greeting, phrases: cleanList(config.greeting.phrases) },
    help: { ...config.help, phrases: cleanList(config.help.phrases) },
  };
}

function ListField({
  label,
  help,
  value,
  onChange,
  testId,
}: {
  label: string;
  help: string;
  value: string[];
  onChange: (next: string[]) => void;
  testId: string;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <div style={{ fontSize: 12, color: C.text2, fontFamily: C.mono }}>{label}</div>
      <div style={{ fontSize: 11, color: C.muted }}>{help}</div>
      <textarea
        aria-label={label}
        data-testid={testId}
        value={value.join("\n")}
        onChange={(e) => onChange(toEditLines(e.target.value))}
        rows={3}
        style={{
          width: "100%",
          background: C.input,
          color: C.text,
          border: "1px solid " + C.border,
          borderRadius: 4,
          fontFamily: C.mono,
          fontSize: 12.5,
          padding: 8,
          resize: "vertical",
        }}
      />
    </div>
  );
}

function TextField({
  label,
  value,
  onChange,
  testId,
  multiline,
}: {
  label: string;
  value: string;
  onChange: (next: string) => void;
  testId: string;
  multiline?: boolean;
}) {
  const shared = {
    "aria-label": label,
    "data-testid": testId,
    value,
    onChange: (e: { target: { value: string } }) => onChange(e.target.value),
    style: {
      width: "100%",
      background: C.input,
      color: C.text,
      border: "1px solid " + C.border,
      borderRadius: 4,
      fontFamily: C.mono,
      fontSize: 12.5,
      padding: 8,
    } as const,
  };
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <div style={{ fontSize: 12, color: C.text2, fontFamily: C.mono }}>{label}</div>
      {multiline ? (
        <textarea {...shared} rows={2} style={{ ...shared.style, resize: "vertical" }} />
      ) : (
        <input {...shared} type="text" />
      )}
    </div>
  );
}

function PackSection({
  name,
  title,
  description,
  enabled,
  onToggle,
  children,
}: {
  name: string;
  title: string;
  description: string;
  enabled: boolean;
  onToggle: (next: boolean) => void;
  children?: React.ReactNode;
}) {
  return (
    <div
      data-testid={`pack-${name}`}
      style={{
        border: "1px solid " + C.border,
        borderRadius: 6,
        padding: "10px 12px",
        background: C.darkest,
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
    >
      <label style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}>
        <input
          type="checkbox"
          data-testid={`pack-toggle-${name}`}
          checked={enabled}
          onChange={(e) => onToggle(e.target.checked)}
          style={{ cursor: "pointer" }}
        />
        <span style={{ fontFamily: C.mono, fontSize: 13, color: C.text }}>{title}</span>
        <span style={{ marginLeft: "auto", fontSize: 11, color: enabled ? C.brand : C.muted, fontFamily: C.mono }}>
          {enabled ? "on" : "off"}
        </span>
      </label>
      <div style={{ fontSize: 11.5, color: C.muted, lineHeight: 1.4 }}>{description}</div>
      {children ? <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>{children}</div> : null}
    </div>
  );
}

export function WiredAgentBehaviorPacks({ agentId }: { agentId: string }) {
  const [config, setConfig] = useState<ResolvedPacks | null>(null);
  const [baseline, setBaseline] = useState<ResolvedPacks | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const cfg = resolvePacks(await getBehaviorPacks(agentId));
      setConfig(cfg);
      setBaseline(cfg);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [agentId]);

  useEffect(() => {
    void load();
  }, [load]);

  // Serialize for dirty-tracking: the working copy differs from the last-saved
  // baseline. Cheap and exact for this small object.
  const dirty = useMemo(
    () => !!config && !!baseline && JSON.stringify(config) !== JSON.stringify(baseline),
    [config, baseline],
  );

  // Typed patch helper: mutate one pack's slice, clear the transient saved flag.
  const patch = useCallback((next: Partial<ResolvedPacks>) => {
    setSaved(false);
    setSaveError(null);
    setConfig((prev) => (prev ? { ...prev, ...next } : prev));
  }, []);

  const save = async () => {
    if (!config || saving || !dirty) return;
    setSaving(true);
    setSaveError(null);
    try {
      // Send the cleaned config (blank list lines stripped); adopt the server's
      // echo as the new working copy + baseline so the panel reflects exactly
      // what persisted (and pending blank lines collapse in the editor).
      const updated = resolvePacks(await putBehaviorPacks(agentId, cleanConfig(config)));
      setConfig(updated);
      setBaseline(updated);
      setSaved(true);
    } catch (e) {
      setSaveError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const reset = () => {
    if (baseline) setConfig(baseline);
    setSaved(false);
    setSaveError(null);
  };

  return (
    <Card>
      <div data-testid="agent-behavior-packs" style={{ padding: "4px 4px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
          <div style={{ fontWeight: 600, fontSize: 14, color: C.text2 }}>Behavior packs</div>
          <CliHint
            noCliEquivalent={PARITY_TRACKING_ISSUE}
            actionIds={["behavior-packs-edit"]}
            label="No CLI equivalent"
          />
          <div style={{ marginLeft: "auto" }}>
            <Button label="Refresh" variant="ghost" size="sm" onClick={() => void load()} />
          </div>
        </div>
        <div style={{ color: C.muted, fontSize: 12.5, marginBottom: 12 }}>
          Opt-in deterministic behaviors for this agent. {APPLIES_NOTE}
        </div>

        {error ? (
          <div
            data-testid="behavior-packs-error"
            style={{ color: C.destructive, fontSize: 12.5, marginBottom: 10, fontFamily: C.mono }}
          >
            {error}
          </div>
        ) : null}

        {loading ? (
          <Notice padding="28px 20px">Loading behavior packs…</Notice>
        ) : !config ? (
          <Notice padding="28px 20px">No behavior-pack config available.</Notice>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <PackSection
              name="load"
              title="load"
              description="Rotating “working…” lines shown while the agent is thinking."
              enabled={config.load.enabled}
              onToggle={(enabled) => patch({ load: { ...config.load, enabled } })}
            >
              <ListField
                label="Load lines"
                help="One line per row; shown in rotation. Blank lines are dropped."
                value={config.load.lines}
                onChange={(lines) => patch({ load: { ...config.load, lines } })}
                testId="load-lines"
              />
            </PackSection>

            <PackSection
              name="tips"
              title="tips"
              description="Rotating capability tips — what the agent can do, vs. what it is doing now."
              enabled={config.tips.enabled}
              onToggle={(enabled) => patch({ tips: { ...config.tips, enabled } })}
            >
              <ListField
                label="Tips"
                help="One tip per row; shown in rotation. Blank lines are dropped."
                value={config.tips.tips}
                onChange={(tips) => patch({ tips: { ...config.tips, tips } })}
                testId="tips-tips"
              />
            </PackSection>

            <PackSection
              name="greeting"
              title="greeting"
              description="Deterministic reply that short-circuits a matching greeting."
              enabled={config.greeting.enabled}
              onToggle={(enabled) => patch({ greeting: { ...config.greeting, enabled } })}
            >
              <ListField
                label="Trigger phrases"
                help="Messages matching one of these short-circuit to the reply below."
                value={config.greeting.phrases}
                onChange={(phrases) => patch({ greeting: { ...config.greeting, phrases } })}
                testId="greeting-phrases"
              />
              <TextField
                label="Reply"
                value={config.greeting.reply}
                onChange={(reply) => patch({ greeting: { ...config.greeting, reply } })}
                testId="greeting-reply"
                multiline
              />
            </PackSection>

            <PackSection
              name="help"
              title="help"
              description="Deterministic reply to a “what can you do” style message."
              enabled={config.help.enabled}
              onToggle={(enabled) => patch({ help: { ...config.help, enabled } })}
            >
              <ListField
                label="Trigger phrases"
                help="Messages matching one of these short-circuit to the reply below."
                value={config.help.phrases}
                onChange={(phrases) => patch({ help: { ...config.help, phrases } })}
                testId="help-phrases"
              />
              <TextField
                label="Reply"
                value={config.help.reply}
                onChange={(reply) => patch({ help: { ...config.help, reply } })}
                testId="help-reply"
                multiline
              />
            </PackSection>

            <PackSection
              name="nav"
              title="nav"
              description="The no-dead-ends hub button offered when the agent has nothing else to say."
              enabled={config.nav.enabled}
              onToggle={(enabled) => patch({ nav: { ...config.nav, enabled } })}
            >
              <TextField
                label="Hub label"
                value={config.nav.hub_label}
                onChange={(hub_label) => patch({ nav: { ...config.nav, hub_label } })}
                testId="nav-hub-label"
              />
              <TextField
                label="Hub command"
                value={config.nav.hub_command}
                onChange={(hub_command) => patch({ nav: { ...config.nav, hub_command } })}
                testId="nav-hub-command"
              />
            </PackSection>

            <PackSection
              name="settings"
              title="settings"
              description="Declarative allowlist of user-editable runtime knobs. Schema-only today (the per-user override store is a deferred runtime), so the declared knobs are shown read-only here."
              enabled={config.settings.enabled}
              onToggle={(enabled) => patch({ settings: { ...config.settings, enabled } })}
            >
              {config.settings.settings.length === 0 ? (
                <div style={{ fontSize: 11.5, color: C.muted, fontFamily: C.mono }}>No settings declared.</div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {config.settings.settings.map((s) => (
                    <div
                      key={s.key}
                      data-testid="settings-knob"
                      style={{
                        border: "1px solid " + C.border,
                        borderRadius: 4,
                        padding: "6px 8px",
                        background: C.input,
                        fontFamily: C.mono,
                        fontSize: 11.5,
                        color: C.text2,
                      }}
                    >
                      <span style={{ color: C.text }}>{s.key}</span>
                      <span style={{ color: C.muted }}>{` · ${s.kind ?? "str"}`}</span>
                      {s.default ? <span style={{ color: C.muted }}>{` · default ${s.default}`}</span> : null}
                      {s.label ? <span style={{ color: C.muted }}>{` — ${s.label}`}</span> : null}
                    </div>
                  ))}
                </div>
              )}
            </PackSection>

            <div style={{ display: "flex", alignItems: "center", gap: 12, marginTop: 2 }}>
              {saved ? (
                <span data-testid="behavior-packs-saved" style={{ fontSize: 12.5, color: C.brand, fontFamily: C.mono }}>
                  ✓ Saved
                </span>
              ) : (
                <span style={{ fontSize: 12, color: C.muted, fontFamily: C.mono }}>
                  {dirty ? "unsaved changes" : "no changes"}
                </span>
              )}
              {saveError ? (
                <span
                  data-testid="behavior-packs-save-error"
                  style={{ fontSize: 12, color: C.destructive, fontFamily: C.mono }}
                >
                  Save failed: {saveError}
                </span>
              ) : null}
              <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
                <Button label="Reset" variant="ghost" size="sm" disabled={!dirty || saving} onClick={reset} />
                <Button
                  label={saving ? "Saving…" : "Save"}
                  variant="primary"
                  size="sm"
                  testId="behavior-packs-save"
                  disabled={!dirty || saving}
                  onClick={() => void save()}
                />
              </div>
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
