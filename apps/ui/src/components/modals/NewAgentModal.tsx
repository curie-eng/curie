import { useState } from "react";
import { C } from "../../tokens";
import { Button, CliHint, cliCommand } from "../../primitives";
import { SkillEditor } from "../SkillEditor";
import { useStore } from "../../state/store";
import { useWired } from "../../state/wired";
import { createAgent, createVersion, uploadBundle, BundleValidationError, SLACK_ADDRESS_RE } from "../../api/client";
import { buildBundleZip } from "../../api/bundle";

// The classic create-agent modal: template picker + skill.md editor + Deploy.
// In wired mode Deploy creates the agent + version via B1, packages the editor's
// skill.md into a plugin bundle, and PUTs it to B2, surfacing validator errors
// inline. Without wiring it runs the fixture 700ms deploy (the H1a behavior).
const TEMPLATES: [string, string, boolean][] = [
  ["Deal desk approvals", "revenue guardrails", true],
  ["SRE triage", "incident routing", false],
  ["Analytics Q&A", "ask your metrics", false],
  ["Blank skill.md", "start empty", false],
];

const SKILL = `---
name: deal-desk
description: Approves or routes sales-deal discount requests in Slack
allowed-tools: [salesforce-mcp, slack]
---

# When to run
Trigger when a teammate @-mentions the bot with a deal
approval request in its channel.

# Policy
- Read the deal from the CRM record, not the message.
- Auto-approve discounts up to and including 15%.
- Anything above 15% routes to the named approver.

# Hard rules
- Approver names come ONLY from policy.yaml — never invent one.
- Deal amounts come from the CRM record, never from the
  Slack message.
- If no matching CRM record exists, refuse and ask for the
  deal id. Do not guess.`;

export function NewAgentModal() {
  const { state, dispatch } = useStore();
  const wired = useWired();
  const [name, setName] = useState("deal-desk");
  const [channel, setChannel] = useState("");
  const [model, setModel] = useState("");
  const [skill, setSkill] = useState(SKILL);
  const modelValue = model.trim();
  const channelValue = channel.trim();
  // The worker resolves an agent's binding against the Slack channel ID, not the
  // name, so the field captures the ID. This is a soft check: non-matching values
  // warn but still deploy (the CLI's synthetic channels are arbitrary strings).
  // "slack" is the only kind the console can create today (E4/EB-20).
  const channelLooksOff = channelValue !== "" && !SLACK_ADDRESS_RE.test(channelValue);
  // A blank channel can never match a Slack mention, so deploy requires one.
  // This is distinct from the format warning above, which never blocks: arbitrary
  // non-ID strings (the CLI's synthetic channels) still deploy — empty does not.
  const channelBlank = channelValue === "";

  const deploy = async () => {
    if (state.deploying) return;
    if (channelValue === "") return;
    dispatch({ type: "deployStart" });
    try {
      // Store the trimmed ID: a copied ID with surrounding spaces would silently
      // drop every mention because the worker matches the channel value exactly.
      // Only send model when set, so the agent falls back to the platform
      // default (CURIE_MODEL unset) rather than pinning an empty string.
      const agent = await createAgent({
        name,
        channel: { kind: "slack", address: channelValue },
        ...(modelValue !== "" ? { model: modelValue } : {}),
      });
      const version = await createVersion(agent.id, { version_label: "v0.1.0", created_by: "ui" });
      const archive = await buildBundleZip({ agentName: name, versionLabel: version.version_label, skillMd: skill });
      await uploadBundle(agent.id, version.id, archive);
      // Backend-driven success: record the real next step, refresh the real agent
      // list, and fire confetti without the fixture level/success-panel machinery.
      wired.markDeployed({ name, channel: channelValue });
      wired.refetch();
      dispatch({ type: "confettiFire" });
    } catch (e) {
      if (e instanceof BundleValidationError) {
        dispatch({ type: "deployFailedValidation", issues: e.issues });
      } else {
        dispatch({ type: "deployFailed", message: e instanceof Error ? e.message : String(e) });
      }
    }
  };

  return (
    <div
      style={{
        width: 820,
        maxWidth: "92vw",
        background: C.card,
        border: "1px solid " + C.borderStrong,
        borderRadius: 16,
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        maxHeight: "86vh",
      }}
    >
      <div style={{ padding: "18px 24px", borderBottom: "1px solid " + C.border, display: "flex", alignItems: "center" }}>
        <h2 style={{ fontSize: 18, fontWeight: 500, margin: 0 }}>New agent</h2>
        <button
          type="button"
          onClick={() => dispatch({ type: "closeModal" })}
          style={{ marginLeft: "auto", background: "none", border: "none", color: C.muted, fontSize: 18, cursor: "pointer" }}
        >
          ✕
        </button>
      </div>
      <div style={{ display: "flex", gap: 0, flex: 1, minHeight: 0 }}>
        <div style={{ width: 300, flexShrink: 0, padding: "20px 24px", borderRight: "1px solid " + C.border, overflow: "auto" }}>
          <label style={{ fontSize: 13, fontWeight: 500, display: "block", marginBottom: 6 }}>Name</label>
          <input
            data-testid="agent-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            onFocus={(e) => e.target.select()}
            style={{
              width: "100%",
              background: C.input,
              border: "1px solid " + C.borderStrong,
              borderRadius: 7,
              padding: "8px 10px",
              color: C.text,
              fontFamily: C.mono,
              fontSize: 13,
              marginBottom: 20,
            }}
          />
          <label style={{ fontSize: 13, fontWeight: 500, display: "block", marginBottom: 6 }}>Slack channel ID</label>
          <input
            data-testid="agent-channel"
            value={channel}
            onChange={(e) => setChannel(e.target.value)}
            placeholder="C0123ABCD"
            style={{
              width: "100%",
              background: C.input,
              border: "1px solid " + (channelLooksOff ? C.warn : C.borderStrong),
              borderRadius: 7,
              padding: "8px 10px",
              color: C.text,
              fontFamily: C.mono,
              fontSize: 13,
              marginBottom: 6,
            }}
          />
          {channelLooksOff ? (
            <div data-testid="channel-warn" style={{ fontSize: 11, color: C.warn, marginBottom: 6 }}>
              That does not look like a channel ID (C…). Mentions match on the ID, not the name — deploy anyway if you
              are using the CLI.
            </div>
          ) : null}
          <div style={{ fontSize: 11, color: C.muted, marginBottom: 20, lineHeight: 1.5 }}>
            In Slack: open the channel, click its name, and copy the ID at the bottom of Channel details (or copy the
            channel link and take the C… segment). Invite the bot there after deploy.
          </div>
          <label style={{ fontSize: 13, fontWeight: 500, display: "block", marginBottom: 6 }}>Model</label>
          <input
            data-testid="agent-model"
            value={model}
            onChange={(e) => setModel(e.target.value)}
            placeholder="platform default"
            style={{
              width: "100%",
              background: C.input,
              border: "1px solid " + C.borderStrong,
              borderRadius: 7,
              padding: "8px 10px",
              color: C.text,
              fontFamily: C.mono,
              fontSize: 13,
              marginBottom: 6,
            }}
          />
          <div style={{ fontSize: 11, color: C.muted, marginBottom: 20, lineHeight: 1.5 }}>
            Optional. Sets <span style={{ fontFamily: C.mono }}>CURIE_MODEL</span> for this agent at boot. Leave blank
            to use the platform default model.
          </div>
          <label style={{ fontSize: 13, fontWeight: 500, display: "block", marginBottom: 8 }}>Template</label>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            {TEMPLATES.map((t, i) => (
              <button
                key={i}
                type="button"
                style={{
                  textAlign: "left",
                  padding: "10px 11px",
                  borderRadius: 9,
                  cursor: "pointer",
                  background: t[2] ? "rgba(62,207,142,.08)" : C.input,
                  border: "1px solid " + (t[2] ? C.brand : C.border),
                  color: C.text,
                }}
              >
                <div style={{ fontSize: 12.5, fontWeight: 500, marginBottom: 2 }}>{t[0]}</div>
                <div style={{ fontSize: 11, color: C.muted }}>{t[1]}</div>
              </button>
            ))}
          </div>
        </div>
        <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SkillEditor
            path={`skills/${name || "agent"}/SKILL.md`}
            value={skill}
            onChange={(next) => {
              setSkill(next);
              dispatch({ type: "clearDeployErrors" });
            }}
          />
          {state.deployIssues && state.deployIssues.length > 0 ? (
            <div
              data-testid="deploy-errors"
              style={{
                borderTop: "1px solid rgba(229,77,46,.3)",
                background: "rgba(229,77,46,.06)",
                padding: "10px 16px",
                maxHeight: 140,
                overflow: "auto",
              }}
            >
              <div style={{ fontSize: 12, fontWeight: 600, color: C.destructive, marginBottom: 6 }}>
                Bundle validation failed
              </div>
              {state.deployIssues.map((issue, i) => (
                <div key={i} style={{ fontFamily: C.mono, fontSize: 11.5, color: C.text2, marginBottom: 3 }}>
                  <span style={{ color: C.destructive }}>{issue.code}</span>
                  {issue.location ? <span style={{ color: C.muted }}> · {issue.location}</span> : null}
                  <span style={{ color: C.text2 }}> — {issue.message}</span>
                </div>
              ))}
            </div>
          ) : null}
          {state.deployError ? (
            <div
              data-testid="deploy-error"
              style={{
                borderTop: "1px solid rgba(229,77,46,.3)",
                background: "rgba(229,77,46,.06)",
                padding: "10px 16px",
                fontSize: 12.5,
                color: C.destructive,
                fontFamily: C.mono,
              }}
            >
              Deploy failed: {state.deployError}
            </div>
          ) : null}
        </div>
      </div>
      <div style={{ padding: "14px 24px", borderTop: "1px solid " + C.border, display: "flex", alignItems: "center", gap: 12 }}>
        <span style={{ fontSize: 12, color: C.muted, fontFamily: C.mono }}>
          deploys to <span style={{ color: C.text2 }}>{channel.trim() || "your channel"}</span>
        </span>
        <CliHint command={cliCommand("init", { name: name.trim() || "agent" })} label="scaffold via CLI" />
        <div style={{ marginLeft: "auto", display: "flex", gap: 10 }}>
          <Button label="Cancel" variant="ghost" onClick={() => dispatch({ type: "closeModal" })} />
          {state.deploying ? (
            <div
              data-testid="deploy-skeleton"
              style={{
                width: 120,
                height: 36,
                borderRadius: 7,
                background: `linear-gradient(90deg,${C.hover} 0%,${C.sel} 40%,${C.hover} 80%)`,
                backgroundSize: "400px 100%",
                animation: "shimmer 1s linear infinite",
              }}
            />
          ) : (
            <Button
              label="Deploy"
              variant="primary"
              disabled={channelBlank}
              title={channelBlank ? "Enter the Slack channel ID first" : undefined}
              onClick={() => void deploy()}
            />
          )}
        </div>
      </div>
    </div>
  );
}
