import { useEffect, useId, useState } from "react";
import { C } from "../../tokens";
import { Button, CliHint, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import {
  ApiError,
  FactorySlackChannelError,
  SLACK_PROGRESS_CHANNEL_FORMAT_MESSAGE,
  SLACK_PROGRESS_CHANNEL_RE,
  setFactorySlackChannel,
  type ChannelBinding,
} from "../../api/client";

type ProgressError = {
  kind: "unbound" | "format" | "other";
  message: string;
  // The channel the failed write named, so the unbound hint adds that exact one.
  channel: string | null;
  // Which copyable hint the refusal gets. "add" only when the page's snapshot
  // of the agent's bindings shows none for the channel (the one case it can
  // explain). "list" when the snapshot shows one or is absent, since it cannot
  // explain the refusal and the bindings may have changed since the page loaded.
  // "none" when two or more show (remove the extra) or there is nothing to name.
  hint: "add" | "list" | "none";
};

// Both stacks are shown for every hint: the DEV/PROD toggle only picks which
// deployments the page lists, and the progress write goes to the same API either
// way, so the console cannot tell which stack a copied command should target.
const HELPER_STYLE = { fontSize: 10.5, color: C.muted, maxWidth: 360, textAlign: "right", lineHeight: 1.4 } as const;

// The Slack progress channel for one factory agent: where its runs post status.
// Setting it needs the agent to already hold a Slack binding for that channel, so
// the unbound refusal names the surfaces command that adds one. Lives in the
// agent-detail header column beside the model field and follows its pattern.
export function WiredAgentFactoryProgress({
  agentId,
  channel,
  channels,
  onSaved,
}: {
  agentId: string;
  channel: string | null;
  channels?: readonly ChannelBinding[];
  onSaved: () => void;
}) {
  const { dispatch } = useStore();
  const inputId = useId();
  const [value, setValue] = useState(channel ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ProgressError | null>(null);

  useEffect(() => {
    setValue(channel ?? "");
    setError(null);
  }, [agentId, channel]);

  const trimmed = value.trim();
  const hintChannel = SLACK_PROGRESS_CHANNEL_RE.test(trimmed) ? trimmed : undefined;

  const write = async (next: string | null) => {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      await setFactorySlackChannel(agentId, next);
      onSaved();
      dispatch({
        type: "toast",
        message: next === null ? "Progress channel cleared" : `Progress channel set to ${next}`,
      });
    } catch (e) {
      if (e instanceof FactorySlackChannelError && e.code === "unbound") {
        const slackBindings =
          next === null || !channels
            ? null
            : channels.filter((b) => b.kind === "slack" && b.address === next).length;
        let message: string;
        let hint: ProgressError["hint"];
        if (slackBindings !== null && slackBindings >= 2) {
          message = `This agent has ${slackBindings} Slack bindings for ${next}, one per Slack identity, and it needs exactly one. Remove the extra identity, then set it here.`;
          hint = "none";
        } else if (slackBindings === 0) {
          message = `This agent has no Slack binding for ${next}. Add the channel as a surface first, then set it here. If another agent already holds the default Slack identity on that channel, add it under a free declared identity instead: replace <identity> with that identity's name in the second command.`;
          hint = "add";
        } else {
          message = `This agent needs exactly one Slack binding for ${next ?? "that channel"}, and the bindings this page shows do not explain why it has not. List the agent's current bindings and make sure exactly one Slack binding for ${next ?? "that channel"} remains, then set it here.`;
          hint = next === null ? "none" : "list";
        }
        setError({ kind: "unbound", message, channel: next, hint });
      } else if (e instanceof FactorySlackChannelError && e.code === "invalid") {
        setError({ kind: "format", message: SLACK_PROGRESS_CHANNEL_FORMAT_MESSAGE, channel: next, hint: "none" });
      } else {
        const message = e instanceof ApiError || e instanceof Error ? e.message : String(e);
        setError({ kind: "other", message: `Could not update progress channel: ${message}`, channel: next, hint: "none" });
      }
    } finally {
      setBusy(false);
    }
  };

  const save = () => {
    if (busy) return;
    if (trimmed === "") {
      // A blank box is not a clear gesture here; Clear is its own button.
      setError({
        kind: "format",
        message: `Enter a Slack channel id, or use Clear to stop posting progress. ${SLACK_PROGRESS_CHANNEL_FORMAT_MESSAGE}`,
        channel: null,
        hint: "none",
      });
      return;
    }
    if (!SLACK_PROGRESS_CHANNEL_RE.test(trimmed)) {
      setError({ kind: "format", message: SLACK_PROGRESS_CHANNEL_FORMAT_MESSAGE, channel: trimmed, hint: "none" });
      return;
    }
    void write(trimmed);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4, marginTop: 6 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", flexWrap: "wrap", gap: 6 }}>
        <label htmlFor={inputId} style={{ fontSize: 11, color: C.muted, fontFamily: C.mono }}>
          progress channel
        </label>
        <input
          id={inputId}
          data-testid="factory-progress-input"
          value={value}
          onChange={(e) => {
            setValue(e.target.value);
            setError(null);
          }}
          placeholder="none"
          style={{
            background: C.input,
            border: "1px solid " + C.borderStrong,
            borderRadius: 7,
            padding: "5px 9px",
            color: C.text,
            fontFamily: C.mono,
            fontSize: 12.5,
            width: 150,
          }}
        />
        <Button
          label={busy ? "Saving…" : "Save"}
          variant="secondary"
          size="sm"
          testId="factory-progress-save"
          disabled={busy}
          onClick={save}
        />
        <Button
          label="Clear"
          variant="ghost"
          size="sm"
          testId="factory-progress-clear"
          disabled={busy || channel === null}
          onClick={() => void write(null)}
        />
        <CliHint
          label="cluster"
          command={cliCommand("cluster.factory-progress", { agent: agentId, "slack-channel": hintChannel })}
        />
        <CliHint
          label="local"
          command={cliCommand("local.factory-progress", { agent: agentId, "slack-channel": hintChannel })}
        />
      </div>
      <div data-testid="factory-progress-stack-note" style={HELPER_STYLE}>
        Run the one for the stack serving this Console
      </div>
      {error ? (
        <div
          data-testid="factory-progress-error"
          style={{ fontSize: 11, color: C.destructive, maxWidth: 360, textAlign: "right", lineHeight: 1.4 }}
        >
          {error.message}
          {error.kind === "unbound" && error.channel && error.hint === "add" ? (
            <div style={{ display: "flex", justifyContent: "flex-end", flexWrap: "wrap" }}>
              <CliHint
                label="cluster"
                command={cliCommand("cluster.surfaces", { agent: agentId, add: `slack=${error.channel}` })}
              />
              <CliHint
                label="local"
                command={cliCommand("local.surfaces", { agent: agentId, add: `slack=${error.channel}` })}
              />
              <CliHint
                label="cluster, named identity"
                command={cliCommand("cluster.surfaces", {
                  agent: agentId,
                  add: `slack=${error.channel}`,
                  adapter: "<identity>",
                })}
              />
              <CliHint
                label="local, named identity"
                command={cliCommand("local.surfaces", {
                  agent: agentId,
                  add: `slack=${error.channel}`,
                  adapter: "<identity>",
                })}
              />
            </div>
          ) : null}
          {error.kind === "unbound" && error.hint === "list" ? (
            <div style={{ display: "flex", justifyContent: "flex-end", flexWrap: "wrap" }}>
              <CliHint label="cluster" command={cliCommand("cluster.surfaces", { agent: agentId })} />
              <CliHint label="local" command={cliCommand("local.surfaces", { agent: agentId })} />
            </div>
          ) : null}
        </div>
      ) : null}
      <div data-testid="factory-progress-current" style={HELPER_STYLE}>
        {channel ? `Posting progress to ${channel}` : "No Slack progress channel set"}
      </div>
    </div>
  );
}
