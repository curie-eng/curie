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
} from "../../api/client";

type ProgressError = {
  kind: "unbound" | "format" | "other";
  message: string;
  // The channel the failed write named, so the unbound hint adds that exact one.
  channel: string | null;
};

// The Slack progress channel for one factory agent: where its runs post status.
// Setting it needs the agent to already hold a Slack binding for that channel, so
// the unbound refusal names the surfaces command that adds one. Lives in the
// agent-detail header column beside the model field and follows its pattern.
export function WiredAgentFactoryProgress({
  agentId,
  agentName,
  channel,
  onSaved,
}: {
  agentId: string;
  agentName: string;
  channel: string | null;
  onSaved: () => void;
}) {
  const { state, dispatch } = useStore();
  const inputId = useId();
  const [value, setValue] = useState(channel ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ProgressError | null>(null);

  useEffect(() => {
    setValue(channel ?? "");
    setError(null);
  }, [agentId, channel]);

  const cluster = state.env === "prod";
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
        setError({ kind: "unbound", message: e.message, channel: next });
      } else if (e instanceof FactorySlackChannelError && e.code === "invalid") {
        setError({ kind: "format", message: SLACK_PROGRESS_CHANNEL_FORMAT_MESSAGE, channel: next });
      } else {
        const message = e instanceof ApiError || e instanceof Error ? e.message : String(e);
        setError({ kind: "other", message: `Could not update progress channel: ${message}`, channel: next });
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
      });
      return;
    }
    if (!SLACK_PROGRESS_CHANNEL_RE.test(trimmed)) {
      setError({ kind: "format", message: SLACK_PROGRESS_CHANNEL_FORMAT_MESSAGE, channel: trimmed });
      return;
    }
    void write(trimmed);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4, marginTop: 6 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
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
          command={cliCommand(cluster ? "cluster.factory-progress" : "local.factory-progress", {
            agent: agentName,
            "slack-channel": hintChannel,
          })}
        />
      </div>
      {error ? (
        <div
          data-testid="factory-progress-error"
          style={{ fontSize: 11, color: C.destructive, maxWidth: 360, textAlign: "right", lineHeight: 1.4 }}
        >
          {error.message}
          {error.kind === "unbound" && error.channel ? (
            <>
              {" "}
              <CliHint
                command={cliCommand(cluster ? "cluster.surfaces" : "local.surfaces", {
                  agent: agentName,
                  add: `slack=${error.channel}`,
                })}
              />
            </>
          ) : null}
        </div>
      ) : null}
      <div
        data-testid="factory-progress-current"
        style={{ fontSize: 10.5, color: C.muted, maxWidth: 360, textAlign: "right", lineHeight: 1.4 }}
      >
        {channel ? `Posting progress to ${channel}` : "No Slack progress channel set"}
      </div>
    </div>
  );
}
