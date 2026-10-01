import { useEffect, useRef, useState } from "react";
import { C } from "../../tokens";
import { Button, Card, CliHint, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import { resetThread, getThreadResetState, ApiError } from "../../api/client";

// How long we wait for the worker to actually release the sandbox before we
// report the release as still pending, and how often we re-poll. Mirrors the
// CLI reset-thread verb's wait loop (RESET_RELEASE_TIMEOUT / _POLL_INTERVAL):
// comfortably covers the worker's default 30s reclaim tick.
const RELEASE_TIMEOUT_MS = 45_000;
const POLL_INTERVAL_MS = 1_000;

type Phase =
  | "idle"
  | "confirm"
  | "requesting"
  | "waiting"
  | "released"
  | "no-route"
  | "pending";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// The wired "reset a thread" operator control (#871). Thread reset exists as a
// backend operation + CLI verb (#737) but had no console affordance; this closes
// the drift. An operator enters the thread key (e.g. the Slack thread ts) and
// forces its sandbox to be released, so the thread's next message cold-creates a
// fresh sandbox instead of adopting one running stale env. It interrupts any
// live turn on the thread, so — mirroring the kill switch and the CLI verb's
// --yes gate — it takes a confirm step before firing.
//
// The POST only queues the release (the worker drains it on its next
// maintenance tick, up to ~30s later), so after the request we poll the reset
// state to completion, exactly as the CLI does, and report "released" vs. "still
// pending" honestly. When the worker drained the reset but the key matched no
// route (#3699), nothing was released: that is its own outcome, never "released".
// Does not delete conversation history.
export function WiredThreadReset({ agentId, agentName }: { agentId: string; agentName: string }) {
  const { state, dispatch } = useStore();
  const [threadKey, setThreadKey] = useState("");
  const [phase, setPhase] = useState<Phase>("idle");
  const [error, setError] = useState<string | null>(null);
  const [resetKey, setResetKey] = useState<string | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const key = threadKey.trim();
  const blank = key === "";
  const busy = phase === "requesting" || phase === "waiting";

  const run = async () => {
    if (blank || busy) return;
    setPhase("requesting");
    setError(null);
    setResetKey(key);
    try {
      await resetThread(agentId, key);
    } catch (e) {
      if (!mounted.current) return;
      setError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
      setPhase("idle");
      return;
    }
    if (!mounted.current) return;
    setPhase("waiting");
    // Poll the reset state to completion so the operator does not move on until
    // the sandbox has actually been released, closing the window where the next
    // message adopts the pre-reset sandbox (#735). A poll failure never fails an
    // already-accepted reset: it degrades to "release unconfirmed, still
    // pending", same as the CLI.
    const deadline = Date.now() + RELEASE_TIMEOUT_MS;
    let released = false;
    let routeExisted: boolean | null | undefined;
    for (;;) {
      try {
        const st = await getThreadResetState(agentId, key);
        if (!st.requested) {
          released = true;
          routeExisted = st.route_existed;
          break;
        }
      } catch {
        break;
      }
      if (Date.now() >= deadline) break;
      await sleep(POLL_INTERVAL_MS);
      if (!mounted.current) return;
    }
    if (!mounted.current) return;
    const noRoute = released && routeExisted === false;
    setPhase(noRoute ? "no-route" : released ? "released" : "pending");
    dispatch({
      type: "toast",
      message: noRoute
        ? `no route matched thread ${key}; nothing was released`
        : released
          ? `Thread ${key} reset: sandbox released`
          : `Thread ${key} reset queued; release still pending`,
    });
  };

  // Reset the control back to the entry state for a fresh reset.
  const clear = () => {
    setPhase("idle");
    setError(null);
    setResetKey(null);
  };

  return (
    <Card>
      <div data-testid="agent-thread-reset" style={{ padding: "4px 4px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
          <div style={{ fontWeight: 600, fontSize: 14, color: C.text2 }}>Reset a thread</div>
          <CliHint
            command={cliCommand(
              state.env === "prod" ? "cluster.reset-thread" : "local.reset-thread",
              { agent: agentName, "thread-key": key || undefined, yes: true },
            )}
          />
        </div>
        <div style={{ color: C.muted, fontSize: 12.5, marginBottom: 12, maxWidth: 620, lineHeight: 1.5 }}>
          Forces the thread's sandbox to be released, so its next message cold-creates a fresh
          sandbox instead of adopting one running stale env (a rotated credential, an unpicked-up
          redeploy, a wedge). Interrupts any live turn on the thread. Does not delete conversation
          history — a fresh sandbox still rehydrates from the durable transcript.
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <input
            data-testid="thread-reset-key"
            value={threadKey}
            onChange={(e) => {
              setThreadKey(e.target.value);
              if (phase !== "requesting" && phase !== "waiting") clear();
            }}
            disabled={busy || phase === "confirm"}
            placeholder="thread key, e.g. 1699912345.001200"
            aria-label="thread key"
            style={{
              background: C.input,
              border: "1px solid " + C.borderStrong,
              borderRadius: 7,
              padding: "6px 10px",
              color: C.text,
              fontFamily: C.mono,
              fontSize: 12.5,
              width: 280,
            }}
          />
          {phase === "confirm" ? (
            <>
              <span style={{ fontSize: 12.5, color: C.text2, fontFamily: C.mono }}>
                Reset this thread? It interrupts any live turn.
              </span>
              <Button label="Cancel" variant="ghost" size="sm" testId="thread-reset-cancel" onClick={clear} />
              <Button
                label="Confirm reset"
                variant="danger"
                size="sm"
                testId="thread-reset-confirm"
                onClick={() => void run()}
              />
            </>
          ) : (
            <Button
              label={
                phase === "requesting"
                  ? "Requesting…"
                  : phase === "waiting"
                    ? "Waiting for release…"
                    : "Reset thread"
              }
              variant="danger"
              size="sm"
              testId="thread-reset-start"
              disabled={blank || busy}
              title={blank ? "Enter the thread key first" : undefined}
              onClick={() => {
                setError(null);
                setPhase("confirm");
              }}
            />
          )}
        </div>

        {error ? (
          <div
            data-testid="thread-reset-error"
            style={{ color: C.destructive, fontSize: 12.5, marginTop: 10, fontFamily: C.mono }}
          >
            Reset failed: {error}
          </div>
        ) : phase === "released" ? (
          <div
            data-testid="thread-reset-released"
            style={{ color: C.brand, fontSize: 12.5, marginTop: 10, fontFamily: C.mono }}
          >
            ✓ Thread {resetKey} reset — sandbox released.
          </div>
        ) : phase === "no-route" ? (
          <div
            data-testid="thread-reset-no-route"
            style={{ color: C.warn, fontSize: 12.5, marginTop: 10, fontFamily: C.mono }}
          >
            ✗ no route matched thread {resetKey}; nothing was released. Check the key — a named
            bot's key carries its identity as its own segment (kind:identity:channel:conversation).
          </div>
        ) : phase === "pending" ? (
          <div
            data-testid="thread-reset-pending"
            style={{ color: C.warn, fontSize: 12.5, marginTop: 10, fontFamily: C.mono }}
          >
            Reset queued for {resetKey}; release still pending — the worker will drain it on its next
            maintenance tick.
          </div>
        ) : phase === "waiting" ? (
          <div
            data-testid="thread-reset-waiting"
            style={{ color: C.muted, fontSize: 12.5, marginTop: 10, fontFamily: C.mono }}
          >
            Reset requested for {resetKey}; waiting for the sandbox to be released…
          </div>
        ) : null}
      </div>
    </Card>
  );
}
