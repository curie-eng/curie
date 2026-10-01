// Everything you can do to one agent, on that agent.
//
// Twenty-six of the CLI's commands are agent-scoped: thirteen verbs, each at the
// local and the cluster tier, each taking the agent as its first positional. In
// a flat list that is twenty-six strings you have to already know the names of.
// Here it is one sheet, opened from the agent's own row, with the agent already
// filled in and the tier chosen once at the top instead of twenty-six times in
// the middle of a command name.
//
// Which tier is a real question, not a preference: `local kill` and `cluster
// kill` stop different deployments of the same agent. So it is a segmented
// control at the top of the sheet, remembered across openings (it is a UI
// position, like the Build cursor, so it lives in the same localStorage), and
// every button below re-points at the chosen tier.

import { useState } from "react";
import { channelLabel } from "../lib/channels";

import { useApp, type AgentSummary } from "../bridge/app";
import { surfacesById } from "../lib/surfaces";
import { Actions } from "./Actions";
import { F, HUE, LINE, STATUS, T } from "../tokens";
import { Badge, Mono, Sheet } from "../primitives";

export type AgentTier = "local" | "cluster";

const TIER_KEY = "curie.desktop.agentTier";

function storedTier(): AgentTier {
  return localStorage.getItem(TIER_KEY) === "cluster" ? "cluster" : "local";
}

/** The four agent surfaces, in the order an operator works through them: say
 *  something, look at it, change it, stop it. */
const GROUPS = ["agent.talk", "agent.inspect", "agent.configure", "agent.control"] as const;

export function AgentSheet({ agent, onClose }: { agent: AgentSummary; onClose(): void }) {
  const [tier, setTier] = useState<AgentTier>(storedTier);

  const choose = (next: AgentTier) => {
    setTier(next);
    localStorage.setItem(TIER_KEY, next);
  };

  // Agent-scoped commands take the agent as their first positional, which is
  // why the sticky-flag mechanism cannot carry it and `Prefill` exists. The
  // CLI resolves an agent by name or id; the name is what the operator is
  // looking at, so that is what goes in.
  const prefill = { positionals: [agent.name] };

  return (
    <Sheet
      title={agent.name}
      onClose={onClose}
      width={660}
      footer={
        <span style={{ ...F.footnote, color: T.quaternary }}>
          Each of these runs <Mono style={{ fontSize: 10 }}>curie {tier} …</Mono> against{" "}
          <Mono style={{ fontSize: 10 }}>{agent.name}</Mono>.
        </span>
      }
    >
      <Identity agent={agent} />

      <div style={{ display: "flex", flexDirection: "column", gap: 14, marginTop: 14 }}>
        {GROUPS.map((id) => (
          <Actions
            key={id}
            surface={surfacesById.get(id)!}
            prefill={prefill}
            // Each of these surfaces declares both tiers' half; the chosen tier
            // decides which half is on screen. Declaring both is what lets the
            // coverage test see that `cluster budget` has a home even when the
            // operator is looking at the local one.
            only={(action) => action.id.startsWith(`${tier}.`)}
            right={
              id === "agent.talk" ? (
                <TierChoice tier={tier} onChoose={choose} />
              ) : undefined
            }
          />
        ))}
      </div>
    </Sheet>
  );
}

/** Which deployment these commands act on. A segmented control, not a dropdown:
 *  there are two of them and both should be visible. */
function TierChoice({ tier, onChoose }: { tier: AgentTier; onChoose(next: AgentTier): void }) {
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 7 }}>
      <span style={{ ...F.footnote, color: T.quaternary }}>acting on</span>
      <span
        style={{
          display: "inline-flex",
          borderRadius: 6,
          border: `1px solid ${LINE.separator}`,
          overflow: "hidden",
        }}
      >
        {(["local", "cluster"] as const).map((value) => (
          <button
            key={value}
            onClick={() => onChoose(value)}
            aria-pressed={tier === value}
            title={
              value === "local"
                ? "The Docker Compose stack on this machine"
                : "The Helm release on a Kubernetes cluster"
            }
            style={{
              border: "none",
              padding: "2px 9px",
              cursor: "default",
              ...F.caption,
              background: tier === value ? (value === "local" ? STATUS.info : HUE.violet) : "transparent",
              color: tier === value ? "var(--on-accent)" : T.tertiary,
            }}
          >
            {value}
          </button>
        ))}
      </span>
    </span>
  );
}

/** What this agent is, above the controls, so the sheet is not four toolbars
 *  floating over nothing. Facts only -- everything here came from the API. */
function Identity({ agent }: { agent: AgentSummary }) {
  const gates = agent.approval_required_tools?.length ?? 0;
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "auto 1fr",
        gap: "4px 14px",
        ...F.callout,
        paddingBottom: 12,
        borderBottom: `1px solid ${LINE.separator}`,
      }}
    >
      <span style={{ color: T.tertiary }}>id</span>
      <Mono style={{ fontSize: 11, color: T.secondary, wordBreak: "break-all" }}>{agent.id}</Mono>

      <span style={{ color: T.tertiary }}>model</span>
      <Mono style={{ fontSize: 11, color: T.secondary }}>{agent.model ?? "platform default"}</Mono>

      <span style={{ color: T.tertiary }}>surface</span>
      <span>
        {channelLabel(agent) ? (
          <Badge color={STATUS.warn} filled>
            {channelLabel(agent)}
          </Badge>
        ) : (
          <span style={{ color: T.quaternary }}>no channel bound</span>
        )}
      </span>

      <span style={{ color: T.tertiary }}>gates</span>
      <span style={{ color: gates ? STATUS.warn : T.quaternary }}>
        {gates ? `${gates} tool${gates === 1 ? "" : "s"} need approval` : "nothing gated"}
      </span>
    </div>
  );
}

/** Hook shape used by the views that open this sheet: keep the selected agent in
 *  state, render the sheet when there is one. Exported so Overview and Canvas
 *  cannot each invent their own. */
export function useAgentSheet() {
  const [agent, setAgent] = useState<AgentSummary | null>(null);
  const app = useApp();
  return {
    open: setAgent,
    // The sheet is dismissed when a command form takes over: two stacked sheets
    // is a modal on top of a modal, and the run sheet is the one being read.
    element: agent && !app.runTarget ? <AgentSheet agent={agent} onClose={() => setAgent(null)} /> : null,
  };
}
