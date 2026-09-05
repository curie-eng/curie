// What a new agent starts as.
//
// "New agent" used to run the scaffolder and hand back an empty bundle, which
// answers "how do I make one" with a blank page. Every builder worth using --
// Retell's agent dashboard is the obvious comparison -- opens on a set of
// starting points, because the useful question is not "what is a bundle" but
// "which of these is closest to what I want".
//
// A template is the platform's own scaffold PLUS an overlay. `curie init` still
// writes the base, so the shape of a bundle has exactly one definition and this
// file cannot drift from it; a template only replaces the files that make it a
// particular agent rather than a generic one.
//
// Copy rule for anything in here: it is read by somebody deciding what to
// build, not by somebody operating a platform. No container, no tier, no
// bundle, no command. Say what the agent DOES.

export interface Template {
  readonly id: string;
  /** What it is called in the gallery. A thing, not a verb. */
  readonly name: string;
  /** One line under the name. What it does, in the user's words. */
  readonly tagline: string;
  /** A paragraph for the detail pane, once one is picked. */
  readonly about: string;
  /** A sample exchange. Far more use than a description: it is the fastest way
   *  to know whether this is the one you want. */
  readonly example: readonly { readonly from: "you" | "agent"; readonly text: string }[];
  /**
   * Files written over the scaffold, keyed by path relative to the bundle root.
   * `agent` is the name the operator typed, so a template can refer to it.
   */
  files(agent: string): Readonly<Record<string, string>>;
}

const STACK_SKILL = (agent: string) => `---
name: ${agent}
description: Push a non-empty message onto one shared list, and hand back the newest entry when the message is empty. Invoke on EVERY turn; the message text alone decides which.
allowed-tools:
  - mcp__curie-state__get
  - mcp__curie-state__set
  - mcp__curie-state__append
---

# ${agent}

One shared list, kept for as long as this agent exists. The message decides the
operation and nothing else does.

## The rule

Trim the incoming message first. Then:

- **Something was said — remember it.** Add the trimmed text to the list.
  Reply with exactly \`Got it.\`
- **Nothing was said — hand back the newest thing.** Remove the most recent
  entry and reply with exactly \`Last thing: <text>\`
- **Nothing was said and the list is empty.**
  Reply with exactly \`Nothing saved.\`

Reply with that line and nothing else. No preamble, no explanation, no
follow-up question. Those three lines are the entire output surface.

## Where the list lives

Namespace \`${agent}\`, key \`items\`. The value is a JSON array, oldest first,
so the newest entry is the LAST element.

Never touch the \`memory\` or \`transcript\` namespaces. They are this agent's
learned lessons and its own conversation history.

## Remembering

Call \`mcp__curie-state__append\` with namespace \`${agent}\`, key \`items\`, and
\`item\` set to the trimmed message. Append is atomic, so two people saving at
the same moment both land — do not read the list first and write it back, which
would lose one of them. Then reply \`Got it.\`

## Handing one back

This is read-modify-write, so it needs the version guard or two people asking at
once both get the same entry.

1. \`mcp__curie-state__get\` with namespace \`${agent}\`, key \`items\`. Keep the
   \`version\` it returns.
2. If the value is missing, not an array, or empty, reply \`Nothing saved.\` and
   stop. Write nothing.
3. Take the LAST element.
4. \`mcp__curie-state__set\` with the array minus that element, passing
   \`expected_version\` from step 1.
5. If that is rejected as a conflict, somebody changed the list between your read
   and your write. Start again from step 1, up to five times. If it still
   conflicts, say \`Busy, try again.\` — never report an entry you did not
   successfully remove.
6. Reply \`Last thing: \` followed by the entry you removed.

## What counts as nothing

Whitespace only is nothing. A message that is only a mention of this agent is
also nothing: the mention is stripped before the turn reaches you, so by then
there is nothing left.
`;

const STACK_CASES = (agent: string) =>
  JSON.stringify(
    {
      name: agent,
      cases: [
        {
          id: "saving-acknowledges-and-says-nothing-else",
          input: "the deploy is blocked on the migration",
          grader: { kind: "regex", expected: "^\\s*Got it\\.\\s*$", case_sensitive: true },
          note: "Anchored at both ends on purpose. The failure this catches is not a wrong word, it is a correct answer followed by 'Anything else I can help with?' -- the extra sentence is what makes a precise agent read as a chatbot.",
        },
        {
          id: "handing-back-answers-in-one-of-the-two-valid-shapes",
          input: "",
          grader: {
            kind: "regex",
            expected: "^\\s*(Last thing:\\s+\\S|Nothing saved\\.)",
            case_sensitive: true,
          },
          note: "An empty message hands one back, and there are exactly two right answers: the entry, or that there is none. Both are accepted because the list is shared and durable, so what is on it depends on what was said earlier -- demanding an entry would fail a brand new agent, which is not a bug. What it rejects is a bare acknowledgement, which is well formed for saving and meaningless here.",
        },
        {
          id: "whitespace-counts-as-nothing",
          input: "   ",
          grader: {
            kind: "regex",
            expected: "^\\s*(Last thing:\\s+\\S|Nothing saved\\.)",
            case_sensitive: true,
          },
          note: "Trimming decides which branch runs, so three spaces must not be saved as an entry. Same two accepted shapes as above; what is asserted here is narrower, that whitespace did not get stored. The failure mode is testing `if message:` instead of `if message.strip():` and quietly collecting blank rows.",
        },
      ],
    },
    null,
    2,
  ) + "\n";


const OWN_CODE_PY = `"""Your agent's own code.

Anything you write here that is wrapped in @mcp.tool becomes something the agent
can call. Keep each one small and predictable: the point of putting work here
rather than in the agent's instructions is that a program gives the same answer
every time, and a model does not.

Run it yourself while you are working on it:

    python tools/tools.py

It talks to the agent over its input and output, so it never listens on a port
and nothing outside this computer can reach it.
"""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("tools")


@mcp.tool()
def word_count(text: str) -> int:
    """Count the words in a piece of text."""
    return len(text.split())


@mcp.tool()
def reading_time_minutes(text: str) -> float:
    """Roughly how many minutes it takes to read a piece of text."""
    return round(len(text.split()) / 200, 1)


if __name__ == "__main__":
    mcp.run()
`;

const OWN_CODE_MCP = `{
  "mcpServers": {
    "tools": {
      "command": "python",
      "args": ["tools/tools.py"]
    }
  }
}
`;

const OWN_CODE_SKILL = (agent: string) => `---
name: ${agent}
description: Answer questions about a piece of text by measuring it, using this agent's own code rather than estimating.
allowed-tools:
  - tools
---

# ${agent}

## When to use this

Somebody asks how long a piece of text is, how many words it has, or how long it
takes to read.

## How to answer

Call the tool, do not estimate. That is the whole reason the code is here: a
program gives the same answer every time and a guess does not.

- \`word_count\` for a number of words.
- \`reading_time_minutes\` for how long it takes to read.

Say the number plainly, and say what it refers to.

## Making it yours

The two above are examples. Open \`tools/tools.py\`, replace them with whatever
your agent actually needs, and describe those here instead — this file is what
tells the agent when to reach for them.
`;

export const TEMPLATES: readonly Template[] = [
  {
    id: "stack",
    name: "Shared list",
    tagline: "Tell it something to save it. Ask with an empty message to get the newest one back.",
    about:
      "A running list that everyone talking to this agent shares, and that survives restarts. Useful as a hand-off note between people or shifts, and as the smallest agent that does something real — it keeps state, handles two people writing at once, and needs no keys or outside services.",
    example: [
      { from: "you", text: "the deploy is blocked on the migration" },
      { from: "agent", text: "Got it." },
      { from: "you", text: "(nothing)" },
      { from: "agent", text: "Last thing: the deploy is blocked on the migration" },
      { from: "you", text: "(nothing)" },
      { from: "agent", text: "Nothing saved." },
    ],
    files: (agent) => ({
      [`skills/${agent}/SKILL.md`]: STACK_SKILL(agent),
      "evals/cases.json": STACK_CASES(agent),
    }),
  },
  {
    id: "own-code",
    name: "Your own code",
    tagline:
      "You write a small program; the agent calls it. For anything a set of instructions cannot do on its own.",
    about:
      "Some work is arithmetic, or a lookup, or a call to a system you already run — things a model should not be improvising. This starts you with a small program the agent can call like any other tool, plus the wiring already done. Written in Python, kept inside the agent's own folder, and running on your computer rather than anywhere else.",
    example: [
      { from: "you", text: "how long is this document?" },
      { from: "agent", text: "1,284 words — about 6 minutes to read." },
    ],
    files: (agent) => ({
      "tools/tools.py": OWN_CODE_PY,
      ".mcp.json": OWN_CODE_MCP,
      [`skills/${agent}/SKILL.md`]: OWN_CODE_SKILL(agent),
    }),
  },
  {
    id: "blank",
    name: "Start from scratch",
    tagline: "An empty agent with the pieces in place, ready for you to describe what it should do.",
    about:
      "Everything a working agent needs and nothing that decides what it is for: one empty instruction file to write, a place for the examples that check it, and the wiring already done. Pick this when none of the others is close.",
    example: [],
    files: () => ({}),
  },
];

export function templateById(id: string): Template | undefined {
  return TEMPLATES.find((t) => t.id === id);
}
