# 119. A resumed thread rebuilds its prefix so the prompt cache still hits

Date: 2026-08-22

Status: Draft

Depends on [ADR-0116](0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md),
whose decision 5 creates the problem this one answers. It changes **how**
[ADR-0003](0003-stateless-first-rehydrate-on-resume.md)'s rehydrate is delivered
and therefore revises [ADR-0029](0029-conversation-history-port-and-first-loader.md)'s
design; it does not touch ADR-0003's decision that history lives outside the
sandbox, which stands.

## Context

ADR-0116 makes releasing a sandbox cheap and therefore makes releasing it early
correct. Its decision 5 does that, and its own consequences record the bill:
**a cheap re-bind fixes the latency of a resume and not its token cost.** A
pre-bound pod is a fresh runner with no transcript, so a returning thread
rehydrates, and ADR-0003's finding is that a rehydrate is cache-cold. Measured on
a *scaffolded* bundle, before any conversation history is added: **20,875 input
tokens per turn.**

So decision 5 as it stands is a good trade for one-shot traffic, which the chart
says is most of it, and a bad one for a thread somebody keeps talking to. The
worse the conversation is worth having, the more it costs.

### The cache does not belong to the process

Anthropic's prompt cache is **server-side and keyed by the request prefix**, not
by the session or the process that sends it. A different runner sending the same
prefix hits the same cache entry. Nothing about releasing a pod destroys a cache
entry; only the TTL does.

Which means caching across a resume is not blocked by releasing the sandbox. It
is blocked by **what the resumed request looks like.**

### What the resumed request looks like today

Two properties of
[`runner/src/curie_runner/history.py`](../../runner/src/curie_runner/history.py),
both deliberate for their own reasons, both fatal to a cache hit:

- The transcript is delivered as a **boot-prompt preamble**. The module's own
  docstring names it: "the same durable state store, the same boot-preamble
  delivery, a different scope" as `memory.py`. So a resumed thread's prompt is
  *system instructions plus a rendered transcript*, where the original
  conversation's prompt was *system instructions plus message history*. Different
  shape, different bytes, different prefix.
- It is **truncated to roughly 16 KB**, "to bound the rendered history to a few KB
  of the boot prompt". So even the surviving content is not what the original
  turns contained.

Either one alone is enough to miss. ADR-0003 measured the upside that is being
left on the table in the other direction: inside a single claim,
`cache_read_input_tokens = 16045`, exactly what the first call created.

## Decision

**A resumed thread reconstructs the prefix its earlier turns produced, so the
prompt cache is reachable from a different runner. Residency and caching stop
being the same question.**

1. **History is replayed as message history, not rendered into the system
   prompt.** A resumed session's request is the same shape as the session it
   resumes: the bundle's system instructions, then the prior turns as turns, then
   the new one. Byte-stability of everything before the new turn is the property
   this decision is about; a change that reorders or re-renders that region is a
   defect, not a refactor.

2. **The prefix carries explicit cache breakpoints, and they sit where the prefix
   is stable.** The bundle's instructions and tool definitions are the same for
   every thread of a version and belong in the first cached span. Turn history is
   the second. A breakpoint placed after content that varies per request caches
   nothing and pays the write premium for it.

3. **The prompt is bounded by summarising old turns, not by truncating the
   rendered preamble.** The 16 KB ceiling exists because an unbounded transcript
   in a boot prompt is unbounded cost, and removing the preamble does not remove
   that concern. A summary is itself a stable prefix once written, so the bound
   and the cache are compatible; a *truncation* is not, because it changes where
   the prefix ends every time the transcript grows.

4. **Cache behaviour is an observable, not an assumption.** The runner already
   reports usage per turn; `cache_read_input_tokens` on the first turn after a
   resume is the number that says whether this decision works, and it is recorded
   per turn rather than inferred from cost.

5. **Residency is then set by slots alone.** With a resume able to hit cache,
   ADR-0116 decision 5's residency no longer has a token term to balance against,
   and its ceiling-derived-from-cache-TTL reasoning collapses into the simpler
   rule: hold a sandbox only while a turn is in flight.

**The caching invariant** (what we test and review to): the first turn after a
resume, inside the cache TTL, reads its prefix from cache rather than paying for
it, and does so on a runner that is not the one that wrote it.

## What is not known yet

Stated plainly because this ADR's central mechanism is unverified, and because
ADR-0116 was already corrected once for reasoning past its measurements.

- **No cache hit across a resume has ever been observed here.** Every turn figure
  in ADR-0116 came from a fake model or a local Ollama, neither of which has a
  prompt cache. The 20,875 input tokens is real; the saving this ADR proposes is
  arithmetic on it.
- **Whether the harness can express this is unverified.** `claude-agent-sdk` has
  to accept prior turns as messages and let a caller place cache breakpoints. If
  it does not, decisions 1 and 2 are a request to the SDK rather than a change to
  Curie.
- **The effective cache TTL Curie's requests carry is unmeasured.** The API offers
  a 5-minute default and a 1-hour option, and which one applies decides how much
  of real traffic lands inside the window at all.
- **The 16 KB truncation may be load-bearing for behaviour, not just cost.** A
  bundle written against a truncated history may depend on that truncation. This
  is exactly what [ADR-0081](0081-nightly-graded-parity-ladder.md)'s parity ladder
  is for, and it is the most likely way this decision breaks something quietly.

## Alternatives considered

- **Hold the sandbox for the cache TTL instead.** This is the fallback and it
  works: a follow-up inside the window finds the process and the cache warm, and
  past the window holding buys nothing because the cache is gone anyway. It is
  rejected as the *primary* answer because it pays a quota slot for every idle
  conversation to buy something a correctly-shaped request gets for free, and
  ADR-0116 measured what paying quota slots for idleness costs: 10 of 14
  conversations blocked and two dropped. Worth keeping as the degraded mode when
  a prefix cannot be reconstructed.

- **Keep the preamble and accept cache-cold resumes.** Rejected: it makes
  ADR-0116 decision 5 a token-for-slots trade whose price rises with how valuable
  the conversation is. One-shot traffic would be fine and long threads would be
  penalised, which is the wrong way round.

- **Raise the cache TTL to an hour and hold nothing.** Considered and not
  sufficient on its own. A longer TTL widens the window but does nothing about the
  prefix mismatch, so every resume still misses; it is complementary to this
  decision, not an alternative.

- **Rely on the golden-checkpoint proposal** (ADR-0118, in review alongside this
  one). Rejected as unrelated. A restored runner there is generic and unbound; it
  carries no conversation, so it has nothing to do with whether a *conversation's*
  prefix survives. The two decisions touch the same word, "restore", and different
  problems.

## Consequences

- **Prefix stability becomes a contract with a visible cost at deploy time.** Any
  change to a bundle's instructions or tool definitions changes the first cached
  span, so every live thread of the previous version misses once. That is correct
  behaviour and it is also a real bill on a repository that deploys often, which
  makes it a thing to state in the deploy path rather than discover from an
  invoice.

- **The bound on prompt size moves from a truncation to a summary, and a summary
  is a behaviour change.** Decision 3 replaces "drop everything past 16 KB" with
  "compact the old turns", which is a different input to the agent even when it is
  a better one. The parity ladder is where that shows up.

- **A per-turn `cache_read_input_tokens` is a new thing to keep honest.** Decision
  4 makes the mechanism observable, which also means a regression becomes visible
  as a number rather than as a slow increase in spend. That is the point, and it
  needs somewhere to be looked at.

- **ADR-0116 decision 5 gets simpler if this lands and stays as written if it does
  not.** That is deliberate: ADR-0116 does not depend on this ADR, and this ADR
  removes a caveat from it rather than a requirement.

## Out of scope

- **Memory compaction.** `memory.py` shares the boot-preamble delivery this ADR
  moves away from for history, and
  [ADR-0111](0111-the-default-memory-compaction-algorithm.md) already owns the
  compaction question. Whether memory should follow history onto the message path
  is that record's to answer, not this one's.

- **Choosing the cache TTL.** Which TTL Curie's requests should carry is a cost
  decision with a different shape, and it needs the measurement this ADR is
  waiting on before it can be made on evidence rather than preference.

- **Anything about the runner's own restore.** The golden-checkpoint proposal
  covers making a *fresh* runner cheap to produce; this ADR covers making a
  *resumed conversation* cheap to continue. They are independent.
