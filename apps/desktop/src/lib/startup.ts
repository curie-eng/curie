// Whether the local stack is coming up, and how far along it is.
//
// The app cannot ask the CLI this. `curie local up` runs `docker compose up -d
// --wait`, and with no TTY the CLI's checklist writes nothing until the whole
// step resolves -- so between "Start the stack" and forty seconds later there
// is not one line of output to render. A progress bar driven by that stream
// would have to invent its own steps, which is the fixture rule this app is
// built against: a bar that moves because time passed is lying.
//
// Docker is the honest source, and it is the SAME source the CLI is waiting on.
// `compose up --wait` blocks until every service with a healthcheck reports
// healthy, so "ready" here is defined as exactly that, and the count is measured
// from the same daemon rather than modelled.
//
// The one subtlety is a container with no healthcheck at all. Docker appends no
// verdict for those, and treating a missing verdict as "starting" would leave a
// stack that is genuinely up sitting at "8 of 10" forever. Running with no
// opinion is ready.

import type { ResourceSample } from "../bridge/bridge";

export interface StackProgress {
  /** Containers compose has created. Zero before it has created any -- which is
   *  a real phase (image pulls) and not a failure. */
  readonly total: number;
  /** Running, and either healthy or declaring no healthcheck. */
  readonly ready: number;
  /** Service names not ready yet, in order, for the step line. */
  readonly waiting: readonly string[];
  /** Service names that failed a healthcheck or are no longer running. These
   *  are not "still starting" and saying so is the difference between a stack
   *  that is slow and one that is broken. */
  readonly failed: readonly string[];
}

/**
 * Where one container is in the start.
 *
 * `done` is the case that is easy to get wrong and looks obviously wrong on
 * screen: a compose stack is not all long-lived services. `curie-migrate`,
 * `rustfs-init` and the two `*-perms` containers run once and exit 0, and that
 * IS them succeeding. Reading "stopped" as "broken" reported four failures on a
 * stack that was perfectly healthy, which is exactly the kind of lie the
 * no-fixtures rule exists to prevent -- it just happens to be pessimistic
 * rather than optimistic.
 */
function classify(s: ResourceSample): "ready" | "waiting" | "failed" {
  switch (s.state) {
    case "running":
      if (s.health === "unhealthy") return "failed";
      return s.health === "starting" ? "waiting" : "ready";
    case "exited":
      // A one-shot that finished its job. Anything else stopped on its own.
      return s.exitCode === 0 ? "ready" : "failed";
    case "created":
    case "restarting":
      return "waiting";
    default:
      // `paused`, `dead`, `removing`, or something Docker has not told us
      // about. None of them are going to become ready on their own.
      return "failed";
  }
}

/**
 * Progress across the compose services in the sample frame.
 *
 * Scoped by `service` rather than by a hardcoded project name, matching how the
 * rest of the app counts the stack (`Tiers`, `LadderStrip`). A container started
 * outside compose -- a `curie skill up` runner -- has no service and is not part
 * of this.
 */
export function stackProgress(samples: readonly ResourceSample[]): StackProgress {
  const services = samples.filter((s) => !!s.service);
  const waiting: string[] = [];
  const failed: string[] = [];
  let ready = 0;

  for (const s of services) {
    const name = s.service!;
    switch (classify(s)) {
      case "ready":
        ready++;
        break;
      case "failed":
        failed.push(name);
        break;
      default:
        waiting.push(name);
    }
  }

  return { total: services.length, ready, waiting, failed };
}

/**
 * What the Overview should say about the stack.
 *
 * `starting` covers the case where nothing has been created yet but a run is in
 * flight, because that phase -- pulling images -- is the longest one and the
 * screen going blank through it is what makes the whole thing feel broken.
 *
 * `settling` is every container ready but the API not yet answering. It exists
 * so the card does not flip back to a red error for the second or two between
 * the last healthcheck passing and the next API poll: nothing is wrong then,
 * and saying so would be the screen contradicting itself.
 */
/**
 * `up` is a state the card SHOWS, not one it disappears on.
 *
 * The card used to vanish the moment the stack came up, which meant the only
 * thing that ever told an operator it had worked was the absence of a warning.
 * A screen that reports success by removing something is asking you to have
 * been watching. `up` keeps the card and swaps the spinner for a live marker,
 * so the answer to "is it up" is on screen whether or not you saw it happen.
 *
 * `idle` is now only "there is no local stack here" -- nothing created -- or
 * "something is wrong and the notice below says what". Those are the two cases
 * where a status card would have nothing true to say.
 */
export type StackPhase = "absent" | "idle" | "starting" | "settling" | "up";

/**
 * How long "everything is up, the API just has not answered yet" stays a
 * reasonable thing to say.
 *
 * It has to be bounded or the card is a lie in the other direction: every
 * container healthy and no API means something IS wrong -- a misconfigured base
 * URL, a service that started and then wedged -- and a spinner that never
 * resolves hides exactly that, forever, behind a message saying it is fine.
 * Past the grace period the error comes back, with the command that fixes it.
 */
export const SETTLE_GRACE_MS = 25_000;

export function stackPhase(
  progress: StackProgress,
  {
    apiReachable,
    runActive,
    settlingForMs = 0,
  }: { apiReachable: boolean; runActive: boolean; settlingForMs?: number },
): StackPhase {
  // A start in flight is a start in flight, whatever else is true. This used to
  // check `apiReachable` first and return `idle` on it, which meant the card
  // vanished for every start against a stack that was already answering --
  // `local rebuild`, or `up` run twice -- and, worse, made the card's existence
  // depend on the same fact the errors beside it are about. The run ending is
  // what ends the run.
  if (runActive) return "starting";
  // No containers at all is not a stack that is down, it is a stack nobody has
  // asked for yet -- a first run. That is the one moment the whole app has a
  // single obvious next action, so it gets its own phase rather than being
  // folded into "nothing to report".
  if (progress.total === 0) return "absent";
  if (progress.ready < progress.total) return "starting";
  if (apiReachable) return "up";
  // Every container is up and only the API has yet to answer. Normal for a few
  // seconds; past that it is a real problem, and the error notice -- which
  // carries the fix -- takes over.
  return settlingForMs > SETTLE_GRACE_MS ? "idle" : "settling";
}
