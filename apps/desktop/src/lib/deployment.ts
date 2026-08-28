// Is this bundle actually running anywhere?
//
// The app already knows both halves -- the bundle it has open, and what the
// platform reports it is running -- and nothing was comparing them. Build could
// say "Ready to deploy" forever, which is a claim about the FILES (they would
// load), while the Canvas and Resources views showed nothing running. That reads
// as two views disagreeing when in fact only one of them was answering the
// question.

/**
 * The running agent this bundle was deployed as, matched by name.
 *
 * Name is the only link there is: a deployed agent carries no reference back to
 * the directory it came from. That makes an absence weaker evidence than it
 * looks, which is why the UI phrases it as "nothing is answering as <name>"
 * rather than "not deployed" -- `deploy.yaml` can send one bundle out under a
 * different name per environment (`squawk-dev`, `squawk`), so a bundle really
 * can be running as something this cannot match. Saying the narrower, true
 * thing costs nothing and stops the panel contradicting a deployment that
 * exists.
 */
export function deployedAs<T extends { name: string }>(
  agents: readonly T[],
  bundleName: string,
): T | undefined {
  return agents.find((a) => a.name === bundleName);
}
