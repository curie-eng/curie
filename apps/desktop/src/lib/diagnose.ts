// Turning a failure into the thing to do about it.
//
// One failure mode is worth catching by hand, because it is the one this app is
// most likely to cause and the least likely to be understood: the CLI and the
// platform disagreeing about the shape of a payload. It surfaces as a serde
// error naming a field -- "missing field `channels`" -- from a command whose
// summary line says only "failed", and nothing on screen connects it to the
// version mismatch this app has been quietly reporting in the corner all along.
//
// This app is built against ONE version of the CLI and drives whichever is
// installed, and the local stack's images come from the registry unless somebody
// asked for a source build. So a checkout whose CLI has moved ahead talks to a
// published API that has not, and the first sign of it is a field name.

export interface Diagnosis {
  readonly title: string;
  readonly detail: string;
  /** A command that fixes it, for the console's own prompt. Not run for
   *  anybody: it changes what is running, so it is offered and not performed. */
  readonly fix?: string;
}

/** A serde failure, in any of the shapes the CLI's decoder produces. Matched on
 *  the message rather than the exit code, because the exit code for this is 1 --
 *  the same as every other failure. */
const CONTRACT =
  /(missing field|unknown field|unknown variant|invalid type:|error decoding response body)/i;

/**
 * What went wrong, when the app can say something better than the output does.
 *
 * Returns nothing for the ordinary case. A hint under every failure would be
 * noise, and a wrong hint is worse than none -- an operator who follows it has
 * spent their time on the app's guess instead of the error in front of them.
 */
export function diagnose(
  output: string,
  env: { repoRoot?: string | null; sourceCheckout?: boolean } | null | undefined,
): Diagnosis | undefined {
  if (!CONTRACT.test(output)) return undefined;

  const field = /(?:missing|unknown) field `([^`]+)`/i.exec(output)?.[1];
  const named = field ? ` It is disagreeing about \`${field}\`.` : "";

  // With a checkout, the likely skew is a source-built CLI against registry
  // images, because that is what happens by default -- `local up` pulls
  // published images unless asked not to. Without one, both came from a release
  // and the CLI is the half that can be updated.
  return env?.repoRoot
    ? {
        title: "Curie and the platform are different versions",
        detail:
          `The command worked; reading the answer back did not.${named} Your Curie is built from ` +
          `this checkout, and the platform it just talked to is running published images, so the ` +
          `two have drifted. Rebuilding the platform from the same checkout makes them agree.`,
        fix: "local up --build",
      }
    : {
        title: "Curie and the platform are different versions",
        detail:
          `The command worked; reading the answer back did not.${named} The installed Curie and ` +
          `the platform it just talked to were built at different times. Updating Curie is usually ` +
          `the shorter way round.`,
        fix: "update",
      };
}
