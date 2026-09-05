// The controls a surface turns into, and the sheet one opens.
//
// Every contextual command control in the app comes from here, rendered from
// `src/lib/surfaces.ts`. Nothing hand-writes a button bound to a command id,
// which is what keeps the placement map honest: the map is not a description of
// the UI, it *is* the UI, and `surfaces.test.ts` checks it against the manifest.
//
// Pressing one opens the generated form over the screen you are on rather than
// navigating to the Commands list. That distinction is the whole point of the
// exercise. A button whose behaviour is "go to the list and find it yourself"
// has not placed the command anywhere; it has just added a signpost pointing at
// the problem.

import { useMemo, type CSSProperties, type ReactNode } from "react";

import { useApp, type Prefill } from "../bridge/app";
import { commandsById } from "../lib/manifest";
import { commandTitle, resolve, type Action, type Need, type Surface } from "../lib/surfaces";
import { CommandForm } from "./CommandForm";
import { F, LINE, S, STATUS, T } from "../tokens";
import { Button, Group, Mono, SectionHeader, Sheet, type ButtonTone } from "../primitives";

/** What each precondition means, and the surface that fixes it. Kept here rather
 *  than in `surfaces.ts` so the data file stays free of copy. */
const NEED_COPY: Record<Need, { unmet: string; why: string }> = {
  docker: {
    unmet: "Docker is not reachable",
    why: "Everything in this group runs in containers on this machine.",
  },
  kubectl: {
    unmet: "kubectl is not on PATH",
    why: "This group talks to a Kubernetes cluster.",
  },
  api: {
    unmet: "No platform API is reachable",
    why: "These commands act on agents, which live in the platform API.",
  },
  checkout: {
    unmet: "This is not a source checkout",
    why: "These are contributor scripts, and a released binary refuses them.",
  },
  bundle: {
    unmet: "No bundle is open",
    why: "These run against the bundle directory, so there has to be one.",
  },
};

/** Whether this machine currently satisfies a surface's precondition.
 *  `null` means "not known yet" -- the environment probe has not answered -- and
 *  is deliberately not treated as failure, because greying out every control for
 *  the first second of the app's life is worse than letting one command report
 *  its own problem. */
function useMet(need: Need | undefined): boolean | null {
  const app = useApp();
  const env = app.env;
  if (!need) return true;
  if (!env) return null;
  switch (need) {
    case "docker":
      return env.dockerAvailable;
    case "kubectl":
      return env.kubectlAvailable;
    case "api":
      return !!app.api?.reachable;
    case "checkout":
      return env.sourceCheckout;
    case "bundle":
      return !!app.workspace;
  }
}

/**
 * The one line that says a group's precondition is unmet.
 *
 * One line, not a warning block. Two groups on the same screen can share a
 * precondition, and a full-width banner repeated under each header shouts twice
 * about one fact -- and shouts louder than the controls it is describing. The
 * commands are not hidden either: each one says what is wrong when it is
 * opened, which is where the operator can do something about it.
 */
export function NeedNotice({
  need,
  style,
}: {
  readonly need: Need | undefined;
  readonly style?: CSSProperties;
}) {
  const met = useMet(need);
  if (!need || met !== false) return null;
  return (
    <div
      style={{ ...F.footnote, color: STATUS.warn, display: "flex", gap: 6, ...style }}
      title={NEED_COPY[need].why}
    >
      <span aria-hidden>⚠</span>
      <span>
        {NEED_COPY[need].unmet}. {NEED_COPY[need].why}
      </span>
    </div>
  );
}

/**
 * One resolved action as a control.
 *
 * The single place an `Action` becomes a button, so the tone, the tooltip and
 * the run call are decided once. Layouts that are not a wrapping row -- the
 * tiers matrix, which puts one action per grid cell -- render these directly
 * rather than reimplementing the binding.
 */
export function ActionButton({
  action,
  cmd,
  prefill,
  size = "sm",
  style,
  tone,
  icon,
}: {
  readonly action: Action;
  readonly cmd: { id: string; about: string };
  readonly prefill?: Prefill;
  readonly size?: "sm" | "md";
  readonly style?: CSSProperties;
  readonly icon?: ReactNode;
  /** Overrides the tone the action asks for. The tiers matrix uses it: `quiet`
   *  means "unfilled" in a wrapping row of buttons, where the filled ones around
   *  it still say the row is controls -- but in a grid an unfilled cell beside a
   *  label column reads as a *value*, and "Release health" stopped looking like
   *  something you could press. */
  readonly tone?: ButtonTone;
}) {
  const app = useApp();
  return (
    <Button
      size={size}
      tone={tone ?? action.tone ?? (action.quiet ? "plain" : "default")}
      // The command itself in the tooltip, because a button that hides which
      // command it runs is the failure this app is built against.
      title={`curie ${action.id.replace(/\./g, " ")} — ${action.hint ?? cmd.about}`}
      onClick={() => app.runCommand(action.id, prefill)}
      style={style}
      icon={icon}
    >
      {action.label}
    </Button>
  );
}

export interface ActionsProps {
  readonly surface: Surface;
  /** Seed values handed to whichever command is opened from this group. */
  readonly prefill?: Prefill;
  /** Narrow the group to a subset -- the agent sheet shows one tier's half of a
   *  group that declares both. */
  readonly only?: (action: Action) => boolean;
  /** Rendered under the buttons: the thing this particular screen knows and the
   *  map does not. */
  readonly children?: ReactNode;
  readonly right?: ReactNode;
  /** Replaces the surface's own blurb. Only for a group rendered with `only`:
   *  the surface's sentence describes the whole set, so filtering the controls
   *  without filtering the sentence leaves a group promising a button it is no
   *  longer showing. */
  readonly blurb?: ReactNode;
}

/**
 * One surface, as a titled group of controls.
 *
 * `SectionHeader` outside a `Group`, per the app's grouping rule -- a header
 * inside the box is what makes a native list read as a card with a title bar.
 */
export function Actions({ surface, prefill, only, children, right, blurb }: ActionsProps) {
  const items = useMemo(
    () => resolve(surface).filter(({ action }) => (only ? only(action) : true)),
    [surface, only],
  );

  if (!items.length) return null;

  return (
    <section>
      <SectionHeader right={right}>{surface.title}</SectionHeader>
      <Group style={{ padding: 12 }}>
        <div style={{ ...F.callout, color: T.tertiary, marginBottom: 10, lineHeight: 1.5 }}>
          {blurb ?? surface.blurb}
        </div>

        <NeedNotice need={surface.needs} style={{ marginBottom: 10 }} />

        <ActionButtons actions={items} prefill={prefill} />
        {children}
      </Group>
    </section>
  );
}

/** Just the buttons, for the places that already have their own container: the
 *  Build loop, the resource inspector, the agent sheet. */
export function ActionButtons({
  actions,
  prefill,
  size = "sm",
}: {
  readonly actions: readonly { action: Action; cmd: { id: string; about: string } }[];
  readonly prefill?: Prefill;
  readonly size?: "sm" | "md";
}) {
  return (
    <div style={{ display: "flex", gap: 7, flexWrap: "wrap", alignItems: "center" }}>
      {actions.map(({ action, cmd }) => (
        <ActionButton key={action.id} action={action} cmd={cmd} prefill={prefill} size={size} />
      ))}
    </div>
  );
}

/** A single command as a control, for the one-offs a surface group would be
 *  heavy for (an empty state's call to action, a notice's fix button). Still
 *  goes through the same sheet, so there is one idiom. */
export function RunButton({
  id,
  children,
  tone,
  size = "sm",
  prefill,
}: {
  readonly id: string;
  readonly children: ReactNode;
  readonly tone?: "primary" | "danger" | "plain";
  readonly size?: "sm" | "md";
  readonly prefill?: Prefill;
}) {
  const app = useApp();
  const cmd = commandsById.get(id);
  if (!cmd) return null;
  return (
    <Button
      size={size}
      tone={tone}
      title={`curie ${cmd.path.join(" ")} — ${cmd.about}`}
      onClick={() => app.runCommand(id, prefill)}
    >
      {children}
    </Button>
  );
}

/**
 * The sheet itself, mounted once in the frame.
 *
 * It hosts the same `CommandForm` the Commands view uses -- not a second,
 * simpler form. A contextual control that offered fewer options than the list
 * would make the list the better surface again, one command at a time.
 */
export function RunSheetHost() {
  const app = useApp();
  const target = app.runTarget;
  const cmd = target ? commandsById.get(target.id) : undefined;
  if (!target || !cmd) return null;

  return (
    <Sheet
      // Named for what you pressed, not for the command it will run. See
      // `commandTitle`.
      title={commandTitle(cmd.id, cmd.path)}
      onClose={app.closeRun}
      width={640}
      footer={
        <Button
          tone="plain"
          title="Open this command in the full reference, beside its siblings and its flags"
          onClick={() => app.navigate("commands", cmd.id, target.prefill ?? undefined)}
        >
          Open in the reference
        </Button>
      }
    >
      <div
        style={{
          ...F.callout,
          color: T.tertiary,
          lineHeight: 1.55,
          paddingBottom: 12,
          marginBottom: 12,
          borderBottom: `1px solid ${LINE.separator}`,
        }}
      >
        {cmd.about}
      </div>
      {/* Keyed on the command so opening a different one from the same sheet
          gets a clean form rather than the previous command's values. */}
      <CommandForm
        key={cmd.id}
        cmd={cmd}
        compact
        prefill={target.prefill ?? undefined}
        // The transcript drawer takes over the moment something runs, so the
        // sheet standing over it would be a panel covering its own output.
        onRan={app.closeRun}
      />
    </Sheet>
  );
}

/** A quiet strip naming the command a panel is a front end for. Used where a
 *  screen does the job natively (the secrets panel) and the honest thing is to
 *  say which command it is driving. */
export function DrivenBy({ ids }: { readonly ids: readonly string[] }) {
  const app = useApp();
  const cmds = ids.map((id) => commandsById.get(id)).filter((c) => !!c);
  if (!cmds.length) return null;
  return (
    <div
      style={{
        ...F.footnote,
        color: T.quaternary,
        marginTop: 12,
        paddingTop: 10,
        borderTop: `1px solid ${LINE.separator}`,
        display: "flex",
        alignItems: "center",
        gap: 8,
        flexWrap: "wrap",
      }}
    >
      <span>Drives</span>
      {cmds.map((c) => (
        <button
          key={c.id}
          onClick={() => app.runCommand(c.id)}
          title={c.about}
          style={{
            border: `1px solid ${LINE.separator}`,
            background: S.well,
            borderRadius: 5,
            padding: "1px 6px",
            cursor: "default",
            color: "inherit",
            font: "inherit",
          }}
        >
          <Mono style={{ fontSize: 10 }}>curie {c.path.join(" ")}</Mono>
        </button>
      ))}
    </div>
  );
}
