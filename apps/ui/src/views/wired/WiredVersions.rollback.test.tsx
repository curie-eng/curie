import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Fragment, createElement, useEffect, type PropsWithChildren } from "react";
import { WiredVersions } from "./WiredVersions";
import { StoreProvider, useStore } from "../../state/store";
import type { AgentOut, DeploymentOut, VersionOut } from "../../api/client";
import { createDeployment, getAgents, listDeployments, listVersions } from "../../api/client";

// -----------------------------------------------------------------------------
// Data-layer seam (see file-tail note): we mock the API *client* and let the
// real useAgents / useAgentVersions hooks run against the stubbed fetchers, so
// the component + hook wiring is exercised end to end. `createDeployment` is a
// spy we assert on directly; `reload()` is observed indirectly — the only thing
// that re-fetches versions is the hook's reload(), so a second listVersions call
// after a confirmed rollback proves reload() fired.
//
// vi.importActual keeps the real ApiError class (used with `instanceof` inside
// hooks.ts) and every other export intact; we override only the four calls the
// versions surface touches.
// -----------------------------------------------------------------------------
vi.mock("../../api/client", async (importActual) => {
  const actual = await importActual<typeof import("../../api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    listVersions: vi.fn(),
    listDeployments: vi.fn(),
    createDeployment: vi.fn(),
  };
});

const mkVersion = (id: string, version_label: string, created_at: string): VersionOut => ({
  id,
  agent_id: "a1",
  version_label,
  bundle_ref: "r",
  bundle_sha256: "s",
  created_by: "ui",
  created_at,
});

const mkDep = (
  id: string,
  version_id: string,
  environment: "prod" | "dev",
  deployed_at: string,
  status = "active",
): DeploymentOut => ({
  id,
  agent_id: "a1",
  version_id,
  environment,
  commit_sha: null,
  status,
  deployed_at,
});

// [FIXTURE-ONLY] ADR-0116: AgentOut carries `channels` (plural); unused by any
// assertion in this file.
const AGENT: AgentOut = {
  id: "a1",
  name: "deal-desk",
  channels: [{ kind: "slack", address: "#revenue-ops" }],
  model: null,
  created_at: "2026-06-01T00:00:00Z",
};

// v2 is the version the agent currently serves; v1 was previously deployed and is
// no longer active. Both deployments live in the "dev" environment so the
// env-scoped versions table (which filters rows to the selected env) shows both
// rows once the store is switched to dev. Append-only: v1's record is left as
// "active" — pickActiveVersion ranks by newest here (same env), so v2 wins and
// v1's row is a previously-deployed, non-active row.
const V_OLD = mkVersion("v1", "v0.1.0", "2026-07-01T00:00:00Z");
const V_ACTIVE = mkVersion("v2", "v0.1.1", "2026-07-05T00:00:00Z");
const DEP_OLD = mkDep("dep-old", "v1", "dev", "2026-07-02T00:00:00Z", "active");
const DEP_ACTIVE = mkDep("dep-active", "v2", "dev", "2026-07-06T00:00:00Z", "active");

const VERSIONS = [V_OLD, V_ACTIVE];
const DEPLOYMENTS = [DEP_OLD, DEP_ACTIVE];

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getAgents).mockResolvedValue([AGENT]);
  vi.mocked(listVersions).mockResolvedValue(VERSIONS);
  vi.mocked(listDeployments).mockResolvedValue(DEPLOYMENTS);
  vi.mocked(createDeployment).mockResolvedValue(
    mkDep("dep-new", "v1", "dev", "2026-07-08T00:00:00Z", "active"),
  );
});

// Find a rendered version-history row by the version label it displays.
async function rowFor(label: string): Promise<HTMLElement> {
  const rows = await screen.findAllByTestId("version-row");
  const row = rows.find((r) => within(r).queryByText(label));
  if (!row) throw new Error(`no version-row rendering label ${label}`);
  return row;
}

const ROLLBACK = /roll ?back/i;
const CANCEL = /cancel/i;

// The versions table is env-scoped: it reads the store's selected environment and
// only shows deployment rows in that env. Flip the store to "dev" (the env both
// fixture deployments live in) on mount so the previously-deployed and active rows
// both render.
function EnvDev({ children }: PropsWithChildren) {
  const { dispatch } = useStore();
  useEffect(() => {
    dispatch({ type: "setEnv", env: "dev" });
  }, [dispatch]);
  return createElement(Fragment, null, children);
}

// A fresh client per render with retry off, mirroring main.tsx, so the wired
// hooks (now react-query) resolve on the first response instead of retrying. The
// StoreProvider supplies the env the table scopes to (WiredVersions uses useStore).
function wrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: PropsWithChildren) =>
    createElement(
      QueryClientProvider,
      { client },
      createElement(StoreProvider, null, createElement(EnvDev, null, children)),
    );
}

describe("WiredVersions rollback", () => {
  it("offers Roll back on a previously-deployed non-active row, not on the active row", async () => {
    render(<WiredVersions />, { wrapper: wrapper() });

    const oldRow = await rowFor("v0.1.0");
    const activeRow = await rowFor("v0.1.1");

    // The active row carries the "active" chip and must NOT offer rollback.
    expect(within(activeRow).getByTestId("version-status")).toHaveTextContent("active");
    expect(within(activeRow).queryByRole("button", { name: ROLLBACK })).toBeNull();

    // The previously-deployed, non-active row offers a Roll back action.
    expect(within(oldRow).getByRole("button", { name: ROLLBACK })).toBeInTheDocument();
  });

  it("opens a dialog naming the target version; Cancel closes it without deploying", async () => {
    const user = userEvent.setup();
    render(<WiredVersions />, { wrapper: wrapper() });

    const oldRow = await rowFor("v0.1.0");
    await user.click(within(oldRow).getByRole("button", { name: ROLLBACK }));

    const dialog = await screen.findByRole("dialog");
    // The confirmation names the version being rolled back to.
    expect(dialog).toHaveTextContent("v0.1.0");

    await user.click(within(dialog).getByRole("button", { name: CANCEL }));

    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(createDeployment).not.toHaveBeenCalled();
  });

  it("confirming deploys the old version as active and refreshes the list", async () => {
    const user = userEvent.setup();
    render(<WiredVersions />, { wrapper: wrapper() });

    // one fetch pass on mount (versions + deployments)
    await waitFor(() => expect(listVersions).toHaveBeenCalledTimes(1));

    const oldRow = await rowFor("v0.1.0");
    await user.click(within(oldRow).getByRole("button", { name: ROLLBACK }));

    const dialog = await screen.findByRole("dialog");
    // Confirm is the dialog's affirmative action (Cancel is separate).
    const confirm = within(dialog).getByRole("button", { name: /roll ?back|confirm/i });
    await user.click(confirm);

    // Rollback = a NEW deployment of the old version, in that row's environment,
    // marked active. Exactly once, exact arg shape.
    await waitFor(() => expect(createDeployment).toHaveBeenCalledTimes(1));
    expect(vi.mocked(createDeployment)).toHaveBeenCalledWith({
      agent_id: "a1",
      version_id: "v1",
      environment: "dev",
      status: "active",
    });

    // reload() is the only thing that re-fetches versions -> a second pass proves
    // the list was refreshed after the rollback.
    await waitFor(() => expect(listVersions).toHaveBeenCalledTimes(2));
  });
});
