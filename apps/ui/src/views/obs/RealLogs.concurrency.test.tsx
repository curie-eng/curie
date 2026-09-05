import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { RealLogs } from "./RealLogs";
import { StoreProvider } from "../../state/store";
import {
  ApiError,
  getRunnerLogs,
  listRunnerPods,
  type PodLogs,
  type RunnerPods,
} from "../../api/client";

// Mock only the data-layer calls; keep the real ApiError so RealLogs' error
// branching (instanceof ApiError) is preserved.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return { ...actual, getRunnerLogs: vi.fn(), listRunnerPods: vi.fn() };
});

const PODS = Array.from({ length: 12 }, (_, i) => `pod-${String(i + 1).padStart(2, "0")}`);

// One in-flight getRunnerLogs call, parked until the test releases its round.
type Parked = { pod: string; resolve: (v: PodLogs) => void; reject: (e: unknown) => void };

let parked: Parked[] = [];
let inFlight = 0;
let maxInFlight = 0;

function okLogs(pod: string): PodLogs {
  return { namespace: "curie", pod, container: null, logs: `body of ${pod}` };
}

// Settle every call parked right now, then drain microtasks with a real timer
// hop so the worker pool can issue its next batch and React can re-render. The
// count of release rounds a run needs IS its simulated round-trip count, with
// no wall-clock or fake-timer dependence.
async function releaseRound(settle: (p: Parked) => void, order: "asc" | "desc" = "asc") {
  const round = order === "desc" ? [...parked].reverse() : parked;
  parked = [];
  await act(async () => {
    for (const p of round) settle(p);
    await new Promise((r) => setTimeout(r, 0));
  });
  return round.length;
}

// Release rounds until `done` holds (or nothing is left parked), returning the
// number of rounds it took.
async function drainRounds(
  settle: (p: Parked) => void,
  done: () => boolean,
  order: "asc" | "desc" = "asc",
) {
  let rounds = 0;
  while (!done() && parked.length > 0 && rounds < 20) {
    rounds++;
    await releaseRound(settle, order);
  }
  return rounds;
}

async function renderAndFetch(pods: string[] = PODS) {
  const user = userEvent.setup();
  vi.mocked(listRunnerPods).mockResolvedValue({ namespace: "curie", pods } as RunnerPods);
  render(
    <StoreProvider>
      <RealLogs />
    </StoreProvider>,
  );
  // The pod list has landed once every pod plus the aggregate sentinel is an
  // option, which is also when the Fetch button leaves its disabled state.
  const select = (await screen.findByTestId("logs-pod-select")) as HTMLSelectElement;
  await waitFor(() => expect(select.options).toHaveLength(pods.length + 1));
  await user.click(screen.getByRole("button", { name: "Fetch logs" }));
}

function output() {
  return screen.queryByTestId("logs-output")?.textContent ?? "";
}

beforeEach(() => {
  vi.clearAllMocks();
  parked = [];
  inFlight = 0;
  maxInFlight = 0;
  vi.mocked(getRunnerLogs).mockImplementation((_ns: string, pod: string) => {
    inFlight += 1;
    maxInFlight = Math.max(maxInFlight, inFlight);
    return new Promise<PodLogs>((resolve, reject) => {
      parked.push({ pod, resolve, reject });
    }).finally(() => {
      inFlight -= 1;
    });
  });
});

describe("RealLogs - aggregate pod fetch concurrency", () => {
  it("twelve pods finish in fewer than three round trips", async () => {
    await renderAndFetch();

    // Each release round is one simulated round trip. The serial implementation
    // this replaced needed 12 rounds for these 12 pods, so this assertion fails
    // loudly if the fetch ever goes back to one-at-a-time.
    const rounds = await drainRounds(
      (p) => p.resolve(okLogs(p.pod)),
      () => screen.queryByTestId("logs-output") !== null,
    );

    expect(output()).toContain("=== pod-12 ===");
    expect(rounds).toBeLessThan(3);
    // Both halves of the contract: fast enough, and still capped.
    expect(maxInFlight).toBe(6);
  });

  it("block order matches the listed pod order", async () => {
    await renderAndFetch();

    // Settle each round back-to-front so completion order (06..01, 12..07) is
    // scrambled relative to list order (01..12).
    await drainRounds(
      (p) => p.resolve(okLogs(p.pod)),
      () => screen.queryByTestId("logs-output") !== null,
      "desc",
    );

    const text = output();
    const positions = PODS.map((pod) => text.indexOf(`=== ${pod} ===`));
    expect(positions.every((i) => i >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
    expect(text).toContain(`=== pod-01 ===\nbody of pod-01`);
  });

  it("one rejected pod yields the inline could-not-fetch fallback and the others still render", async () => {
    await renderAndFetch();

    await drainRounds(
      (p) =>
        p.pod === "pod-03"
          ? p.reject(new ApiError(404, "pod not found"))
          : p.resolve(okLogs(p.pod)),
      () => screen.queryByTestId("logs-output") !== null,
    );

    const text = output();
    expect(text).toContain("=== pod-03 ===\n(could not fetch: 404: pod not found)");
    expect(text).toContain("=== pod-02 ===\nbody of pod-02");
    expect(text).toContain("=== pod-04 ===\nbody of pod-04");
    expect(text).toContain("=== pod-12 ===\nbody of pod-12");
  });

  it("a 503 from any pod yields the no-cluster result", async () => {
    await renderAndFetch();

    await drainRounds(
      (p) =>
        p.pod === "pod-09"
          ? p.reject(new ApiError(503, "no cluster configured"))
          : p.resolve(okLogs(p.pod)),
      () => screen.queryByTestId("logs-state") !== null,
    );

    const banner = await screen.findByTestId("logs-state");
    expect(banner).toHaveTextContent("No cluster configured");
    expect(banner).toHaveTextContent("no cluster configured");
    expect(screen.queryByTestId("logs-output")).toBeNull();
  });
});
