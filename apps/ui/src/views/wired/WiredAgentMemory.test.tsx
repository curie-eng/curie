import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoreProvider } from "../../state/store";
import { WiredAgentMemory } from "./WiredAgentMemory";
import {
  listMemory,
  editMemory,
  deleteMemory,
  type MemoryEntry,
} from "../../api/client";

// Mock only the memory data-layer calls; keep everything else real.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    listMemory: vi.fn(),
    editMemory: vi.fn(),
    deleteMemory: vi.fn(),
  };
});

const ENTRIES: MemoryEntry[] = [
  {
    index: 0,
    version: 7,
    content: "deploy is a git push",
    provenance: {
      learned_from_session_id: "sess-1",
      source_trace_ids: ["trace-a", "trace-b"],
      recorded_at: "2026-07-13T00:00:00+00:00",
    },
  },
  {
    index: 1,
    version: 7,
    content: "prod reuses the dev bundle",
    provenance: { learned_from_session_id: null, source_trace_ids: [], recorded_at: "" },
  },
];

function renderPanel() {
  return render(
    <StoreProvider>
      <WiredAgentMemory agentId="a1" />
    </StoreProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("WiredAgentMemory (#267)", () => {
  it("renders entries whose optional provenance fields are omitted", async () => {
    // MemoryProvenanceOut has no required properties in apps/api/openapi.json.
    vi.mocked(listMemory).mockResolvedValue([{
      index: 0,
      version: 7,
      content: "example lesson",
      provenance: {},
    }] as MemoryEntry[]);
    renderPanel();

    expect(await screen.findByText("example lesson")).toBeInTheDocument();
    expect(screen.getByText(/no session.*no traces/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Edit" })).toBeEnabled();
  });

  it("lists learned entries with their provenance", async () => {
    vi.mocked(listMemory).mockResolvedValue(ENTRIES);
    renderPanel();

    expect(await screen.findByText("deploy is a git push")).toBeInTheDocument();
    // Provenance is shown — the differentiator (how it learned that).
    expect(screen.getByText(/session sess-1/)).toBeInTheDocument();
    expect(screen.getByText(/trace-a, trace-b/)).toBeInTheDocument();
    // An entry with no provenance degrades gracefully.
    expect(screen.getByText(/no session/)).toBeInTheDocument();
  });

  it("shows the empty state when nothing has been learned", async () => {
    vi.mocked(listMemory).mockResolvedValue([]);
    renderPanel();
    expect(
      await screen.findByText(/has not learned anything yet/i),
    ).toBeInTheDocument();
  });

  it("edits an entry and reloads", async () => {
    vi.mocked(listMemory)
      .mockResolvedValueOnce(ENTRIES)
      .mockResolvedValueOnce([{ ...ENTRIES[0], content: "corrected" }, ENTRIES[1]]);
    vi.mocked(editMemory).mockResolvedValue({ ...ENTRIES[0], content: "corrected" });
    renderPanel();

    await screen.findByText("deploy is a git push");
    await userEvent.click(screen.getAllByRole("button", { name: "Edit" })[0]);
    const box = screen.getByLabelText("memory-content");
    await userEvent.clear(box);
    await userEvent.type(box, "corrected");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() =>
      expect(editMemory).toHaveBeenCalledWith("a1", 0, "corrected", 7),
    );
    expect(await screen.findByText("corrected")).toBeInTheDocument();
  });

  it("deletes an entry", async () => {
    vi.mocked(listMemory)
      .mockResolvedValueOnce(ENTRIES)
      .mockResolvedValueOnce([ENTRIES[1]]);
    vi.mocked(deleteMemory).mockResolvedValue();
    renderPanel();

    await screen.findByText("deploy is a git push");
    await userEvent.click(screen.getAllByRole("button", { name: "Delete" })[0]);

    await waitFor(() => expect(deleteMemory).toHaveBeenCalledWith("a1", 0, 7));
  });

  it("surfaces a load error", async () => {
    vi.mocked(listMemory).mockRejectedValue(new Error("boom"));
    renderPanel();
    expect(await screen.findByTestId("memory-error")).toHaveTextContent("boom");
  });
});
