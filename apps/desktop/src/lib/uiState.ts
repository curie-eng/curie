// Where the operator left the furniture.
//
// Panel sizes and collapsed/expanded state are a UI POSITION, not platform
// state: they belong to this window on this machine, they mean nothing to the
// API, and losing one costs a drag rather than any work. That is the same
// reasoning that already puts the Build cursor, the agent-sheet tier and the
// Settings tab in `localStorage`, and this is those calls in one place so the
// next one does not invent a fourth spelling of the same try/catch.
//
// Every read is guarded. A disabled or full `localStorage` must cost somebody a
// remembered panel width, never the screen it is on.

const PREFIX = "curie.ui.";

export function readNumber(key: string, fallback: number, min: number, max: number): number {
  try {
    const raw = localStorage.getItem(PREFIX + key);
    const n = raw === null ? NaN : Number(raw);
    // Clamped on the way IN as well as out. A stored value can outlive the
    // layout that produced it -- a smaller window, a panel that changed its
    // bounds -- and a height nobody can drag back is worse than a default.
    return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
  } catch {
    return fallback;
  }
}

export function readBool(key: string, fallback: boolean): boolean {
  try {
    const raw = localStorage.getItem(PREFIX + key);
    return raw === null ? fallback : raw === "true";
  } catch {
    return fallback;
  }
}

export function write(key: string, value: number | boolean | string): void {
  try {
    localStorage.setItem(PREFIX + key, String(value));
  } catch {
    // Nothing to do and nothing worth saying: the panel still works, it just
    // will not be where it was next time.
  }
}

/** Put a position back to "nobody has said", so the default applies again and a
 *  relaunch does not resurrect the number that was just discarded. */
export function forget(key: string): void {
  try {
    localStorage.removeItem(PREFIX + key);
  } catch {
    // As above.
  }
}
