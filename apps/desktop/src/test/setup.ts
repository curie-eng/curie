import "@testing-library/jest-dom/vitest";

// jsdom has no clipboard and no rAF-driven layout; stub the two things the UI
// touches so a component test does not fail on the environment rather than on
// the component.
Object.defineProperty(navigator, "clipboard", {
  value: { writeText: async () => {} },
  configurable: true,
});

if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = () => {};
}

// jsdom in this setup exposes no localStorage (Node's own is behind a flag), and
// the app treats that as normal: `sticky` and the behavior-pack cursor both wrap
// access in try/catch so a missing store degrades to "no remembered value". But
// then nothing could TEST that remembering works, so give the suite a real one.
if (typeof globalThis.localStorage === "undefined") {
  const store = new Map<string, string>();
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, String(v)),
      removeItem: (k: string) => void store.delete(k),
      clear: () => store.clear(),
      key: (i: number) => [...store.keys()][i] ?? null,
      get length() {
        return store.size;
      },
    },
  });
}
