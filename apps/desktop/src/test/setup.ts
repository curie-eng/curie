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
