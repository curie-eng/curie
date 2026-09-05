// Icon paths, kept out of `index.tsx` on purpose.
//
// A module that exports anything other than components opts itself out of React
// Fast Refresh, and `primitives/index.tsx` is imported by every screen in the
// app -- so one constant there stops edits to ANY primitive reaching an open
// window, silently. That happened: `PROMPT` lived next to `Glyph` for two
// commits and the dev loop quietly stopped working, with the window rendering
// stale components while the source plainly said otherwise.
//
// `eslint-plugin-react-refresh` has a rule for exactly this and it is now on,
// so the next constant added to a component module is a lint error rather than
// an afternoon.

/** A shell prompt: chevron and cursor rule. Shared by the sidebar's Commands
 *  row and the toolbar's console button, so one path cannot drift from the
 *  other. */
export const PROMPT = "m3 4.6 3 3-3 3M8.4 11.4H13";
