// The palette variables ThemePreview actually draws.
//
// Separate from the component so a test can import it without pulling React in,
// and because a component file that also exports constants breaks fast refresh.
// Keep it in step with the component: a variable listed here but not rendered
// makes the preview look more distinguishing than it is.

export const PREVIEW_VARS = [
  "--s-window",
  "--s-sidebar-fallback",
  "--s-content",
  "--card-fill",
  "--s-well",
  "--s-field",
  "--s-selected",
  "--s-control",
  "--s-subtle",
  "--accent",
  "--on-accent",
  "--t-primary",
  "--t-secondary",
  "--t-tertiary",
  "--t-quaternary",
  "--line-separator",
  "--line-border",
  "--status-ok",
  "--status-warn",
  "--status-danger",
  "--status-info",
  "--hue-blue",
  "--hue-purple",
  "--hue-orange",
  "--hue-cyan",
  "--hue-teal",
  "--hue-yellow",
  "--hue-red",
  "--hue-grey",
] as const;
