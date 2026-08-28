import js from "@eslint/js";
import globals from "globals";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";

export default [
  // `release` holds the packaged application, which contains Electron's own
  // bundled JavaScript plus a copy of `dist`. Linting a build output is
  // meaningless and it drowns the real findings.
  {
    ignores: [
      "dist",
      "dist-electron",
      "release",
      "src/generated",
      "node_modules",
      "coverage",
    ],
  },
  js.configs.recommended,
  {
    files: ["src/**/*.{ts,tsx}"],
    languageOptions: {
      parser: tsparser,
      parserOptions: { ecmaFeatures: { jsx: true }, sourceType: "module" },
      globals: { ...globals.browser },
    },
    plugins: {
      "@typescript-eslint": tseslint,
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      ...tseslint.configs.recommended.rules,
      ...reactHooks.configs.recommended.rules,
      // `tsc --noEmit` already reports unused values with better messages; the
      // eslint copy would just double every diagnostic.
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": ["warn", { argsIgnorePattern: "^_" }],
      // The manifest gives every flag a `long`, checked by the parity test, so
      // the assertions that rely on it are load-bearing rather than sloppy.
      "@typescript-eslint/no-non-null-assertion": "off",
      // On, and load bearing for the dev loop rather than for style. A module
      // that exports a non-component opts itself out of Fast Refresh, and with
      // this off a single constant in `primitives/index.tsx` silently stopped
      // every primitive edit from reaching an open window -- the source said one
      // thing and the running app another, with nothing to say why.
      "react-refresh/only-export-components": ["error", { allowConstantExport: true }],
    },
  },
  {
    // The deliberate exceptions: a context provider exported beside its own
    // `use*` hook. Splitting those would buy nothing -- editing a provider
    // remounts the tree anyway, so a full reload is the honest outcome there --
    // whereas in a leaf component module the same export silently costs every
    // consumer its hot updates. That asymmetry is the whole reason the rule is
    // scoped rather than blanket-disabled.
    files: [
      "src/bridge/**/*.tsx",
      "src/primitives/charts.tsx",
      "src/views/AgentSheet.tsx",
    ],
    rules: { "react-refresh/only-export-components": "off" },
  },
  {
    files: ["electron/**/*.ts", "scripts/**/*.mjs", "*.config.ts"],
    languageOptions: {
      parser: tsparser,
      parserOptions: { sourceType: "module" },
      globals: { ...globals.node },
    },
    plugins: { "@typescript-eslint": tseslint },
    rules: {
      ...tseslint.configs.recommended.rules,
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": ["warn", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/no-non-null-assertion": "off",
      "no-undef": "off",
    },
  },
  {
    files: ["**/*.test.{ts,tsx}"],
    languageOptions: { globals: { ...globals.node } },
  },
];
