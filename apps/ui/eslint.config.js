import js from "@eslint/js";
import globals from "globals";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";

export default [
  {
    ignores: [
      "dist",
      "playwright-report",
      "test-results",
      "node_modules",
      // Codegen artifact (scripts/gen-command-manifest.mjs); not authored here.
      "src/generated",
      // Codegen artifact (scripts/gen-api-types.mjs); not authored here.
      "src/api/generated.ts",
    ],
  },
  js.configs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2022,
      globals: { ...globals.browser, ...globals.node },
      parser: tsparser,
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    plugins: {
      "@typescript-eslint": tseslint,
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      ...tseslint.configs.recommended.rules,
      ...reactHooks.configs.recommended.rules,
      // New in eslint-plugin-react-hooks 7's recommended set: flags a setState
      // called synchronously inside an effect. Several wired views load data
      // that way (fetch-in-effect -> setState); adopting the rule is a behavioral
      // refactor, out of scope for a dependency bump. Deferred to a dedicated
      // pass (the frontend-toolchain migration, #220) rather than smuggled in here.
      "react-hooks/set-state-in-effect": "off",
      // TypeScript itself resolves identifiers/types; the core rule produces
      // false positives on ambient DOM lib types (e.g. RequestInit) in .ts files.
      "no-undef": "off",
      // This is a hand-rolled design system: primitive files intentionally
      // colocate small color/helper exports next to their component, and the
      // store colocates its hook with the provider. Fast-refresh granularity is
      // not worth splitting those, so the rule is disabled rather than warned.
      "react-refresh/only-export-components": "off",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
    },
  },
];
