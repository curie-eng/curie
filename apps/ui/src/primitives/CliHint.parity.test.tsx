import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import * as ts from "typescript";
import { CliHint } from "./CliHint";
import { cliCommand } from "./cliCommand";
import { WIRED_ACTIONS, PARITY_TRACKING_ISSUE } from "./parity";
import type { WiredActionId } from "./parity";
import { StoreProvider } from "../state/store";

const PRODUCTION_ROOT = join(process.cwd(), "src");
const EXPECTED_COMMAND_IDS = [
  "cluster.deploy",
  "cluster.observability.metrics",
  "cluster.observability.run",
  "cluster.observability.runs",
  "cluster.reset-thread",
  "cluster.status",
  "cluster.work-items",
  "init",
  "local.deploy",
  "local.observability.metrics",
  "local.observability.run",
  "local.observability.runs",
  "local.reset-thread",
  "local.status",
  "local.work-items",
] as const;
const EXPECTED_NO_CLI_ACTION_IDS = [
  "behavior-packs-edit",
  "eval-matrix",
  "memory-delete",
  "memory-edit",
] as const satisfies readonly WiredActionId[];

function isProductionTypeScriptFile(name: string): boolean {
  const isTypeScript = name.endsWith(".ts") || name.endsWith(".tsx");
  const isTest = [".test.ts", ".test.tsx", ".spec.ts", ".spec.tsx"].some((suffix) =>
    name.endsWith(suffix),
  );
  return isTypeScript && !isTest;
}

function discoverProductionSources(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true })
    .sort((left, right) => left.name.localeCompare(right.name))
    .flatMap((entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) return discoverProductionSources(path);
      return entry.isFile() && isProductionTypeScriptFile(entry.name) ? [path] : [];
    });
}

function sourceLocation(sourceFile: ts.SourceFile, node: ts.Node): string {
  const position = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
  return `${relative(process.cwd(), sourceFile.fileName)}:${position.line + 1}:${position.character + 1}`;
}

function unwrapParentheses(expression: ts.Expression): ts.Expression {
  let current = expression;
  while (ts.isParenthesizedExpression(current)) current = current.expression;
  return current;
}

function extractActionIds(argument: ts.Expression, sourceFile: ts.SourceFile): string[] {
  if (ts.isStringLiteral(argument)) return [argument.text];
  if (ts.isParenthesizedExpression(argument)) {
    return extractActionIds(argument.expression, sourceFile);
  }
  if (ts.isConditionalExpression(argument)) {
    return [
      ...extractActionIds(argument.whenTrue, sourceFile),
      ...extractActionIds(argument.whenFalse, sourceFile),
    ];
  }
  throw new Error(
    `${sourceLocation(sourceFile, argument)} uses unsupported cliCommand action syntax: ${ts.SyntaxKind[argument.kind]}`,
  );
}

function isCliCommandCall(node: ts.Node): node is ts.CallExpression {
  return (
    ts.isCallExpression(node) &&
    ts.isIdentifier(node.expression) &&
    node.expression.text === "cliCommand"
  );
}

function findJsxAttribute(
  node: ts.JsxOpeningElement | ts.JsxSelfClosingElement,
  name: string,
): ts.JsxAttribute | undefined {
  return node.attributes.properties.find(
    (attribute): attribute is ts.JsxAttribute =>
      ts.isJsxAttribute(attribute) && attribute.name.getText() === name,
  );
}

function jsxAttributeExpression(attribute: ts.JsxAttribute): ts.Expression | undefined {
  const initializer = attribute.initializer;
  return initializer && ts.isJsxExpression(initializer) ? initializer.expression : undefined;
}

interface ProductionUsage {
  readonly commandIds: readonly { readonly id: string; readonly location: string }[];
  readonly noCliActionIds: readonly { readonly id: string; readonly location: string }[];
  readonly cliCommandViolations: readonly string[];
  readonly cliHintViolations: readonly string[];
}

function scanProductionUsage(): ProductionUsage {
  const commandIds: { id: string; location: string }[] = [];
  const noCliActionIds: { id: string; location: string }[] = [];
  const cliCommandViolations: string[] = [];
  const cliHintViolations: string[] = [];

  for (const path of discoverProductionSources(PRODUCTION_ROOT)) {
    const sourceFile = ts.createSourceFile(
      path,
      readFileSync(path, "utf8"),
      ts.ScriptTarget.Latest,
      true,
      path.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
    );

    function visit(node: ts.Node): void {
      if (isCliCommandCall(node)) {
        const argument = node.arguments[0];
        if (!argument) {
          cliCommandViolations.push(
            `${sourceLocation(sourceFile, node)} calls cliCommand without an action id`,
          );
        } else {
          try {
            for (const id of extractActionIds(argument, sourceFile)) {
              commandIds.push({ id, location: sourceLocation(sourceFile, argument) });
            }
          } catch (error) {
            cliCommandViolations.push(error instanceof Error ? error.message : String(error));
          }
        }
      }

      if (
        (ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node)) &&
        node.tagName.getText(sourceFile) === "CliHint"
      ) {
        const commandAttribute = findJsxAttribute(node, "command");
        const noCliEquivalentAttribute = findJsxAttribute(node, "noCliEquivalent");
        const actionIdsAttribute = findJsxAttribute(node, "actionIds");
        const modeCount = Number(commandAttribute !== undefined) +
          Number(noCliEquivalentAttribute !== undefined);

        if (modeCount !== 1) {
          cliHintViolations.push(
            `${sourceLocation(sourceFile, node)} must pass exactly one CliHint mode: command or noCliEquivalent`,
          );
        }

        if (commandAttribute) {
          const expression = jsxAttributeExpression(commandAttribute);
          const unwrapped = expression ? unwrapParentheses(expression) : undefined;
          if (!unwrapped || !isCliCommandCall(unwrapped)) {
            cliHintViolations.push(
              `${sourceLocation(sourceFile, commandAttribute)} must pass a direct cliCommand call to CliHint command`,
            );
          }
        }

        if (noCliEquivalentAttribute) {
          const issueExpression = jsxAttributeExpression(noCliEquivalentAttribute);
          if (
            !issueExpression ||
            !ts.isIdentifier(issueExpression) ||
            issueExpression.text !== "PARITY_TRACKING_ISSUE"
          ) {
            cliHintViolations.push(
              `${sourceLocation(sourceFile, noCliEquivalentAttribute)} must pass PARITY_TRACKING_ISSUE directly to CliHint noCliEquivalent`,
            );
          }

          if (!actionIdsAttribute) {
            cliHintViolations.push(
              `${sourceLocation(sourceFile, node)} must pass a direct actionIds array in noCliEquivalent mode`,
            );
          } else {
            const actionIdsExpression = jsxAttributeExpression(actionIdsAttribute);
            if (!actionIdsExpression || !ts.isArrayLiteralExpression(actionIdsExpression)) {
              cliHintViolations.push(
                `${sourceLocation(sourceFile, actionIdsAttribute)} must pass a direct actionIds array in noCliEquivalent mode`,
              );
            } else if (actionIdsExpression.elements.length === 0) {
              cliHintViolations.push(
                `${sourceLocation(sourceFile, actionIdsAttribute)} must contain at least one action id string literal`,
              );
            } else {
              for (const element of actionIdsExpression.elements) {
                if (!ts.isStringLiteral(element)) {
                  cliHintViolations.push(
                    `${sourceLocation(sourceFile, element)} uses unsupported actionIds syntax: ${ts.SyntaxKind[element.kind]}`,
                  );
                  continue;
                }
                noCliActionIds.push({
                  id: element.text,
                  location: sourceLocation(sourceFile, element),
                });
              }
            }
          }
        }
      }

      ts.forEachChild(node, visit);
    }

    visit(sourceFile);
  }

  return { commandIds, noCliActionIds, cliCommandViolations, cliHintViolations };
}

// Console/CLI parity gate (epic #145): every wired UI action must map to EITHER
// a real command resolved from the committed manifest OR an explicit
// `noCliEquivalent` marker pointing at the tracking issue. This test enumerates
// the parity registry and fails the moment a wired action ships without a
// deliberate mapping — the honest amber state is only for genuinely-unmapped
// actions, never a silent gap.

describe("console/CLI parity registry (#280)", () => {
  it("lists at least the known wired actions", () => {
    // A guard against the registry being emptied/short-circuited.
    expect(WIRED_ACTIONS.length).toBeGreaterThanOrEqual(10);
  });

  it("every wired action maps to a real command or an explicit noCliEquivalent", () => {
    for (const action of WIRED_ACTIONS) {
      if ("command" in action.mapping) {
        // Resolves against the manifest without throwing, and yields an
        // `curie …` invocation. `cliCommand` throws on an unknown path, so
        // this catches a command id that is not a real manifest leaf.
        const rendered = cliCommand(action.mapping.command);
        expect(rendered.startsWith("curie ")).toBe(true);
      } else {
        // The honest gap: a tracking-issue URL, not an empty string.
        expect(action.mapping.noCliEquivalent).toMatch(/^https?:\/\//);
      }
    }
  });

  it("the four former parity-gap actions now resolve to real CLI verbs", () => {
    // #149 landed kill/resume/budget/delete, so these are commands, not gaps.
    for (const id of ["kill", "resume", "budget", "delete"]) {
      const action = WIRED_ACTIONS.find((a) => a.id === id);
      expect(action, `missing wired action: ${id}`).toBeDefined();
      expect("command" in action!.mapping).toBe(true);
    }
  });

  it("covers every production cliCommand action in the registry", () => {
    const usage = scanProductionUsage();
    expect(usage.cliCommandViolations).toEqual([]);

    const discoveredCommandIds = [...new Set(usage.commandIds.map((action) => action.id))].sort();
    expect(
      discoveredCommandIds,
      "production cliCommand inventory must contain every expected command id",
    ).toEqual(EXPECTED_COMMAND_IDS);

    const mappedCommands = new Set<string>(
      WIRED_ACTIONS.flatMap((action) =>
        "command" in action.mapping ? [action.mapping.command] : [],
      ),
    );
    for (const action of usage.commandIds) {
      expect(
        mappedCommands.has(action.id),
        `${action.location} uses unregistered cliCommand action: ${action.id}`,
      ).toBe(true);
    }
  });

  it("requires every production CliHint to declare one direct parity mode", () => {
    const usage = scanProductionUsage();
    expect(usage.cliHintViolations).toEqual([]);

    const discoveredNoCliActionIds = [
      ...new Set(usage.noCliActionIds.map((action) => action.id)),
    ].sort();
    expect(
      discoveredNoCliActionIds,
      "production noCliEquivalent inventory must contain every expected wired action id",
    ).toEqual(EXPECTED_NO_CLI_ACTION_IDS);

    for (const discovered of usage.noCliActionIds) {
      const action = WIRED_ACTIONS.find((candidate) => candidate.id === discovered.id);
      expect(
        action,
        `${discovered.location} uses an unregistered noCliEquivalent action id: ${discovered.id}`,
      ).toBeDefined();
      expect(
        action && "noCliEquivalent" in action.mapping
          ? action.mapping.noCliEquivalent
          : undefined,
        `${discovered.location} must map ${discovered.id} to PARITY_TRACKING_ISSUE`,
      ).toBe(PARITY_TRACKING_ISSUE);
    }
  });

  it("removes the issue scoped unrendered registry entries", () => {
    const staleIds = ["message-cluster", "message-local", "eval"];
    expect(WIRED_ACTIONS.filter((action) => staleIds.includes(action.id))).toEqual([]);
  });
});

describe("CliHint — noCliEquivalent amber state (#280)", () => {
  function renderHint(el: React.ReactElement) {
    return render(<StoreProvider>{el}</StoreProvider>);
  }

  it("renders the amber glyph and links to the tracking issue instead of copying", () => {
    renderHint(
      <CliHint
        noCliEquivalent={PARITY_TRACKING_ISSUE}
        actionIds={["memory-edit"]}
      />,
    );
    const btn = screen.getByRole("button", { name: /no cli equivalent/i });
    expect(btn).toHaveAttribute("data-no-cli", "true");
    // It carries no "Copy command" affordance.
    expect(screen.queryByRole("button", { name: /copy command/i })).toBeNull();
  });

  it("still renders the copy affordance for a real command", () => {
    renderHint(<CliHint command="curie cluster deploy" />);
    expect(
      screen.getByRole("button", { name: "Copy command: curie cluster deploy" }),
    ).toBeInTheDocument();
  });
});
