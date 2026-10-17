#!/usr/bin/env python3
"""TEMPORARY guard on two incidents, pending real connector authorization.

WHAT THIS IS NOT
----------------
This does not prove that every write path is gated. It cannot: it reads the
source this repository happens to carry, so a connector shipped only as an image
is invisible to it, and a tool's read-only annotation is a hint to the model
rather than a boundary. Passing means "the two specific mistakes below are not
present in an example bundle", nothing wider.

The durable fix is builder-owned policy over a connector's PUBLISHED tool
surface: every tool classified deny / approval-required / allow, with anything
unclassified denied by default, enforced by the platform at call time rather than
by a repository lint. A connector publishes capabilities, a skill owns workflow
logic, and neither may silently widen the builder's authority. When that contract
exists, this script should be deleted rather than extended -- it is scaffolding
around its absence, and the shape below (grep the bundle, infer intent) is
deliberately not a model for how authorization should work.

THE TWO INCIDENTS
-----------------
**A write connector enabled without its gate.** Enabling a connector and
declaring its gate are two edits in two files, and nothing made them happen
together. On a real install they did not: the connector was enabled, the gate was
not, and a tool able to roll production Deployments ran for four and a half days
with nothing in front of it. Nothing reported it, because there is nothing to
report -- the connector is healthy, the tool runs, no approval card is ever
posted. The absence of a gate has no symptom.

**Two ceilings that drifted apart.** A connector's `K8S_*_ALLOWLIST` and the
Role's `resourceNames` are both allowlists of what may be written, in different
files, edited by people thinking about different things. On the same install they
disagreed for four days after the target cluster changed. That direction was
harmless -- the intersection was empty -- but the reverse is not: an allowlist
entry with no matching `resourceName` is a 403 *after* a human approved the call,
which spends the approval and delivers nothing.

KNOWN LIMITS, stated rather than papered over
---------------------------------------------
- Read-only status is read from the source's own `ToolAnnotations`. A tool whose
  status cannot be determined is reported as unclassified rather than assumed
  safe, which is the one place this borrows the tri-state model's default.
- The RBAC side only understands `resourceNames` on rules granting a write verb
  over `apps/deployments` (including its `scale` subresource). Any other shape of
  grant is not compared, and a bundle relying on one is not covered here.
- A bundle with no connector source, or no write-annotated tool, passes trivially.
- A write tool also passes when the bundle's `toolPolicy` gates or denies it by
  literal name (`approvalRequired` or `deny`). The runner enforces that policy, so
  either still keeps a human in front of the write. An `allow` entry does not
  count, since it lets the write run with nobody deciding, and a glob entry does
  not count, because it classifies tools nobody looked at.
"""

import argparse
import ast
import json
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent

# The sanctioned placeholder an example ships before a real target is chosen.
# Matched exactly: a bundle mixing placeholders with real entries is a mistake,
# not an example, and must not skip the comparison.
PLACEHOLDER = re.compile(r"^<[a-z][a-z0-9-]*>/<[a-z][a-z0-9-]*>$")

# Verbs that can change a Deployment. `get` alone is not a write.
WRITE_VERBS = {"create", "update", "patch", "delete", "deletecollection", "*"}

# An allowlist env var: any connector's namespace/name ceiling, so a second write
# connector is covered without editing this file.
ALLOWLIST_ENV = re.compile(r"^K8S_[A-Z0-9_]*ALLOWLIST$")


def bundles(examples: pathlib.Path) -> list[pathlib.Path]:
    return sorted(p.parent.parent for p in examples.glob("*/.claude-plugin/plugin.json"))


def declared_gates(bundle: pathlib.Path) -> set[str]:
    manifest = json.loads((bundle / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))
    return {
        g["gate"].strip()
        for g in (manifest.get("approvalPolicy") or {}).get("gates", [])
        if g.get("gate")
    }


def classified_tools(bundle: pathlib.Path) -> set[str]:
    """`<server>/<tool>` names the bundle's `toolPolicy` gates or denies by literal name.

    The runner enforces `toolPolicy` at call time, so a write tool the owner names
    in `approvalRequired` waits for a person and one named in `deny` never runs.
    `allow` is not counted: it lets the write run with nobody deciding, which is
    what this guard exists to stop. Only literal entries count: a glob such as
    `platform/*` would classify a write tool nobody looked at.
    """
    manifest = json.loads((bundle / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))
    policy = manifest.get("toolPolicy") or {}
    names: set[str] = set()
    for key in ("approvalRequired", "deny"):
        for entry in policy.get(key) or []:
            entry = str(entry).strip()
            if not any(ch in entry for ch in "*?["):
                names.add(entry)
    return names


def _callee_name(node: ast.expr) -> str:
    """Trailing identifier of a call target: `mcp.tool` -> `tool`."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _read_only_of_call(call: ast.Call) -> bool | None:
    """`readOnlyHint` from a `ToolAnnotations(...)` call, or None if unreadable."""
    if _callee_name(call.func) != "ToolAnnotations":
        return None
    for kw in call.keywords:
        if kw.arg == "readOnlyHint":
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, bool):
                return kw.value.value
            return None
    # ToolAnnotations() with the field omitted says nothing about write-ness.
    return None


def _annotation_constants(tree: ast.Module) -> dict[str, bool | None]:
    """Module-level `NAME = ToolAnnotations(...)` -> its readOnlyHint."""
    found: dict[str, bool | None] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        value = _read_only_of_call(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                found[target.id] = value
    return found


def _read_only_of_annotations(expr: ast.expr, consts: dict[str, bool | None]) -> bool | None:
    if isinstance(expr, ast.Name):
        return consts.get(expr.id)
    if isinstance(expr, ast.Call):
        return _read_only_of_call(expr)
    if isinstance(expr, ast.Dict):
        for key, value in zip(expr.keys, expr.values, strict=False):
            if (
                isinstance(key, ast.Constant)
                and key.value == "readOnlyHint"
                and isinstance(value, ast.Constant)
                and isinstance(value.value, bool)
            ):
                return value.value
    return None


def tool_surface(source: pathlib.Path) -> dict[str, bool | None]:
    """Map each `@*.tool`-decorated tool's published name to its readOnlyHint.

    Parsed rather than grepped, and resolved PER TOOL. A file-level substring
    search cannot do this: it marks every tool in a mixed read/write server as a
    write tool, and misses any annotation whose text it does not happen to match.
    `None` means the source does not say, which the caller treats as unclassified
    rather than as read-only.
    """
    try:
        tree = ast.parse(source.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return {}
    consts = _annotation_constants(tree)
    surface: dict[str, bool | None] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for dec in node.decorator_list:
            call = dec if isinstance(dec, ast.Call) else None
            target = call.func if call else dec
            if _callee_name(target) != "tool":
                continue
            name = node.name
            read_only: bool | None = None
            if call:
                for kw in call.keywords:
                    if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                        name = str(kw.value.value)
                    elif kw.arg == "annotations":
                        read_only = _read_only_of_annotations(kw.value, consts)
            surface[name] = read_only
    return surface


def deployment_resource_names(role_file: pathlib.Path) -> set[str]:
    """`namespace/name` pairs a Role grants a WRITE verb on `apps/deployments`.

    Scoped on purpose. Unioning every `resourceNames` entry from every rule lets
    an unrelated named resource -- a ConfigMap, or a read-only grant -- make a
    Deployment allowlist look aligned when it is not.
    """
    names: set[str] = set()
    for doc in yaml.safe_load_all(role_file.read_text(encoding="utf-8")):
        if not doc or doc.get("kind") != "Role":
            continue
        namespace = (doc.get("metadata") or {}).get("namespace")
        if not namespace:
            continue
        for rule in doc.get("rules") or []:
            groups = set(rule.get("apiGroups") or [])
            resources = {str(r).split("/", 1)[0] for r in (rule.get("resources") or [])}
            verbs = set(rule.get("verbs") or [])
            if not ({"apps", "*"} & groups):
                continue
            if not ({"deployments", "*"} & resources):
                continue
            if not (WRITE_VERBS & verbs):
                continue
            names.update(f"{namespace}/{n}" for n in (rule.get("resourceNames") or []))
    return names


def check(bundle: pathlib.Path) -> list[str]:
    problems: list[str] = []
    conn_file = bundle / "connectors.yaml"
    if not conn_file.is_file():
        return problems
    loaded = yaml.safe_load(conn_file.read_text(encoding="utf-8")) or {}
    connectors = loaded.get("connectors") or {}
    gates = declared_gates(bundle)
    classified = classified_tools(bundle)
    role_file = bundle / "manifests" / "write-role.yaml"

    for name, spec in connectors.items():
        source = bundle / "connectors" / name / "server.py"
        for tool, read_only in sorted(tool_surface(source).items()):
            expected = f"mcp__{name}__{tool}"
            if read_only is None:
                problems.append(
                    f"{bundle.name} connector {name!r} publishes {tool} with no readable "
                    f"readOnlyHint, so whether it writes is unknown -- annotate it "
                    f"explicitly with ToolAnnotations(readOnlyHint=...) rather than leaving "
                    f"a reader to guess."
                )
            elif not read_only and expected not in gates and f"{name}/{tool}" not in classified:
                problems.append(
                    f"{bundle.name} declares connector {name!r}, whose {tool} is not "
                    f"read-only, but no approvalPolicy gate names {expected} and no "
                    f"toolPolicy entry names {name}/{tool}. Enabling the connector and "
                    f"declaring the gate are two edits; this is the one that gets forgotten."
                )

        # Ceilings, only where both halves exist.
        env = spec.get("env") or {}
        allow_raw = next((v for k, v in env.items() if ALLOWLIST_ENV.match(str(k))), None)
        if allow_raw is None or not role_file.is_file():
            continue
        entries = [e.strip() for e in str(allow_raw).split(",") if e.strip()]
        placeholders = [e for e in entries if PLACEHOLDER.match(e)]
        if placeholders and len(placeholders) != len(entries):
            problems.append(
                f"{bundle.name} connector {name!r} mixes the placeholder allowlist with real "
                f"entries ({', '.join(sorted(entries))}) -- a half-filled allowlist skips no "
                f"comparison and ships a real grant nobody checked. Use placeholders only, or "
                f"real entries only."
            )
            continue
        if placeholders:
            # A shipped-off example that has not chosen a target yet.
            continue
        allow = set(entries)
        rbac = deployment_resource_names(role_file)
        for target in sorted(allow - rbac):
            problems.append(
                f"{bundle.name} allows {target} in the {name!r} allowlist with no matching "
                f"resourceName on a write rule in manifests/write-role.yaml -- the tool "
                f"accepts it and the API server 403s, after a human approved the call."
            )
        for target in sorted(rbac - allow):
            problems.append(
                f"{bundle.name} grants {target} in manifests/write-role.yaml with no matching "
                f"entry in the {name!r} allowlist -- permitted, unreachable through the tool, "
                f"and unexplained."
            )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--examples",
        type=pathlib.Path,
        default=ROOT / "examples",
        help="directory of example bundles (default: %(default)s)",
    )
    args = parser.parse_args()

    found = bundles(args.examples)
    if not found:
        print(
            f"no bundles under {args.examples}; an empty match would pass vacuously",
            file=sys.stderr,
        )
        return 1
    problems: list[str] = []
    for bundle in found:
        current = check(bundle)
        status = "ok" if not current else f"{len(current)} problem(s)"
        print(f"  {bundle.name:<28} {status}")
        problems += current
    if problems:
        print("", file=sys.stderr)
        for problem in problems:
            print(f"FAIL: {problem}", file=sys.stderr)
        return 1
    print(f"{len(found)} bundle(s): no ungated write tool and no allowlist drift found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
