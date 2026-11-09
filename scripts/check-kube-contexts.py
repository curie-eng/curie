#!/usr/bin/env python3
"""Reject deployment-specific kube contexts throughout tracked source and docs."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

SOURCE_SUFFIXES = frozenset(
    {
        ".sh",
        ".bash",
        ".ksh",
        ".fish",
        ".bats",
        ".zsh",
        ".py",
        ".pyi",
        ".pyw",
        ".rs",
        ".ts",
        ".tsx",
        ".mts",
        ".cts",
        ".yaml",
        ".yml",
        ".md",
        ".markdown",
        ".mdx",
    }
)
EXAMPLE_CONTEXTS = frozenset(
    {
        "scratch-cluster",
        "test-context",
        "other-context",
        "missing-context",
        "fixture-context",
        "stub-context",
        "ctx",
        "kind-quickstart",
        "kind-curie-factory",
        "kind-ci-fixture",
        "kind-curie-e2e",
        "kind-identity-e2e",
        "kind-x",
    }
)
FORBIDDEN_CONTEXT = re.compile(r"\bk[8](?:-?scratch)?\b")
FLAG = re.compile(r"--(?:kube-)?context\b")
TOKEN = re.compile(
    r""""(?:[^"\n\\]|\\.)*"|'(?:[^'\n\\]|\\.)*'|`(?:[^`\n\\]|\\.)*`|<[^<>\n]+>|[^\s,;)\]`"']+"""
)
DYNAMIC = re.compile(r"\$(?:[A-Za-z_0-9({])|\{[^}]*\}|<[^<>]+>|%(?:s|[a-z]|\([^)]+\)[a-z])")
PROSE_AFTER_FLAG = re.compile(
    r"\s+(?:was passed\b|must (?:be|remain|not)\b|sees (?:that|the)\b|"
    r"to every\b|and reads\b|flag(?:[,\.]|\s+(?:is|so|has|must)\b)|is an? (?:option|flag)\b)"
)


def problems(text: str, *, yaml_values: bool = False, shell_values: bool = False) -> list[int]:
    found = []
    # Escaped quotes inside string literals carry the same argument boundary.
    normalized = text.replace('\\"', '"').replace("\\'", "'")
    for match in FLAG.finditer(normalized):
        rest = normalized[match.end() :]
        equals_value = rest.startswith("=")
        if PROSE_AFTER_FLAG.match(rest):
            continue
        shell_argument = shell_values and bool(
            re.search(
                r"(?:^|[\s;])(?:[A-Za-z_]\w*=\([^)]*|(?:kubectl|helm|curie)\s+[^\n]*)$",
                normalized[: match.start()],
            )
        )
        literal_words = yaml_values or shell_argument
        boundary = r",\s*|\)\s*\.arg\(\s*|\s+(?=[\"'`])"
        if shell_argument:
            boundary += r"|\s+"
        array = re.match(
            r"[\"'`](?:\.(?:into|to_string|to_owned)\(\))?\s*"
            rf"(?:{boundary})",
            rest,
        )
        yaml_item = re.match(r"[\"']?[ \t]*\n[ \t]*-[ \t]+", rest) if yaml_values else None
        if yaml_item:
            rest = rest[yaml_item.end() :]
        elif array:
            rest = rest[array.end() :]
            literal = re.match(r"(?:&\s*)?(?:[frb]+\#*)?([\"'`])", rest)
            if literal:
                rest = rest[literal.end() - 1 :]
            elif not literal_words:
                continue  # A language expression supplies the argument.
        elif rest.startswith("="):
            preceding = normalized[match.start() - 1 : match.start()]
            if preceding in {'"', "'", "`"} and rest[1:2] == preceding:
                continue
            rest = rest[1:]
        elif rest[:1].isspace():
            rest = re.sub(r"^(?:\s|\\\n)+", "", rest)
        else:
            continue
        token = TOKEN.match(rest)
        if token is None:
            continue
        value = token.group().strip("\"'`")
        if not value or value.startswith(("(", ")", "[", "]", "|", "&&", ">")):
            continue
        if value.startswith("-") and not equals_value:
            continue
        statement_prefix = normalized[: match.start()].rsplit("\n\n", 1)[-1]
        kube_consumer = re.search(r"\b(?:kubectl|helm|curie)\b", statement_prefix)
        if value.startswith("dir://") and kube_consumer is None:
            continue
        if value in EXAMPLE_CONTEXTS:
            continue
        if not DYNAMIC.search(value):
            found.append(normalized.count("\n", 0, match.start()) + 1)
    return sorted(set(found))


def check(root: Path) -> int:
    tracked = (
        subprocess.run(["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True)
        .stdout.decode()
        .split("\0")
    )
    failures = []
    for name in sorted(filter(None, tracked)):
        path = Path(name)
        if path.name.lower() in {"changelog", "changelog.md", "changelog.rst", "changelog.txt"}:
            continue
        data = (root / path).read_bytes()
        shebang = data.split(b"\n", 1)[0] if data.startswith(b"#!") else b""
        suffix = path.suffix.lower()
        if re.search(rb"\bpython[0-9.]*\b", shebang):
            suffix = ".py"
        elif re.search(rb"\b(?:bash|sh|zsh|ksh|fish)\b", shebang):
            suffix = ".sh"
        if suffix not in SOURCE_SUFFIXES:
            continue
        if b"\0" in data:
            continue
        text = data.decode("utf-8")
        for match in FORBIDDEN_CONTEXT.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            failures.append(f"{name}:{line}: developer kube-context names are forbidden")
        for line in problems(
            text,
            yaml_values=suffix in {".yaml", ".yml"},
            shell_values=suffix
            in {
                ".sh",
                ".bash",
                ".zsh",
                ".ksh",
                ".fish",
                ".bats",
                ".yaml",
                ".yml",
                ".md",
                ".markdown",
                ".mdx",
            },
        ):
            failures.append(
                f"{name}:{line}: context flag requires a variable or <context> placeholder"
            )
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("Kube-context policy passed")
    return 0


def self_test() -> int:
    flag = "--" + "context"
    kube_flag = "--kube-" + "context"
    rejected = [
        f"kubectl {flag} fixed-cluster get pods",
        f"kubectl {flag}=fixed-cluster get pods",
        f"kubectl {flag} kind-unlisted-context get pods",
        f'helm {kube_flag} "fixed-cluster" list',
        f'("{flag}", "fixed-cluster")',
        f'["{kube_flag}",\n    "fixed-cluster"]',
        f'"{flag}=fixed-cluster"',
        f'"{flag}".into(), "fixed-cluster".into()',
        f'("{flag}" "fixed-cluster")',
        f'.arg("{flag}").arg("fixed-cluster")',
        f'.arg("{kube_flag}")\n    .arg("fixed-cluster")',
    ]
    accepted = [
        f'kubectl {flag} "$CONTEXT" get pods',
        f'helm {kube_flag}="${{CONTEXT}}" list',
        f"kubectl {flag} kind-$CLUSTER get pods",
        f'("{flag}", context)',
        f'.arg("{flag}").arg(context)',
        f'("{flag}" "$CONTEXT")',
        f'["{kube_flag}", &options.context]',
        f'("{flag}", f"{{context}}")',
        f'format!("kubectl {flag} {{context}}")',
        f'"kubectl {flag} %s" % context',
        f"kubectl {flag} <context> get pods",
    ]
    with tempfile.TemporaryDirectory(prefix="curie-context-policy-") as directory:
        root = Path(directory)
        subprocess.run(["git", "init", "--quiet", str(root)], check=True)
        cases = [(1, "tools/factory-e2e/tests/test_acceptance.py", item) for item in rejected]
        cases += [(0, "tools/factory-e2e/tests/test_acceptance.py", item) for item in accepted]
        cases += [
            (1, "cli/src/installation.rs", f'.arg("{flag}").arg(r#"fixed-cluster"#)'),
            (1, "apps/ui/src/generated/commandManifest.ts", f'["{flag}", `fixed-cluster`]'),
            (1, "examples/curie.yaml", f'args: ["{flag}", fixed-cluster]'),
            (1, "examples/curie.yaml", f'args:\n  - "{flag}"\n  - fixed-cluster'),
            (1, "scripts/e2e-mail-adapter.sh", f'args=("{flag}" fixed-cluster)'),
            (0, "cli/src/installation.rs", f'.arg("{flag}").arg(r#"{{context}}"#)'),
            (0, "apps/ui/src/generated/commandManifest.ts", f'["{flag}", `${{context}}`]'),
            (0, "examples/curie.yaml", f'args: ["{flag}", "<context>"]'),
            (0, "examples/curie.yaml", f'args:\n  - "{flag}"\n  - "$CONTEXT"'),
            (0, "scripts/e2e-mail-adapter.sh", f'args=("{flag}" $CONTEXT)'),
            (1, "new-context-driver.sh", f"kubectl {flag} fixed-cluster get pods"),
            (1, "docs/new-context-guide.md", f"Use `kubectl {flag} fixed-cluster get pods`."),
            (1, "new-context-driver.py", f'["kubectl", "{flag}", "fixed-cluster"]'),
            (1, "new-context-driver.rs", f'.arg("{flag}").arg("fixed-cluster")'),
            (1, "new-context-driver.ts", f'["kubectl", "{flag}", "fixed-cluster"]'),
            (1, "new-context-driver.tsx", f'["kubectl", "{flag}", "fixed-cluster"]'),
            (
                1,
                ".github/workflows/new-context-driver.yml",
                f"run: kubectl {flag} fixed-cluster get pods",
            ),
            (
                1,
                ".githooks/new-context-hook",
                f'#!/usr/bin/env bash\nargs=("{flag}" fixed-cluster)',
            ),
            (1, "docs/changelog-tool.py", f'["kubectl", "{flag}", "fixed-cluster"]'),
            (
                0,
                "new-context-driver.sh",
                f'''python3 - <<'PY'
def kc(context, namespace, args):
    return run(["kubectl", "{flag}", context, "-n", namespace, *args])
PY''',
            ),
            (0, "new-context-driver.py", f'args = ["{flag}", context]'),
            (
                0,
                ".github/workflows/new-context-driver.yaml",
                f"run: kubectl {flag} kind-ci-fixture get pods",
            ),
            (0, "new-context-driver.py", f'assert "{flag}=dir:///workspace" in build_args'),
            (0, "docs/new-context-guide.md", "The context flag selects a cluster."),
            (0, "new-context-driver.rs", f'let note = "no {flag} was passed";'),
            (1, "new-context-driver.sh", f"kubectl {flag} was get pods"),
            (1, "new-context-driver.sh", f"kubectl {flag} flag get pods"),
            (1, "new-context-driver.sh", f"kubectl {flag}=-fixed-cluster get pods"),
            (1, "new-context-driver.sh", f"kubectl {flag}=https://cluster.example.com get pods"),
            (1, "new-context-driver.sh", f"kubectl {flag}=dir:///workspace get pods"),
            (1, "new-context-driver.py", f'argv = ["kubectl",\n    "{flag}", "dir:///workspace"]'),
            (0, "new-context-driver.py", f'if token in ("{flag}", "-n", "--namespace"): pass'),
            (0, "new-context-driver.py", f'if token.startswith(("{flag}=", "--namespace=")): pass'),
            (
                0,
                "docs/new-context-guide.md",
                f"The `{flag}` and `{kube_flag}` flags select a cluster.",
            ),
        ]
        cases += [
            (0, "new-context-driver.py", f'["{flag}", "{name}"]')
            for name in sorted(EXAMPLE_CONTEXTS)
        ]
        short_name = "k" + str(8)
        cases += [
            (1, "docs/new-context-guide.md", f"Do not use {name} as a cluster name.")
            for name in (short_name, short_name + "scratch", short_name + "-scratch")
        ]
        cases.append((0, "CHANGELOG.md", f"Historical context: {short_name}"))
        for expected, filename, example in cases:
            fixture = root / filename
            fixture.parent.mkdir(parents=True, exist_ok=True)
            fixture.write_text(example + "\n")
            subprocess.run(["git", "add", filename], cwd=root, check=True)
            observed = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--root", str(root)],
                capture_output=True,
                text=True,
                check=False,
            )
            fixture.write_text("")
            if observed.returncode != expected:
                print(
                    f"Policy self-test failed: {filename}: expected exit {expected}, "
                    f"got {observed.returncode}: {observed.stdout}{observed.stderr}",
                    file=sys.stderr,
                )
                return 1
    print(
        f"Kube-context policy self-test passed: {sum(case[0] == 1 for case in cases)} "
        f"invalid cases rejected, {sum(case[0] == 0 for case in cases)} valid cases accepted"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    return self_test() if args.self_test else check(args.root)


if __name__ == "__main__":
    raise SystemExit(main())
