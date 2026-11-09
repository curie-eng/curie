#!/usr/bin/env python3
"""Require caller-supplied context flag values in the context-policy source set."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

# The source set covers the tracked text files in the context-removal contract.
SCOPED_PATHS = frozenset(
    (
        ".github/workflows/ci.yaml",
        "apps/docker-daemon/tests/e2e.sh",
        "apps/e2e-connector/tests/test_workload.py",
        "apps/ui/src/generated/commandManifest.ts",
        "apps/worker/src/curie_worker/sandbox/k8s.py",
        "apps/worker/tests/sandbox/conftest.py",
        "charts/curie/CLAUDE.md",
        "charts/curie/ci/cron-operator-approval-proof.py",
        "charts/curie/ci/hook-approval-proof.py",
        "charts/curie/ci/runner-images-assertions.sh",
        "charts/curie/ci/runtime/connector-readiness-runtime.sh",
        "charts/curie/ci/runtime/metrics-alerts-runtime.sh",
        "cli/scripts/cluster-down-namespace-bound.sh",
        "cli/scripts/cluster-upgrade-matrix.sh",
        "cli/scripts/e2e-cluster-idle-route-reclamation.sh",
        "cli/scripts/e2e-ladder.sh",
        "cli/scripts/recovery-drill.sh",
        "cli/src/args/dev.rs",
        "cli/src/factory_quickstart/mod.rs",
        "cli/src/installation.rs",
        "cli/tests/cluster_upgrade_matrix.rs",
        "cli/tests/repo_runner.rs",
        "compose/tests/test_scripts_bash_3_2.py",
        "examples/curie.yaml",
        "examples/sre-bot/observability/reliability-alerts.test.yaml",
        "runner/tests/test_dark_factory_runner_toolchain.py",
        "scripts/chart-runtime-e2e.sh",
        "scripts/e2e-mail-adapter.sh",
        "tools/factory-e2e/factory_e2e.py",
        "tools/factory-e2e/repo_runner.py",
        "tools/factory-e2e/tests/test_acceptance.py",
        "tools/factory-e2e/tests/test_factory_e2e.py",
        "tools/factory-e2e/tests/test_factory_e2e_loop.py",
        "tools/factory-e2e/tests/test_gitlab_acceptance.py",
        "tools/factory-e2e/tests/test_repo_runner.py",
        "tools/factory-e2e/tests/test_tls_code_host.py",
    )
)
FLAG = re.compile(r"--(?:kube-)?context\b")
TOKEN = re.compile(r""""(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|<[^<>\n]+>|[^\s,;)\]`"']+""")
DYNAMIC = re.compile(r"\$(?:[A-Za-z_0-9({])|\{[^}]*\}|<[^<>]+>|%(?:s|[a-z]|\([^)]+\)[a-z])")


def problems(text: str) -> list[int]:
    found = []
    # Escaped quotes inside string literals carry the same argument boundary.
    normalized = text.replace('\\"', '"').replace("\\'", "'")
    for match in FLAG.finditer(normalized):
        rest = normalized[match.end() :]
        array = re.match(
            r"[\"'](?:\.(?:into|to_string|to_owned)\(\))?\s*"
            r"(?:,\s*|\)\s*\.arg\(\s*|\s+(?=[\"']))",
            rest,
        )
        if array:
            rest = rest[array.end() :]
            literal = re.match(r"(?:[frb]+)?([\"'])", rest)
            if not literal:
                continue  # A language expression supplies the argument.
            rest = rest[literal.end() - 1 :]
        elif rest.startswith("="):
            rest = rest[1:]
        elif rest[:1].isspace():
            rest = re.sub(r"^(?:\s|\\\n)+", "", rest)
        else:
            continue
        token = TOKEN.match(rest)
        if token is None:
            continue
        value = token.group().strip("\"'")
        if not value or value.startswith(("--", "(", ")", "[", "]", "|")):
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
    for name in sorted(set(tracked) & SCOPED_PATHS):
        if "changelog" in name.lower():
            continue
        data = (root / name).read_bytes()
        if b"\0" in data:
            continue
        for line in problems(data.decode("utf-8")):
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
        fixture = root / "examples/curie.yaml"
        fixture.parent.mkdir()
        fixture.touch()
        subprocess.run(["git", "add", "examples/curie.yaml"], cwd=root, check=True)
        for expected, examples in ((1, rejected), (0, accepted)):
            for example in examples:
                fixture.write_text(example + "\n")
                observed = subprocess.run(
                    [sys.executable, str(Path(__file__).resolve()), "--root", str(root)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if observed.returncode != expected:
                    print(
                        f"Policy self-test failed: {example!r}: {observed.stdout}{observed.stderr}",
                        file=sys.stderr,
                    )
                    return 1
    print(
        f"Kube-context policy self-test passed: {len(rejected)} literals rejected, "
        f"{len(accepted)} variables/placeholders accepted"
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
