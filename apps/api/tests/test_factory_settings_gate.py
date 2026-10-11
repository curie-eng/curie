"""#4397 AC 2: no factory code path reads a removed factory setting.

The factory is configured only through ``CURIE_FORGES`` and the ``factory_*``
settings, so the old ``github_factory_*`` settings, their ``GITHUB_FACTORY_*``
env names and the ``api.githubFactory*`` chart keys must appear nowhere in
the shipped surfaces. The one exemption is the chart's legacy-key guard file,
which the chart refusal and the CLI's null-writes both read so neither has to
spell a removed name.

A second gate keeps the install-wide GitHub credential out of the factory:
``credentials_for(`` and the four ``settings.github_*`` credential fields are
read only by the listed non-factory modules (git-flow, review ingress,
publication, workspaces, startup), never by a factory, WorkItem, lineage,
issue-read or forge module.

AC 2 names a grep gate, so the text pins here are the specification (test
policy 3). Both scanners are proven live by mutation self-tests.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
API_PACKAGE = REPO_ROOT / "apps" / "api" / "src" / "curie_api"
GUARD_FILE = "charts/curie/files/legacy-factory-keys.yaml"
ENV_GUARD_FILE = "charts/curie/files/legacy-factory-env.yaml"

# Every shipped surface that configured, rendered or read the factory.
SCANNED: tuple[str, ...] = (
    "apps/api/src",
    "apps/worker/src",
    "charts/curie/templates",
    "charts/curie/files",
    "charts/curie/ci",
    "charts/curie/values.yaml",
    "charts/curie/values.schema.json",
    "compose.dev.yaml",
    "tools/factory-e2e",
    "cli/src",
)
EXEMPT: frozenset[str] = frozenset({GUARD_FILE, ENV_GUARD_FILE})

REMOVED_FIELDS = (
    "github_factory_ingress_enabled",
    "github_factory_label",
    "github_factory_mention",
    "github_factory_reconcile_interval_s",
    "github_factory_reconcile_grace_s",
    "github_factory_intake",
    "github_factory_poll_interval_s",
    "github_factory_card_base_url",
    "github_factory_ci_wait_s",
    "github_factory_python_ci",
    "github_factory_metadata_ci",
    "github_factory_bases",
)
# Each removed field's env name is its upper case. Module names such as
# ``github_factory_review`` are not settings and stay legal.
REMOVED = re.compile(
    r"\b(?:"
    + "|".join(re.escape(field) for field in REMOVED_FIELDS)
    + "|"
    + "|".join(re.escape(field.upper()) for field in REMOVED_FIELDS)
    + r")\b"
    + r"|\bGITHUB_FACTORY_[A-Z0-9_]+"
    + r"|\bgithubFactory[A-Za-z0-9]*"
)

# The chart keys the base release carried (values.yaml at 20fccad31).
BASE_CHART_KEYS = frozenset(
    f"api.githubFactory{suffix}"
    for suffix in (
        "IngressEnabled",
        "Intake",
        "PollIntervalSeconds",
        "Label",
        "Mention",
        "CardBaseUrl",
        "CiWaitSeconds",
        "PythonCi",
        "Bases",
        "MetadataCi",
    )
)

_SKIP_DIRS = frozenset({"__pycache__", "node_modules", "target", ".pytest_cache", ".venv"})


def removed_name_hits(text: str) -> list[tuple[int, str]]:
    return [
        (number, match.group(0))
        for number, line in enumerate(text.splitlines(), start=1)
        for match in REMOVED.finditer(line)
    ]


def _files(target: Path) -> Iterator[Path]:
    if target.is_file():
        yield target
        return
    for path in sorted(target.rglob("*")):
        if path.is_file() and not _SKIP_DIRS.intersection(path.relative_to(target).parts):
            yield path


def scan_removed_names(root: Path = REPO_ROOT) -> dict[str, list[str]]:
    offenders: dict[str, list[str]] = {}
    for scanned in SCANNED:
        for path in _files(root / scanned):
            relative = path.relative_to(root).as_posix()
            if relative in EXEMPT:
                continue
            try:
                text = path.read_text()
            except UnicodeDecodeError:
                continue
            hits = removed_name_hits(text)
            if hits:
                offenders[relative] = [f"{line}: {name}" for line, name in hits]
    return offenders


def test_no_shipped_surface_reads_a_removed_factory_setting() -> None:
    assert scan_removed_names() == {}, (
        "configure the factory through factory.* / CURIE_FORGES (#4397); "
        f"only {GUARD_FILE} and {ENV_GUARD_FILE} may name the removed keys"
    )


def test_the_exemption_is_exactly_the_guard_files() -> None:
    assert EXEMPT == {GUARD_FILE, ENV_GUARD_FILE}
    assert all((REPO_ROOT / path).is_file() for path in EXEMPT)


def test_the_guard_file_lists_every_removed_chart_key() -> None:
    listed = yaml.safe_load((REPO_ROOT / GUARD_FILE).read_text())
    assert isinstance(listed, list)
    assert all(isinstance(key, str) and key.startswith("api.githubFactory") for key in listed)
    assert len(listed) == len(set(listed))
    assert BASE_CHART_KEYS <= set(listed)


def test_the_env_guard_file_lists_every_removed_env_name() -> None:
    listed = yaml.safe_load((REPO_ROOT / ENV_GUARD_FILE).read_text())
    assert sorted(listed) == sorted(field.upper() for field in REMOVED_FIELDS)


def test_every_scanned_surface_exists() -> None:
    # A renamed directory would make the gate pass vacuously.
    assert [path for path in SCANNED if not (REPO_ROOT / path).exists()] == []


# Mutation self-tests: the scanner is live -------------------------------------------


def test_a_removed_name_pasted_into_the_api_template_fails_the_gate(tmp_path: Path) -> None:
    template = REPO_ROOT / "charts/curie/templates/api.yaml"
    mutated = tmp_path / "charts/curie/templates/api.yaml"
    mutated.parent.mkdir(parents=True)
    mutated.write_text(
        template.read_text()
        + "\n            - name: GITHUB_FACTORY_LABEL\n"
        + "              value: {{ .Values.api.githubFactoryLabel | quote }}\n"
    )

    offenders = scan_removed_names(tmp_path)
    assert list(offenders) == ["charts/curie/templates/api.yaml"]
    assert {hit.split(": ")[1] for hit in offenders["charts/curie/templates/api.yaml"]} == {
        "GITHUB_FACTORY_LABEL",
        "githubFactoryLabel",
    }


@pytest.mark.parametrize(
    "injected",
    [
        "label = settings.github_factory_label\n",
        "interval = settings.github_factory_poll_interval_s\n",
        'os.environ["GITHUB_FACTORY_BASES"]\n',
        "values['api']['githubFactoryIntake']\n",
    ],
)
def test_the_scanner_catches_each_removed_name_form(injected: str) -> None:
    assert removed_name_hits("import os\n" + injected) != []


def test_the_scanner_leaves_kept_names_alone() -> None:
    clean = (
        "from curie_api import github_factory_review, github_factory_events\n"
        "enabled = settings.factory_enabled and settings.factory_intake == 'poll'\n"
        "env = {'FACTORY_ENABLED': 'true', 'CURIE_FORGES': '{}'}\n"
        "app = settings.github_app_id\n"
    )
    assert removed_name_hits(clean) == []


# The install-wide GitHub credential stays out of the factory -------------------------

CREDENTIAL_FIELDS = frozenset(
    {"github_app_id", "github_app_private_key", "github_token", "github_api_url"}
)
NON_FACTORY_READERS: frozenset[str] = frozenset(
    {
        "commitpoller.py",
        "gitflow.py",
        "github_app.py",
        "github_review_store.py",
        "github_review_truth.py",
        "main.py",
        "routers/workspaces.py",
        "routers/hooks.py",
        "publication_authority.py",
        "publication_truth.py",
        "routers/publications.py",
        "routers/publication_precheck.py",
        "routers/github.py",
        "crud/publications.py",
        "crud/lineages.py",
        "config.py",
    }
)


def factory_module(relative: str) -> bool:
    parts = relative.split("/")
    name = parts[-1]
    return (
        parts[0] == "forges"
        or any(part.startswith("workitem") for part in parts)
        or name.startswith(("factory_", "github_factory"))
        or name in {"lineage_reconciler.py", "issue_read.py"}
    )


def credential_reads(source: str, path: str = "example.py") -> list[str]:
    """Each ``credentials_for(...)`` call and each read of a credential field
    off any object (``settings.github_token``, ``get_settings().github_api_url``)."""

    found: list[str] = []
    for node in ast.walk(ast.parse(source, filename=path)):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name == "credentials_for":
                found.append(f"{node.lineno}: credentials_for(")
        elif isinstance(node, ast.Attribute) and node.attr in CREDENTIAL_FIELDS:
            found.append(f"{node.lineno}: .{node.attr}")
    return found


def _credential_offenders() -> dict[str, list[str]]:
    offenders: dict[str, list[str]] = {}
    for path in sorted(API_PACKAGE.rglob("*.py")):
        relative = path.relative_to(API_PACKAGE).as_posix()
        reads = credential_reads(path.read_text(), relative)
        if reads:
            offenders[relative] = reads
    return offenders


def test_the_install_credential_is_read_only_by_non_factory_modules() -> None:
    outside = {
        path: reads
        for path, reads in _credential_offenders().items()
        if path not in NON_FACTORY_READERS
    }
    assert outside == {}, "factory paths take credentials from the forge registry (#4397)"


def test_no_factory_module_reads_the_install_credential() -> None:
    factory = {
        path: reads for path, reads in _credential_offenders().items() if factory_module(path)
    }
    assert factory == {}


def test_the_non_factory_list_names_no_factory_module_and_only_real_files() -> None:
    assert [path for path in NON_FACTORY_READERS if factory_module(path)] == []
    assert sorted(p for p in NON_FACTORY_READERS if not (API_PACKAGE / p).is_file()) == []


def test_the_factory_module_rule_covers_the_factory_paths() -> None:
    for path in (
        "factory_poll_intake.py",
        "factory_ci.py",
        "github_factory.py",
        "github_factory_review.py",
        "workitem_reconciler.py",
        "workitems/lifecycle.py",
        "lineage_reconciler.py",
        "issue_read.py",
        "forges/github/ci.py",
        "forges/registry.py",
    ):
        assert factory_module(path), path
    assert not factory_module("commitpoller.py")


@pytest.mark.parametrize(
    "injected",
    [
        "token = credentials_for(settings).token_for(repo)\n",
        "label = settings.github_factory_label\napp = settings.github_app_id\n",
        "api = get_settings().github_api_url\n",
        "from curie_api import github_app\nr = github_app.credentials_for(s)\n",
    ],
)
def test_the_credential_scanner_catches_an_injected_factory_read(injected: str) -> None:
    source = (API_PACKAGE / "factory_poll_intake.py").read_text() + "\n" + injected
    assert credential_reads(source, "factory_poll_intake.py") != []


def test_a_non_factory_read_in_commitpoller_passes() -> None:
    source = (API_PACKAGE / "commitpoller.py").read_text()
    source += (
        "\ndef _probe(settings):\n    return settings.github_token, credentials_for(settings)\n"
    )
    assert credential_reads(source, "commitpoller.py") != []
    assert "commitpoller.py" in NON_FACTORY_READERS
    assert not factory_module("commitpoller.py")
