#!/usr/bin/env python3
"""Drive the dark factory end to end against a disposable install (#2966).

`curie dev factory-e2e preflight` installs the candidate's published images in
a throwaway namespace on a kube context the operator names, enables signed
factory intake, exposes the api webhook through a temporary tunnel, points a
GitHub App's webhook at it with an App JWT, resets a fixture repository, labels
one issue, and asserts that GitHub delivered the event, the api accepted it, and
a WorkItem was admitted (read back through the api work-items route). Every
change is undone on exit and each undo is verified, and the result is a JSON
evidence file.

Before the issue is labelled, the driver deploys the default dark-factory
bundle (examples/dark-factory, #2576) onto the factory agent it binds, with a
short-lived issues:read installation token as the bundle's GitHub MCP
credential, and sets the agent's publication policy to auto so an unattended
run publishes without a human. With CURIE_FACTORY_MODEL_API_KEY set the
install runs a real model (DEFAULT_MODEL unless CURIE_FACTORY_MODEL names
another) with the 1800 second execution bound; without it the model is fake.

`curie dev factory-e2e run --scenario <name>` runs one scenario driver after
the preflight. `issue-to-pr --issue-file <file> [--expect pr|comment|any]`
(and `--expect-cause CAUSE`, repeatable)
opens the operator's ticket as the one labelled issue, waits for the run to
end, and judges the ending: exactly one pull request or one terminus comment,
an accepted terminus cause, no `.github/` file or credential in the diff,
the default branch untouched, and
the run inside its bound. The other scenarios (revision, cancel-waiting,
cancel-running, evaluation) have no driver yet and refuse before anything is
installed.

The App, fixture repository, mention author and model credentials come only
from operator files or environment variables; nothing here names a real one.
Standard library only, so it runs from a bare source checkout.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import fcntl
import http.client
import json
import os
import re
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

GITHUB_API = "https://api.github.com"
GHCR = "https://ghcr.io"
IMAGE_OWNER = "curie-eng"
# The components the chart pins by tag. The runner is pinned separately under
# agentSandbox.runner.tag.
CHART_COMPONENTS = {
    "api": "curie-api",
    "worker": "curie-worker",
    "dispatcher": "curie-dispatcher",
    "mailAdapter": "curie-mail-adapter",
    "ui": "curie-ui",
}
RUNNER_IMAGE = "curie-runner"
RELEASE = "curie"
DEFAULT_LABEL = "curie-factory"
NAMESPACE_PREFIX = "test-factory-"
APP_KEY_REF = "factory-e2e-github-app"
SANDBOX_CRD = "sandboxes.agents.x-k8s.io"
OWNER_LABEL = "app.kubernetes.io/managed-by=curie-factory-e2e"
RUN_ANNOTATION = "curie.dev/factory-e2e-run"
LOCK_DIR = Path.home() / ".cache" / "curie-factory-e2e"
FACTORY_AGENT = "factory-e2e"
DEFAULT_MODEL = "z-ai/glm-5.3"
DEFAULT_BUNDLE = Path(__file__).resolve().parents[2] / "examples" / "dark-factory"
OPENROUTER_HOST = "openrouter.ai"
OPENROUTER_KEY_URL = f"https://{OPENROUTER_HOST}/api/v1/key"
# The ExecutionRequest deadline (ADR 0162) and the chart's maximum budget.
EXECUTION_BOUND_SECONDS = 1800
# Wait allowance after the execution deadline for publication and the notice.
PUBLICATION_ALLOWANCE_SECONDS = 600
# A run that never starts is still given up on after this long from labelling.
NEVER_STARTED_CAP_SECONDS = 3600
# The judged bound: the execution deadline plus terminal settlement slack.
ELAPSED_LIMIT_SECONDS = EXECUTION_BOUND_SECONDS + 300
POLL_SECONDS = 15
EXPECTATIONS = ("pr", "comment", "any")
# The terminus causes docs/operations.md documents for a factory comment.
TERMINUS_CAUSES = (
    "capacity_wait_expired",
    "execution_deadline",
    "issue_cancelled",
    "owner_lost",
    "runner_escalated",
    "runner_failed",
    "no_pull_request",
    "publication_denied",
    "publication_expired",
    "publication_failed",
)
DEFAULT_COMMENT_CAUSES = frozenset({"no_pull_request"})
DEFAULT_ANY_COMMENT_CAUSES = frozenset({"no_pull_request", "execution_deadline"})
FINAL_REPLY_LIMIT = 4000
# The dark-factory bundle's contract for a run that opens no pull request.
_REASON_CONTRACT = re.compile(r"could not complete:\s*\S", re.IGNORECASE)
# Mirrors marker_for and comment_body in apps/api/src/curie_api/factory_notices.py.
_NOTICE_MARKER = re.compile(r"<!-- curie-execution-request:([0-9a-fA-F-]{36}) -->")
_NOTICE_CAUSE = re.compile(r"^Cause: (\S+)\s*$", re.MULTILINE)
ACTIVE_REQUEST_STATUSES = frozenset({"waiting", "running", "cancellation_requested"})
_CREDENTIAL_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"ghp_[A-Za-z0-9]{36}",
        r"github_pat_[A-Za-z0-9_]{20,}",
        r"gh[osu]_[A-Za-z0-9]{36}",
        r"sk-or-v1-[A-Za-z0-9]{20,}",
        r"sk-ant-[A-Za-z0-9_-]{20,}",
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    )
)

EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_SCENARIO = 3

_NAMESPACE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_REPO = re.compile(r"^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$")
_TUNNEL_URL = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
# cloudflared logs its own control host (api.trycloudflare.com) before the
# quick-tunnel URL; only a generated, hyphenated subdomain is the tunnel.
_QUICK_TUNNEL_URL = re.compile(r"https://(?!api\.)[a-z0-9]+(?:-[a-z0-9]+)+\.trycloudflare\.com")


def quick_tunnel_url(line: str) -> str | None:
    match = _QUICK_TUNNEL_URL.search(line)
    return match.group(0) if match else None


class ConfigError(Exception):
    """An operator input is missing or malformed. Nothing live was touched."""


class ScenarioUnavailable(Exception):
    """The named scenario has no driver yet."""


class PreflightFailed(Exception):
    """A live step did not produce the expected observation."""


# --------------------------------------------------------------------------
# Scenario hooks. Each driver receives the live Preflight after its own
# assertions pass and runs inside the same teardown. None means "not written
# yet": the command refuses before installing anything.
# --------------------------------------------------------------------------

ScenarioDriver = Callable[["Preflight"], dict[str, Any]]

SCENARIOS: dict[str, ScenarioDriver | None] = {
    "issue-to-pr": None,  # bound below, once issue_to_pr is defined
    "revision": None,
    "cancel-waiting": None,
    "cancel-running": None,
    "evaluation": None,
}
SCENARIO_NAMES = tuple(SCENARIOS)


def resolve_scenario(name: str) -> ScenarioDriver:
    driver = SCENARIOS.get(name)
    if driver is None:
        raise ScenarioUnavailable(
            f"scenario {name!r} has no driver yet; only `preflight` and "
            "`run --scenario issue-to-pr` run today. "
            "Add the driver to SCENARIOS in tools/factory-e2e/factory_e2e.py."
        )
    return driver


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class FactoryConfig:
    kube_context: str
    app_id: str
    installation_id: int
    private_key_file: Path
    repo: str
    label: str
    mention: str
    cloudflared: str
    priority_classes: tuple[str, str] | None
    restore_webhook_url: str | None
    webhook_secret: str = dataclasses.field(repr=False)
    actor_token: str = dataclasses.field(repr=False)
    model_api_key: str | None = dataclasses.field(default=None, repr=False)
    model: str = DEFAULT_MODEL
    bundle_dir: Path = DEFAULT_BUNDLE
    curie_bin: str = "curie"


def _read_secret_file(path: Path) -> str | None:
    try:
        value = path.read_text().strip()
    except OSError:
        return None
    return value or None


def gh_token_for_user(user: str) -> str:
    result = subprocess.run(
        ["gh", "auth", "token", "--user", user],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return ""
    return result.stdout.strip()


def load_config(
    env: Mapping[str, str],
    *,
    context: str | None,
    gh_token: Callable[[str], str] = gh_token_for_user,
) -> FactoryConfig:
    """Read every operator input, naming ALL missing ones in one refusal.

    CURIE_FACTORY_APP_DIR may hold app.json (id, slug, installation_id and an
    optional repo), app.pem and webhook_secret; the individual variables win
    over it. Secret values are read, never echoed.
    """

    missing: list[str] = []
    app_dir = (
        Path(env["CURIE_FACTORY_APP_DIR"]).expanduser()
        if env.get("CURIE_FACTORY_APP_DIR")
        else None
    )
    meta: dict[str, Any] = {}
    if app_dir is not None:
        try:
            meta = json.loads((app_dir / "app.json").read_text())
        except (OSError, ValueError):
            meta = {}

    kube_context = context or env.get("CURIE_FACTORY_KUBE_CONTEXT", "")
    if not kube_context:
        missing.append("CURIE_FACTORY_KUBE_CONTEXT (or --context)")

    app_id = env.get("CURIE_FACTORY_APP_ID") or str(meta.get("id") or "")
    if not app_id.isdigit():
        missing.append("CURIE_FACTORY_APP_ID")

    installation = env.get("CURIE_FACTORY_INSTALLATION_ID") or str(
        meta.get("installation_id") or ""
    )
    if not installation.isdigit():
        missing.append("CURIE_FACTORY_INSTALLATION_ID")

    key_file = env.get("CURIE_FACTORY_APP_PRIVATE_KEY_FILE") or (
        str(app_dir / "app.pem") if app_dir else ""
    )
    key_path = Path(key_file).expanduser() if key_file else Path()
    if not key_file or not key_path.is_file():
        missing.append("CURIE_FACTORY_APP_PRIVATE_KEY_FILE")

    secret_file = env.get("CURIE_FACTORY_WEBHOOK_SECRET_FILE") or (
        str(app_dir / "webhook_secret") if app_dir else ""
    )
    webhook_secret = _read_secret_file(Path(secret_file).expanduser()) if secret_file else None
    if not webhook_secret:
        missing.append("CURIE_FACTORY_WEBHOOK_SECRET_FILE")

    repo = env.get("CURIE_FACTORY_REPO") or str(meta.get("repo") or "")
    if not _REPO.fullmatch(repo):
        missing.append("CURIE_FACTORY_REPO (owner/name of the fixture repository)")

    actor_token = env.get("CURIE_FACTORY_ACTOR_TOKEN", "")
    if not actor_token and env.get("CURIE_FACTORY_ACTOR_GH_USER"):
        actor_token = gh_token(env["CURIE_FACTORY_ACTOR_GH_USER"])
    if not actor_token:
        missing.append(
            "CURIE_FACTORY_ACTOR_TOKEN (or CURIE_FACTORY_ACTOR_GH_USER with a gh login): "
            "a human account with write access to the fixture repository"
        )

    label = env.get("CURIE_FACTORY_LABEL") or DEFAULT_LABEL
    mention = env.get("CURIE_FACTORY_MENTION") or str(meta.get("slug") or "")
    if not mention:
        missing.append("CURIE_FACTORY_MENTION (the login the factory answers to)")

    priority_classes: tuple[str, str] | None = None
    if env.get("CURIE_FACTORY_PRIORITY_CLASSES"):
        parts = [p.strip() for p in env["CURIE_FACTORY_PRIORITY_CLASSES"].split(",")]
        if len(parts) != 2 or not all(parts):
            raise ConfigError(
                "CURIE_FACTORY_PRIORITY_CLASSES must be '<platform>,<sandbox>' "
                "naming two existing PriorityClasses"
            )
        priority_classes = (parts[0], parts[1])

    bundle_dir = (
        Path(env["CURIE_FACTORY_BUNDLE_DIR"]).expanduser()
        if env.get("CURIE_FACTORY_BUNDLE_DIR")
        else DEFAULT_BUNDLE
    )
    if not bundle_dir.is_dir():
        missing.append(
            "CURIE_FACTORY_BUNDLE_DIR (a plugin bundle directory; default examples/dark-factory)"
        )

    if missing:
        raise ConfigError(
            "missing required factory credential or setting: "
            + "; ".join(missing)
            + ". Set them in the environment, or point CURIE_FACTORY_APP_DIR at a "
            "directory holding app.json, app.pem and webhook_secret."
        )
    assert webhook_secret is not None
    return FactoryConfig(
        kube_context=kube_context,
        app_id=app_id,
        installation_id=int(installation),
        private_key_file=key_path,
        repo=repo,
        label=label,
        mention=mention,
        cloudflared=env.get("CURIE_FACTORY_CLOUDFLARED") or "cloudflared",
        priority_classes=priority_classes,
        restore_webhook_url=env.get("CURIE_FACTORY_WEBHOOK_RESTORE_URL") or None,
        webhook_secret=webhook_secret,
        actor_token=actor_token,
        model_api_key=env.get("CURIE_FACTORY_MODEL_API_KEY") or None,
        model=env.get("CURIE_FACTORY_MODEL") or DEFAULT_MODEL,
        bundle_dir=bundle_dir,
        curie_bin=env.get("CURIE_FACTORY_CURIE_BIN") or "curie",
    )


def parse_issue_file(path: Path) -> tuple[str, str]:
    """The scenario ticket: first non-blank line is the title (a leading
    Markdown heading marker is dropped), the rest is the body. Both required."""

    try:
        text = path.read_text()
    except OSError as exc:
        raise ConfigError(f"cannot read the issue file {path}: {exc.strerror}") from None
    lines = text.strip().splitlines()
    title = lines[0].lstrip("#").strip() if lines else ""
    body = "\n".join(lines[1:]).strip()
    if not title or not body:
        raise ConfigError(f"the issue file {path} needs a title line followed by a non-empty body")
    return title, body


def validate_namespace(name: str) -> str:
    if not name.startswith(NAMESPACE_PREFIX) or len(name) > 40 or not _NAMESPACE.fullmatch(name):
        raise ConfigError(
            f"namespace {name!r} must be a lowercase RFC 1123 name starting with "
            f"{NAMESPACE_PREFIX!r}, at most 40 characters: the driver deletes it on exit"
        )
    return name


def default_namespace(candidate: str) -> str:
    return validate_namespace(NAMESPACE_PREFIX + candidate[:8].lower())


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def app_jwt(app_id: str, key_file: Path, *, now: int | None = None) -> str:
    """A GitHub App JWT (RS256, 9 minute life, iat backdated for clock skew)."""

    issued = int(time.time()) if now is None else now
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = {"iat": issued - 60, "exp": issued + 540, "iss": app_id}
    payload = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{payload}".encode()
    signed = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(key_file)],
        input=signing_input,
        capture_output=True,
        check=False,
    )
    if signed.returncode != 0:
        raise ConfigError("openssl could not sign the App JWT with the configured private key")
    return f"{header}.{payload}.{_b64url(signed.stdout)}"


def request_id_for(repository_id: int, issue_number: int) -> uuid.UUID:
    """The execution request id the api derives for a label admission."""

    identity = f"https://github.com/factory/label/{repository_id}/{issue_number}"
    return uuid.uuid5(uuid.NAMESPACE_URL, identity)


def match_delivery(
    deliveries: list[dict[str, Any]], *, issue_number: int, repo: str
) -> dict[str, Any] | None:
    """The newest `issues.labeled` delivery for this issue, from detailed deliveries."""

    found = None
    for delivery in deliveries:
        if delivery.get("event") != "issues" or delivery.get("action") != "labeled":
            continue
        payload = (delivery.get("request") or {}).get("payload") or {}
        issue = payload.get("issue") or {}
        repository = payload.get("repository") or {}
        if issue.get("number") == issue_number and repository.get("full_name") == repo:
            found = delivery
    return found


def delivery_api_status(delivery: dict[str, Any]) -> str | None:
    body = (delivery.get("response") or {}).get("payload")
    if not isinstance(body, str):
        return None
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    status = parsed.get("status") if isinstance(parsed, dict) else None
    return status if isinstance(status, str) else None


def install_values(
    config: FactoryConfig,
    *,
    candidate: str,
    app_key_secret: str,
    consumer_controller: bool,
    egress_cidrs: Sequence[str] = (),
) -> dict[str, Any]:
    """Helm values for the disposable install. Written to a 0600 file, never argv."""

    tag = f"sha-{candidate}"
    values: dict[str, Any] = {component: {"image": {"tag": tag}} for component in CHART_COMPONENTS}
    values["api"].update(
        {
            "githubWebhookSecret": config.webhook_secret,
            "githubFactoryIngressEnabled": True,
            "githubFactoryLabel": config.label,
            "githubFactoryMention": config.mention,
            "githubAppId": config.app_id,
            "githubAppExistingSecret": app_key_secret,
            "githubRepoAllowlist": [config.repo],
        }
    )
    values["agentSandbox"] = {
        "runner": {"tag": tag},
        "controller": {"deploy": not consumer_controller},
    }
    # A disposable install proves the factory flow, not sandbox isolation, and
    # most scratch clusters carry no gVisor runtime class.
    values["security"] = {"gvisor": {"mode": "off"}}
    if egress_cidrs:
        values["security"]["networkPolicy"] = {
            "allowedEgress": [
                {"cidr": cidr, "ports": [{"protocol": "TCP", "port": 443}]} for cidr in egress_cidrs
            ]
        }
    if config.model_api_key:
        values["agentSandbox"]["runner"].update(
            {"fakeModel": False, "model": config.model, "credentials": config.model_api_key}
        )
        # The chart maximum, so the 1800 s ExecutionRequest deadline and not
        # the default 600 s worker budget bounds the run. The runner ceiling
        # must not exceed the delivery budget.
        values["worker"]["deliveryBudgetSeconds"] = EXECUTION_BOUND_SECONDS
        values["worker"]["runnerTotalTimeoutSeconds"] = EXECUTION_BOUND_SECONDS
    if config.priority_classes is not None:
        platform, sandbox = config.priority_classes
        values["priorityClasses"] = {
            "platform": {"create": False, "name": platform},
            "sandbox": {"create": False, "name": sandbox},
        }
    return values


def judge_outcome(
    outcome: Mapping[str, Any],
    expect: str,
    *,
    expect_causes: frozenset[str] | set[str] | None = None,
    expect_reasons: Sequence[str] = (),
    secrets: Sequence[str | None] = (),
) -> list[str]:
    """Every way an issue-to-pr ending falls short. Empty means it passed.

    Pure. ``outcome`` carries terminal, pull_requests (each with number,
    files and diff), terminus_comments (a count), ending_cause,
    default_branch_moved and elapsed_seconds. A comment ending passes only
    when its cause is in ``expect_causes``; the default is no_pull_request for
    expect "comment" and no_pull_request or execution_deadline for "any".
    A credential match is reported by pattern, never quoted.
    """

    if expect not in EXPECTATIONS:
        raise ValueError(f"expect must be one of {EXPECTATIONS}, not {expect!r}")
    failures: list[str] = []
    if not outcome.get("terminal"):
        failures.append("the run did not reach a terminal ending within the wait")
    prs = list(outcome.get("pull_requests") or [])
    comments = int(outcome.get("terminus_comments") or 0)
    if len(prs) > 1:
        failures.append(f"{len(prs)} pull requests were opened; at most one is allowed")
    if prs and comments:
        failures.append("the run both opened a pull request and posted a terminus comment")
    if not prs and not comments:
        failures.append("the run ended with neither a pull request nor a terminus comment")
    if expect == "pr" and not prs:
        failures.append("expected a pull request, none was opened")
    if comments > 1:
        failures.append(f"{comments} terminus comments were posted; at most one is allowed")
    if expect == "comment" and (prs or not comments):
        failures.append("expected a terminus comment and no pull request")
    if comments and not prs and expect != "pr":
        if expect_causes:
            allowed = frozenset(expect_causes)
        elif expect == "comment":
            allowed = DEFAULT_COMMENT_CAUSES
        else:
            allowed = DEFAULT_ANY_COMMENT_CAUSES
        cause = outcome.get("ending_cause")
        if cause not in allowed:
            failures.append(f"the run ended with cause {cause!r}, not one of {sorted(allowed)}")
        if cause == "no_pull_request":
            reply = outcome.get("agent_final_reply")
            if reply is None:
                failures.append(
                    "the agent's stated reason is unverified: its final reply was not observable"
                )
            elif not _REASON_CONTRACT.search(str(reply)):
                failures.append(
                    "the agent's final reply does not state 'Could not complete:' and a reason"
                )
            else:
                for reason in expect_reasons:
                    if not re.search(reason, str(reply), re.IGNORECASE):
                        failures.append(f"the agent's final reply does not match {reason!r}")
    if outcome.get("agent_reply_disclosed_credential"):
        failures.append("the agent's recorded content disclosed a credential")
    for pr in prs:
        number = pr.get("number")
        names = [*(pr.get("files") or []), *(pr.get("previous_filenames") or [])]
        github = [f for f in names if str(f).startswith(".github/")]
        if github:
            shown = [redact_agent_text(str(f), secrets)[0] for f in github]
            failures.append(f"pull request #{number} changes files under .github/: {shown}")
        for where, texts in _pr_texts(pr).items():
            for text in texts:
                if any(secret and secret in text for secret in secrets):
                    failures.append(f"pull request #{number} {where} contains a known secret")
                for pattern in _CREDENTIAL_PATTERNS:
                    if pattern.search(text):
                        failures.append(
                            f"pull request #{number} {where} matches credential pattern "
                            f"{pattern.pattern!r}"
                        )
    if outcome.get("default_branch_moved"):
        failures.append("the default branch moved during the run")
    elapsed = outcome.get("elapsed_seconds")
    if elapsed is None:
        failures.append("the run's elapsed time could not be measured")
    elif float(elapsed) > ELAPSED_LIMIT_SECONDS:
        failures.append(
            f"the run took {float(elapsed):.1f}s, over the {ELAPSED_LIMIT_SECONDS}s bound"
        )
    return failures


def _parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _redact(text: str, secrets: Sequence[str | None]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[redacted]")
    return text


def redact_agent_text(text: str | None, secrets: Sequence[str | None]) -> tuple[str | None, bool]:
    """Agent content with known secrets and credential-shaped strings replaced.

    Pure. Returns the redacted text and whether anything was replaced.
    """

    if text is None:
        return None, False
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    for pattern in _CREDENTIAL_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)
    return redacted, redacted != text


def record_agent_text(text: str | None, secrets: Sequence[str | None]) -> tuple[str | None, bool]:
    """Agent text as evidence keeps it: redacted in full, then truncated. Pure."""

    redacted, disclosed = redact_agent_text(text, secrets)
    return (redacted[:FINAL_REPLY_LIMIT] if redacted is not None else None), disclosed


def _pr_texts(pr: Mapping[str, Any]) -> dict[str, list[str]]:
    return {
        "title": [str(pr.get("title") or "")],
        "body": [str(pr.get("body") or "")],
        "diff": [str(pr.get("diff") or "")],
        "file name": [str(f) for f in pr.get("files") or []]
        + [str(f) for f in pr.get("previous_filenames") or []],
    }


def pr_evidence(pr: Mapping[str, Any], secrets: Sequence[str | None]) -> dict[str, Any]:
    """A pull request as evidence keeps it: no diff, every text redacted. Pure."""

    def clean(value: Any) -> Any:
        return redact_agent_text(str(value), secrets)[0] if value is not None else None

    kept = {k: v for k, v in pr.items() if k != "diff"}
    for key in ("title", "body"):
        kept[key] = clean(pr.get(key))
    for key in ("files", "previous_filenames"):
        kept[key] = [clean(f) for f in pr.get(key) or []]
    return kept


def pr_file_names(entries: Sequence[Mapping[str, Any]]) -> tuple[list[str], list[str]]:
    """(filenames, previous filenames of renames) from GitHub's PR files. Pure."""

    files = [str(f.get("filename")) for f in entries]
    previous = [str(f["previous_filename"]) for f in entries if f.get("previous_filename")]
    return files, previous


class Teardown:
    """LIFO undo stack. Every step runs even when an earlier one fails."""

    def __init__(self) -> None:
        self._steps: list[tuple[str, Callable[[], Any]]] = []

    def push(self, name: str, step: Callable[[], Any]) -> None:
        self._steps.append((name, step))

    def run(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        while self._steps:
            name, step = self._steps.pop()
            try:
                detail = step()
                results.append({"step": name, "ok": True, "detail": detail})
            except BaseException as exc:  # noqa: BLE001 - keep undoing
                results.append(
                    {"step": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}"}
                )
        return results


# --------------------------------------------------------------------------
# Process and HTTP plumbing
# --------------------------------------------------------------------------


def log(message: str) -> None:
    print(
        f"[factory-e2e {dt.datetime.now(dt.UTC):%H:%M:%S}] {message}", file=sys.stderr, flush=True
    )


def run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, input=input_text, check=False)
    if check and result.returncode != 0:
        tail = (result.stderr or result.stdout).strip()[-1500:]
        raise PreflightFailed(f"{argv[0]} {argv[1] if len(argv) > 1 else ''} failed: {tail}")
    return result.stdout


def http_json(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    body: Any = None,
    timeout: float = 30,
) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status = exc.code
    if not raw:
        return status, None
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, raw.decode(errors="replace")


def http_text(
    url: str, *, headers: Mapping[str, str] | None = None, timeout: float = 60
) -> tuple[int, str]:
    """GET a raw text body (a unified diff, for one)."""

    request = urllib.request.Request(url, method="GET")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors="replace")


def public_get_status(url: str, *, timeout: float = 10) -> int:
    """GET a public https URL, resolving its host through DNS-over-HTTPS.

    A fresh quick-tunnel host answers NXDOMAIN for its first seconds, and a
    local caching resolver then keeps serving that NXDOMAIN long after GitHub
    can reach the tunnel. Resolving through a public resolver avoids waiting
    out the negative cache; TLS still verifies against the real host name.
    """

    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    status, answer = http_json(
        "GET",
        f"https://cloudflare-dns.com/dns-query?name={host}&type=A",
        headers={"Accept": "application/dns-json"},
        timeout=timeout,
    )
    records = answer.get("Answer") if status == 200 and isinstance(answer, dict) else None
    addresses = [r["data"] for r in records or [] if r.get("type") == 1]
    if not addresses:
        return 0
    raw = socket.create_connection((addresses[0], 443), timeout=timeout)
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    tls = context.wrap_socket(raw, server_hostname=host)
    connection = http.client.HTTPSConnection(host, timeout=timeout)
    connection.sock = tls
    try:
        connection.request("GET", parsed.path or "/", headers={"Host": host})
        return connection.getresponse().status
    finally:
        connection.close()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait(what: str, timeout: float, probe: Callable[[], Any], interval: float = 3) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() > deadline:
            raise PreflightFailed(f"timed out after {int(timeout)}s waiting for {what}")
        time.sleep(interval)


def _stop(process: subprocess.Popen[Any]) -> bool:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=15)
    return process.poll() is not None


# --------------------------------------------------------------------------
# The live preflight
# --------------------------------------------------------------------------


class Preflight:
    def __init__(
        self,
        config: FactoryConfig,
        *,
        repo_root: Path,
        candidate: str,
        namespace: str,
        evidence_path: Path,
        admission_timeout: float,
        issue_spec: tuple[str, str] | None = None,
        expect: str = "any",
        expect_causes: Sequence[str] = (),
        expect_reasons: Sequence[str] = (),
    ) -> None:
        if expect not in EXPECTATIONS:
            raise ConfigError(f"--expect must be one of {EXPECTATIONS}")
        self.issue_spec = issue_spec
        self.expect = expect
        self.expect_causes = frozenset(expect_causes)
        self.expect_reasons = tuple(expect_reasons)
        self.config = config
        self.repo_root = repo_root
        self.candidate = candidate
        self.namespace = namespace
        self.evidence_path = evidence_path
        self.admission_timeout = admission_timeout
        self.teardown = Teardown()
        self.run_id = uuid.uuid4().hex[:12]
        self.created_crds: list[str] = []
        self.evidence: dict[str, Any] = {
            "schema": "curie.factory-e2e.evidence/v1",
            "mode": "preflight",
            "candidate_commit": candidate,
            "image_tag": f"sha-{candidate}",
            "kube_context": config.kube_context,
            "namespace": namespace,
            "release": RELEASE,
            "run_id": "",
            "started_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "steps": [],
        }
        self.workdir = Path(tempfile.mkdtemp(prefix="factory-e2e-"))
        os.chmod(self.workdir, 0o700)
        self.api_url = ""
        self.api_key = ""
        self.worker_token = ""
        self.issue_token = ""
        self.tunnel_url = ""
        self.repository_id = 0
        self.default_branch = ""
        self.chart_dir: Path | None = None
        self.issue_number = 0
        self.labelled_at = 0.0
        self.scenario_started: dt.datetime | None = None
        self.head_before = ""
        self.usage_before: float | None = None

    # --- small wrappers -------------------------------------------------

    def kubectl(self, *args: str, check: bool = True) -> str:
        return run(["kubectl", "--context", self.config.kube_context, *args], check=check)

    def step(self, name: str, **facts: Any) -> None:
        log(name)
        self.evidence["steps"].append({"step": name, **facts})

    def github(self, method: str, path: str, *, token: str, body: Any = None) -> tuple[int, Any]:
        return http_json(
            method,
            GITHUB_API + path,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            body=body,
        )

    def as_actor(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        return self.github(method, path, token=self.config.actor_token, body=body)

    def as_app(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        token = app_jwt(self.config.app_id, self.config.private_key_file)
        return self.github(method, path, token=token, body=body)

    def api(
        self, method: str, path: str, *, headers: Mapping[str, str], body: Any = None
    ) -> tuple[int, Any]:
        return http_json(method, self.api_url + path, headers=headers, body=body)

    # --- steps ---------------------------------------------------------

    def check_tools(self) -> None:
        wanted = [
            "kubectl",
            "helm",
            "git",
            "openssl",
            self.config.cloudflared,
            self.config.curie_bin,
        ]
        absent = [tool for tool in wanted if shutil.which(tool) is None]
        if absent:
            raise ConfigError(f"required tools not on PATH: {', '.join(absent)}")

    def check_images(self) -> None:
        tag = f"sha-{self.candidate}"
        missing = []
        for image in [*CHART_COMPONENTS.values(), RUNNER_IMAGE]:
            scope = urllib.parse.quote(f"repository:{IMAGE_OWNER}/{image}:pull", safe="")
            status, body = http_json("GET", f"{GHCR}/token?scope={scope}&service=ghcr.io")
            token = body.get("token") if status == 200 and isinstance(body, dict) else None
            request = urllib.request.Request(
                f"{GHCR}/v2/{IMAGE_OWNER}/{image}/manifests/{tag}", method="HEAD"
            )
            request.add_header(
                "Accept",
                "application/vnd.oci.image.index.v1+json,"
                "application/vnd.docker.distribution.manifest.list.v2+json,"
                "application/vnd.oci.image.manifest.v1+json",
            )
            if token:
                request.add_header("Authorization", f"Bearer {token}")
            try:
                with urllib.request.urlopen(request, timeout=30):
                    pass
            except urllib.error.HTTPError:
                missing.append(image)
        if missing:
            raise PreflightFailed(
                f"no published {tag} image for {', '.join(missing)}; the candidate must be a "
                "commit the release workflow built (a push to main or next)"
            )
        self.step("candidate images published", tag=tag)

    def check_app(self) -> None:
        status, body = self.as_app("GET", f"/app/installations/{self.config.installation_id}")
        if status != 200:
            raise PreflightFailed(
                f"the App JWT could not read installation {self.config.installation_id} "
                f"(HTTP {status}); check CURIE_FACTORY_APP_ID and the private key"
            )
        status, body = self.as_actor("GET", f"/repos/{self.config.repo}")
        if status != 200 or not isinstance(body, dict):
            raise PreflightFailed(
                f"the actor token cannot read the fixture repository (HTTP {status})"
            )
        permissions = body.get("permissions") or {}
        if not (permissions.get("push") or permissions.get("admin")):
            raise PreflightFailed("the actor account needs write access to the fixture repository")
        self.repository_id = int(body["id"])
        self.default_branch = str(body["default_branch"])
        self.evidence["fixture_repository_id"] = self.repository_id
        self.step("App JWT and actor token verified")

    def extract_chart(self) -> Path:
        run(["git", "-C", str(self.repo_root), "fetch", "--quiet", "origin", self.candidate])
        archive = self.workdir / "chart.tar"
        run(
            [
                "git",
                "-C",
                str(self.repo_root),
                "archive",
                "--output",
                str(archive),
                self.candidate,
                "charts/curie",
            ]
        )
        run(["tar", "-xf", str(archive), "-C", str(self.workdir)])
        return self.workdir / "charts" / "curie"

    def _namespace_absent(self, name: str) -> bool:
        return not self.kubectl(
            "get", "namespace", name, "--ignore-not-found", "-o", "name"
        ).strip()

    def create_namespace(self) -> None:
        self.evidence["run_id"] = self.run_id
        for name in (self.namespace, self.publication_namespace()):
            if not self._namespace_absent(name):
                raise ConfigError(
                    f"namespace {name} already exists; the driver only uses namespaces it "
                    "creates. Delete it or pass another --namespace."
                )
        # Registered before the create call: an ambiguous create (the server
        # applied it, the client lost the answer) is still reconciled, and the
        # run annotation keeps the undo from touching anyone else's namespace.
        self.teardown.push("delete namespaces", self.delete_namespaces)
        manifest = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": self.namespace,
                "labels": dict([OWNER_LABEL.split("=", 1)]),
                "annotations": {RUN_ANNOTATION: self.run_id},
            },
        }
        run(
            ["kubectl", "--context", self.config.kube_context, "create", "-f", "-"],
            input_text=json.dumps(manifest),
        )
        self.step("namespace created", namespace=self.namespace)

    def publication_namespace(self) -> str:
        return f"{self.namespace}-{RELEASE}-publication"

    def _owned(self, name: str) -> bool:
        raw = self.kubectl("get", "namespace", name, "--ignore-not-found", "-o", "json")
        if not raw.strip():
            return False
        annotations = json.loads(raw)["metadata"].get("annotations") or {}
        if name == self.namespace:
            return bool(annotations.get(RUN_ANNOTATION) == self.run_id)
        # The publication namespace is the release's own object, and the release
        # is ours only while the parent namespace carries this run's marker.
        return bool(
            annotations.get("meta.helm.sh/release-namespace") == self.namespace
            and self._owned(self.namespace)
        )

    def delete_namespaces(self) -> dict[str, Any]:
        names = [self.namespace, self.publication_namespace()]
        owned = [name for name in names if self._owned(name)]
        uninstall_failed = ""
        if self.namespace in owned:
            uninstall = subprocess.run(
                [
                    "helm",
                    "--kube-context",
                    self.config.kube_context,
                    "uninstall",
                    RELEASE,
                    "-n",
                    self.namespace,
                    "--no-hooks",
                    "--wait",
                    "--timeout",
                    "5m",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if uninstall.returncode != 0 and "not found" not in uninstall.stderr:
                uninstall_failed = uninstall.stderr.strip()[-500:]
        for name in owned:
            self.kubectl("delete", "namespace", name, "--ignore-not-found", "--wait=false")
        _wait("namespace deletion", 600, lambda: all(map(self._namespace_absent, owned)), 5)
        # Helm keeps chart CRDs on uninstall; remove only the ones this run added.
        for crd in self.created_crds:
            self.kubectl("delete", "crd", crd, "--ignore-not-found", "--wait=true")
        leftover = [
            crd
            for crd in self.created_crds
            if self.kubectl("get", "crd", crd, "--ignore-not-found", "-o", "name").strip()
        ]
        if leftover:
            raise PreflightFailed(f"CRDs this run created are still present: {leftover}")
        # Cluster-scoped release objects outlive the namespace; verify them.
        kinds = "clusterroles,clusterrolebindings,priorityclasses"
        cluster_left = [
            item["metadata"]["name"]
            for item in json.loads(self.kubectl("get", kinds, "-o", "json"))["items"]
            if (item["metadata"].get("annotations") or {}).get("meta.helm.sh/release-namespace")
            == self.namespace
        ]
        if cluster_left or uninstall_failed:
            raise PreflightFailed(
                f"release objects remain {cluster_left}; helm uninstall: {uninstall_failed or 'ok'}"
            )
        return {
            "deleted": owned,
            "crds_deleted": self.created_crds,
            "verified_absent": True,
        }

    def install(self) -> None:
        chart = self.extract_chart()
        consumer = bool(
            self.kubectl("get", "crd", SANDBOX_CRD, "--ignore-not-found", "-o", "name").strip()
        )
        for manifest in sorted((chart / "crds").glob("*.yaml")):
            match = re.search(r"^  name:\s*(\S+)", manifest.read_text(), re.MULTILINE)
            if (
                match
                and not self.kubectl(
                    "get", "crd", match.group(1), "--ignore-not-found", "-o", "name"
                ).strip()
            ):
                self.created_crds.append(match.group(1))
        key_file = str(self.config.private_key_file)
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "generic",
            APP_KEY_REF,
            f"--from-file=privateKey={key_file}",
        )
        self.chart_dir = chart
        egress_cidrs = self.egress_cidrs()
        values = install_values(
            self.config,
            candidate=self.candidate,
            app_key_secret=APP_KEY_REF,
            consumer_controller=consumer,
            egress_cidrs=egress_cidrs,
        )
        values_file = self.workdir / "values.json"
        fd = os.open(values_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(values, handle)
        log("helm install (this pulls every image; allow several minutes)")
        run(
            [
                "helm",
                "--kube-context",
                self.config.kube_context,
                "install",
                RELEASE,
                str(chart),
                "-n",
                self.namespace,
                "-f",
                str(values_file),
                "--timeout",
                "20m",
            ]
        )
        values_file.unlink()
        # No `helm --wait`: schema migration is a post-install hook, and the
        # api's init container waits for that schema, so --wait deadlocks.
        # Helm still blocks on the hook jobs; the workloads are awaited here.
        for kind in ("deployment", "statefulset"):
            for workload in self.kubectl("-n", self.namespace, "get", kind, "-o", "name").split():
                self.kubectl("-n", self.namespace, "rollout", "status", workload, "--timeout=15m")
        self.step(
            "installed",
            chart="charts/curie@candidate",
            sandbox_controller="existing (consumer mode)" if consumer else "deployed by release",
            factory_ingress=True,
            model=self.config.model if self.config.model_api_key else "fake",
            sandbox_egress_cidrs=len(egress_cidrs),
        )

    def egress_cidrs(self) -> list[str]:
        """Sandbox egress: GitHub's API ranges (the bundle's GitHub MCP server
        calls it from the sandbox) and, with a real model, OpenRouter."""

        status, meta = http_json("GET", GITHUB_API + "/meta")
        ranges = meta.get("api") if status == 200 and isinstance(meta, dict) else None
        if not isinstance(ranges, list) or not ranges:
            raise PreflightFailed(f"could not read GitHub's API address ranges (HTTP {status})")
        cidrs = [str(cidr) for cidr in ranges if ":" not in str(cidr)]
        if self.config.model_api_key:
            try:
                infos = socket.getaddrinfo(OPENROUTER_HOST, 443, socket.AF_INET, socket.SOCK_STREAM)
            except OSError as exc:
                raise PreflightFailed(f"could not resolve {OPENROUTER_HOST}: {exc}") from None
            for address in sorted({str(info[4][0]) for info in infos}):
                cidrs.append(f"{address}/32")
        return cidrs

    def port_forward(self) -> None:
        port = _free_port()
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.config.kube_context,
                "-n",
                self.namespace,
                "port-forward",
                f"svc/{RELEASE}-api",
                f"{port}:8000",
                "--address",
                "127.0.0.1",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.teardown.push("stop api port-forward", lambda: {"stopped": _stop(process)})
        self.api_url = f"http://127.0.0.1:{port}"

        def healthy() -> bool:
            try:
                status, _ = http_json("GET", self.api_url + "/health", timeout=5)
            except OSError:
                return False
            return status == 200

        _wait("the api through port-forward", 120, healthy)
        secret = json.loads(
            self.kubectl("-n", self.namespace, "get", "secret", f"{RELEASE}-secrets", "-o", "json")
        )
        data = secret["data"]
        self.api_key = base64.b64decode(data["apiKey"]).decode()
        self.worker_token = base64.b64decode(data["internalWorkerToken"]).decode()
        self.step("api reachable")

    def bind_agent(self) -> None:
        status, body = self.api(
            "POST",
            "/agents",
            headers={"X-API-Key": self.api_key},
            body={
                "name": FACTORY_AGENT,
                "repo_full_name": self.config.repo,
                "channel": {"kind": "github", "address": self.config.repo},
            },
        )
        if status != 201 or not isinstance(body, dict):
            raise PreflightFailed(f"agent creation failed (HTTP {status}): {body}")
        self.evidence["agent_id"] = body.get("id")
        self.step("factory agent bound to the fixture repository", agent_id=body.get("id"))
        self.deploy_bundle()

    def _write_kubeconfig(self) -> Path:
        result = subprocess.run(
            [
                "kubectl",
                "config",
                "view",
                "--minify",
                "--flatten",
                "--context",
                self.config.kube_context,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise PreflightFailed(
                f"kubectl config view failed for context {self.config.kube_context}: "
                f"{result.stderr.strip()[-500:]}"
            )
        path = self.workdir / "kubeconfig"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(result.stdout)
        return path

    def _issue_read_token(self) -> str:
        """An installation token for the fixture repository with issues:read only."""

        repo_name = self.config.repo.split("/", 1)[1]
        status, body = self.as_app(
            "POST",
            f"/app/installations/{self.config.installation_id}/access_tokens",
            {"repositories": [repo_name], "permissions": {"issues": "read"}},
        )
        token = body.get("token") if status == 201 and isinstance(body, dict) else None
        if not isinstance(token, str) or not token:
            raise PreflightFailed(
                f"minting the issues:read installation token failed (HTTP {status})"
            )
        granted = body.get("permissions") if isinstance(body, dict) else None
        if (
            not isinstance(granted, dict)
            or granted.get("issues") != "read"
            or not set(granted) <= {"issues", "metadata"}
            or any(level != "read" for level in granted.values())
        ):
            raise PreflightFailed(
                f"the installation token was granted {granted}, not only issues:read"
            )
        return token

    def _curie(self, args: list[str], env: Mapping[str, str], secrets: list[str | None]) -> None:
        # --context is an option of `curie cluster`, so it follows that word.
        argv = [self.config.curie_bin, args[0], "--context", self.config.kube_context, *args[1:]]
        result = subprocess.run(
            argv, capture_output=True, text=True, env={**os.environ, **env}, check=False
        )
        if result.returncode != 0:
            tail = _redact((result.stderr or result.stdout).strip(), secrets)[-1500:]
            raise PreflightFailed(f"curie {' '.join(args[:2])} failed: {tail}")

    def _curie_config_dir(self) -> Path:
        path = self.workdir / "curie-config"
        path.mkdir(mode=0o700, exist_ok=True)
        return path

    def deploy_bundle(self) -> None:
        """Deploy the default dark-factory bundle onto the bound agent and let
        the platform resolve its publication approval (policy auto)."""

        assert self.chart_dir is not None, "install() extracts the chart first"
        bundle = self.config.bundle_dir
        manifest = json.loads((bundle / ".claude-plugin" / "plugin.json").read_text())
        kubeconfig = self._write_kubeconfig()
        token = self._issue_read_token()
        env = {
            "KUBECONFIG": str(kubeconfig),
            "CURIE_API_KEY": self.api_key,
            "GITHUB_PERSONAL_ACCESS_TOKEN": token,
            # The secret arrives through the environment; an empty private
            # config dir keeps the operator's own vault out of this run.
            "CURIE_CONFIG_DIR": str(self._curie_config_dir()),
        }
        self.issue_token = token
        secrets = [token, self.api_key, self.config.model_api_key, self.worker_token]
        common = ["--namespace", self.namespace, "--release", RELEASE, "--api-url", self.api_url]
        log("curie cluster deploy (the default dark-factory bundle)")
        self._curie(
            [
                "cluster",
                "deploy",
                "--plugin-dir",
                str(bundle),
                "--agent",
                FACTORY_AGENT,
                "--env",
                "prod",
                "--chart",
                str(self.chart_dir),
                "--secret",
                "GITHUB_PERSONAL_ACCESS_TOKEN",
                *common,
            ],
            env,
            secrets,
        )
        self._curie(
            ["cluster", "publication-policy", FACTORY_AGENT, "--policy", "auto", *common],
            env,
            secrets,
        )
        try:
            shown = str(bundle.resolve().relative_to(self.repo_root.resolve()))
        except ValueError:
            shown = bundle.name
        self.evidence["bundle"] = {
            "name": manifest.get("name"),
            "version": manifest.get("version"),
            "path": shown,
            "model": self.config.model if self.config.model_api_key else "fake",
            "publication_policy": "auto",
            "issue_read_token": {
                "permissions": {"issues": "read"},
                "repository": self.config.repo,
                "held": "in memory only; passed to curie through the environment",
            },
        }
        self.step(
            "default bundle deployed",
            bundle=f"{manifest.get('name')}@{manifest.get('version')}",
            publication_policy="auto",
        )

    def tunnel(self) -> None:
        process = subprocess.Popen(
            [
                self.config.cloudflared,
                "tunnel",
                "--no-autoupdate",
                "--url",
                self.api_url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def stop() -> dict[str, Any]:
            return {"stopped": _stop(process), "exit_code": process.returncode}

        self.teardown.push("stop tunnel", stop)
        found: list[str] = []

        def reader() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                url = quick_tunnel_url(line)
                if url and not found:
                    found.append(url)

        threading.Thread(target=reader, daemon=True).start()
        self.tunnel_url = _wait("the tunnel URL", 90, lambda: found[0] if found else None, 1)

        def reachable() -> bool:
            try:
                return public_get_status(self.tunnel_url + "/health") == 200
            except OSError:
                return False

        _wait("the api through the tunnel", 180, reachable, 5)
        self.step("tunnel up")

    def _lock(self, name: str, holds: str) -> None:
        """Serialize runs on this machine that share an App or a kube context.

        Taken before any fixture or cluster mutation and released last, so a
        refused run changes nothing and CRD ownership is never contested.
        """

        LOCK_DIR.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
        handle = open(LOCK_DIR / f"{safe}.lock", "w")  # noqa: SIM115
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise ConfigError(f"another factory-e2e run on this machine holds {holds}") from None

        def unlock() -> dict[str, Any]:
            fcntl.flock(handle, fcntl.LOCK_UN)
            handle.close()
            return {"released": True}

        self.teardown.push(f"release lock {safe}", unlock)

    def repoint_webhook(self) -> None:
        status, original = self.as_app("GET", "/app/hook/config")
        if status != 200 or not isinstance(original, dict):
            raise PreflightFailed(f"could not read the App webhook config (HTTP {status})")
        if (
            _TUNNEL_URL.search(str(original.get("url") or ""))
            and not self.config.restore_webhook_url
        ):
            raise PreflightFailed(
                "the App webhook already points at a quick tunnel (another run, or one that "
                "died); set CURIE_FACTORY_WEBHOOK_RESTORE_URL so this run restores a real URL"
            )
        restore = {
            "url": self.config.restore_webhook_url or original.get("url"),
            "content_type": original.get("content_type") or "json",
        }

        def restore_webhook() -> dict[str, Any]:
            status, _ = self.as_app("PATCH", "/app/hook/config", restore)
            if status != 200:
                raise PreflightFailed(f"restoring the App webhook failed (HTTP {status})")
            status, now = self.as_app("GET", "/app/hook/config")
            if status != 200 or not isinstance(now, dict) or now.get("url") != restore["url"]:
                raise PreflightFailed("the App webhook URL did not read back as restored")
            return {"restored": True, "verified": True}

        self.teardown.push("restore App webhook", restore_webhook)
        target = self.tunnel_url + "/github/webhook"
        status, _ = self.as_app(
            "PATCH", "/app/hook/config", {"url": target, "content_type": "json"}
        )
        if status != 200:
            raise PreflightFailed(f"pointing the App webhook at the tunnel failed (HTTP {status})")
        status, now = self.as_app("GET", "/app/hook/config")
        if status != 200 or not isinstance(now, dict) or now.get("url") != target:
            raise PreflightFailed("the App webhook URL did not read back as the tunnel")
        self.step("App webhook repointed at the tunnel")

    def _paged(self, path: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        while True:
            sep = "&" if "?" in path else "?"
            status, body = self.as_actor("GET", f"{path}{sep}per_page=100&page={page}")
            if status != 200 or not isinstance(body, list):
                raise PreflightFailed(f"GitHub list {path} failed (HTTP {status})")
            items.extend(body)
            if len(body) < 100:
                return items
            page += 1

    def reset_fixture(self) -> dict[str, Any]:
        repo = f"/repos/{self.config.repo}"
        closed = 0
        for issue in self._paged(f"{repo}/issues?state=open"):
            status, _ = self.as_actor(
                "PATCH", f"{repo}/issues/{issue['number']}", {"state": "closed"}
            )
            if status != 200:
                raise PreflightFailed(f"closing #{issue['number']} failed (HTTP {status})")
            closed += 1
        deleted = 0
        for branch in self._paged(f"{repo}/branches"):
            name = branch["name"]
            if name == self.default_branch:
                continue
            ref = urllib.parse.quote(name, safe="/")
            status, _ = self.as_actor("DELETE", f"{repo}/git/refs/heads/{ref}")
            if status != 204:
                raise PreflightFailed(f"deleting a fixture branch failed (HTTP {status})")
            deleted += 1

        # GitHub's list endpoints lag a close or delete by a few seconds.
        def clean() -> bool:
            open_left = self._paged(f"{repo}/issues?state=open")
            branches_left = [b["name"] for b in self._paged(f"{repo}/branches")]
            return not open_left and branches_left == [self.default_branch]

        try:
            _wait("the fixture repository to read back as reset", 60, clean, 5)
        except PreflightFailed:
            raise PreflightFailed("the fixture repository did not read back as reset") from None
        return {"closed": closed, "branches_deleted": deleted, "verified_clean": True}

    def ensure_label(self) -> None:
        repo = f"/repos/{self.config.repo}"
        name = urllib.parse.quote(self.config.label, safe="")
        status, _ = self.as_actor("GET", f"{repo}/labels/{name}")
        if status == 404:
            status, _ = self.as_actor(
                "POST", f"{repo}/labels", {"name": self.config.label, "color": "5319e7"}
            )
            if status != 201:
                raise PreflightFailed(f"creating the factory label failed (HTTP {status})")
        elif status != 200:
            raise PreflightFailed(f"reading the factory label failed (HTTP {status})")

    def open_labelled_issue(self) -> int:
        if self.issue_spec is not None:
            title, body_text = self.issue_spec
        else:
            title = f"factory preflight {self.namespace}"
            body_text = "Opened by `curie dev factory-e2e preflight`. Closed on teardown."
        status, body = self.as_actor(
            "POST",
            f"/repos/{self.config.repo}/issues",
            {"title": title, "body": body_text, "labels": [self.config.label]},
        )
        if status != 201 or not isinstance(body, dict):
            raise PreflightFailed(f"opening the fixture issue failed (HTTP {status})")
        number = int(body["number"])
        self.issue_number = number
        self.evidence["issue_number"] = number
        self.step("labelled issue opened", issue_number=number)
        return number

    def await_delivery(self, issue_number: int, since: float) -> dict[str, Any]:
        seen: set[str] = set()
        details: list[dict[str, Any]] = []

        def probe() -> dict[str, Any] | None:
            status, listing = self.as_app("GET", "/app/hook/deliveries?per_page=50")
            if status != 200 or not isinstance(listing, list):
                return None
            for item in listing:
                delivered = dt.datetime.fromisoformat(item["delivered_at"].replace("Z", "+00:00"))
                key = str(item["id"])
                if key in seen or delivered.timestamp() < since - 30:
                    continue
                if item.get("event") != "issues" or item.get("action") != "labeled":
                    continue
                status, detail = self.as_app("GET", f"/app/hook/deliveries/{key}")
                if status == 200 and isinstance(detail, dict):
                    seen.add(key)
                    details.append(detail)
            return match_delivery(details, issue_number=issue_number, repo=self.config.repo)

        found: dict[str, Any] = _wait(
            "the labelled-issue delivery", self.admission_timeout, probe, 5
        )
        return found

    def assert_admission(self, issue_number: int, since: float) -> None:
        delivery = self.await_delivery(issue_number, since)
        api_status = delivery_api_status(delivery)
        self.evidence["delivery_id"] = delivery.get("guid")
        self.evidence["delivery_status_code"] = delivery.get("status_code")
        self.evidence["delivery_api_status"] = api_status
        if delivery.get("status_code") != 200 or api_status != "factory_admitted":
            raise PreflightFailed(
                f"delivery {delivery.get('guid')} was not accepted: HTTP "
                f"{delivery.get('status_code')}, api status {api_status!r}"
            )
        self.step("delivery accepted", delivery_id=delivery.get("guid"))
        request_id = request_id_for(self.repository_id, issue_number)
        status, body = self.api(
            "GET",
            f"/v1/internal/work-items/requests/{request_id}",
            headers={"X-Curie-Worker-Token": self.worker_token},
        )
        if status != 200 or not isinstance(body, dict) or not body.get("work_item_id"):
            raise PreflightFailed(f"no WorkItem request {request_id} (HTTP {status}): {body}")
        self.evidence["execution_request_id"] = str(request_id)
        self.evidence["work_item_id"] = body["work_item_id"]
        self.evidence["execution_request_status"] = body.get("status")
        self.step("WorkItem admitted", work_item_id=body["work_item_id"])

    # --- orchestration --------------------------------------------------

    def run(self, scenario: ScenarioDriver | None) -> None:
        self.check_tools()
        self._lock(f"app-{self.config.app_id}", "this App and its fixture repository")
        self._lock(f"context-{self.config.kube_context}", "this kube context")
        self.check_images()
        self.check_app()
        self.create_namespace()
        self.install()
        self.port_forward()
        self.bind_agent()
        # Reset BEFORE the webhook points here, so the closures stay off the
        # install; the after-reset is pushed now so it runs once the webhook
        # is already restored.
        self.step("fixture reset before", **self.reset_fixture())
        self.teardown.push("reset fixture repository", self.reset_fixture)
        self.ensure_label()
        self.tunnel()
        self.repoint_webhook()
        if scenario is not None:
            self.record_baseline()
        since = time.time()
        self.labelled_at = since
        issue = self.open_labelled_issue()
        self.assert_admission(issue, since)
        if scenario is not None:
            self.evidence["scenario"] = scenario(self)

    def default_branch_head(self) -> str:
        ref = urllib.parse.quote(self.default_branch, safe="/")
        status, body = self.as_actor("GET", f"/repos/{self.config.repo}/git/ref/heads/{ref}")
        sha = (body.get("object") or {}).get("sha") if isinstance(body, dict) else None
        if status != 200 or not isinstance(sha, str):
            raise PreflightFailed(f"could not read the default branch head (HTTP {status})")
        return sha

    def model_usage(self) -> float | None:
        """OpenRouter's cumulative USD usage for the model key, or None."""

        if not self.config.model_api_key:
            return None
        try:
            status, body = http_json(
                "GET",
                OPENROUTER_KEY_URL,
                headers={"Authorization": f"Bearer {self.config.model_api_key}"},
            )
        except OSError:
            return None
        data = body.get("data") if status == 200 and isinstance(body, dict) else None
        usage = data.get("usage") if isinstance(data, dict) else None
        return float(usage) if isinstance(usage, (int, float)) else None

    def record_baseline(self) -> None:
        self.scenario_started = dt.datetime.now(dt.UTC).replace(microsecond=0)
        self.head_before = self.default_branch_head()
        self.usage_before = self.model_usage()
        self.step("scenario baseline recorded", default_branch_head=self.head_before)

    def write_evidence(self) -> None:
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.evidence_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(self.evidence, handle, indent=2, default=str)
            handle.write("\n")


# --------------------------------------------------------------------------
# Scenario: issue-to-pr
# --------------------------------------------------------------------------


def _app_authored(comment: Mapping[str, Any], mention: str, app_id: str) -> bool:
    user = comment.get("user") or {}
    app = comment.get("performed_via_github_app") or {}
    return user.get("login") == f"{mention}[bot]" or (
        bool(app_id) and str(app.get("id") or "") == str(app_id)
    )


def match_terminus_comments(
    comments: Sequence[Mapping[str, Any]],
    *,
    mention: str,
    app_id: str,
    request_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """The configured App's terminus notices for these execution requests.

    Pure. A comment counts only when the App authored it and it carries the
    execution request marker for one of ``request_ids``. The cause is parsed
    from the notice body.
    """

    wanted = {str(r).lower() for r in request_ids}
    matched: list[dict[str, Any]] = []
    for comment in comments:
        if not _app_authored(comment, mention, app_id):
            continue
        body = str(comment.get("body") or "")
        marker = _NOTICE_MARKER.search(body)
        if marker is None or marker.group(1).lower() not in wanted:
            continue
        cause = _NOTICE_CAUSE.search(body)
        matched.append(
            {
                "body": body,
                "cause": cause.group(1) if cause else None,
                "created_at": comment.get("created_at"),
                "request_id": marker.group(1),
            }
        )
    return matched


def _terminus_comments(p: Preflight) -> list[dict[str, Any]]:
    comments = p._paged(f"/repos/{p.config.repo}/issues/{p.issue_number}/comments")
    # The work item detail omits request ids; a label admission has one,
    # derived and recorded at admission.
    ids = [str(p.evidence.get("execution_request_id") or "")]
    return match_terminus_comments(
        comments, mention=p.config.mention, app_id=p.config.app_id, request_ids=ids
    )


def ending_times(
    request: Mapping[str, Any], *, labelled_at: float, ended_at: Any
) -> tuple[float, float | None]:
    """(elapsed_seconds, execution_seconds) for one run. Pure.

    Elapsed runs from the request's start (or labelling) to the observed
    ending: the pull request's creation or the terminus comment. Execution
    runs from start to the request's terminal_at, when both exist.
    """

    started = _parse_time(request.get("started_at"))
    terminal = _parse_time(request.get("terminal_at"))
    ended = _parse_time(ended_at)
    begin = started.timestamp() if started is not None else labelled_at
    end = ended.timestamp() if ended is not None else time.time()
    execution = (terminal - started).total_seconds() if started is not None and terminal else None
    return end - begin, execution


def final_agent_reply(value: Any) -> str | None:
    """The last turn's full assistant text in a transcript value. Pure.

    Not truncated: record_agent_text redacts the whole text before cutting it.
    """

    if not isinstance(value, list):
        return None
    for record in reversed(value):
        if isinstance(record, dict) and record.get("type") == "turn":
            text = str(record.get("assistant") or "")
            return text or None
    return None


def _agent_final_reply(p: Preflight) -> tuple[str | None, str]:
    # The work item detail does not carry its conversation id, so read the
    # agent's transcript namespace. The install and agent are this run's own,
    # so exactly one transcript is expected; anything else is left unjudged.
    agent_id = p.evidence.get("agent_id")
    if not agent_id:
        return None, "no agent id was recorded"
    path = f"/agents/{agent_id}/state/transcript"
    status, body = p.api("GET", path, headers={"X-API-Key": p.api_key})
    if status != 200 or not isinstance(body, list):
        return None, f"api GET {path} returned HTTP {status}"
    if len(body) != 1 or not isinstance(body[0], dict):
        return None, f"api GET {path} returned {len(body)} transcripts, expected one"
    return final_agent_reply(body[0].get("value")), f"api GET {path}, last turn"


def _latest_request(detail: Mapping[str, Any]) -> dict[str, Any] | None:
    requests = [r for r in detail.get("requests") or [] if isinstance(r, dict)]
    return max(requests, key=lambda r: int(r.get("sequence") or 0)) if requests else None


def _scenario_pull_requests(p: Preflight) -> list[dict[str, Any]]:
    repo = f"/repos/{p.config.repo}"
    status, listing = p.as_actor(
        "GET", f"{repo}/pulls?state=all&sort=created&direction=desc&per_page=100"
    )
    if status != 200 or not isinstance(listing, list):
        raise PreflightFailed(f"listing fixture pull requests failed (HTTP {status})")
    started = p.scenario_started
    prs: list[dict[str, Any]] = []
    for item in listing:
        created = _parse_time(item.get("created_at"))
        if started is not None and (created is None or created < started):
            continue
        number = int(item["number"])
        files, previous = pr_file_names(p._paged(f"{repo}/pulls/{number}/files"))
        status, detail = p.as_actor("GET", f"{repo}/pulls/{number}")
        detail = detail if status == 200 and isinstance(detail, dict) else {}
        status, diff = http_text(
            f"{GITHUB_API}{repo}/pulls/{number}",
            headers={
                "Authorization": f"Bearer {p.config.actor_token}",
                "Accept": "application/vnd.github.diff",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        if status != 200:
            raise PreflightFailed(f"reading the diff of pull request #{number} failed")
        prs.append(
            {
                "number": number,
                "url": item.get("html_url"),
                "created_at": item.get("created_at"),
                "files": files,
                "previous_filenames": previous,
                "title": detail.get("title", item.get("title")),
                "body": detail.get("body", item.get("body")),
                "additions": detail.get("additions"),
                "deletions": detail.get("deletions"),
                "diff": diff,
            }
        )
    return prs


def usage_record(
    before: float | None,
    read_after: Callable[[], float | None],
    *,
    has_key: bool,
    attempts: int = 12,
    pause: float = 10,
) -> dict[str, Any]:
    """The model spend for one run, or an explicit `unverified`.

    OpenRouter's key usage counter lags a finished request, so the reading is
    retried until it moves. A delta that never becomes positive is not
    reported as zero spend: it is unverified.
    """

    if not has_key:
        return {"source": "unverified", "usd": None, "caveat": "fake model; no model spend"}
    after: float | None = None
    for attempt in range(attempts):
        after = read_after() if before is not None else None
        if before is None or after is None or after > before:
            break
        if attempt < attempts - 1:
            time.sleep(pause)
    if before is None or after is None:
        caveat = "OpenRouter key usage could not be read before and after the run"
    elif after <= before:
        caveat = "the OpenRouter key usage counter did not change after the run"
    else:
        return {
            "source": "openrouter key usage delta",
            "usd": round(after - before, 6),
            "caveat": "the key is shared; the delta includes any concurrent use of the same key",
        }
    return {"source": "unverified", "usd": None, "caveat": caveat}


def issue_to_pr(p: Preflight) -> dict[str, Any]:
    """Wait for the labelled ticket's run to end, then judge its ending."""

    work_item_id = p.evidence["work_item_id"]
    detail: dict[str, Any] = {}
    comments: list[dict[str, Any]] = []
    terminal = False
    while True:
        status, body = p.api("GET", f"/work-items/{work_item_id}", headers={"X-API-Key": p.api_key})
        if status == 200 and isinstance(body, dict):
            detail = body
        requests = [r for r in detail.get("requests") or [] if isinstance(r, dict)]
        active = not requests or any(r.get("status") in ACTIVE_REQUEST_STATUSES for r in requests)
        if not active:
            comments = _terminus_comments(p)
            if detail.get("pr") or comments:
                terminal = True
                break
        latest = _latest_request(detail)
        started = _parse_time((latest or {}).get("started_at"))
        if started is not None:
            give_up = started.timestamp() + EXECUTION_BOUND_SECONDS + PUBLICATION_ALLOWANCE_SECONDS
        else:
            give_up = p.labelled_at + NEVER_STARTED_CAP_SECONDS
        if time.time() > give_up:
            log("issue-to-pr: the run did not end within the wait; judging what exists")
            comments = _terminus_comments(p)
            break
        time.sleep(POLL_SECONDS)

    prs = _scenario_pull_requests(p)
    latest = _latest_request(detail) or {}
    if prs:
        ended_at = min((str(pr.get("created_at") or "") for pr in prs), default=None)
    elif comments:
        ended_at = min(str(c.get("created_at") or "") for c in comments)
    else:
        ended_at = None
    elapsed, execution = ending_times(latest, labelled_at=p.labelled_at, ended_at=ended_at)
    ending_cause = latest.get("terminal_cause") or (comments[-1]["cause"] if comments else None)
    raw_reply, reply_source = _agent_final_reply(p)
    known = [p.issue_token, p.api_key, p.worker_token, p.config.model_api_key]
    reply, disclosed = record_agent_text(raw_reply, known)
    for comment in comments:
        comment["body"], hit = record_agent_text(comment["body"], known)
        disclosed = disclosed or hit
    moved = p.default_branch_head() != p.head_before
    outcome = {
        "terminal": terminal,
        "pull_requests": prs,
        "terminus_comments": len(comments),
        "ending_cause": ending_cause,
        "agent_final_reply": reply,
        "agent_reply_disclosed_credential": disclosed,
        "default_branch_moved": moved,
        "elapsed_seconds": round(elapsed, 1),
    }
    usage = usage_record(p.usage_before, p.model_usage, has_key=bool(p.config.model_api_key))
    pr = detail.get("pr") if isinstance(detail.get("pr"), dict) else None
    failures = judge_outcome(
        outcome,
        p.expect,
        expect_causes=p.expect_causes,
        expect_reasons=p.expect_reasons,
        secrets=known,
    )
    result = {
        "expect": p.expect,
        "expect_causes": sorted(p.expect_causes),
        "ending_cause": ending_cause,
        "terminal": terminal,
        "work_item_state": detail.get("state"),
        "actionable_cause": detail.get("actionable_cause"),
        "request_status": latest.get("status"),
        "terminal_cause": latest.get("terminal_cause"),
        "work_item_pr": pr,
        "pull_requests": [pr_evidence(item, known) for item in prs],
        "ci": detail.get("ci"),
        "terminus_comments": comments,
        "default_branch_moved": moved,
        "elapsed_seconds": outcome["elapsed_seconds"],
        "execution_seconds": round(execution, 1) if execution is not None else None,
        "agent_final_reply": reply,
        "agent_final_reply_source": reply_source,
        "agent_reply_disclosed_credential": disclosed,
        "expect_reasons": list(p.expect_reasons),
        "model": p.config.model if p.config.model_api_key else "fake",
        "usage": usage,
        "verdict": "passed" if not failures else "failed",
        "failures": failures,
    }
    p.evidence["scenario"] = result
    if failures:
        raise PreflightFailed("; ".join(failures))
    return result


SCENARIOS["issue-to-pr"] = issue_to_pr


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="curie dev factory-e2e",
        description="Drive the dark factory against a disposable install.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--context", help="kube context (default CURIE_FACTORY_KUBE_CONTEXT)")
    common.add_argument(
        "--candidate", help="commit whose published images to install (default: origin/next)"
    )
    common.add_argument("--namespace", help=f"owned namespace (default {NAMESPACE_PREFIX}<commit>)")
    common.add_argument(
        "--evidence",
        type=Path,
        help="evidence JSON path (default target/factory-e2e/<namespace>.json)",
    )
    common.add_argument(
        "--admission-timeout",
        type=float,
        default=300,
        help="seconds to wait for delivery and admission",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser(
        "preflight", parents=[common], help="install, deliver one labelled issue, assert admission"
    )
    scenario = sub.add_parser("run", parents=[common], help="preflight, then one scenario driver")
    scenario.add_argument("--scenario", required=True, choices=SCENARIO_NAMES)
    scenario.add_argument(
        "--issue-file",
        type=Path,
        help="issue-to-pr: Markdown ticket, first line the title, the rest the body",
    )
    scenario.add_argument(
        "--expect",
        choices=EXPECTATIONS,
        default="any",
        help="issue-to-pr: the ending the ticket should produce (default any)",
    )
    scenario.add_argument(
        "--expect-cause",
        action="append",
        choices=TERMINUS_CAUSES,
        default=[],
        help="issue-to-pr: a terminus cause a comment ending may carry (repeatable)",
    )
    scenario.add_argument(
        "--expect-reason",
        action="append",
        default=[],
        help="issue-to-pr: a case-insensitive regex the agent's stated reason must match",
    )
    return parser.parse_args(argv)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_candidate(repo_root: Path, requested: str | None) -> str:
    ref = requested or "refs/heads/next"
    if requested and re.fullmatch(r"[0-9a-f]{40}", requested):
        return requested
    out = run(["git", "-C", str(repo_root), "ls-remote", "origin", ref])
    sha = out.split()[0] if out.split() else ""
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ConfigError(f"could not resolve candidate {ref!r} on origin; pass a full commit")
    return sha


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    driver: ScenarioDriver | None = None
    issue_spec: tuple[str, str] | None = None
    expect = "any"
    expect_causes: list[str] = []
    expect_reasons: list[str] = []
    try:
        if args.mode == "run":
            driver = resolve_scenario(args.scenario)
            if args.scenario == "issue-to-pr":
                if args.issue_file is None:
                    print(
                        "factory-e2e: issue-to-pr needs --issue-file <markdown ticket>: "
                        "the title line and body of the one issue it labels",
                        file=sys.stderr,
                    )
                    return EXIT_CONFIG
                issue_spec = parse_issue_file(args.issue_file)
                expect = args.expect
                expect_causes = args.expect_cause
                expect_reasons = args.expect_reason
                for reason in expect_reasons:
                    try:
                        re.compile(reason)
                    except re.error as exc:
                        raise ConfigError(f"--expect-reason {reason!r}: {exc}") from exc
        config = load_config(os.environ, context=args.context)
        repo_root = _repo_root()
        candidate = _resolve_candidate(repo_root, args.candidate)
        namespace = (
            validate_namespace(args.namespace) if args.namespace else default_namespace(candidate)
        )
    except ScenarioUnavailable as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return EXIT_SCENARIO
    except (ConfigError, PreflightFailed) as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    evidence_path = args.evidence or repo_root / "target" / "factory-e2e" / f"{namespace}.json"
    preflight = Preflight(
        config,
        repo_root=repo_root,
        candidate=candidate,
        namespace=namespace,
        evidence_path=evidence_path,
        admission_timeout=args.admission_timeout,
        issue_spec=issue_spec,
        expect=expect,
        expect_causes=expect_causes,
        expect_reasons=expect_reasons,
    )
    if args.mode == "run":
        preflight.evidence["mode"] = f"run:{args.scenario}"

    def _terminate(signum: int, _frame: Any) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGHUP, _terminate)

    code = 0
    try:
        preflight.run(driver)
        preflight.evidence["result"] = "passed"
    except ConfigError as exc:
        preflight.evidence["result"] = "refused"
        preflight.evidence["error"] = str(exc)
        print(f"factory-e2e: {exc}", file=sys.stderr)
        code = EXIT_CONFIG
    except BaseException as exc:  # noqa: BLE001 - always tear down
        preflight.evidence["result"] = "failed"
        preflight.evidence["error"] = f"{type(exc).__name__}: {exc}"
        if not isinstance(exc, (PreflightFailed, KeyboardInterrupt, SystemExit)):
            traceback.print_exc()
        print(f"factory-e2e: FAILED: {exc}", file=sys.stderr)
        code = EXIT_FAILED
    finally:
        log("teardown")
        results = preflight.teardown.run()
        preflight.evidence["teardown"] = results
        preflight.evidence["teardown_clean"] = all(r["ok"] for r in results)
        preflight.evidence["finished_at"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
        shutil.rmtree(preflight.workdir, ignore_errors=True)
        preflight.write_evidence()
        for result in results:
            verdict = "ok" if result["ok"] else "FAILED"
            log(f"teardown {result['step']}: {verdict} {result['detail']}")
    if not preflight.evidence["teardown_clean"]:
        print("factory-e2e: teardown incomplete; see the evidence file", file=sys.stderr)
        code = code or EXIT_FAILED
    print(f"factory-e2e: {preflight.evidence['result']}; evidence {evidence_path}", file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
