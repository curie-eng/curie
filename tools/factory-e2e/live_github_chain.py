"""Live driver of the ``live-github-chain`` scenario (#5808).

``curie dev factory-e2e run --scenario live-github-chain`` runs this after the
shared preflight installed the candidate in a throwaway namespace. The judge
and the evidence sanitizer live in ``live_github_chain_judge.py``; this module
does the I/O and builds the judge's record from raw responses with pure
functions the tests call directly (a driver module that does the I/O beside
a judge module that decides).

Order, each stage a step of the evidence:

1. ``before_run`` (the first statement of ``Preflight.run``, before any lock or
   kubectl call): the configuration's sanitizer, the reader account id from
   CURIE_FACTORY_READER_ACCOUNT_ID, the fake model for ``--stop-after links``,
   the identities and the full sanitizer, the readiness of #5808's blockers
   (full mode refuses an unready candidate; ``--stop-after links`` records it
   and goes on), and a refusal of any open fixture issue or pull request.
2. ``install_values`` (after the namespace, before the install): the OIDC client
   Secret and a disposable Dex, and a values transformer that adds the API's
   ``CURIE_OIDC_*`` settings to the install and every upgrade.
   ``ensure_label`` replaces the shared intake label step: ``--stop-after
   links`` creates none, and the full mode deletes a label it created.
3. ``drive``: the first check (no open item, no work item), two real OIDC
   logins, the session probe, the code host links,
   then either the stop or beats (a) to (h).
4. ``finish`` (after teardown): the teardown section, the verdict, and the
   run's pass or fail.

Standard library only, like ``factory_e2e.py``.
"""

from __future__ import annotations

import copy
import dataclasses
import datetime as dt
import functools
import http.client
import json
import math
import os
import re
import secrets
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import factory_e2e as fe
import live_github_chain_judge as judge
import personal_two_owner_support as pto

SCENARIO = "live-github-chain"
HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "live-github-chain"

# --- Readiness (the blockers of #5808 on the public product repository) ------
PRODUCT_OWNER = "curie-eng"
PRODUCT_NAME = "curie"
PRODUCT_REPOSITORY = f"{PRODUCT_OWNER}/{PRODUCT_NAME}"
CHAIN_ISSUE = 5808
INTEGRATION_BRANCH = "next"
GRAPHQL_URL = "https://api.github.com/graphql"
BLOCKED_BY_HEADING = "Blocked by"
BODY_QUERY = (
    "query($owner: String!, $name: String!, $number: Int!) { "
    "repository(owner: $owner, name: $name) { issue(number: $number) { number body } } }"
)
# A pull request merged into a branch other than the default one never links
# as closing (GitHub links closing keywords for the default branch only), so
# the cross references are read too, and a pull request whose body says
# ``Closes #<n>`` counts as closing ``#<n>``.
BLOCKER_QUERY = (
    "query($owner: String!, $name: String!, $number: Int!) { "
    "repository(owner: $owner, name: $name) { issue(number: $number) { number state "
    "closedByPullRequestsReferences(first: 10, includeClosedPrs: true) { "
    "nodes { number state merged baseRefName baseRepository { nameWithOwner } "
    "mergeCommit { oid } } } "
    "timelineItems(itemTypes: [CROSS_REFERENCED_EVENT], first: 100) { "
    "nodes { ... on CrossReferencedEvent { source { __typename ... on PullRequest { "
    "number state merged baseRefName baseRepository { nameWithOwner } "
    "mergeCommit { oid } body } } } } } } } }"
)

# --- Operator inputs ------------------------------------------------------------
READER_ENV = "CURIE_FACTORY_READER_ACCOUNT_ID"
STOP_AFTER_LINKS = "links"

# --- Dex and the API's OIDC settings -------------------------------------------
DEX_NAME = "live-chain-dex"
DEX_LABEL_KEY = "app.kubernetes.io/name"
DEX_LABELS = {DEX_LABEL_KEY: DEX_NAME}
DEX_SELECTOR = f"{DEX_LABEL_KEY}={DEX_NAME}"
DEX_CONFIG_SECRET_NAME = "live-chain-dex-config"
OIDC_SECRET_NAME = "live-chain-oidc-client"
OIDC_SECRET_KEY = "client-secret"
WRITER_EMAIL = "writer@example.com"
READER_EMAIL = "reader@example.com"
ROLES = ("writer", "reader")
EMAILS = {"writer": WRITER_EMAIL, "reader": READER_EMAIL}
OIDC_PREFIX = "CURIE_OIDC_"

# --- Code host links -------------------------------------------------------------
# The identity host the resolver matches for github.com: ``forges/hosts.py``
# ``github_host`` (the authority of ``github_html_base``, default github.com),
# which ``resolve_github_requester`` compares with the namespace authority.
CODE_HOST = "github.com"

# --- Teardown step names (finish reads them back) -------------------------------
DEX_STEP = "delete live chain dex and oidc secret"
SESSION_STEP = "delete live chain session files"
LINKS_STEP = "revoke live chain code host links"
ISSUES_STEP = "close live chain issues"
PORT_FORWARD_STEP = "stop live chain dex port-forward"
DEFAULT_BRANCH_STEP = "restore live chain default branch"
LABEL_STEP = "delete live chain unbound label"
INTAKE_LABEL_STEP = "delete live chain intake label"

USER_AGENT = "curie-factory-e2e"
SPEND_SAMPLE_CAP = judge.SPEND_CAP_USD


class ChainStopped(fe.PreflightFailed):
    """A check of the scenario failed before any further live step."""


# --------------------------------------------------------------------------
# Run state
# --------------------------------------------------------------------------


@dataclasses.dataclass
class ChainRun:
    """What one run carries between its stages; never written to evidence whole."""

    stop_after: str | None = None
    attempt: int = 1
    reader_id: int | None = None
    readiness: Readiness | None = None
    identifiers: dict[str, list[str | int]] = dataclasses.field(default_factory=dict)
    record: dict[str, Any] = dataclasses.field(default_factory=dict)
    sessions: dict[str, Any] = dataclasses.field(default_factory=dict, repr=False)
    session_files: dict[str, Path] = dataclasses.field(default_factory=dict)
    tenant_id: str = ""
    link_ids: list[str] = dataclasses.field(default_factory=list)
    opened_issues: list[int] = dataclasses.field(default_factory=list)
    dex: dict[str, str] = dataclasses.field(default_factory=dict)


def chain_of(p: fe.Preflight) -> ChainRun:
    chain = p.scenario_options.get("chain")
    if not isinstance(chain, ChainRun):
        chain = ChainRun(
            stop_after=p.scenario_options.get("stop_after"),
            attempt=int(p.scenario_options.get("attempt") or 1),
        )
        p.scenario_options["chain"] = chain
    return chain


# --------------------------------------------------------------------------
# Readiness (pure core)
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Readiness:
    passed: bool
    missing: tuple[str, ...]
    blockers: tuple[dict[str, Any], ...]

    def evidence(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "missing": list(self.missing),
            "blockers": [dict(b) for b in self.blockers],
        }


def blocker_numbers(issue_body: str) -> tuple[int, ...]:
    """The ``#NNNN`` numbers under the issue's ``## Blocked by`` heading, in order."""

    seen: list[int] = []
    for line in judge.section_lines(issue_body, BLOCKED_BY_HEADING):
        for found in re.findall(r"#(\d+)\b", line):
            number = int(found)
            if number not in seen:
                seen.append(number)
    return tuple(seen)


def graphql_issue(response: Any) -> Mapping[str, Any]:
    """The ``repository.issue`` node of a GraphQL answer; ValueError otherwise."""

    if not isinstance(response, Mapping) or response.get("errors"):
        raise ValueError("the GraphQL answer carries errors")
    issue = ((response.get("data") or {}).get("repository") or {}).get("issue")
    if not isinstance(issue, Mapping):
        raise ValueError("the GraphQL answer has no issue")
    return issue


def _closes(body: Any, number: int) -> bool:
    pattern = rf"(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#{number}(?!\d)"
    return re.search(pattern, str(body or "")) is not None


def _base_repository(node: Mapping[str, Any]) -> str | None:
    base = node.get("baseRepository")
    name = base.get("nameWithOwner") if isinstance(base, Mapping) else None
    return str(name) if name else None


def closing_pulls(number: int, issue: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The pull requests that close ``#number``: GitHub's closing links plus cross
    referencing pull requests whose body says ``Closes #<number>``. Bodies are dropped.

    Only a pull request into the product repository counts: a fork's pull
    request into its own ``next`` that says ``Closes #<number>`` merges nothing
    the candidate can carry.
    """

    found: dict[int, dict[str, Any]] = {}

    def keep(node: Any) -> None:
        if not isinstance(node, Mapping) or not isinstance(node.get("number"), int):
            return
        if _base_repository(node) != PRODUCT_REPOSITORY:
            return
        merge = node.get("mergeCommit")
        found.setdefault(
            node["number"],
            {
                "number": node["number"],
                "state": node.get("state"),
                "merged": node.get("merged") is True,
                "base": node.get("baseRefName"),
                "merge_commit": merge.get("oid") if isinstance(merge, Mapping) else None,
            },
        )

    for node in (issue.get("closedByPullRequestsReferences") or {}).get("nodes") or []:
        keep(node)
    for item in (issue.get("timelineItems") or {}).get("nodes") or []:
        source = item.get("source") if isinstance(item, Mapping) else None
        if (
            isinstance(source, Mapping)
            and source.get("__typename") == "PullRequest"
            and _closes(source.get("body"), number)
        ):
            keep(source)
    return [found[key] for key in sorted(found)]


def readiness_verdict(
    issue_body: str,
    blocker_nodes: Mapping[int, Mapping[str, Any] | None],
    is_ancestor: Callable[[str], bool],
) -> Readiness:
    """Every blocker has a closing pull request merged into next and in the candidate."""

    numbers = blocker_numbers(issue_body)
    if not numbers:
        return Readiness(False, ("the issue body names no blocker under Blocked by",), ())
    missing: list[str] = []
    evidence: list[dict[str, Any]] = []
    for number in numbers:
        node = blocker_nodes.get(number)
        if node is None:
            missing.append(f"#{number}: the issue could not be read")
            evidence.append({"issue": number, "state": None, "closing_pulls": []})
            continue
        pulls = closing_pulls(number, node)
        into_next = [
            pull
            for pull in pulls
            if pull["merged"] and pull["base"] == INTEGRATION_BRANCH and pull["merge_commit"]
        ]
        for pull in into_next:
            pull["ancestor"] = bool(is_ancestor(str(pull["merge_commit"])))
        evidence.append({"issue": number, "state": node.get("state"), "closing_pulls": pulls})
        if not into_next:
            missing.append(f"#{number}: no merged pull request into {INTEGRATION_BRANCH} closes it")
        elif not any(pull["ancestor"] for pull in into_next):
            names = ", ".join(f"#{pull['number']}" for pull in into_next)
            missing.append(
                f"#{number}: pull request {names} merge commit is not an ancestor of the candidate"
            )
    return Readiness(not missing, tuple(missing), tuple(evidence))


# --------------------------------------------------------------------------
# Readiness (I/O)
# --------------------------------------------------------------------------


def graphql(token: str, query: str, variables: Mapping[str, Any]) -> Any:
    status, body = fe.http_json(
        "POST",
        GRAPHQL_URL,
        headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
        body={"query": query, "variables": dict(variables)},
    )
    if status != 200:
        raise ValueError(f"GraphQL answered HTTP {status}")
    return body


def git_ancestry(repo_root: Path, candidate: str) -> Callable[[str], bool]:
    """``git merge-base --is-ancestor <oid> <candidate>`` after fetching next once."""

    subprocess.run(
        ["git", "-C", str(repo_root), "fetch", "--quiet", "origin", INTEGRATION_BRANCH],
        capture_output=True,
        check=False,
    )

    def is_ancestor(oid: str) -> bool:
        if not re.fullmatch(r"[0-9a-f]{40}", oid):
            return False
        result = subprocess.run(
            ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", oid, candidate],
            capture_output=True,
            check=False,
        )
        # 1 is "not an ancestor"; anything else (128: unknown object) counts the same.
        return result.returncode == 0

    return is_ancestor


def check_readiness(
    token: str,
    *,
    repo_root: Path,
    candidate: str,
    query: Callable[[str, str, Mapping[str, Any]], Any] = graphql,
    ancestry: Callable[[Path, str], Callable[[str], bool]] = git_ancestry,
) -> Readiness:
    names = {"owner": PRODUCT_OWNER, "name": PRODUCT_NAME}
    try:
        body = graphql_issue(query(token, BODY_QUERY, {**names, "number": CHAIN_ISSUE}))
        issue_body = str(body.get("body") or "")
    except (OSError, ValueError) as exc:
        return Readiness(False, (f"#{CHAIN_ISSUE} could not be read: {exc}",), ())
    nodes: dict[int, Mapping[str, Any] | None] = {}
    for number in blocker_numbers(issue_body):
        try:
            nodes[number] = graphql_issue(query(token, BLOCKER_QUERY, {**names, "number": number}))
        except (OSError, ValueError):
            nodes[number] = None
    return readiness_verdict(issue_body, nodes, ancestry(repo_root, candidate))


def reader_account_id(environ: Mapping[str, str]) -> int:
    """CURIE_FACTORY_READER_ACCOUNT_ID as a number; refusals never echo the value."""

    raw = environ.get(READER_ENV, "")
    if not raw:
        raise fe.ConfigError(f"{SCENARIO} needs {READER_ENV}: the reader's numeric account id")
    if not re.fullmatch(r"[1-9][0-9]{0,19}", raw):
        raise fe.ConfigError(f"{READER_ENV} must be a numeric account id (digits only)")
    return int(raw)


def before_run(p: fe.Preflight) -> None:
    """The scenario's config step. No lock, kubectl or helm call happens before it ends.

    In order: the configuration's sanitizer (so even a refused reader id writes
    no raw namespace or context), the reader id, the fake model for
    ``--stop-after links``, the identities and the full sanitizer (before
    ``check_app`` logs the repository id and the actor), readiness, and the
    fixture's open items (poll intake could admit a leftover labelled issue
    between the agent binding and the fixture reset).
    """

    chain = chain_of(p)
    install_sanitizer(p, config_identifiers(p))
    chain.reader_id = reader_account_id(os.environ)
    if chain.stop_after == STOP_AFTER_LINKS:
        use_fake_model(p)
    stage_identities(p, chain)
    readiness = check_readiness(
        p.config.actor_token,
        repo_root=p.repo_root,
        candidate=p.candidate,
        query=graphql,
        ancestry=git_ancestry,
    )
    chain.readiness = readiness
    p.evidence["live_chain_readiness"] = readiness.evidence()
    p.evidence["live_chain_attempt"] = chain.attempt
    if readiness.passed:
        p.step("live chain readiness met", blockers=len(readiness.blockers))
    elif chain.stop_after == STOP_AFTER_LINKS:
        p.step(
            "live chain readiness not met; --stop-after links goes on",
            missing=list(readiness.missing),
        )
    else:
        raise fe.ConfigError(f"{SCENARIO} is not ready: " + "; ".join(readiness.missing))
    refuse_open_fixture_items(p)


def use_fake_model(p: fe.Preflight) -> None:
    """``--stop-after links`` installs the fake model: no model request is possible."""

    dropped = bool(p.config.model_api_key or p.config.model_base_url)
    p.config = dataclasses.replace(p.config, model_api_key=None, model_base_url=None)
    p.step("live chain stop mode installs the fake model", model_settings_dropped=dropped)


def refuse_open_fixture_items(p: fe.Preflight) -> None:
    """Refuse a fixture repository with any open issue or pull request (actor token)."""

    open_items = p._paged(f"/repos/{p.config.repo}/issues?state=open")
    if open_items:
        raise ChainStopped(
            f"the fixture repository has {len(open_items)} open issues or pull requests; "
            "close them before a live chain run"
        )
    p.step("live chain fixture has no open item")


# --------------------------------------------------------------------------
# Install values and Dex
# --------------------------------------------------------------------------


def _env(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": value}


def oidc_env(issuer: str, redirect_uri: str) -> list[dict[str, Any]]:
    """The ``CURIE_OIDC_*`` entries ``pto.two_owner_install_values`` writes, and only those."""

    return [
        _env("CURIE_OIDC_ISSUER", issuer),
        _env("CURIE_OIDC_AUDIENCE", pto.OIDC_CLIENT_ID),
        _env("CURIE_OIDC_JWKS_URL", f"{issuer}/keys"),
        {
            "name": "CURIE_OIDC_CLIENT_SECRET",
            "valueFrom": {"secretKeyRef": {"name": OIDC_SECRET_NAME, "key": OIDC_SECRET_KEY}},
        },
        _env("CURIE_OIDC_REDIRECT_URI", redirect_uri),
        _env("CURIE_OIDC_SCOPES", pto.OIDC_SCOPES),
        _env("CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "true"),
    ]


def oidc_values(values: dict[str, Any], *, issuer: str, redirect_uri: str) -> dict[str, Any]:
    """``values`` with the api's ``CURIE_OIDC_*`` entries replaced; nothing else changes."""

    result = copy.deepcopy(values)
    api = result.setdefault("api", {})
    kept = [
        entry
        for entry in api.get("extraEnv") or []
        if not (isinstance(entry, dict) and str(entry.get("name") or "").startswith(OIDC_PREFIX))
    ]
    api["extraEnv"] = kept + oidc_env(issuer, redirect_uri)
    return result


def client_secret_manifest(namespace: str, value: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": OIDC_SECRET_NAME, "namespace": namespace, "labels": dict(DEX_LABELS)},
        "type": "Opaque",
        "stringData": {OIDC_SECRET_KEY: value},
    }


def dex_objects(
    namespace: str, *, issuer: str, redirect_uri: str, secret: str
) -> list[dict[str, Any]]:
    config = pto.dex_config(
        issuer=issuer,
        client_id=pto.OIDC_CLIENT_ID,
        client_secret=secret,
        redirect_uri=redirect_uri,
        users=[WRITER_EMAIL, READER_EMAIL],
        password_hash=pto.DEX_PASSWORD_HASH,
    )
    return pto.dex_manifests(
        namespace=namespace,
        name=DEX_NAME,
        image_ref=pto.DEX_IMAGE,
        config_secret=DEX_CONFIG_SECRET_NAME,
        labels=DEX_LABELS,
        config=config,
    )


def _kubectl_create(p: fe.Preflight, manifest: Mapping[str, Any]) -> None:
    # Secrets travel on stdin, never in argv.
    fe.run(
        ["kubectl", "--context", p.config.kube_context, "-n", p.namespace, "create", "-f", "-"],
        input_text=json.dumps(manifest),
    )


def delete_dex(p: fe.Preflight) -> dict[str, Any]:
    """Delete Dex's Deployment and Service and both Secrets by label, then verify none remains."""

    kinds = "deployment,service,secret"
    p.kubectl(
        "-n",
        p.namespace,
        "delete",
        kinds,
        "-l",
        DEX_SELECTOR,
        "--ignore-not-found",
        "--wait=true",
        check=False,
    )
    remaining = p.kubectl("-n", p.namespace, "get", kinds, "-l", DEX_SELECTOR, "-o", "name").split()
    if remaining:
        raise fe.PreflightFailed(f"{len(remaining)} Dex objects remain after the delete")
    return {"remaining": 0}


def install_values(p: fe.Preflight) -> fe.ValuesTransform:
    """Create the OIDC client Secret and Dex in the run namespace; return the values transformer."""

    chain = chain_of(p)
    issuer = pto.dex_issuer(DEX_NAME, p.namespace)
    redirect_uri = pto.api_redirect_uri(p.release, p.namespace)
    chain.dex = {"issuer": issuer, "redirect_uri": redirect_uri}
    # Pushed before the first create, so an ambiguous create is still undone.
    p.teardown.push(DEX_STEP, lambda: delete_dex(p))
    secret = secrets.token_urlsafe(32)
    _kubectl_create(p, client_secret_manifest(p.namespace, secret))
    objects = dex_objects(p.namespace, issuer=issuer, redirect_uri=redirect_uri, secret=secret)
    _kubectl_create(p, {"apiVersion": "v1", "kind": "List", "items": objects})
    p.step("live chain dex created", objects=len(objects) + 1)
    return functools.partial(oidc_values, issuer=issuer, redirect_uri=redirect_uri)


# --------------------------------------------------------------------------
# Identities and the sanitizer
# --------------------------------------------------------------------------


def registry_host(registry: str | None) -> str | None:
    if not registry:
        return None
    return registry.split("/", 1)[0] or None


def sanitizer_identifiers(
    *,
    actor: Mapping[str, Any],
    reader: Mapping[str, Any],
    repository: Mapping[str, Any],
    app_id: str,
    app_slug: str,
    installation_id: int,
    namespace: str,
    release: str,
    kube_context: str,
    registry: str | None,
) -> dict[str, list[str | int]]:
    """Every raw identifier of the run by role, for ``judge.serialize_evidence``."""

    full_name = str(repository["full_name"])
    owner, name = full_name.split("/", 1)
    repo_ids: list[str | int] = [full_name, name, int(repository["id"])]
    if owner.lower() != str(actor["login"]).lower():
        repo_ids.append(owner)
    ids: dict[str, list[str | int]] = {
        "fixture-repo": repo_ids,
        "writer": [str(actor["login"]), int(actor["id"])],
        "reader": [str(reader["login"]), int(reader["id"])],
        "app": [str(app_id), app_slug, f"{app_slug}[bot]", int(installation_id)],
        "namespace": [namespace, release],
        "context": [kube_context],
    }
    host = registry_host(registry)
    if host:
        ids["registry"] = [host]
    return ids


def identities_record(
    actor: Mapping[str, Any],
    reader: Mapping[str, Any],
    repository: Mapping[str, Any],
    app_slug: str,
) -> dict[str, Any]:
    return {
        "app_login": f"{app_slug}[bot]",
        "writer_id": actor["id"],
        "reader_id": reader["id"],
        "repository_id": repository["id"],
        "repository_url": repository["html_url"],
        "default_branch": repository["default_branch"],
        "repository_path": repository["full_name"],
    }


def check_identities(actor: Any, reader: Any, repository: Any, *, reader_id: int) -> list[str]:
    """Why the account and repository reads do not fit the scenario; never a value."""

    reasons: list[str] = []
    if (
        not isinstance(actor, Mapping)
        or not isinstance(actor.get("id"), int)
        or not actor.get("login")
    ):
        reasons.append("the actor account could not be read")
    if not isinstance(reader, Mapping) or reader.get("id") != reader_id:
        reasons.append("the reader account could not be read")
    elif reader.get("type") != "User":
        reasons.append("the reader account is not a User")
    elif isinstance(actor, Mapping) and actor.get("id") == reader_id:
        reasons.append("the reader account is the actor")
    if not isinstance(repository, Mapping) or not all(
        repository.get(key) for key in ("id", "full_name", "html_url", "default_branch")
    ):
        reasons.append("the fixture repository could not be read")
    return reasons


def evidence_serializer(identifiers: judge.Identifiers) -> Callable[[dict[str, Any]], str]:
    def serialize(evidence: dict[str, Any]) -> str:
        plain = json.loads(json.dumps(evidence, default=str))
        return judge.serialize_evidence(plain, identifiers)

    return serialize


def local_identifiers(p: fe.Preflight) -> dict[str, list[str | int]]:
    """Operator-machine values: the CLI and tunnel binaries' paths and the restore URL.

    ``main`` keeps the binary path out of the log and the evidence (the hook's
    ``private_binary``); these catch it anywhere else, the configured tunnel
    binary (``check_tools`` names a missing one), and the restore URL a dead
    tunnel's step names. A bare command name is not a path and is skipped.
    """

    ids: dict[str, list[str | int]] = {}
    binaries = {p.config.curie_bin, os.environ.get("CURIE_BIN") or ""}
    paths: list[str | int] = sorted(b for b in binaries if os.path.dirname(b))
    if paths:
        ids["binary"] = paths
    if os.path.dirname(p.config.cloudflared):
        ids["cloudflared"] = [p.config.cloudflared]
    if p.config.restore_webhook_url:
        ids["restore-url"] = [p.config.restore_webhook_url]
    return ids


def config_identifiers(p: fe.Preflight) -> dict[str, list[str | int]]:
    """The raw identifiers the configuration alone names, before any account read.

    ``before_run`` installs the sanitizer with these, so the install, bundle
    and teardown logs of a run that fails before ``drive`` are sanitized too.
    """

    full_name = p.config.repo
    owner, name = full_name.split("/", 1)
    slug = p.config.mention
    ids: dict[str, list[str | int]] = {
        "fixture-repo": [full_name, name, owner],
        "app": [str(p.config.app_id), slug, f"{slug}[bot]", int(p.config.installation_id)],
        "namespace": [p.namespace, p.release],
        "context": [p.config.kube_context],
    }
    host = registry_host(p.config.layer_registry)
    if host:
        ids["registry"] = [host]
    return {**ids, **local_identifiers(p)}


_INSTALLED_FILTERS: list[Callable[[str], str]] = []


def install_sanitizer(p: fe.Preflight, identifiers: judge.Identifiers) -> None:
    """Evidence and every later log line pass through the judge's sanitizer.

    A later call replaces the filter an earlier one installed (the full set
    read from the accounts replaces the configuration's set).
    """

    p.evidence_serializer = evidence_serializer(identifiers)
    for old in _INSTALLED_FILTERS:
        if old in fe.LOG_FILTERS:
            fe.LOG_FILTERS.remove(old)
    _INSTALLED_FILTERS.clear()

    def sanitize(text: str) -> str:
        return judge.sanitize_log(text, identifiers)

    _INSTALLED_FILTERS.append(sanitize)
    fe.LOG_FILTERS.append(sanitize)


# --------------------------------------------------------------------------
# First check, session probe and links (pure builders)
# --------------------------------------------------------------------------


def parse_cli_json(exit_code: int, stdout: str) -> Any:
    try:
        return json.loads(stdout)
    except ValueError:
        return {"exit_code": exit_code, "unparsed": True}


# The switch's field of ``curie cluster overrides <agent> --json`` (#5857,
# ``cli/src/commands/overrides.rs``).
PROPOSALS_FIELD = "memory_rule_proposals"


def overrides_proposals(exit_code: int, stdout: str) -> bool | None:
    """Whether the agent read shows rule proposals on; None when the read failed."""

    parsed = parse_cli_json(exit_code, stdout)
    value = parsed.get(PROPOSALS_FIELD) if isinstance(parsed, dict) and exit_code == 0 else None
    return value if isinstance(value, bool) else None


def work_item_list(exit_code: int, stdout: str) -> list[Any]:
    """The ``items`` of ``curie cluster work-items --agent <a> --json``; failure raises."""

    parsed = parse_cli_json(exit_code, stdout)
    items = parsed.get("items") if isinstance(parsed, dict) else None
    if exit_code != 0 or not isinstance(items, list):
        raise fe.PreflightFailed(f"curie cluster work-items --json failed (exit {exit_code})")
    return items


def preflight_record(
    open_items: Sequence[Mapping[str, Any]], work_items: Sequence[Any]
) -> dict[str, Any]:
    """Decision 2's first check: open issues and pull requests, and the agent's work items."""

    return {
        "open_items": [
            {"number": item.get("number"), "pull_request": "pull_request" in item}
            for item in open_items
        ],
        "work_items": [
            {"id": item.get("id")} if isinstance(item, Mapping) else item for item in work_items
        ],
    }


def session_side(
    session_status: int, session_body: Any, principal_status: int, principal_body: Any
) -> dict[str, Any]:
    """One role of the session probe from ``GET /console/session`` and ``/console/principal``."""

    session = session_body if session_status == 200 and isinstance(session_body, Mapping) else {}
    principal = (
        principal_body if principal_status == 200 and isinstance(principal_body, Mapping) else {}
    )
    raw_nested = session.get("principal")
    nested: Mapping[str, Any] = raw_nested if isinstance(raw_nested, Mapping) else {}
    return {
        "session_status": session_status,
        "session_kind": session.get("kind"),
        "principal_id": str(nested.get("id") or principal.get("id") or "") or None,
        "principal_status": principal_status,
        "tenant_id": str(principal["tenant_id"]) if principal.get("tenant_id") else None,
    }


def agent_tenant(status: int, policy: Any) -> str | None:
    """The agent's tenant from ``GET /agents/{id}/access-policy`` read with the platform key."""

    if status != 200 or not isinstance(policy, Mapping) or not policy.get("tenant_id"):
        return None
    return str(policy["tenant_id"])


def code_host_link_body(*, principal_id: str, account_id: int, created_by: str) -> dict[str, Any]:
    """The ``CodeHostLinkIn`` body: the account by its numeric id as a decimal string."""

    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id <= 0:
        raise ValueError("a code host link names an account by its positive numeric id")
    return {
        "principal_id": principal_id,
        "code_host_kind": "github",
        "host": CODE_HOST,
        "account_id": str(account_id),
        "created_by_principal_id": created_by,
    }


def revoke_verified(status: int, body: Any) -> bool:
    """A revoke answered 200 with a ``revoked_at`` (``IdentityLinkRevokedOut``)."""

    return status == 200 and isinstance(body, Mapping) and bool(body.get("revoked_at"))


def teardown_section(
    results: Sequence[Mapping[str, Any]], *, namespace_gone: bool
) -> dict[str, Any]:
    """Decision 10 from the teardown results. A step never pushed created nothing."""

    by_step = {str(r.get("step")): bool(r.get("ok")) for r in results}

    def undone(step: str) -> bool:
        return by_step.get(step, True)

    return {
        "verified": all(bool(r.get("ok")) for r in results),
        "namespace_gone": namespace_gone,
        "session_files_deleted": undone(SESSION_STEP),
        "code_host_links_removed": undone(LINKS_STEP),
        "dex_deleted": undone(DEX_STEP),
        "issues_closed": undone(ISSUES_STEP),
    }


# --------------------------------------------------------------------------
# Beat records (pure builders from raw GitHub, API and CLI answers)
# --------------------------------------------------------------------------


def _comment(raw: Mapping[str, Any]) -> dict[str, Any]:
    user = raw.get("user") or {}
    return {
        "id": raw.get("id"),
        "body": raw.get("body") or "",
        "created_at": raw.get("created_at"),
        "updated_at": raw.get("updated_at"),
        "user": {"login": user.get("login"), "id": user.get("id")},
    }


def comments_record(raw: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """GitHub issue comments as the judge reads them.

    Shape: https://docs.github.com/en/rest/issues/comments#list-issue-comments
    """

    return [_comment(item) for item in raw]


# A turn record is appended when its runner turn ends, just before the worker
# marks the request terminal; the margin absorbs the clock of the runner's pod.
TRANSCRIPT_TURN_MARGIN = dt.timedelta(seconds=60)


def turn_tool_names(record: Mapping[str, Any]) -> list[str]:
    """The ``tool_use`` names of one transcript turn record, in order.

    Shape: ``curie_runner.history.TurnRecord.to_dict`` (``type`` ``turn``, ``ts``,
    ``messages`` whose content blocks carry ``{"type": "tool_use", "name"}``).
    """

    names: list[str] = []
    for message in record.get("messages") or []:
        content = message.get("content") if isinstance(message, Mapping) else None
        for block in content if isinstance(content, list) else []:
            if isinstance(block, Mapping) and block.get("type") == "tool_use":
                names.append(str(block.get("name") or ""))
    return names


def request_tool_calls(
    rows: Iterable[Mapping[str, Any]], transcripts: Iterable[Any]
) -> dict[str, list[str]]:
    """The tool calls the thread transcript recorded for each request, by request id.

    ``transcripts`` are the stored values of the work item's thread
    (``thread_transcripts.value``, a JSON array of records). A turn belongs to
    the newest request created at or before its ``ts`` that had not ended more
    than ``TRANSCRIPT_TURN_MARGIN`` before it; a turn no request owns, and a
    record that is not a turn, is dropped.
    """

    spans = sorted(
        (
            judge.parse_time(row["created_at"]),
            judge.parse_time(row["terminal_at"]) if row.get("terminal_at") else None,
            str(row["id"]),
        )
        for row in rows
    )
    calls: dict[str, list[str]] = {request_id: [] for _start, _end, request_id in spans}
    for value in transcripts:
        for record in value if isinstance(value, list) else []:
            if not isinstance(record, Mapping) or record.get("type") != "turn":
                continue
            try:
                ts = judge.parse_time(record.get("ts"))
            except ValueError:
                continue
            owners = [span for span in spans if span[0] <= ts]
            if not owners:
                continue
            _start, end, request_id = owners[-1]
            if end is None or ts <= end + TRANSCRIPT_TURN_MARGIN:
                calls[request_id].extend(turn_tool_names(record))
    return calls


def request_records(
    rows: Iterable[Mapping[str, Any]], tool_calls: Mapping[str, Sequence[str]] | None = None
) -> list[dict[str, Any]]:
    """Execution request rows of the install, in the judge's shape.

    ``tool_calls`` are the tool names the thread transcript recorded for each
    request (``request_tool_calls``); they become the request's tool notes.
    """

    recorded = tool_calls or {}
    records = []
    for row in rows:
        request_id = str(row["id"])
        records.append(
            {
                "id": request_id,
                "work_item_id": str(row["work_item_id"]),
                "status": row["status"],
                "created_at": row["created_at"],
                "terminal_at": row.get("terminal_at"),
                "terminal_cause": row.get("terminal_cause"),
                "objective": row.get("objective") or "",
                judge.TOOL_NOTES_KEY: list(recorded.get(request_id, ())),
            }
        )
    return sorted(records, key=lambda r: judge.parse_time(r["created_at"]))


def pull_record(raw: Mapping[str, Any]) -> dict[str, Any]:
    """A pull request (https://docs.github.com/en/rest/pulls/pulls) as the judge reads it."""

    head = raw.get("head") or {}
    return {
        "number": raw.get("number"),
        "body": raw.get("body") or "",
        "created_at": raw.get("created_at"),
        "html_url": raw.get("html_url"),
        "head": {"ref": head.get("ref"), "sha": head.get("sha")},
    }


def investigating_comment(
    comments: Iterable[Mapping[str, Any]], app_login: str
) -> Mapping[str, Any] | None:
    """The App's comment that carries the investigating lead in this read, or None.

    The status comment is edited in place (``factory_notices._sync_comment``):
    its lead opens every non-final body and is gone from the terminal one, so
    beat (a) keeps the first read that shows it.
    """

    for comment in comments:
        by_app = (comment.get("user") or {}).get("login") == app_login
        if by_app and str(comment.get("body") or "").startswith(judge.STATUS_COMMENT_PREFIX):
            return comment
    return None


def beat_a_record(
    *,
    issue_number: int,
    labelled_at: str,
    investigating: tuple[Mapping[str, Any], str] | None,
    comments: Sequence[Mapping[str, Any]],
    labels: Sequence[Mapping[str, Any]],
    quiet_rows: Sequence[Mapping[str, Any]],
    observed_at: str,
    work_item_id: str,
) -> dict[str, Any]:
    """``investigating`` is the first read showing the lead: the comment and when it was read."""

    return {
        "issue_number": issue_number,
        "labelled_at": labelled_at,
        "investigating": (
            {"comment": _comment(investigating[0]), "observed_at": investigating[1]}
            if investigating is not None
            else None
        ),
        "comments": comments_record(comments),
        "issue_labels": [str(label.get("name")) for label in labels],
        "quiet_snapshot": {"observed_at": observed_at, "requests": request_records(quiet_rows)},
        "work_item_id": work_item_id,
    }


def beat_b_record(
    *,
    answer: Mapping[str, Any],
    issue_number: int,
    request_rows: Sequence[Mapping[str, Any]],
    open_pulls: Sequence[Mapping[str, Any]],
    issues: Sequence[Mapping[str, Any]],
    ticket_comments: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "answer": {**_comment(answer), "issue_number": issue_number},
        "requests": request_records(request_rows),
        "open_pulls": [pull_record(pull) for pull in open_pulls],
        "issues": [
            {"number": i.get("number"), "user": {"login": (i.get("user") or {}).get("login")}}
            for i in issues
            if "pull_request" not in i
        ],
        "ticket_comments": comments_record(ticket_comments),
    }


def added_lines_by_path(files: Iterable[Mapping[str, Any]]) -> dict[str, list[int]]:
    """Right-side line numbers each file's patch adds (``GET /pulls/{n}/files``)."""

    added: dict[str, list[int]] = {}
    for entry in files:
        line = 0
        numbers: list[int] = []
        for text in str(entry.get("patch") or "").splitlines():
            hunk = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", text)
            if hunk:
                line = int(hunk.group(1))
                continue
            if text.startswith("+"):
                numbers.append(line)
                line += 1
            elif text.startswith("-"):
                continue
            else:
                line += 1
        added[str(entry.get("filename"))] = numbers
    return added


def added_text_lines(files: Iterable[Mapping[str, Any]]) -> list[str]:
    """Every added line of every file's patch, without its ``+``."""

    lines: list[str] = []
    for entry in files:
        for text in str(entry.get("patch") or "").splitlines():
            if text.startswith("+") and not text.startswith("+++"):
                lines.append(text[1:])
    return lines


def review_target(added: Mapping[str, Sequence[int]]) -> tuple[str, int]:
    """The line the review comments on: the first added line of a test file, else any."""

    paths = sorted(added, key=lambda path: (not re.search(r"(^|/)tests?/|test_", path), path))
    for path in paths:
        if added[path]:
            return path, added[path][0]
    raise fe.PreflightFailed("the pull request adds no line to comment on")


def review_body(head_sha: str, path: str, line: int) -> dict[str, Any]:
    """``POST /repos/{r}/pulls/{n}/reviews``: COMMENT, no body, one inline rule comment.

    https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
    """

    return {
        "commit_id": head_sha,
        "event": "COMMENT",
        "body": "",
        "comments": [{"path": path, "line": line, "side": "RIGHT", "body": judge.REVIEW_RULE_TEXT}],
    }


def beat_c_record(
    *,
    repo: str,
    pull_number: int,
    pull_before: Mapping[str, Any],
    posted: Mapping[str, Any],
    review: Mapping[str, Any],
    review_comments: Sequence[Mapping[str, Any]],
    files: Sequence[Mapping[str, Any]],
    request_rows: Sequence[Mapping[str, Any]],
    transcripts: Sequence[Any],
    pull_after: Mapping[str, Any],
    committed_at: str,
    branches: Sequence[Mapping[str, Any]],
    pull_comments: Sequence[Mapping[str, Any]],
    check_runs: Mapping[str, Any],
    combined_status: Mapping[str, Any],
) -> dict[str, Any]:
    after = pull_record(pull_after)
    return {
        "pull_number": pull_number,
        "pull_before": {"head": pull_record(pull_before)["head"]},
        "post": {
            "method": "POST",
            "path": f"/repos/{repo}/pulls/{pull_number}/reviews",
            "body": dict(posted),
        },
        "review": {
            "id": review.get("id"),
            "state": review.get("state"),
            "submitted_at": review.get("submitted_at"),
            "user": {"id": (review.get("user") or {}).get("id")},
            "pull_request_url": review.get("pull_request_url"),
        },
        "review_comments": [
            {
                "path": c.get("path"),
                "line": c.get("line"),
                "body": c.get("body"),
                "pull_request_review_id": c.get("pull_request_review_id"),
            }
            for c in review_comments
        ],
        "diff_added_lines": added_lines_by_path(files),
        "requests": request_records(request_rows, request_tool_calls(request_rows, transcripts)),
        "pull_after": {"head": {**after["head"], "committed_at": committed_at}},
        "branches": [str(b.get("name")) for b in branches],
        "pull_comments": comments_record(pull_comments),
        "revision_checks": {
            "sha": after["head"]["sha"],
            "check_runs": dict(check_runs),
            "status": dict(combined_status),
        },
    }


def beat_d_record(
    *,
    rules_before: Mapping[str, Any],
    pull_merged: Mapping[str, Any],
    rules_after: Mapping[str, Any],
    observed_at: str,
    issue_after: Mapping[str, Any],
    ticket_comments: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "rules_before_merge": dict(rules_before),
        "merged_at": pull_merged.get("merged_at"),
        "merged_by_id": (pull_merged.get("merged_by") or {}).get("id"),
        "rules_after_merge": {"observed_at": observed_at, "output": dict(rules_after)},
        "issue_after_merge": {
            "number": issue_after.get("number"),
            "state": issue_after.get("state"),
            "closed_at": issue_after.get("closed_at"),
        },
        "ticket_comments_after_merge": comments_record(ticket_comments),
    }


def cli_command_record(rule_id: str, exit_code: int, stdout: str, elapsed: float) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "exit_code": exit_code,
        "output": parse_cli_json(exit_code, stdout),
        "elapsed_seconds": round(elapsed, 3),
    }


def beat_e_record(
    *,
    reader_permission: Mapping[str, Any],
    rule_before: Mapping[str, Any],
    reader: tuple[int, str, float],
    rule_after_reader: Mapping[str, Any],
    writer: tuple[int, str, float],
) -> dict[str, Any]:
    rule_id = str(rule_before["id"])
    return {
        "reader_permission": reader_permission.get("permission"),
        "rule_before": {"id": rule_id, "version": rule_before.get("version")},
        "reader_command": cli_command_record(rule_id, *reader),
        "rule_after_reader": {"version": rule_after_reader.get("version")},
        "writer_command": cli_command_record(rule_id, *writer),
    }


def beat_f_record(
    *,
    issue_number: int,
    instruction_set: bool,
    labelled_at: str,
    open_pulls: Sequence[Mapping[str, Any]],
    work_item: Mapping[str, Any],
    files: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "issue_number": issue_number,
        "instruction_set": instruction_set,
        "labelled_at": labelled_at,
        "open_pulls": [pull_record(pull) for pull in open_pulls],
        "work_item": {"memory_rules": work_item.get("memory_rules")},
        "diff_added_lines": added_text_lines(files),
    }


def beat_g_record(
    *,
    issue_number: int,
    labelled_at: str,
    rule_id: str,
    removal: tuple[int, str, float],
    open_pulls: Sequence[Mapping[str, Any]],
    work_item: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "issue_number": issue_number,
        "labelled_at": labelled_at,
        "remove_command": cli_command_record(rule_id, *removal),
        "open_pulls": [pull_record(pull) for pull in open_pulls],
        "work_item": {"memory_rules": work_item.get("memory_rules")},
    }


def beat_h_record(
    *,
    label: str,
    observed_at: str,
    work_items: Sequence[Any],
    comments: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    deploy: tuple[int, str],
    version_before: Any,
    version_after: Any,
) -> dict[str, Any]:
    """Issue events (https://docs.github.com/en/rest/issues/events) keep their label and actor."""

    exit_code, stdout = deploy
    return {
        "label": label,
        "observed_at": observed_at,
        "work_items": list(work_items),
        "comments": comments_record(comments),
        "label_events": [
            {
                "event": e.get("event"),
                "label": {"name": (e.get("label") or {}).get("name")},
                "actor": {
                    "login": (e.get("actor") or {}).get("login"),
                    "id": (e.get("actor") or {}).get("id"),
                },
                "created_at": e.get("created_at"),
            }
            for e in events
            if e.get("event") in {"labeled", "unlabeled"}
        ],
        "retired_bundle": {
            "exit_code": exit_code,
            "output": parse_cli_json(exit_code, stdout),
            "version_before": version_before,
            "version_after": version_after,
        },
    }


def usage_breach(samples: Sequence[Mapping[str, Any]]) -> float | None:
    """The delta over the first sample once it reaches the spend cap, else None."""

    values = [s["usage"] for s in samples if isinstance(s.get("usage"), (int, float))]
    if len(values) < 2:
        return None
    delta = float(values[-1]) - float(values[0])
    return delta if delta >= SPEND_SAMPLE_CAP else None


# --------------------------------------------------------------------------
# Loopback opener for the OIDC logins
# --------------------------------------------------------------------------


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


class LoopbackOpener:
    """``pto.Opener`` over urllib: no redirects, no proxy, only ``http://127.0.0.1``."""

    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def __repr__(self) -> str:
        return "<loopback opener>"

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> tuple[int, Sequence[tuple[str, str]], bytes]:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme != "http" or parts.hostname != "127.0.0.1":
            raise pto.LoginFailed("opener_origin")
        request = urllib.request.Request(
            url, data=body, headers={"User-Agent": USER_AGENT, **dict(headers)}, method=method
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return int(response.status), list(response.headers.items()), response.read(1 << 20)
        except urllib.error.HTTPError as error:
            pairs = list(error.headers.items()) if error.headers is not None else []
            return int(error.code), pairs, error.read(1 << 20)
        except (OSError, http.client.HTTPException):
            raise pto.LoginFailed("transport") from None


# The opener factory; a test replaces it to prove no login happens.
make_opener: Callable[[], pto.Opener] = LoopbackOpener


def origin_of(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


# --------------------------------------------------------------------------
# I/O helpers
# --------------------------------------------------------------------------


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def cluster_cli(
    p: fe.Preflight, args: Sequence[str], *, timeout: float = 300
) -> tuple[int, str, float]:
    """``curie cluster <args> --json`` against this install: exit code, stdout, seconds."""

    if p._kubeconfig is None:
        p._kubeconfig = p._write_kubeconfig()
    argv = [
        p.config.curie_bin,
        "cluster",
        "--context",
        p.config.kube_context,
        *args,
        "--json",
        "--namespace",
        p.namespace,
        "--release",
        p.release,
        "--api-url",
        p.api_url,
    ]
    env = {
        **os.environ,
        "CURIE_API_KEY": p.api_key,
        "KUBECONFIG": str(p._kubeconfig),
        "CURIE_CONFIG_DIR": str(p._curie_config_dir()),
    }
    started = time.monotonic()
    try:
        result = subprocess.run(
            argv, capture_output=True, text=True, env=env, check=False, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        return 124, "", time.monotonic() - started
    return result.returncode, result.stdout, time.monotonic() - started


def sql_json(p: fe.Preflight, query: str) -> list[dict[str, Any]]:
    """Rows of ``query`` as JSON objects (one ``row_to_json`` per line, so text stays whole)."""

    rows = p.sql(f"SELECT row_to_json(q)::text FROM ({query}) q")
    return [json.loads(row[0]) for row in rows if row and row[0]]


def _uuid(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f-]{36}", value):
        raise fe.PreflightFailed("an id read from the install is not a uuid")
    return value


# The keys of a request record read from the install's request route
# (``GET /v1/internal/work-items/requests/{id}``, ``workitem_dispatch.request_view``).
REQUEST_VIEW_KEYS = ("work_item_id", "status", "terminal_at", "terminal_cause", "objective")


def request_row(listed: Mapping[str, Any], view: Mapping[str, Any]) -> dict[str, Any]:
    """One request: its id and ``created_at`` from the listing, the rest from the API.

    ``request_view`` carries no ``created_at`` and no route lists a work item's
    requests, so only those two come from ``execution_requests``.
    """

    if str(view.get("id")) != str(listed["id"]):
        raise fe.PreflightFailed("the request route answered for another request")
    return {
        "id": str(listed["id"]),
        "created_at": listed["created_at"],
        **{key: view.get(key) for key in REQUEST_VIEW_KEYS},
    }


def request_rows(p: fe.Preflight, work_item_id: str) -> list[dict[str, Any]]:
    """The work item's requests: listed by SQL, each read through the request route."""

    listed = sql_json(
        p,
        "SELECT id, created_at FROM execution_requests "
        f"WHERE work_item_id = '{_uuid(work_item_id)}' ORDER BY sequence",
    )
    rows = []
    for item in listed:
        view = p.execution_request(_uuid(str(item["id"])))
        if view is None:
            raise fe.PreflightFailed("the request route did not answer for a listed request")
        rows.append(request_row(item, view))
    return rows


def thread_transcripts(p: fe.Preflight, work_item_id: str) -> list[Any]:
    """The stored transcripts of the work item's thread (``thread_transcripts``).

    The runner appends one turn record per turn, with its ``tool_use`` blocks,
    to the thread whose key is the work item's ``conversation_id``; the row is
    deleted only when the work item ends, so it is still there in beat (c).
    """

    rows = sql_json(
        p,
        "SELECT t.value AS value FROM thread_transcripts t JOIN work_items w "
        "ON t.agent_id = w.agent_id AND t.thread_key = w.conversation_id "
        f"WHERE w.id = '{_uuid(work_item_id)}' ORDER BY t.created_at",
    )
    return [row["value"] for row in rows]


def work_item_for_issue(
    p: fe.Preflight, repository_id: int, issue: Mapping[str, Any]
) -> str | None:
    keys = {str(int(issue["number"])), str(int(issue["id"]))}
    listed = ", ".join(f"'{key}'" for key in sorted(keys))
    rows = sql_json(
        p,
        "SELECT id FROM work_items WHERE tracker_kind = 'github' "
        f"AND tracker_scope_id = '{int(repository_id)}' AND tracker_issue_id IN ({listed})",
    )
    return str(rows[0]["id"]) if rows else None


def check_runs_of(p: fe.Preflight, repo: str, sha: str) -> dict[str, Any]:
    """Every check run of ``sha``, page by page, until ``total_count`` are collected.

    https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
    returns at most 100 runs a page. A page that adds nothing ends the read, so a
    short read keeps fewer runs than ``total_count`` and the judge fails it.
    """

    runs: list[Any] = []
    total = 0
    page = 1
    while True:
        body = _github(p, "GET", f"/repos/{repo}/commits/{sha}/check-runs?per_page=100&page={page}")
        total = int(body["total_count"])
        batch = list(body["check_runs"])
        runs.extend(batch)
        if not batch or len(runs) >= total:
            return {"total_count": total, "check_runs": runs}
        page += 1


def _github(p: fe.Preflight, method: str, path: str, body: Any = None, *, want: int = 200) -> Any:
    status, answer = p.as_actor(method, path, body)
    if status != want:
        raise fe.PreflightFailed(f"GitHub {method} {path.split('?')[0]} answered HTTP {status}")
    return answer


def installation_get(p: fe.Preflight, path: str) -> Any:
    """GET with a fresh installation token of the App (a read through the App installation)."""

    status, body = p.as_app(
        "POST", f"/app/installations/{p.config.installation_id}/access_tokens", {}
    )
    token = body.get("token") if status == 201 and isinstance(body, dict) else None
    if not isinstance(token, str):
        raise fe.PreflightFailed(f"minting an installation token failed (HTTP {status})")
    status, answer = p.github("GET", path, token=token)
    if status != 200:
        raise fe.PreflightFailed(f"the installation read answered HTTP {status}")
    return answer


def _wait(what: str, timeout: float, probe: Callable[[], Any]) -> Any:
    return fe._wait(what, timeout, probe, interval=fe.POLL_SECONDS)


def _session_headers(chain: ChainRun, role: str) -> dict[str, str]:
    return {"Cookie": f"{pto.SESSION_COOKIE}={chain.sessions[role].reveal()}"}


def write_session_file(path: Path, value: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(value + "\n")


def delete_session_files(paths: Iterable[Path]) -> dict[str, Any]:
    paths = list(paths)
    for path in paths:
        path.unlink(missing_ok=True)
    remaining = [path for path in paths if path.exists()]
    if remaining:
        raise fe.PreflightFailed(f"{len(remaining)} session files remain")
    return {"deleted": len(paths)}


def revoke_links(p: fe.Preflight, chain: ChainRun) -> dict[str, Any]:
    failed = 0
    for link_id in chain.link_ids:
        status, body = p.api(
            "POST",
            f"/identity/admin/{chain.tenant_id}/links/{link_id}/revoke",
            headers={"X-API-Key": p.api_key},
        )
        failed += not revoke_verified(status, body)
    if failed:
        raise fe.PreflightFailed(f"{failed} code host links were not revoked")
    return {"revoked": len(chain.link_ids)}


def close_issues(p: fe.Preflight, chain: ChainRun) -> dict[str, Any]:
    open_left = 0
    for number in chain.opened_issues:
        p.as_actor("PATCH", f"/repos/{p.config.repo}/issues/{number}", {"state": "closed"})
        status, issue = p.as_actor("GET", f"/repos/{p.config.repo}/issues/{number}")
        open_left += not (status == 200 and isinstance(issue, dict) and issue["state"] == "closed")
    if open_left:
        raise fe.PreflightFailed(f"{open_left} scenario issues are still open")
    return {"closed": len(chain.opened_issues)}


def namespace_gone(p: fe.Preflight) -> bool:
    if not p.evidence.get("run_id"):
        return True  # create_namespace never ran: nothing was created
    try:
        return not p.kubectl(
            "get", "namespace", p.namespace, "--ignore-not-found", "-o", "name"
        ).strip()
    except fe.PreflightFailed:
        return False


# --------------------------------------------------------------------------
# drive: identities, first check, logins, probe, links
# --------------------------------------------------------------------------


def stage_identities(p: fe.Preflight, chain: ChainRun) -> None:
    assert chain.reader_id is not None, "before_run reads the reader id"
    status_a, actor = p.as_actor("GET", "/user")
    status_r, reader = p.as_actor("GET", f"/user/{chain.reader_id}")
    status_p, repository = p.as_actor("GET", f"/repos/{p.config.repo}")
    reasons = check_identities(
        actor if status_a == 200 else None,
        reader if status_r == 200 else None,
        repository if status_p == 200 else None,
        reader_id=chain.reader_id,
    )
    if reasons:
        raise ChainStopped("; ".join(reasons))
    chain.identifiers = sanitizer_identifiers(
        actor=actor,
        reader=reader,
        repository=repository,
        app_id=p.config.app_id,
        app_slug=p.config.mention,
        installation_id=p.config.installation_id,
        namespace=p.namespace,
        release=p.release,
        kube_context=p.config.kube_context,
        registry=p.config.layer_registry,
    )
    chain.identifiers.update(local_identifiers(p))
    install_sanitizer(p, chain.identifiers)
    chain.record["identities"] = identities_record(actor, reader, repository, p.config.mention)
    chain.record.setdefault("agent", {})
    p.step("live chain identities read", roles=sorted(chain.identifiers))


def stage_first_check(p: fe.Preflight, chain: ChainRun) -> None:
    open_items = p._paged(f"/repos/{p.config.repo}/issues?state=open")
    code, out, _elapsed = cluster_cli(p, ["work-items", "--agent", fe.FACTORY_AGENT])
    chain.record["preflight"] = preflight_record(open_items, work_item_list(code, out))
    verdict = judge.judge_beat("preflight", chain.record)
    p.step("live chain first check", passed=verdict.passed, reasons=list(verdict.reasons))
    if not verdict.passed:
        raise ChainStopped("first check failed: " + "; ".join(verdict.reasons))


def _dex_port_forward(p: fe.Preflight) -> int:
    port = fe._free_port()
    process = subprocess.Popen(
        [
            "kubectl",
            "--context",
            p.config.kube_context,
            "-n",
            p.namespace,
            "port-forward",
            f"svc/{DEX_NAME}",
            f"{port}:{pto.DEX_PORT}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    p.teardown.push(PORT_FORWARD_STEP, lambda: {"stopped": fe._stop(process)})

    def listening() -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return True
        except OSError:
            return False

    fe._wait("the Dex port-forward", 60, listening, interval=1)
    return port


def stage_logins(p: fe.Preflight, chain: ChainRun) -> None:
    p.kubectl("-n", p.namespace, "rollout", "status", f"deployment/{DEX_NAME}", "--timeout=5m")
    port = _dex_port_forward(p)
    opener = make_opener()
    files = {role: p.workdir / f"live-chain-{role}.session" for role in ROLES}
    chain.session_files = files
    p.teardown.push(SESSION_STEP, lambda: delete_session_files(files.values()))
    for role in ROLES:
        try:
            held = pto.oidc_login(
                opener,
                api_origin=p.api_url,
                issuer_origin=origin_of(chain.dex["issuer"]),
                dex_local_origin=f"http://127.0.0.1:{port}",
                redirect_origin=origin_of(chain.dex["redirect_uri"]),
                email=EMAILS[role],
                password=pto.DEX_PASSWORD,
            )
        except pto.LoginFailed as exc:
            raise ChainStopped(f"the {role} OIDC login failed at {exc.step}") from None
        chain.sessions[role] = held
        write_session_file(files[role], held.reveal())
    p.step("live chain sessions minted", roles=list(ROLES))


def stage_session_probe(p: fe.Preflight, chain: ChainRun) -> None:
    probe: dict[str, Any] = {}
    for role in ROLES:
        headers = _session_headers(chain, role)
        session = p.api("GET", "/console/session", headers=headers)
        principal = p.api("GET", "/console/principal", headers=headers)
        probe[role] = session_side(*session, *principal)
    agent_id = p.evidence.get("agent_id")
    policy = p.api("GET", f"/agents/{agent_id}/access-policy", headers={"X-API-Key": p.api_key})
    chain.record["session_probe"] = probe
    chain.record.setdefault("agent", {})["tenant_id"] = agent_tenant(*policy)
    verdict = judge.judge_beat("session_probe", chain.record)
    p.step("live chain session probe", passed=verdict.passed, reasons=list(verdict.reasons))
    if not verdict.passed:
        raise ChainStopped("session probe failed: " + "; ".join(verdict.reasons))
    chain.tenant_id = str(chain.record["agent"]["tenant_id"])


def stage_links(p: fe.Preflight, chain: ChainRun) -> None:
    ids = chain.record["identities"]
    probe = chain.record["session_probe"]
    writer = str(probe["writer"]["principal_id"])
    p.teardown.push(LINKS_STEP, lambda: revoke_links(p, chain))
    for role, account in (("writer", int(ids["writer_id"])), ("reader", int(ids["reader_id"]))):
        body = code_host_link_body(
            principal_id=str(probe[role]["principal_id"]), account_id=account, created_by=writer
        )
        status, answer = p.api(
            "POST",
            f"/identity/admin/{chain.tenant_id}/code-host-links",
            headers={"X-API-Key": p.api_key},
            body=body,
        )
        if status != 201 or not isinstance(answer, dict) or not answer.get("link_id"):
            raise ChainStopped(f"the {role} code host link was refused (HTTP {status})")
        chain.link_ids.append(str(answer["link_id"]))
    p.step("live chain code host links created", links=len(chain.link_ids))


def stop_evidence(chain: ChainRun) -> dict[str, Any]:
    return {
        "stop_after": STOP_AFTER_LINKS,
        "attempt": chain.attempt,
        "readiness": chain.readiness.evidence() if chain.readiness else None,
        "identities": sorted(chain.identifiers),
        "preflight": chain.record.get("preflight"),
        "session_probe": chain.record.get("session_probe"),
        "links": len(chain.link_ids),
    }


def drive(p: fe.Preflight) -> dict[str, Any]:
    chain = chain_of(p)
    chain.record["attempt"] = chain.attempt
    if "identities" not in chain.record:  # before_run read them already
        stage_identities(p, chain)
    stage_first_check(p, chain)
    stage_logins(p, chain)
    stage_session_probe(p, chain)
    stage_links(p, chain)
    if chain.stop_after == STOP_AFTER_LINKS:
        # The fake model is installed, no ticket exists and the seed issue is
        # off: no model request was possible.
        return stop_evidence(chain)
    run_beats(p, chain)
    return {"stop_after": None, "attempt": chain.attempt, "beats": sorted(chain.record)}


# --------------------------------------------------------------------------
# Full mode: beats (a) to (h)
# --------------------------------------------------------------------------


def sample_usage(p: fe.Preflight, chain: ChainRun, label: str) -> None:
    """Record one usage sample; stop on a missing or non-finite one, or at the cap.

    The ``before_a`` sample is the baseline of the spend cap, so ticket A is
    never labelled without it; a later missing sample leaves the spend beat
    failed (``finish`` judges the samples recorded so far).
    """

    samples = chain.record.setdefault("usage", {"samples": []})["samples"]
    usage = p.model_usage()
    samples.append({"label": label, "usage": usage})
    if (
        isinstance(usage, bool)
        or not isinstance(usage, (int, float))
        or not math.isfinite(float(usage))
    ):
        raise ChainStopped(f"the model usage could not be read at {label}")
    breach = usage_breach(samples)
    if breach is not None:
        raise ChainStopped(f"the model usage delta reached {breach:.2f} USD at {label}")


def _issue_text(name: str) -> tuple[str, str]:
    return fe.parse_issue_file(FIXTURES / name)


def open_and_label(
    p: fe.Preflight, chain: ChainRun, fixture: str, label: str
) -> tuple[dict[str, Any], str]:
    """Open a fixture issue as the writer, label it, return it and GitHub's label time."""

    title, body = _issue_text(fixture)
    if not chain.opened_issues:
        p.teardown.push(ISSUES_STEP, lambda: close_issues(p, chain))
    issue = _github(
        p, "POST", f"/repos/{p.config.repo}/issues", {"title": title, "body": body}, want=201
    )
    chain.opened_issues.append(int(issue["number"]))
    _github(
        p, "POST", f"/repos/{p.config.repo}/issues/{issue['number']}/labels", {"labels": [label]}
    )
    events = p._paged(f"/repos/{p.config.repo}/issues/{issue['number']}/events")
    labelled = [
        e
        for e in events
        if e.get("event") == "labeled" and (e.get("label") or {}).get("name") == label
    ]
    if not labelled:
        raise fe.PreflightFailed("the label event was not recorded")
    return issue, str(labelled[-1]["created_at"])


def issue_comments(p: fe.Preflight, number: int) -> list[dict[str, Any]]:
    return p._paged(f"/repos/{p.config.repo}/issues/{number}/comments")


def open_pulls(p: fe.Preflight) -> list[dict[str, Any]]:
    return p._paged(f"/repos/{p.config.repo}/pulls?state=open")


def _pulls_closing(p: fe.Preflight, number: int) -> list[dict[str, Any]]:
    return [pull for pull in open_pulls(p) if judge._closes(str(pull.get("body") or ""), number)]


def _rules(p: fe.Preflight) -> dict[str, Any]:
    code, out, _ = cluster_cli(p, ["memory", fe.FACTORY_AGENT, "--rules"])
    parsed = parse_cli_json(code, out)
    if code != 0 or not isinstance(parsed, dict):
        raise fe.PreflightFailed(f"curie cluster memory --rules failed (exit {code})")
    return parsed


def _rule_action(
    p: fe.Preflight, chain: ChainRun, verb: str, rule: Mapping[str, Any], role: str
) -> tuple[int, str, float]:
    return cluster_cli(
        p,
        [
            "memory",
            fe.FACTORY_AGENT,
            f"--{verb}",
            str(rule["id"]),
            "--expected-version",
            str(rule["version"]),
            "--session-file",
            str(chain.session_files[role]),
        ],
        timeout=judge.CLI_WITHIN + 30,
    )


def beat_a(p: fe.Preflight, chain: ChainRun) -> None:
    code, out, _ = cluster_cli(p, ["overrides", fe.FACTORY_AGENT, "--memory-rule-proposals", "on"])
    if code != 0:
        raise fe.PreflightFailed(f"turning rule proposals on failed (exit {code})")
    code, out, _ = cluster_cli(p, ["overrides", fe.FACTORY_AGENT])
    proposals = overrides_proposals(code, out)
    chain.record["agent"]["memory_rule_proposals"] = proposals
    if proposals is not True:
        raise ChainStopped(f"the agent read does not show rule proposals on (read {proposals})")
    sample_usage(p, chain, "before_a")
    issue, labelled_at = open_and_label(p, chain, "issue-a.md", p.config.label)
    number = int(issue["number"])
    app = chain.record["identities"]["app_login"]
    repository_id = int(chain.record["identities"]["repository_id"])
    # The first read, from label time on, that shows the investigating lead.
    seen: list[tuple[Mapping[str, Any], str]] = []

    def question() -> dict[str, Any] | None:
        comments = issue_comments(p, number)
        lead = investigating_comment(comments, app) if not seen else None
        if lead is not None:
            seen.append((copy.deepcopy(dict(lead)), now_iso()))
        for comment in comments:
            if (comment.get("user") or {}).get("login") == app and judge.QUESTIONS_MARKER in str(
                comment.get("body")
            ):
                return comment
        return None

    asked = _wait("the questions comment", judge.QUESTIONS_WITHIN + 60, question)
    work_item_id = _wait(
        "ticket A's work item", 60, lambda: work_item_for_issue(p, repository_id, issue)
    )
    quiet_until = judge.parse_time(asked["created_at"]) + dt.timedelta(
        seconds=judge.QUIET_AFTER_QUESTIONS + 5
    )
    while dt.datetime.now(dt.UTC) < quiet_until:
        time.sleep(fe.POLL_SECONDS)
    observed_at = now_iso()
    chain.record["beat_a"] = beat_a_record(
        issue_number=number,
        labelled_at=labelled_at,
        investigating=seen[0] if seen else None,
        comments=issue_comments(p, number),
        labels=_github(p, "GET", f"/repos/{p.config.repo}/issues/{number}/labels"),
        quiet_rows=request_rows(p, work_item_id),
        observed_at=observed_at,
        work_item_id=work_item_id,
    )
    sample_usage(p, chain, "after_a")


def beat_b(p: fe.Preflight, chain: ChainRun) -> None:
    a = chain.record["beat_a"]
    number = int(a["issue_number"])
    answer_text = (FIXTURES / "answer-a.md").read_text().strip()
    answer = _github(
        p,
        "POST",
        f"/repos/{p.config.repo}/issues/{number}/comments",
        {"body": answer_text},
        want=201,
    )
    _wait(
        "the pull request of ticket A",
        judge.PULL_REQUEST_WITHIN + 60,
        lambda: _pulls_closing(p, number),
    )
    since = (judge.parse_time(a["labelled_at"]) - dt.timedelta(minutes=5)).isoformat()
    issues = p._paged(f"/repos/{p.config.repo}/issues?state=all&since={urllib.parse.quote(since)}")
    chain.record["beat_b"] = beat_b_record(
        answer=answer,
        issue_number=number,
        request_rows=request_rows(p, a["work_item_id"]),
        open_pulls=open_pulls(p),
        issues=issues,
        ticket_comments=issue_comments(p, number),
    )
    sample_usage(p, chain, "after_b")


def beat_c(p: fe.Preflight, chain: ChainRun) -> None:
    repo = p.config.repo
    work_item_id = chain.record["beat_a"]["work_item_id"]
    pull = chain.record["beat_b"]["open_pulls"][0]
    number = int(pull["number"])
    before = _github(p, "GET", f"/repos/{repo}/pulls/{number}")
    files = p._paged(f"/repos/{repo}/pulls/{number}/files")
    path, line = review_target(added_lines_by_path(files))
    posted = review_body(str(before["head"]["sha"]), path, line)
    review = _github(p, "POST", f"/repos/{repo}/pulls/{number}/reviews", posted)
    review_id = int(review["id"])
    revision_id = judge.revision_request_id(
        chain.record["identities"]["repository_id"], number, review_id
    )

    def revised() -> dict[str, Any] | None:
        rows = {str(r["id"]): r for r in request_rows(p, work_item_id)}
        row = rows.get(revision_id)
        if row is None or row.get("terminal_at") is None:
            return None
        after = _github(p, "GET", f"/repos/{repo}/pulls/{number}")
        return after if after["head"]["sha"] != before["head"]["sha"] else None

    after = _wait("the review revision", judge.PULL_REQUEST_WITHIN + 60, revised)
    head = str(after["head"]["sha"])

    def replied() -> list[dict[str, Any]] | None:
        comments = issue_comments(p, number)
        return comments if any(head in str(c.get("body")) for c in comments) else None

    pull_comments = _wait(
        "the reply naming the revision commit", judge.PULL_REQUEST_WITHIN, replied
    )
    commit = _github(p, "GET", f"/repos/{repo}/commits/{head}")
    chain.record["beat_c"] = beat_c_record(
        repo=repo,
        pull_number=number,
        pull_before=before,
        posted=posted,
        review=_github(p, "GET", f"/repos/{repo}/pulls/{number}/reviews/{review_id}"),
        review_comments=_github(
            p, "GET", f"/repos/{repo}/pulls/{number}/reviews/{review_id}/comments"
        ),
        files=files,
        request_rows=request_rows(p, work_item_id),
        transcripts=thread_transcripts(p, work_item_id),
        pull_after=after,
        committed_at=str(commit["commit"]["committer"]["date"]),
        branches=p._paged(f"/repos/{repo}/branches"),
        pull_comments=pull_comments,
        check_runs=check_runs_of(p, repo, head),
        combined_status=_github(p, "GET", f"/repos/{repo}/commits/{head}/status"),
    )
    sample_usage(p, chain, "after_c")


def beat_d(p: fe.Preflight, chain: ChainRun) -> None:
    repo = p.config.repo
    number = int(chain.record["beat_c"]["pull_number"])
    issue = int(chain.record["beat_a"]["issue_number"])
    head = p.default_branch_head()
    # Pushed before the merge, so a merge that lands but fails to answer is undone.
    p.teardown.push(DEFAULT_BRANCH_STEP, lambda: restore_default_branch(p, head))
    rules_before = _rules(p)
    status, _ = p.as_actor("PUT", f"/repos/{repo}/pulls/{number}/merge", {"merge_method": "merge"})
    if status != 200:
        _github(p, "PUT", f"/repos/{repo}/pulls/{number}/merge", {"merge_method": "squash"})
    merged = _github(p, "GET", f"/repos/{repo}/pulls/{number}")
    notice = judge.MERGED_NOTICE_PREFIX + str(merged["html_url"])

    def listed() -> dict[str, Any] | None:
        rules = _rules(p)
        return rules if rules.get("rules") else None

    def closed() -> dict[str, Any] | None:
        read = _github(p, "GET", f"/repos/{repo}/issues/{issue}")
        return read if isinstance(read, dict) and read.get("state") == "closed" else None

    def noticed() -> list[dict[str, Any]] | None:
        comments = issue_comments(p, issue)
        return comments if any(notice in str(c.get("body")) for c in comments) else None

    rules_after = _wait("the proposed rule", judge.PROMOTION_WITHIN + 60, listed)
    observed_at = now_iso()
    issue_after = _wait("ticket A closed", judge.PROMOTION_WITHIN + 60, closed)
    comments = _wait("the merge notice on ticket A", judge.PROMOTION_WITHIN + 60, noticed)
    chain.record["beat_d"] = beat_d_record(
        rules_before=rules_before,
        pull_merged=merged,
        rules_after=rules_after,
        observed_at=observed_at,
        issue_after=issue_after,
        ticket_comments=comments,
    )
    sample_usage(p, chain, "after_d")


def restore_default_branch(p: fe.Preflight, head: str) -> dict[str, Any]:
    """Move the default branch back to ``head`` when the merge moved it; verify it."""

    moved = p.default_branch_head() != head
    if moved:
        p.move_default_branch(head)
    if p.default_branch_head() != head:
        raise fe.PreflightFailed(
            "the default branch did not read back as its head before the merge"
        )
    return {"moved_back": moved}


def beat_e(p: fe.Preflight, chain: ChainRun) -> None:
    reader_login = next(str(v) for v in chain.identifiers["reader"] if isinstance(v, str))
    permission = installation_get(
        p, f"/repos/{p.config.repo}/collaborators/{urllib.parse.quote(reader_login)}/permission"
    )
    if permission.get("permission") in judge.WRITE_PERMISSIONS:
        raise ChainStopped("the reader can write to the fixture repository")
    rule = judge.rules_of(chain.record["beat_d"]["rules_after_merge"]["output"])[0]
    reader = _rule_action(p, chain, "accept", rule, "reader")
    after_reader = next(r for r in _rules(p)["rules"] if r["id"] == rule["id"])
    writer = _rule_action(p, chain, "accept", rule, "writer")
    chain.record["beat_e"] = beat_e_record(
        reader_permission=permission,
        rule_before=rule,
        reader=reader,
        rule_after_reader=after_reader,
        writer=writer,
    )
    sample_usage(p, chain, "after_e")


def beat_f(p: fe.Preflight, chain: ChainRun) -> None:
    repo = p.config.repo
    code, _, _ = cluster_cli(
        p,
        [
            "coding-instructions",
            "set",
            "--file",
            str(FIXTURES / "requester-instructions.md"),
            "--expected-revision",
            "0",
            "--session-file",
            str(chain.session_files["writer"]),
        ],
    )
    if code != 0:
        raise ChainStopped(f"setting the conflicting coding instruction failed (exit {code})")
    issue, labelled_at = open_and_label(p, chain, "issue-b.md", p.config.label)
    number = int(issue["number"])
    pulls = _wait(
        "ticket B's pull request", judge.PULL_REQUEST_WITHIN + 60, lambda: _pulls_closing(p, number)
    )
    repository_id = int(chain.record["identities"]["repository_id"])
    work_item_id = _wait(
        "ticket B's work item", 60, lambda: work_item_for_issue(p, repository_id, issue)
    )
    detail = p.work_item_detail(work_item_id) or {}
    chain.record["beat_f"] = beat_f_record(
        issue_number=number,
        instruction_set=code == 0,
        labelled_at=labelled_at,
        open_pulls=open_pulls(p),
        work_item=detail,
        files=p._paged(f"/repos/{repo}/pulls/{pulls[0]['number']}/files"),
    )
    sample_usage(p, chain, "after_f")


def beat_g(p: fe.Preflight, chain: ChainRun) -> None:
    rule_id = str(chain.record["beat_e"]["rule_before"]["id"])
    current = next(r for r in _rules(p)["rules"] if r["id"] == rule_id)
    removal = _rule_action(p, chain, "remove", current, "writer")
    refusal = removal_refusal(rule_id, removal)
    if refusal:
        raise ChainStopped(refusal)
    issue, labelled_at = open_and_label(p, chain, "issue-c.md", p.config.label)
    number = int(issue["number"])
    _wait(
        "ticket C's pull request", judge.PULL_REQUEST_WITHIN + 60, lambda: _pulls_closing(p, number)
    )
    repository_id = int(chain.record["identities"]["repository_id"])
    work_item_id = _wait(
        "ticket C's work item", 60, lambda: work_item_for_issue(p, repository_id, issue)
    )
    chain.record["beat_g"] = beat_g_record(
        issue_number=number,
        labelled_at=labelled_at,
        rule_id=rule_id,
        removal=removal,
        open_pulls=open_pulls(p),
        work_item=p.work_item_detail(work_item_id) or {},
    )
    sample_usage(p, chain, "after_g")


def removal_refusal(rule_id: str, removal: tuple[int, str, float]) -> str | None:
    """Why the writer's removal did not remove the rule, or None when it did."""

    exit_code, stdout, _elapsed = removal
    if exit_code != 0:
        return f"the writer's removal exited {exit_code}"
    try:
        rule = judge.accepted_rule_of(parse_cli_json(exit_code, stdout))
    except (KeyError, TypeError):
        return "the writer's removal printed no rule"
    if not isinstance(rule, Mapping) or rule.get("id") != rule_id:
        return "the writer's removal changed another rule"
    if rule.get("state") != judge.REMOVED_STATE:
        return f"the writer's removal left the rule {rule.get('state')}"
    return None


def retired_bundle_copy(p: fe.Preflight) -> Path:
    """A private copy of the bundle the deploy uploaded, with ``workflow/result.json`` added.

    The copy ``build_runner_layer`` prepared carries the runner lock ``curie
    build`` wrote; the source bundle does not, so the CLI would refuse it at the
    connector lock preflight before any upload reaches the API's refusal.
    """

    if p.deployed_bundle is None:
        raise ChainStopped("no bundle was deployed to copy for the retired declaration")
    target = p.workdir / "retired-bundle"
    shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(p.deployed_bundle, target)
    (target / "workflow").mkdir(exist_ok=True)
    (target / "workflow" / "result.json").write_text("{}\n")
    return target


def delete_label(p: fe.Preflight, label: str) -> dict[str, Any]:
    """Delete a label the run created, then read it back: only a 404 is gone."""

    path = f"/repos/{p.config.repo}/labels/{urllib.parse.quote(label, safe='')}"
    status, _ = p.as_actor("DELETE", path)
    if status not in (204, 404):
        raise fe.PreflightFailed(f"deleting the label answered HTTP {status}")
    read, _ = p.as_actor("GET", path)
    if read != 404:
        raise fe.PreflightFailed(f"the label still reads back (HTTP {read})")
    return {"deleted": True}


def ensure_label(p: fe.Preflight) -> None:
    """The scenario's intake label step, in place of the shared ``ensure_label``.

    ``--stop-after links`` opens no ticket, so it creates no label. The full
    mode creates it when missing and then deletes it in teardown, verified by a
    404 read-back; the undo is pushed before the create, so an ambiguous create
    is undone too.
    """

    if chain_of(p).stop_after == STOP_AFTER_LINKS:
        p.step("live chain stop mode creates no intake label")
        return
    label = p.config.label
    p.ensure_label(
        on_create=lambda: p.teardown.push(INTAKE_LABEL_STEP, lambda: delete_label(p, label))
    )


def serving_version(rows: Iterable[Mapping[str, Any]]) -> str | None:
    """The version the worker boots for the agent, or None when none serves.

    The worker's selection (``apps/worker/src/curie_worker/binding.py``
    ``_RESOLVE_AGENT_SQL``): an ``active`` deployment whose version is the
    agent's, prod before any other environment, then the newest
    ``deployed_at``, then the greatest deployment id.
    """

    active = [row for row in rows if row.get("status") == "active" and row.get("version_id")]
    if not active:
        return None
    best = max(
        active,
        key=lambda row: (
            row.get("environment") == "prod",
            judge.parse_time(row["deployed_at"]),
            str(row["id"]),
        ),
    )
    return str(best["version_id"])


def _in_force_version(p: fe.Preflight) -> str | None:
    """The agent's serving version: a refused deploy must leave it serving and unchanged."""

    agent_id = _uuid(str(p.evidence.get("agent_id") or ""))
    rows = sql_json(
        p,
        "SELECT d.id, d.status, d.environment, d.deployed_at, d.version_id "
        "FROM deployments d JOIN agent_versions v "
        "ON v.id = d.version_id AND v.agent_id = d.agent_id "
        f"WHERE d.agent_id = '{agent_id}'",
    )
    return serving_version(rows)


def beat_h(p: fe.Preflight, chain: ChainRun) -> None:
    repo = p.config.repo
    label = f"{judge.UNBOUND_LABEL_PREFIX}{p.run_id}"
    p.teardown.push(LABEL_STEP, lambda: delete_label(p, label))
    p.as_actor("POST", f"/repos/{repo}/labels", {"name": label, "color": "ededed"})
    issue, labelled_at = open_and_label(p, chain, "issue-unbound.md", label)
    number = int(issue["number"])
    quiet_until = judge.parse_time(labelled_at) + dt.timedelta(
        seconds=judge.UNBOUND_QUIET_WINDOW + 5
    )
    while dt.datetime.now(dt.UTC) < quiet_until:
        time.sleep(fe.POLL_SECONDS)
    observed_at = now_iso()
    repository_id = int(chain.record["identities"]["repository_id"])
    unbound = work_item_for_issue(p, repository_id, issue)
    before = _in_force_version(p)
    code, out, _ = cluster_cli(
        p,
        [
            "deploy",
            "--plugin-dir",
            str(retired_bundle_copy(p)),
            "--agent",
            fe.FACTORY_AGENT,
            "--env",
            "prod",
            "--chart",
            str(p.chart_dir),
        ],
        timeout=600,
    )
    chain.record["beat_h"] = beat_h_record(
        label=label,
        observed_at=observed_at,
        work_items=[{"id": unbound}] if unbound else [],
        comments=issue_comments(p, number),
        events=p._paged(f"/repos/{repo}/issues/{number}/events"),
        deploy=(code, out),
        version_before=before,
        version_after=_in_force_version(p),
    )
    sample_usage(p, chain, "after_h")


def run_beats(p: fe.Preflight, chain: ChainRun) -> None:
    """Each beat, then its judge: a failed beat stops before the next one's first side effect.

    So, for example, beat (c) with its revision checks is judged before beat
    (d) merges the pull request.
    """

    for name, beat in (
        ("beat_a", beat_a),
        ("beat_b", beat_b),
        ("beat_c", beat_c),
        ("beat_d", beat_d),
        ("beat_e", beat_e),
        ("beat_f", beat_f),
        ("beat_g", beat_g),
        ("beat_h", beat_h),
    ):
        beat(p, chain)
        verdict = judge.judge_beat(name, chain.record)
        p.step(
            f"live chain {name.replace('_', ' ')} recorded",
            passed=verdict.passed,
            reasons=list(verdict.reasons),
        )
        if not verdict.passed:
            raise ChainStopped(f"{name} failed: " + "; ".join(verdict.reasons))


# --------------------------------------------------------------------------
# finish
# --------------------------------------------------------------------------


def finish(p: fe.Preflight, results: list[dict[str, Any]]) -> bool:
    """Judge what exists after teardown, store the verdict, and pass or fail the run."""

    chain = chain_of(p)
    record = {
        **chain.record,
        "attempt": chain.attempt,
        "teardown": teardown_section(results, namespace_gone=namespace_gone(p)),
    }
    if chain.stop_after == STOP_AFTER_LINKS:
        beats = tuple(
            judge.judge_beat(name, record) for name in ("preflight", "session_probe", "teardown")
        )
        verdict = judge.ChainVerdict(chain.attempt, beats)
    else:
        verdict = judge.judge_chain(record)
    p.evidence["live_chain"] = verdict.evidence()
    for beat in verdict.beats:
        if not beat.passed:
            fe.log(f"live chain {beat.beat} failed: {'; '.join(beat.reasons)}")
    return verdict.passed
