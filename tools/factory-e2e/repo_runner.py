"""Repository runner observations for Accepted ADR 0216.

The public judge entry point is ``curie dev factory-e2e judge-repo-runner
--observation <file>``. It checks observations without contacting a cluster or
provider. Synthetic samples prove the judge's contract only; they are not live
evidence of repository routing or a tool manifest.

Live acceptance is deferred to the w1 runtime wave. The current API fixes a
repository selection for the lifetime of a thread, so its
``workspace.selection_conflict`` fails the required change on one thread.

``curie dev factory-e2e run --scenario repo-runner --context k8`` reads the
ordinary operator GitHub App and fixture settings. It refuses model keys and
external model URLs, and serves its own scripted endpoint without forwarding.
Set ``CURIE_LOCAL_STUB_HOST`` to an IPv4 the cluster can reach, and
``CURIE_FACTORY_REPO_RUNNER_PLAN`` to an operator-maintained JSON file.

The plan has exactly ``bindings`` and ``turns``. Bindings are complete chart
entries: ``name``, ``tracker: github``, ``trackerScopeId``, and one ``repos``
entry with ``alias``, ``codeHost: github``, matching ``projectId``, ``path``,
``runner``, ``docker: {enabled: false}``, ``bases`` and ``ci``. Runner and probe
fields follow ``charts/curie/values.schema.json``. For example, fixture scope
``101`` binds only ``acme-corp/repo-x`` and scope ``102`` binds only
``acme-corp/repo-y``. The real fixture ids must be read from GitHub. At w1,
operator tooling builds the digest-pinned repository images before this run.

Each turn names ``repository``, ``thread``, ``text`` and ``expect``. Four turns
are required: completed X, completed Y on X's thread, then
``repo_runner_pool_missing`` and ``repo_runner_base_mismatch`` on different
fresh threads using a repository that succeeded. The driver adds the canonical
GitHub repository URL to each message. Refusal turns also supply a complete
``worker_pools`` map following ``apps/worker/schema/repo-runner-pools.schema.json``.
Preserve the rendered entries except for the target pool name in the first
fault or target base digest in the second. The chart renders pool names as
``factory-tools-agent-factory-e2e-img-<first 12 index hex digits>-runner-pool``.
The missing name must be absent, and the stale base must differ from the actual
installed platform index digest. Both facts are checked before sending.

Only the new namespace and its worker Deployment are mutated. The namespace
carries ``owner=factory-tools`` alongside the harness ownership marker. The
Deployment pool input is patched, read back after rollout, and restored before
namespace deletion. No stale namespace sweep or SQL mutation occurs. The judge
requires the runner's named container spec/imageID, actual Bash tool result,
boot manifest count, zero model requests or claims on refusal, and retirement
of X's claim observed after Y completes with a fresh sandbox and pod UID.
"""

from __future__ import annotations

import argparse
import copy
import dataclasses
import hashlib
import ipaddress
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote

import factory_e2e as fe

SCENARIO = "repo-runner"
MANIFEST_COMMAND = "cat /etc/curie/tool-manifest.json"
REFUSAL_REASONS = frozenset({"repo_runner_pool_missing", "repo_runner_base_mismatch"})
SCRIPTED_KEY = "scripted-no-upstream"
TEST_RELEASE = "factory-tools"
ROOT = Path(__file__).resolve().parents[2]
_REPOSITORY = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._~-]*/[A-Za-z0-9_][A-Za-z0-9._~-]*\Z")
_MANIFEST_COUNT = re.compile(r"\btool[ _-]manifest\b[^\n]*?\b([0-9]+)\s+tools\b", re.I)


def scenario_values(base: Mapping[str, Any], bindings: list[dict[str, Any]]) -> dict[str, Any]:
    """Copy install values and preserve complete canonical repository bindings."""

    values = copy.deepcopy(dict(base))
    values.setdefault("factory", {})["bindings"] = copy.deepcopy(bindings)
    return values


def _nonblank(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


@lru_cache(maxsize=3)
def _schema_resource(relative: str) -> dict[str, Any]:
    schema = json.loads((ROOT / relative).read_text())
    if not isinstance(schema, dict):
        raise ValueError(f"canonical schema {relative} is not an object")
    return schema


def _runner_schema() -> dict[str, Any]:
    return _schema_resource("charts/curie/values.schema.json")["definitions"]["repositoryRunner"]


def _schema_failures(
    value: object, schema: Mapping[str, Any], authority: Mapping[str, Any], path: str
) -> list[str]:
    """Evaluate the artifact schemas' field constraints with the standard library.

    This deliberately covers the object, array and scalar vocabulary used by
    the existing runner, pool and manifest resources. An unhandled constraint
    fails closed if those canonical resources grow.
    """

    supported = {
        "$ref", "$schema", "$id", "$defs", "definitions", "title", "description", "default",
        "type", "properties", "required", "additionalProperties", "propertyNames", "items",
        "additionalItems", "prefixItems", "minItems", "maxItems", "uniqueItems", "minLength",
        "maxLength", "pattern", "minimum", "maximum", "enum", "const", "minProperties",
        "maxProperties", "patternProperties",
    }
    unknown = set(schema) - supported
    if unknown:
        return [f"{path}: unsupported canonical schema constraints {sorted(unknown)}"]
    if "$ref" in schema:
        target: Any = authority
        for part in schema["$ref"].removeprefix("#/").split("/"):
            target = target[part]
        return _schema_failures(value, target, authority, path)
    kind = schema.get("type")
    valid = {
        "object": isinstance(value, Mapping),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": type(value) is int,
        "boolean": type(value) is bool,
    }
    if kind is not None and not valid.get(kind, False):
        return [f"{path}: expected {kind}"]
    failures: list[str] = []
    if "enum" in schema and value not in schema["enum"]:
        failures.append(f"{path}: value is outside the canonical enum")
    if "const" in schema and value != schema["const"]:
        failures.append(f"{path}: value differs from the canonical constant")
    if isinstance(value, Mapping):
        properties = schema.get("properties", {})
        for field in schema.get("required", []):
            if field not in value:
                failures.append(f"{path}: missing required {field}")
        for field, item in value.items():
            if "propertyNames" in schema:
                failures.extend(
                    _schema_failures(field, schema["propertyNames"], authority, f"{path} key")
                )
            rules = [properties[field]] if field in properties else []
            rules.extend(
                rule for pattern, rule in schema.get("patternProperties", {}).items()
                if isinstance(field, str) and re.search(pattern, field)
            )
            if not rules:
                rules = [schema.get("additionalProperties", {})]
            for rule in rules:
                if rule is False:
                    failures.append(f"{path}: unknown field {field}")
                elif isinstance(rule, Mapping):
                    failures.extend(_schema_failures(item, rule, authority, f"{path}.{field}"))
        if "minProperties" in schema and len(value) < schema["minProperties"]:
            failures.append(f"{path}: fewer than the canonical minimum properties")
        if "maxProperties" in schema and len(value) > schema["maxProperties"]:
            failures.append(f"{path}: more than the canonical maximum properties")
    elif isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            failures.append(f"{path}: fewer than the canonical minimum items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            failures.append(f"{path}: more than the canonical maximum items")
        if schema.get("uniqueItems") and any(item in value[:i] for i, item in enumerate(value)):
            failures.append(f"{path}: duplicate items")
        items = schema.get("items", {})
        prefixes = schema.get("prefixItems", items if isinstance(items, list) else [])
        for index, item in enumerate(value):
            rule = prefixes[index] if index < len(prefixes) else (
                schema.get("additionalItems", {}) if isinstance(items, list) else items
            )
            if rule is False:
                failures.append(f"{path}: unexpected array item {index}")
            elif isinstance(rule, Mapping):
                failures.extend(_schema_failures(item, rule, authority, f"{path}[{index}]"))
    elif isinstance(value, str):
        if "pattern" in schema:
            pattern = schema["pattern"]
            search = (
                re.fullmatch if pattern.startswith("^") and pattern.endswith("$") else re.search
            )
            if search(pattern, value) is None:
                failures.append(f"{path}: value does not match the canonical pattern")
        if "minLength" in schema and len(value) < schema["minLength"]:
            failures.append(f"{path}: shorter than the canonical minimum")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            failures.append(f"{path}: longer than the canonical maximum")
    elif type(value) is int:
        if "minimum" in schema and value < schema["minimum"]:
            failures.append(f"{path}: below the canonical minimum")
        if "maximum" in schema and value > schema["maximum"]:
            failures.append(f"{path}: above the canonical maximum")
    return failures


def _image_digest(value: object) -> str | None:
    if not isinstance(value, str) or any(character.isspace() for character in value):
        return None
    pattern = (
        _runner_schema()["properties"]["base"]["pattern"]
        .removeprefix("^")
        .removesuffix("$")
    )
    match = re.search(r"(?:^|@|://)(" + pattern + r")\Z", value)
    return match.group(1) if match else None


def _runner_failures(repository: str, runner: object) -> list[str]:
    prefix = f"binding {repository}: "
    authority = _schema_resource("charts/curie/values.schema.json")
    schema = _runner_schema()
    failures = _schema_failures(runner, schema, authority, f"binding {repository}")
    if not isinstance(runner, Mapping):
        return failures
    tools = runner.get("tools")
    if not isinstance(tools, list) or not tools:
        failures.append(prefix + "this scenario requires declared catalog tools")
    probes = runner.get("probes", [])
    if isinstance(probes, list):
        names = [probe.get("name") for probe in probes if isinstance(probe, Mapping)]
        if any(name in names[:index] for index, name in enumerate(names)):
            failures.append(prefix + "probes must have unique names")
    return failures


def _exact_digest(value: object) -> bool:
    pattern = _runner_schema()["properties"]["base"]["pattern"]
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _manifest_failures(turn: Mapping[str, Any], runner: Mapping[str, Any]) -> list[str]:
    output = turn.get("manifest_output")
    if not isinstance(output, str) or not output.strip():
        return ["no manifest output was captured from the tool result"]
    try:
        manifest = json.loads(output)
    except ValueError:
        return ["the tool result is not valid manifest JSON"]
    if not isinstance(manifest, dict):
        return ["the tool result is not a manifest object"]
    schema = _schema_resource("docs/schemas/tool-manifest.schema.json")
    failures = _schema_failures(manifest, schema, schema, "manifest")
    tools = manifest.get("tools")
    if not isinstance(tools, list):
        return [*failures, "the manifest tools field must be a list"]
    names: set[str] = set()
    catalog: set[str] = set()
    probed: set[str] = set()
    for tool in tools:
        if not isinstance(tool, Mapping):
            continue
        if any(not _nonblank(tool.get(field)) for field in ("name", "version", "path")):
            continue
        name = tool["name"]
        if name in names:
            failures.append(f"the manifest repeats tool {name}")
        names.add(name)
        if tool.get("source") == "catalog":
            catalog.add(name)
        elif tool.get("source") == "probe":
            probed.add(name)
    declared = runner.get("tools")
    if isinstance(declared, list) and all(isinstance(tool, str) for tool in declared):
        if catalog != set(declared):
            failures.append("the manifest catalog tools do not equal the binding's declared tools")
        if manifest.get("docker_cli") != ("docker-cli" in declared):
            failures.append("manifest docker_cli does not match the binding's declared tools")
    probes = runner.get("probes", [])
    if isinstance(probes, list):
        for probe in probes:
            if (
                isinstance(probe, Mapping) and _nonblank(probe.get("name"))
                and probe["name"] not in probed
            ):
                failures.append(f"the manifest does not include declared probe {probe.get('name')}")
    log = turn.get("boot_log")
    counts = _MANIFEST_COUNT.findall(log) if isinstance(log, str) else []
    if not counts:
        failures.append("no boot log line names the loaded tool manifest and tool count")
    elif any(int(count) != len(tools) for count in counts):
        failures.append("the boot log manifest tool count differs from the actual tool result")
    return failures


def judge_repo_runner(observation: object) -> list[str]:
    """Return each unmet repository runner requirement; an empty list means passed."""

    if not isinstance(observation, Mapping):
        return ["the repository runner observation must be an object"]
    try:
        _runner_schema()
        _schema_resource("docs/schemas/tool-manifest.schema.json")
    except (OSError, ValueError, KeyError, TypeError):
        return ["canonical repository runner and manifest schemas are unavailable"]
    failures: list[str] = []
    bindings = observation.get("bindings")
    if not isinstance(bindings, Mapping) or len(bindings) < 2:
        failures.append("at least two repository runner bindings are required")
        bindings = bindings if isinstance(bindings, Mapping) else {}
    for repository, runner in bindings.items():
        if not isinstance(repository, str) or _REPOSITORY.fullmatch(repository) is None:
            failures.append("a binding has an invalid repository path")
        failures.extend(_runner_failures(str(repository), runner))
    turns = observation.get("turns")
    if not isinstance(turns, list) or not turns:
        return [*failures, "the observation requires a list of observed turns"]
    completed: list[Mapping[str, Any]] = []
    refused: set[str] = set()
    for index, turn in enumerate(turns):
        label = f"turn {index + 1}: "
        if not isinstance(turn, Mapping):
            failures.append(label + "turn observation must be an object")
            continue
        repository = turn.get("repository")
        if not isinstance(repository, str) or _REPOSITORY.fullmatch(repository) is None:
            failures.append(label + "repository path is missing or malformed")
        if not _nonblank(turn.get("thread")):
            failures.append(label + "thread is missing")
        errors = turn.get("errors")
        if not isinstance(errors, list):
            failures.append(label + "scripted model error evidence is missing or malformed")
        elif errors:
            failures.extend(label + f"scripted model error: {error}" for error in errors)
        status = turn.get("status")
        if status == "refused":
            reason = turn.get("reason")
            if not isinstance(reason, str) or reason not in REFUSAL_REASONS:
                failures.append(label + f"unexpected repository runner refusal: {reason!r}")
            else:
                refused.add(reason)
            for field in ("sandbox", "pod_uid", "image_id", "spec_image", "claim"):
                if turn.get(field):
                    failures.append(label + f"refused turn acquired {field}")
            if type(turn.get("model_requests")) is not int or turn.get("model_requests") != 0:
                failures.append(label + "refusal must precede every model request")
            if not isinstance(turn.get("new_claims"), list) or turn.get("new_claims") != []:
                failures.append(
                    label + "refusal requires explicit evidence of no new sandbox claims"
                )
            if reason == "repo_runner_pool_missing" and turn.get("missing_pool_absent") is not True:
                failures.append(label + "the missing pool was not confirmed absent before the turn")
            if reason == "repo_runner_base_mismatch":
                installed, configured = turn.get("installed_base"), turn.get("configured_base")
                if not _exact_digest(installed) or not _exact_digest(configured):
                    failures.append(
                        label + "stale-base proof requires exact installed/configured digests"
                    )
                elif installed == configured:
                    failures.append(
                        label + "the configured base equals the installed platform digest"
                    )
            continue
        if status != "completed":
            reason = turn.get("reason")
            failures.append(label + f"turn did not complete or refuse: {status!r} ({reason!r})")
            continue
        completed.append(turn)
        if type(turn.get("cli_exit_code")) is not int or turn.get("cli_exit_code") != 0:
            failures.append(label + "completed turn requires CLI exit code 0")
        runner_status = turn.get("runner_status")
        if (
            not isinstance(runner_status, Mapping)
            or runner_status.get("status") != "done"
            or runner_status.get("turn_active") is not False
        ):
            failures.append(label + "completed turn has no terminal runner status evidence")
        if turn.get("observed_repository") != repository:
            failures.append(label + "captured claim repository does not match the turn")
        if "capture_error" not in turn or turn["capture_error"] is not None:
            failures.append(label + "completed turn has no explicit successful capture evidence")
        if type(turn.get("model_requests")) is not int or turn.get("model_requests", 0) < 2:
            failures.append(label + "completion needs both the tool request and its result request")
        for field in ("sandbox", "pod_uid", "image_id", "spec_image", "claim"):
            if not _nonblank(turn.get(field)):
                failures.append(label + f"completed turn has no {field}")
        runner = bindings.get(repository) if isinstance(repository, str) else None
        if not isinstance(runner, Mapping):
            failures.append(label + "completed repository has no runner binding")
            continue
        platforms = runner.get("platformDigests")
        allowed = (
            {digest for digest in platforms.values() if isinstance(digest, str)}
            if isinstance(platforms, Mapping)
            else set()
        )
        index_digest = _image_digest(runner.get("image"))
        if index_digest:
            allowed.add(index_digest)
        actual = _image_digest(turn.get("image_id"))
        if actual is None or actual not in allowed:
            failures.append(
                label + "runner imageID does not equal a binding index or platform digest"
            )
        spec_image = turn.get("spec_image")
        if (
            not isinstance(spec_image, str)
            or re.fullmatch(_runner_schema()["properties"]["image"]["pattern"], spec_image) is None
            or _image_digest(spec_image) not in allowed
        ):
            failures.append(label + "runner spec image does not equal a binding digest")
        failures.extend(label + failure for failure in _manifest_failures(turn, runner))
    successes = {
        turn["repository"] for turn in completed if isinstance(turn.get("repository"), str)
    }
    for repository in bindings:
        if repository not in successes:
            failures.append(f"repository {repository} has no completed turn")
    for turn in turns:
        if (
            isinstance(turn, Mapping)
            and isinstance(turn.get("reason"), str) and turn["reason"] in REFUSAL_REASONS
            and (
                not isinstance(turn.get("repository"), str) or turn["repository"] not in successes
            )
        ):
            failures.append("a refusal did not target a repository that completed successfully")
    fault_threads: list[str] = []
    success_threads = {turn["thread"] for turn in completed if _nonblank(turn.get("thread"))}
    for turn in turns:
        if not isinstance(turn, Mapping) or turn.get("status") != "refused":
            continue
        thread = turn.get("thread")
        if not isinstance(thread, str) or thread in success_threads or thread in fault_threads:
            failures.append("each refusal must use its own fresh thread")
        elif thread:
            fault_threads.append(thread)
    changed_thread = False
    for left_index, left in enumerate(completed):
        for right in completed[left_index + 1 :]:
            if (
                left.get("repository") == right.get("repository")
                or left.get("thread") != right.get("thread")
            ):
                continue
            changed_thread = True
            if left.get("sandbox") == right.get("sandbox"):
                failures.append("the repository change on one thread reused the sandbox")
            if left.get("pod_uid") == right.get("pod_uid"):
                failures.append("the repository change on one thread reused the runner pod UID")
            if left.get("claim") == right.get("claim"):
                failures.append("the repository change on one thread reused the sandbox claim")
            retired = right.get("retired_claims")
            if not isinstance(retired, list) or left.get("claim") not in retired:
                failures.append("the repository change did not retire the previous sandbox claim")
    if not changed_thread:
        failures.append("no completed repository change was observed on the same thread")
    for reason in sorted(REFUSAL_REASONS - refused):
        failures.append(f"no terminal {reason} refusal was observed")
    return failures


def _plan_repositories(plan: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {repo["path"]: repo for binding in plan["bindings"] for repo in binding["repos"]}


def load_plan(env: Mapping[str, str]) -> dict[str, Any]:
    """Read and validate the operator's plan before any live resource is created."""

    filename = env.get("CURIE_FACTORY_REPO_RUNNER_PLAN")
    if not filename:
        raise fe.ConfigError("repo-runner needs CURIE_FACTORY_REPO_RUNNER_PLAN")
    try:
        plan = json.loads(Path(filename).expanduser().read_text())
    except (OSError, ValueError) as exc:
        raise fe.ConfigError(f"cannot read the repo-runner plan: {exc}") from None
    if not isinstance(plan, dict) or set(plan) != {"bindings", "turns"}:
        raise fe.ConfigError("repo-runner plan requires exactly bindings and turns")
    bindings = plan["bindings"]
    if not isinstance(bindings, list) or len(bindings) != 2:
        raise fe.ConfigError("repo-runner plan needs exactly two native GitHub bindings")
    names: set[str] = set()
    paths: set[str] = set()
    scopes: set[str] = set()
    for binding in bindings:
        if (
            not isinstance(binding, dict)
            or not _nonblank(binding.get("name"))
            or binding.get("tracker") != "github"
            or not isinstance(binding.get("repos"), list)
            or len(binding["repos"]) != 1
            or "agent" in binding or "codeHost" in binding
        ):
            raise fe.ConfigError(
                "each plan binding needs name, tracker=github, trackerScopeId and one repo"
            )
        repo = binding["repos"][0]
        scope = binding.get("trackerScopeId")
        if (
            not isinstance(scope, str) or re.fullmatch(r"[1-9][0-9]*", scope) is None
            or not isinstance(repo, dict) or repo.get("projectId") != scope
            or repo.get("codeHost") != "github" or not _nonblank(repo.get("alias"))
            or not isinstance(repo.get("path"), str)
            or _REPOSITORY.fullmatch(repo["path"]) is None
        ):
            raise fe.ConfigError("native GitHub scope/project IDs must match their repository")
        if binding["name"] in names or repo["path"] in paths or scope in scopes:
            raise fe.ConfigError(
                "plan binding names, repositories and native scopes must be unique"
            )
        names.add(binding["name"])
        paths.add(repo["path"])
        scopes.add(scope)
        failures = _runner_failures(repo["path"], repo.get("runner"))
        if failures:
            raise fe.ConfigError("; ".join(failures))
        if repo.get("docker") != {"enabled": False}:
            raise fe.ConfigError(
                "repo-runner plan requires docker.enabled=false for each repository"
            )
        if not isinstance(repo.get("bases"), list) or not repo["bases"]:
            raise fe.ConfigError("each plan repository must name its allowed bases")
        if not isinstance(repo.get("ci"), dict):
            raise fe.ConfigError("each plan repository must declare its ci policy")
    turns = plan["turns"]
    if not isinstance(turns, list) or len(turns) != 4:
        raise fe.ConfigError("plan needs exactly two completed turns and the two refusal turns")
    repositories = _plan_repositories(plan)
    for index, turn in enumerate(turns):
        if (
            not isinstance(turn, dict)
            or set(turn) - {"repository", "thread", "text", "expect", "worker_pools"}
            or not isinstance(turn.get("repository"), str) or turn["repository"] not in paths
            or not _nonblank(turn.get("thread")) or not _nonblank(turn.get("text"))
            or not isinstance(turn.get("expect"), str)
            or turn["expect"] not in {"completed", *REFUSAL_REASONS}
        ):
            raise fe.ConfigError(f"plan turn {index + 1} is missing or has unsupported inputs")
        if turn["expect"] == "completed":
            if "worker_pools" in turn:
                raise fe.ConfigError("successful turns cannot override worker pool inputs")
            continue
        pools = turn.get("worker_pools")
        schema = _schema_resource("apps/worker/schema/repo-runner-pools.schema.json")
        failures = _schema_failures(pools, schema, schema, f"turn {index + 1} worker_pools")
        if failures:
            raise fe.ConfigError("; ".join(failures))
        if not isinstance(pools, dict) or set(pools) != paths:
            raise fe.ConfigError(
                "a refusal worker_pools override must include every bound repository"
            )
        for repository, pool in pools.items():
            runner = repositories[repository]["runner"]
            expected = [
                _image_digest(runner["image"]), runner["platformDigests"]["linux/amd64"],
                runner["platformDigests"]["linux/arm64"],
            ]
            if pool["digests"] != expected or pool["docker"] is not False:
                raise fe.ConfigError(
                    "fault overrides must preserve declared runner digests and Docker"
                )
            if repository != turn["repository"] and pool["base"] != runner["base"]:
                raise fe.ConfigError("a fault override changes an unrelated repository base")
        configured = pools[turn["repository"]]["base"]
        declared = repositories[turn["repository"]]["runner"]["base"]
        if (configured != declared) != (turn["expect"] == "repo_runner_base_mismatch"):
            raise fe.ConfigError("only the stale-base fault may change the target's runner base")
    first, second, missing, stale = turns
    if (
        first["expect"] != "completed" or second["expect"] != "completed"
        or first["repository"] == second["repository"] or first["thread"] != second["thread"]
        or {missing["expect"], stale["expect"]} != REFUSAL_REASONS
        or any(turn["repository"] not in {first["repository"], second["repository"]}
               for turn in (missing, stale))
        or len({first["thread"], missing["thread"], stale["thread"]}) != 3
    ):
        raise fe.ConfigError(
            "plan requires X then Y on one thread, followed by both faults on fresh threads"
        )
    return plan


def load_repo_runner_config(env: Mapping[str, str], *, context: str | None) -> fe.FactoryConfig:
    """Use the operator's GitHub fixtures and an exclusively scripted model."""

    load_plan(env)
    if env.get("CURIE_FACTORY_MODEL_API_KEY") or env.get("CURIE_FACTORY_MODEL_BASE_URL"):
        raise fe.ConfigError("repo-runner refuses model credentials and external model endpoints")
    if (context or env.get("CURIE_FACTORY_KUBE_CONTEXT")) != "k8":
        raise fe.ConfigError("repo-runner only uses --context k8")
    host = env.get("CURIE_LOCAL_STUB_HOST", "")
    try:
        address = ipaddress.IPv4Address(host)
    except ValueError:
        raise fe.ConfigError(
            "repo-runner needs CURIE_LOCAL_STUB_HOST as a cluster-reachable IPv4"
        ) from None
    if address.is_loopback or address.is_unspecified or address.is_multicast:
        raise fe.ConfigError("CURIE_LOCAL_STUB_HOST must be reachable from the k8 cluster")
    inputs = dict(env)
    inputs["CURIE_FACTORY_MODEL_API_KEY"] = SCRIPTED_KEY
    inputs["CURIE_FACTORY_MODEL"] = "claude-sonnet-4-5"
    config = fe.load_config(inputs, context="k8")
    return dataclasses.replace(config, model_base_url=None)


class ScriptedManifestModel:
    """A Messages endpoint that issues one manifest probe and never forwards HTTP."""

    def __init__(self, *, host: str = "0.0.0.0", port: int = 0) -> None:
        self._lock = threading.Lock()
        self._record: dict[str, Any] | None = None
        self._stage = 0
        script = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def do_POST(self) -> None:  # noqa: N802
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if length < 1 or length > 16 * 1024 * 1024:
                        raise ValueError("invalid model request length")
                    request = json.loads(self.rfile.read(length))
                    status, response = script._response(self.path.split("?", 1)[0], request)
                except (ValueError, TypeError):
                    status, response = script._reject("malformed model request")
                    request = {}
                if status == 200 and isinstance(request, dict) and request.get("stream"):
                    module = fe._load_by_path(
                        "factory_e2e_manifest_sse", ROOT / "tools/model-script/model_script.py"
                    )
                    body = module.message_to_sse(response)
                    content_type = "text/event-stream"
                else:
                    body = json.dumps(response).encode()
                    content_type = "application/json"
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._closed = False

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        self._thread.start()

    def begin_turn(self, turn_id: str) -> None:
        with self._lock:
            identifier = hashlib.sha256(turn_id.encode()).hexdigest()[:20]
            self._record = {
                "model_requests": 0, "manifest_output": "", "errors": [],
                "tool_use_id": f"toolu_manifest_{identifier}",
            }
            self._stage = 0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            if self._record is None:
                raise RuntimeError("no scripted turn is active")
            return copy.deepcopy(self._record)

    def _reject(self, reason: str) -> tuple[int, dict[str, Any]]:
        with self._lock:
            if self._record is not None:
                self._record["errors"].append(reason)
        return 422, {"type": "error", "error": {"type": "invalid_request_error", "message": reason}}

    def _response(self, path: str, request: object) -> tuple[int, dict[str, Any]]:
        with self._lock:
            if self._record is None:
                return 422, {"error": "no scripted turn is active"}
            record = self._record
            record["model_requests"] += 1
            identifier = record["tool_use_id"]
            messages = request.get("messages") if isinstance(request, dict) else None
            tools = request.get("tools") if isinstance(request, dict) else None
            if (
                path != "/v1/messages" or not isinstance(messages, list)
                or not isinstance(tools, list)
                or not any(isinstance(tool, dict) and tool.get("name") == "Bash" for tool in tools)
                or self._stage > 1
            ):
                record["errors"].append("unexpected model request")
                return 422, {"error": "unexpected model request"}
            if self._stage == 0:
                content = [{
                    "type": "tool_use", "id": identifier, "name": "Bash",
                    "input": {"command": MANIFEST_COMMAND},
                }]
                stop = "tool_use"
            else:
                uses: list[dict[str, Any]] = []
                results: list[dict[str, Any]] = []
                for message in messages:
                    if (
                        not isinstance(message, dict)
                        or not isinstance(message.get("content"), list)
                    ):
                        continue
                    for block in message["content"]:
                        if not isinstance(block, dict):
                            continue
                        if message.get("role") == "assistant" and block.get("id") == identifier:
                            uses.append(block)
                        if message.get("role") == "user" and block.get("tool_use_id") == identifier:
                            results.append(block)
                if (
                    len(uses) != 1 or len(results) != 1
                    or uses[0].get("type") != "tool_use" or uses[0].get("name") != "Bash"
                    or not isinstance(uses[0].get("input"), dict)
                    or uses[0]["input"].get("command") != MANIFEST_COMMAND
                    or results[0].get("type") != "tool_result" or results[0].get("is_error")
                ):
                    record["errors"].append("no matching manifest Bash tool result")
                    return 422, {"error": "no matching manifest Bash tool result"}
                output = results[0].get("content")
                if isinstance(output, list):
                    output = "\n".join(
                        block["text"] for block in output
                        if isinstance(block, dict) and block.get("type") == "text"
                        and isinstance(block.get("text"), str)
                    )
                if not isinstance(output, str):
                    record["errors"].append("manifest tool result has no text")
                    return 422, {"error": "manifest tool result has no text"}
                record["manifest_output"] = output
                content = [{"type": "text", "text": "Manifest captured."}]
                stop = "end_turn"
            self._stage += 1
            return 200, {
                "id": f"msg_manifest_{self._stage}", "type": "message", "role": "assistant",
                "model": "scripted", "content": content, "stop_reason": stop,
                "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 1},
            }

    def close(self) -> dict[str, bool]:
        if not self._closed:
            self._closed = True
            if self._thread.is_alive():
                self._server.shutdown()
                self._thread.join(timeout=5)
            self._server.server_close()
        if self._thread.is_alive():
            raise RuntimeError("scripted model endpoint did not stop")
        return {"stopped": True}


class RepoRunnerRun:
    """Install and observe one operator-owned repository runner scenario."""

    def __init__(self, p: fe.Preflight, plan: dict[str, Any]) -> None:
        self.p = p
        self.plan = plan
        self.repositories = _plan_repositories(plan)
        self.baseline_pools: dict[str, Any] = {}
        self.fault_active = False
        self.installed_base = ""
        self.channel = f"C0EXAMPLE{p.run_id.upper()}"
        self.model: ScriptedManifestModel | None = None
        self.obs: dict[str, Any] = {
            "provenance": "live CLI, scripted endpoint and owned Kubernetes resource observations",
            "bindings": {path: repo["runner"] for path, repo in self.repositories.items()},
            "turns": [],
        }

    def _worker_pool_slot(self) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
        deployment = json.loads(self.p.kubectl(
            "-n", self.p.namespace, "get", "deployment", f"{self.p.release}-worker", "-o", "json"
        ))
        containers = deployment["spec"]["template"]["spec"]["containers"]
        workers = [(index, item) for index, item in enumerate(containers)
                   if item.get("name") == "worker"]
        if len(workers) != 1:
            raise fe.PreflightFailed("owned deployment has no unique worker container")
        worker_index, worker = workers[0]
        entries = [
            (index, item) for index, item in enumerate(worker.get("env", []))
            if item.get("name") == "CURIE_REPO_RUNNER_POOLS"
        ]
        if len(entries) != 1 or not isinstance(entries[0][1].get("value"), str):
            raise fe.PreflightFailed("worker has no unique rendered CURIE_REPO_RUNNER_POOLS input")
        env_index, entry = entries[0]
        pools = json.loads(entry["value"])
        schema = _schema_resource("apps/worker/schema/repo-runner-pools.schema.json")
        failures = _schema_failures(pools, schema, schema, "rendered worker pools")
        if failures or not isinstance(pools, dict):
            raise fe.PreflightFailed("rendered worker pool input does not match its schema")
        pointer = f"/spec/template/spec/containers/{worker_index}/env/{env_index}/value"
        return pools, pointer, entry["value"], deployment

    def _worker_pools(self) -> dict[str, Any]:
        return self._worker_pool_slot()[0]

    def _set_pools(self, pools: dict[str, Any] | None) -> dict[str, Any]:
        if pools is None and not self.fault_active:
            return self._worker_pools()
        expected = self.baseline_pools if pools is None else pools
        _current, pointer, value, _deployment = self._worker_pool_slot()
        patch = [
            {"op": "test", "path": pointer.removesuffix("/value") + "/name",
             "value": "CURIE_REPO_RUNNER_POOLS"},
            {"op": "test", "path": pointer, "value": value},
            {"op": "replace", "path": pointer, "value": json.dumps(expected)},
        ]
        self.fault_active = True
        # The chart reserves this input. Faults change only this execution's
        # worker Deployment, with name/value tests and a restore registered already.
        self.p.kubectl(
            "-n", self.p.namespace, "patch", "deployment", f"{self.p.release}-worker",
            "--type=json", "-p", json.dumps(patch),
        )
        self.p.kubectl(
            "-n", self.p.namespace, "rollout", "status",
            f"deployment/{self.p.release}-worker", "--timeout=15m",
        )
        observed = self._worker_pools()
        if observed != expected:
            raise fe.PreflightFailed("worker pool fault input differs from the scoped override")
        self.fault_active = pools is not None
        return observed

    def _platform_base(self) -> str:
        template = json.loads(self.p.kubectl(
            "-n", self.p.namespace, "get", "sandboxtemplate",
            f"{self.p.release}-runner", "-o", "json",
        ))
        runner = next(
            item for item in template["spec"]["podTemplate"]["spec"]["containers"]
            if item.get("name") == "runner"
        )
        reference = runner["image"]
        digest = _image_digest(reference)
        if digest is None:
            # The documented inspect output reports the index's Digest before
            # its platform manifests, including for a tag-based template.
            # https://docs.docker.com/reference/cli/docker/buildx/imagetools/inspect/
            inspected = fe.run(["docker", "buildx", "imagetools", "inspect", reference])
            match = re.search(r"^Digest:\s*(sha256:[0-9a-f]{64})\s*$", inspected, re.M)
            digest = match.group(1) if match else None
        if not _exact_digest(digest):
            raise fe.PreflightFailed("the installed platform runner has no observed index digest")
        self.obs["platform_runner"] = {"spec_image": reference, "digest": digest}
        return str(digest)

    def _claims(self, thread: str) -> list[dict[str, Any]]:
        scoped = ":".join(quote(value, safe="-._~") for value in ("slack", self.channel, thread))
        thread_hash = hashlib.sha256(scoped.encode()).hexdigest()[:10]
        selector = f"curietech.ai/thread-hash={thread_hash},curietech.ai/agent={fe.FACTORY_AGENT}"
        listing = json.loads(self.p.kubectl(
            "-n", self.p.namespace, "get", "sandboxclaims.extensions.agents.x-k8s.io",
            "-l", selector, "-o", "json",
        ))
        return list(listing["items"])

    def _capture_runner(self, claims: list[dict[str, Any]], turn: dict[str, Any]) -> None:
        bound = []
        for claim in claims:
            env = {item.get("name"): item.get("value") for item in claim["spec"].get("env", [])}
            if (
                not claim["metadata"].get("deletionTimestamp")
                and (claim.get("status") or {}).get("sandbox")
                and env.get("CURIE_REPO_PATH") == turn["repository"]
            ):
                bound.append(claim)
        if len(bound) > 1:
            turn["capture_error"] = "multiple bound claims were observed for the turn"
            return
        if not bound:
            return
        turn["capture_error"] = None
        claim = bound[0]
        sandbox = claim["status"]["sandbox"]["name"]
        turn["claim"] = claim["metadata"]["name"]
        turn["sandbox"] = sandbox
        env = {item.get("name"): item.get("value") for item in claim["spec"].get("env", [])}
        turn["observed_repository"] = env.get("CURIE_REPO_PATH")
        raw = self.p.kubectl(
            "-n", self.p.namespace, "get", "pod", sandbox,
            "--ignore-not-found", "-o", "json",
        )
        if not raw.strip():
            return
        pod = json.loads(raw)
        runner = next(
            (item for item in pod["spec"]["containers"] if item.get("name") == "runner"), None
        )
        status = next(
            (item for item in pod.get("status", {}).get("containerStatuses", [])
             if item.get("name") == "runner"), None
        )
        if runner is None or status is None:
            return
        turn["pod_uid"] = pod["metadata"]["uid"]
        turn["spec_image"] = runner["image"]
        turn["image_id"] = status.get("imageID")
        logs = self.p.kubectl(
            "-n", self.p.namespace, "logs", sandbox, "-c", "runner", check=False
        )
        turn["boot_log"] = "\n".join(
            line for line in logs.splitlines() if _MANIFEST_COUNT.search(line)
        )
        port = next((entry["containerPort"] for entry in runner.get("ports", [])
                     if entry.get("name") == "aci"), None)
        if type(port) is not int:
            return
        # This is the real runner's documented probe route, not an invented
        # platform session API. It exposes no serving bearer credential.
        probe = (
            "import urllib.request; "
            f"print(urllib.request.urlopen('http://127.0.0.1:{port}/status',timeout=5)"
            ".read().decode())"
        )
        response = self.p.kubectl(
            "-n", self.p.namespace, "exec", sandbox, "-c", "runner", "--",
            "python3", "-c", probe, check=False,
        )
        try:
            turn["runner_status"] = json.loads(response)
        except ValueError:
            turn["runner_status"] = None

    def _turn(self, index: int, planned: dict[str, Any]) -> dict[str, Any]:
        assert self.model is not None
        override = planned.get("worker_pools")
        if override is not None:
            normalized = copy.deepcopy(override)
            for entry in normalized.values():
                entry.setdefault("egress", [])
            expected = copy.deepcopy(self.baseline_pools)
            field = "pool" if planned["expect"] == "repo_runner_pool_missing" else "base"
            repository = planned["repository"]
            expected[repository][field] = normalized[repository][field]
            if normalized != expected:
                raise fe.ConfigError("a fault override changes fields outside its one target fault")
            override = normalized
        pools = self._set_pools(override)
        turn: dict[str, Any] = {
            "repository": planned["repository"], "thread": planned["thread"],
            "status": "failed", "reason": None, "sandbox": None, "claim": None,
            "pod_uid": None, "spec_image": None, "image_id": None, "boot_log": "",
            "retired_claims": [], "capture_error": None,
        }
        self.obs["turns"].append(turn)
        if planned["expect"] != "completed":
            configured = pools[planned["repository"]]
            turn.update(installed_base=self.installed_base, configured_base=configured["base"])
            if planned["expect"] == "repo_runner_pool_missing":
                absent = not self.p._pool_exists(configured["pool"])
                turn["missing_pool_absent"] = absent
                if not absent:
                    raise fe.ConfigError("missing-pool fault names a pool that exists")
            elif configured["base"] == self.installed_base:
                raise fe.ConfigError(
                    "stale-base fault equals the observed installed platform digest"
                )
        before = self._claims(planned["thread"])
        before_names = {claim["metadata"]["name"] for claim in before}
        self.model.begin_turn(f"{self.p.run_id}-{index}")
        assert self.p.chart_dir is not None
        argv = [
            self.p.config.curie_bin, "cluster", "--context", "k8", "message",
            f"Repository: https://github.com/{planned['repository']}\n{planned['text']}",
            "--agent", fe.FACTORY_AGENT, "--channel", self.channel,
            "--thread", planned["thread"], "--namespace", self.p.namespace,
            "--release", self.p.release, "--chart", str(self.p.chart_dir),
            "--listen-host", os.environ["CURIE_LOCAL_STUB_HOST"],
            "--timeout-secs", str(max(1, math.ceil(self.p.admission_timeout))), "--json",
        ]
        env = {
            **os.environ, "CURIE_API_KEY": self.p.api_key,
            "KUBECONFIG": str(self.p._write_kubeconfig()),
            "CURIE_CONFIG_DIR": str(self.p._curie_config_dir()),
        }
        process = subprocess.Popen(
            argv, cwd=self.p.workdir, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True,
        )
        self.p.teardown.push(
            f"stop repository turn {index + 1}", lambda: {"stopped": fe._stop(process)}
        )
        new_claims: set[str] = set()
        deadline = time.monotonic() + self.p.admission_timeout + 60
        while True:
            claims = self._claims(planned["thread"])
            new_claims.update(claim["metadata"]["name"] for claim in claims
                              if claim["metadata"]["name"] not in before_names)
            self._capture_runner(claims, turn)
            if process.poll() is not None:
                break
            if time.monotonic() >= deadline:
                fe._stop(process)
                break
            time.sleep(1)
        stdout, _stderr = process.communicate(timeout=10)
        claims = self._claims(planned["thread"])
        new_claims.update(
            claim["metadata"]["name"] for claim in claims
            if claim["metadata"]["name"] not in before_names
        )
        turn["runner_status"] = None
        self._capture_runner(claims, turn)
        outcome: dict[str, Any] = {}
        for line in stdout.splitlines():
            try:
                candidate = json.loads(line)
            except ValueError:
                continue
            if isinstance(candidate, dict):
                outcome = candidate
        turn.update(cli_exit_code=process.returncode, new_claims=sorted(new_claims))
        turn.update(self.model.snapshot())
        failure_class = outcome.get("failure_class")
        reply = str(outcome.get("reply") or "")
        reason = next(
            (reason for reason in sorted(REFUSAL_REASONS)
             if failure_class == reason or re.search(r"\b" + reason + r"\b", reply)), None
        )
        if reason is None and "This thread is already bound to a different repository." in reply:
            # workspace.py maps this exact API code to this exact public reply.
            reason = "workspace.selection_conflict"
        if outcome.get("failed") and reason:
            turn.update(status="refused", reason=reason)
        else:
            probe = turn.get("runner_status")
            if (process.returncode == 0 and outcome.get("finalized") is True
                and isinstance(probe, Mapping)
                and probe.get("status") == "done" and probe.get("turn_active") is False
                and turn.get("observed_repository") == planned["repository"]
                and not turn.get("capture_error") and not turn["errors"]):
                turn["status"] = "completed"
            else:
                turn["reason"] = reason or failure_class or "runner_completion_unconfirmed"
        if turn["status"] == "completed":
            for earlier in self.obs["turns"][:-1]:
                if (earlier["thread"] == turn["thread"]
                    and earlier["repository"] != turn["repository"] and earlier.get("claim")):
                    claim = earlier["claim"]
                    try:
                        fe._wait(
                            "previous repository claim retirement", 60,
                            lambda claim=claim: not self.p.kubectl(
                                "-n", self.p.namespace, "get", "sandboxclaim", claim,
                                "--ignore-not-found", "-o", "name",
                            ).strip(), 1,
                        )
                    except fe.PreflightFailed:
                        continue
                    turn["retired_claims"].append(claim)
        return turn

    def drive(self) -> dict[str, Any]:
        p = self.p
        if p.config.kube_context != "k8" or p.config.model_api_key != SCRIPTED_KEY:
            raise fe.ConfigError(
                "repo-runner requires k8 and its owned scripted model configuration"
            )
        if p.config.repo not in self.repositories:
            raise fe.ConfigError("CURIE_FACTORY_REPO must be one of the plan's bound repositories")
        p.release = TEST_RELEASE
        p.evidence["release"] = p.release
        p.evidence["scenario"] = self.obs
        wanted = ["kubectl", "helm", "git", "docker", "openssl", p.config.curie_bin]
        absent = [tool for tool in wanted if shutil.which(tool) is None]
        if absent:
            raise fe.ConfigError(f"required tools not on PATH: {', '.join(absent)}")
        p._lock(f"app-{p.config.app_id}", "this App and its fixture repositories")
        p._lock("context-k8", "this kube context")
        p.check_images()
        p.check_app()
        for path, repo in self.repositories.items():
            status, observed = p.as_actor("GET", f"/repos/{path}")
            if (
                status != 200 or not isinstance(observed, dict)
                or str(observed.get("id")) != repo["projectId"]
            ):
                raise fe.ConfigError(
                    "a plan repository projectId differs from GitHub's observed id"
                )
        if p.candidate_image_mode == "build":
            p.build_candidate_images()
        self.model = ScriptedManifestModel(host=os.environ["CURIE_LOCAL_STUB_HOST"])
        p.teardown.push("stop repository scripted model", self.model.close)
        self.model.start()
        p.config = dataclasses.replace(
            p.config,
            model_base_url=f"http://{os.environ['CURIE_LOCAL_STUB_HOST']}:{self.model.port}",
        )
        p.create_namespace()

        def layer(base: dict[str, Any]) -> dict[str, Any]:
            values = scenario_values(base, self.plan["bindings"])
            values["fullnameOverride"] = p.release
            values["factory"]["intake"] = "webhook"
            values["factory"]["sandboxEgress"] = {fe.FACTORY_AGENT: True}
            values["agentSandbox"]["deploy"] = True
            values["agentSandbox"]["runner"]["workspace"] = {"enabled": True}
            values["api"]["githubRepoAllowlist"] = list(self.repositories)
            return values

        p.install(egress_cidrs=[], extra_values=layer)
        self.installed_base = self._platform_base()
        if any(
            repo["runner"]["base"] != self.installed_base for repo in self.repositories.values()
        ):
            raise fe.ConfigError("a normal repository image was built on another platform runner")
        p.port_forward()
        p.bind_agent(github_token=False)
        channels = [
            ("github", path) for path in self.repositories if path != p.config.repo
        ] + [("slack", self.channel)]
        for kind, address in channels:
            status, _ = p.api(
                "POST", f"/agents/{p.evidence['agent_id']}/channels",
                headers={"X-API-Key": p.api_key}, body={"kind": kind, "address": address},
            )
            if status != 201:
                raise fe.PreflightFailed("temporary factory channel binding was not created")
        self.baseline_pools = self._worker_pools()
        p.teardown.push("restore repository worker pool input", lambda: self._set_pools(None))
        for index, turn in enumerate(self.plan["turns"]):
            self._turn(index, turn)
        self._set_pools(None)
        failures = judge_repo_runner(self.obs)
        self.obs.update(verdict="failed" if failures else "passed", failures=failures)
        if failures:
            raise fe.PreflightFailed("; ".join(failures))
        return self.obs


def drive(p: fe.Preflight) -> dict[str, Any]:
    """The whole scenario, using only resources this Preflight owns."""

    return RepoRunnerRun(p, load_plan(os.environ)).drive()


def main(argv: list[str] | None = None) -> int:
    """Implementation entry point for the public factory-e2e judge command."""

    parser = argparse.ArgumentParser(description="Judge repository runner observations offline.")
    commands = parser.add_subparsers(dest="command", required=True)
    judge = commands.add_parser("judge")
    judge.add_argument("observation", type=Path)
    args = parser.parse_args(argv)
    try:
        evidence = json.loads(args.observation.read_text())
    except (OSError, ValueError) as exc:
        print(f"repo-runner: cannot read observation: {exc}", file=sys.stderr)
        return 1
    observation = evidence.get("scenario") if isinstance(evidence, Mapping) else None
    failures = judge_repo_runner(observation)
    print(json.dumps({"verdict": "failed" if failures else "passed", "failures": failures}))
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
