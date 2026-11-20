"""Drive the scripted ``learned-rules`` scenario on an owned kind install (#5799).

``LearnedRulesRun`` reuses ``ScriptedRun.execute``: the owned cluster and
registry, the GitHub stub, the scripted model endpoint, the candidate install,
the factory agent and ticket A (stub issue 3815) through to its pull request.
Its hooks add what the chain needs:

1. Three ticket lanes, one transcript per ticket, routed by the raw issue
   identity (``learned_rules_lanes``) with no fallback, and a capture of every
   model request's system prompt and tool names.
2. A disposable Dex in the release namespace and the API's ``CURIE_OIDC_*``
   values, so the writer and the reader each get a principal console session
   from a real password login. Sessions live in memory, in ``secrets`` for
   redaction, and in mode 0600 files in the run directory for the CLI only.
3. Before ticket A is labelled: the session probe (two principal sessions, two
   principals, both in the agent's tenant, no model request yet), the code host
   links of both principals and proposals turned on.

After ticket A's pull request it runs the chain (steps a to g of
``learned_rules_scenario``), appending each step's observations as it
completes, and judges the record with ``learned_rules_scenario.assert_chain``.
A recording splits the proxied exchanges by ticket at cleanup and writes the A,
B and C transcripts, scrubbed of the stub's address.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import secrets
import selectors
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import factory_e2e as fe
import learned_rules_lanes as lrl
import learned_rules_scenario as lrs
import one_ticket_scenario as ots
import personal_two_owner_support as pto
import scripted_kind as sk
import scripted_lanes

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "tools/factory-e2e/fixtures/learned-rules"
REVIEW_COMMENT = FIXTURES / "review-comment.md"
ISSUE_FILES = {lrs.TICKET_B: FIXTURES / "issue-b.md", lrs.TICKET_C: FIXTURES / "issue-c.md"}
REQUESTER_FILE = FIXTURES / "requester.md"
MODEL_SCRIPT = REPO_ROOT / "tools/model-script/model_script.py"
# Where a recording writes when the command names no ticket A file.
RECORD_TARGET = Path("tools/model-script/transcripts/learned-rules-chain-a.json")
AGENT = "factory-e2e"
LABEL = "factory"
USERS = {"writer": "writer@example.com", "reader": "reader@example.com"}
DEX_NAME = "learned-rules-dex"
DEX_CONFIG_SECRET = "learned-rules-dex-config"
DEX_LABELS = {"app.kubernetes.io/name": DEX_NAME}
DEX_REPOSITORY = "dexidp/dex"
OIDC_SECRET = "learned-rules-oidc-client"
OIDC_SECRET_KEY = "clientSecret"
STUB_ORIGIN = "https://stub.example.test"
STUB_HOST = "stub.example.test"
# The repository's secret scan refuses an issue reference qualified by owner and
# repository name; review prompts name the ticket that way.
REPOSITORY_REF = f"{lrs.REPOSITORY}#"
REPOSITORY_REF_PLACEHOLDER = "<repository>#"
POLL_SECONDS = 2.0


class LearnedRulesError(sk.ScriptedScenarioError):
    """The chain could not be driven to an observation."""


Opener = Callable[
    [str, str, Mapping[str, str], bytes | None], tuple[int, Sequence[tuple[str, str]], bytes]
]
Http = Callable[..., tuple[int, Any]]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def loopback_opener(timeout: float = 30.0) -> Opener:
    """``oidc_login``'s opener: no redirects, no proxy, only ``http://127.0.0.1``."""

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def open_url(
        method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> tuple[int, Sequence[tuple[str, str]], bytes]:
        parts = urlsplit(url)
        if parts.scheme != "http" or parts.hostname != "127.0.0.1":
            raise pto.LoginFailed("opener_origin")
        request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
        try:
            with opener.open(request, timeout=timeout) as response:
                return int(response.status), list(response.headers.items()), response.read(1 << 20)
        except urllib.error.HTTPError as error:
            pairs = list(error.headers.items()) if error.headers is not None else []
            return int(error.code), pairs, error.read(1 << 20)
        except (OSError, http.client.HTTPException):
            raise pto.LoginFailed("transport") from None

    return open_url


def oidc_env(
    *, issuer: str, redirect_uri: str, secret_name: str, secret_key: str
) -> list[dict[str, Any]]:
    """The API's ``CURIE_OIDC_*`` entries, in ``two_owner_install_values``'s shape.

    The client secret comes from its Secret by ``secretKeyRef``; every
    authenticated person is admitted.
    """

    return [
        {"name": "CURIE_OIDC_ISSUER", "value": issuer},
        {"name": "CURIE_OIDC_AUDIENCE", "value": pto.OIDC_CLIENT_ID},
        {"name": "CURIE_OIDC_JWKS_URL", "value": f"{issuer}/keys"},
        {
            "name": "CURIE_OIDC_CLIENT_SECRET",
            "valueFrom": {"secretKeyRef": {"name": secret_name, "key": secret_key}},
        },
        {"name": "CURIE_OIDC_REDIRECT_URI", "value": redirect_uri},
        {"name": "CURIE_OIDC_SCOPES", "value": pto.OIDC_SCOPES},
        {"name": "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "value": "true"},
    ]


def requests_sql(issue: int) -> str:
    """The execution requests of a ticket's WorkItem, read only, in sequence order."""

    number = int(issue)
    return (
        "SELECT coalesce(json_agg(json_build_object("
        "'id',r.id,'sequence',r.sequence,'status',r.status,"
        "'reply_target_kind',r.reply_target_kind,'reply_target_url',r.reply_target_url,"
        "'reply_target_pr_number',r.reply_target_pr_number) ORDER BY r.sequence),'[]'::json) "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id=r.work_item_id "
        f"WHERE w.tracker_issue_id = '{number}'"
    )


def work_items_sql(issue: int) -> str:
    return (
        "SELECT coalesce(json_agg(w.id ORDER BY w.created_at),'[]'::json) "
        f"FROM curie.work_items w WHERE w.tracker_issue_id = '{int(issue)}'"
    )


def write_private(path: Path, text: str) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)
    os.chmod(path, 0o600)


def rule_view(rule: Mapping[str, Any]) -> dict[str, Any]:
    return {key: rule.get(key) for key in ("id", "label", "state", "version", "source_url", "text")}


def key_digest(key: Any) -> str:
    """A short digest of a request's lane key, to match a capture to the answer served."""

    return hashlib.sha256(str(key).encode()).hexdigest()[:16]


class LearnedRulesRun(sk.ScriptedRun):
    """The learned rule chain on ``ScriptedRun``'s install."""

    runs_one_ticket = False

    def __init__(self, args: Any) -> None:
        if args.record and Path(args.transcript) == sk.DEFAULT_TRANSCRIPT_ARG:
            args.transcript = RECORD_TARGET
        super().__init__(args)
        self.transcripts = lrl.transcript_paths(Path(args.transcript))
        self.capture_dir: Path | None = None
        self.identity: Callable[[dict[str, Any]], int | None] | None = None
        self.issuer = pto.dex_issuer(DEX_NAME, self.namespace)
        self.redirect_uri = pto.api_redirect_uri("curie", self.namespace)
        self.dex_image: str | None = None
        self.dex_forward: subprocess.Popen[str] | None = None
        self.dex_port: int | None = None
        self.opener: Opener = loopback_opener()
        self.http: Http = fe.http_json
        self.sessions: dict[str, str] = {}
        self.session_files: dict[str, Path] = {}
        self.principals: dict[str, dict[str, str]] = {}
        self.agent_tenant: str | None = None
        self.proposals_on = False
        self.label_events_before = 0
        self.chain: dict[str, Any] = {"scenario": sk.LEARNED_RULES, "result": "failed", "steps": []}
        self.evidence[lrs.EVIDENCE_KEY] = self.chain

    def gateway_enabled(self) -> bool:
        return False

    # ScriptedRun hooks ------------------------------------------------------

    def model_options(self, model_module: Any) -> dict[str, Any]:
        assert self.workdir is not None
        self.capture_dir = self.workdir / "model-captures"
        self.identity = lrl.issue_identity(self.stub)
        options: dict[str, Any] = {"capture_dir": self.capture_dir, "issue_of": self.identity}
        if self.args.record:
            options["tag"] = lrl.ticket_tag(self.identity)
        return options

    def model_transcript(self) -> Path:
        """A recording's raw dump goes to the run directory: ModelScript.close writes
        every exchange, unsplit and unscrubbed, and only a clean split may reach the
        committed ticket files."""

        if self.args.record:
            assert self.workdir is not None
            return self.workdir / "recording-raw.json"
        return super().model_transcript()

    def make_router(self, model_module: Any) -> scripted_lanes.LaneRouter:
        assert self.identity is not None
        return lrl.build_router(model_module, self.transcripts, self.identity, self.replacements())

    def router_fallback(self) -> bool:
        return False

    def before_install(self, registry_ref: str) -> None:
        """Mirror the pinned Dex, then create its client Secret, config, Deployment and Service."""

        assert self.workdir is not None
        self.dex_image = self.mirror_dex(registry_ref)
        client_secret = secrets.token_urlsafe(32)
        self.secrets.append(client_secret)
        secret_file = self.workdir / "oidc-client-secret"
        write_private(secret_file, client_secret)
        self.kubectl(
            "-n",
            self.namespace,
            "create",
            "secret",
            "generic",
            OIDC_SECRET,
            f"--from-file={OIDC_SECRET_KEY}={secret_file}",
        )
        config = pto.dex_config(
            issuer=self.issuer,
            client_id=pto.OIDC_CLIENT_ID,
            client_secret=client_secret,
            redirect_uri=self.redirect_uri,
            users=list(USERS.values()),
        )
        manifests = pto.dex_manifests(
            namespace=self.namespace,
            name=DEX_NAME,
            image_ref=self.dex_image,
            config_secret=DEX_CONFIG_SECRET,
            labels=DEX_LABELS,
            config=config,
        )
        manifest_file = self.workdir / "dex.json"
        write_private(
            manifest_file, json.dumps({"apiVersion": "v1", "kind": "List", "items": manifests})
        )
        self.kubectl("apply", "-f", str(manifest_file))

    def extend_install_values(self, values: dict[str, Any]) -> None:
        api = values.setdefault("api", {})
        api["extraEnv"] = [
            entry
            for entry in api.get("extraEnv") or []
            if not (
                isinstance(entry, dict) and str(entry.get("name", "")).startswith("CURIE_OIDC_")
            )
        ] + oidc_env(
            issuer=self.issuer,
            redirect_uri=self.redirect_uri,
            secret_name=OIDC_SECRET,
            secret_key=OIDC_SECRET_KEY,
        )

    def before_label(self, github: Any) -> None:
        """Sessions, the probe, links and proposals; any failure stops before ticket A."""

        self.label_events_before = self.label_events(lrs.TICKET_A)
        self.start_dex_forward()
        self.login()
        self.probe()
        self.link_accounts(github)
        self.enable_proposals()

    def run_scenarios(self, github: Any) -> None:
        self.observe()
        try:
            self.run_chain(github)
        finally:
            if self.router is not None:
                self.chain["lanes"] = self.router.evidence()
        try:
            lrs.assert_chain(self.chain, record=bool(self.args.record))
        except lrs.LearnedRulesAssertionError as exc:
            raise LearnedRulesError(f"learned-rules: {exc}") from None
        self.chain["result"] = "passed"

    # Infrastructure ---------------------------------------------------------

    def command_result(self, *argv: str, timeout: float = 300) -> tuple[int, str]:
        """Like ``command`` but returns the exit status and the redacted output."""

        if self.deadline is not None:
            timeout = min(timeout, max(0.1, self.deadline - time.monotonic()))
        result = subprocess.run(
            argv,
            cwd=sk.REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        output, _ = fe.redact_agent_text(
            (result.stdout or "") + (result.stderr or ""), self.secrets
        )
        return result.returncode, output or ""

    def mirror_dex(self, registry_ref: str) -> str:
        """Push the pinned Dex to the run's registry; kind pulls it through the mirror."""

        source = pto.DEX_IMAGE.split(":", 1)[0] + "@" + pto.DEX_IMAGE.split("@", 1)[1]
        target = (
            f"{registry_ref}/{DEX_REPOSITORY}:{pto.DEX_IMAGE.split(':', 1)[1].split('@', 1)[0]}"
        )
        present = (
            self.command_result("docker", "image", "inspect", "--format", "{{.Id}}", source)[0] == 0
        )
        if not present:
            self.command("docker", "pull", source, timeout=900)
        self.command("docker", "tag", source, target, timeout=60)
        self.command("docker", "push", target, timeout=900)
        if not present:
            # The run's own tag keeps the image until cleanup removes registry tags.
            self.command("docker", "image", "rm", source, timeout=120)
        # The pod names the run registry's tag. A pushed copy of a pinned
        # multi-platform image answers under a digest other than the source's,
        # so the source digest pins what was pulled and the tag names the push.
        self.evidence["dex_image"] = {"source": source, "mirrored": target}
        return target

    def port_forward(self, target: str, remote: int) -> tuple[subprocess.Popen[str], int]:
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.context,
                "-n",
                self.namespace,
                "port-forward",
                target,
                f":{remote}",
                "--address",
                "127.0.0.1",
            ],
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        selector = selectors.DefaultSelector()
        assert process.stdout is not None
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            if not selector.select(timeout=30):
                raise LearnedRulesError(f"{target} port-forward did not become ready")
            line = process.stdout.readline()
            port = int(line.split("127.0.0.1:", 1)[1].split(" ", 1)[0])
        except (IndexError, ValueError):
            raise LearnedRulesError(f"{target} port-forward printed no port") from None
        finally:
            selector.close()
        return process, port

    def start_dex_forward(self) -> None:
        try:
            self.kubectl(
                "-n",
                self.namespace,
                "rollout",
                "status",
                f"deployment/{DEX_NAME}",
                "--timeout=300s",
                timeout=330,
            )
        except (sk.ScriptedScenarioError, subprocess.SubprocessError):
            # Pod state and events only; the config Secret is never printed.
            for argv in (
                ("get", "pods", "-l", f"app.kubernetes.io/name={DEX_NAME}", "-o", "wide"),
                ("get", "events", "--field-selector", "involvedObject.kind=Pod"),
            ):
                code, output = self.command_result(
                    "kubectl", "--context", self.context, "-n", self.namespace, *argv, timeout=30
                )
                print(f"factory-e2e dex diagnostics ({code}):\n{output[-3000:]}", file=sys.stderr)
            raise LearnedRulesError("the Dex deployment did not become ready") from None
        self.dex_forward, self.dex_port = self.port_forward(f"svc/{DEX_NAME}", pto.DEX_PORT)

    def stop_dex_forward(self) -> None:
        if self.dex_forward is None:
            return
        self.dex_forward.terminate()
        try:
            self.dex_forward.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.dex_forward.kill()
            self.dex_forward.wait(timeout=10)
        if self.dex_forward.stdout is not None:
            self.dex_forward.stdout.close()
        self.dex_forward = None

    def api_url(self) -> str:
        return self.env["CURIE_API_URL"]

    def api(
        self, method: str, path: str, *, session: str | None = None, body: Any = None
    ) -> tuple[int, Any]:
        """One API call with the platform key, or with ``session``'s console cookie."""

        if session is not None:
            headers = {"Cookie": f"{pto.SESSION_COOKIE}={self.sessions[session]}"}
        else:
            headers = {"X-API-Key": self.env["CURIE_API_KEY"]}
        return self.http(method, self.api_url() + path, headers=headers, body=body)

    def common(self) -> list[str]:
        return ["--namespace", self.namespace, "--release", "curie", "--api-url", self.api_url()]

    def cluster_cli(self, *argv: str) -> list[str]:
        return [self.curie, "--json", "cluster", "--context", self.context, *argv]

    def replacements(self) -> dict[str, str]:
        """The stub's real address, origin first, as transcripts and evidence spell
        it, and the fixture repository's owner-qualified issue reference."""

        host = str(self.stub.host)
        authority = f"[{host}]" if ":" in host else host
        return {
            str(self.stub.base_url): STUB_ORIGIN,
            f"{authority}:{self.stub.port}": STUB_HOST,
            host: STUB_HOST,
            REPOSITORY_REF: REPOSITORY_REF_PLACEHOLDER,
        }

    def scrub(self, value: Any) -> Any:
        if self.stub is None:
            return value
        return lrl.scrub_recording([value], self.replacements())[0]

    def record_step(self, name: str, **observations: Any) -> None:
        self.chain["steps"].append(self.scrub({"step": name, **observations}))

    # Model observations -----------------------------------------------------

    def model_requests(self) -> dict[str, int]:
        captured = (
            len(list(self.capture_dir.glob("*.json")))
            if self.capture_dir is not None and self.capture_dir.is_dir()
            else 0
        )
        served = 0
        if self.router is not None:
            served = sum(len(lane.used) for lane in self.router.lanes.values())
        proxied = 0
        if self.args.record and self.model is not None:
            proxied = len(self.model._exchanges)
        unexpected = 0 if self.model is None else len(self.model.unexpected)
        return {
            "captured": captured,
            "served": served,
            "unexpected": unexpected,
            "proxied": proxied,
        }

    def captures(self) -> list[dict[str, Any]]:
        if self.capture_dir is None or not self.capture_dir.is_dir():
            return []
        rows = [json.loads(path.read_text()) for path in sorted(self.capture_dir.glob("*.json"))]
        return sorted(rows, key=lambda row: int(row["sequence"]))

    def capture_count(self) -> int:
        return max((int(row["sequence"]) for row in self.captures()), default=0)

    def window_captures(
        self,
        window: tuple[int, int],
        issue: int,
        rule_text: str | None,
        requester_text: str | None = None,
    ) -> list[dict[str, Any]]:
        lo, hi = window
        found = []
        for row in self.captures():
            if not lo <= int(row["sequence"]) <= hi or row.get("issue") != issue:
                continue
            positions = lrs.prompt_positions(
                str(row.get("system") or ""), rule_text, requester_text
            )
            found.append(
                {
                    "sequence": int(row["sequence"]),
                    "issue": row.get("issue"),
                    "ordinal": row.get("ordinal"),
                    "implementer": str(row.get("lane_key") or "").startswith(lrs.SENDER_FRAME),
                    "mounted": lrs.PROPOSE_TOOL in (row.get("tools") or []),
                    **positions,
                }
            )
        return found

    def ticket_exchanges(self, letter: str) -> list[dict[str, Any]]:
        """What the model answered on one ticket: served from its lane, or recorded."""

        if self.args.record:
            with self.model._lock:
                pairs = list(zip(self.model._exchanges, self.model.recorded_tags, strict=False))
            return [exchange for exchange, tag in pairs if tag == letter]
        assert self.router is not None
        lane = self.router.lanes[(scripted_lanes.IMPLEMENTATION, lrs.TICKETS[letter])]
        return [lane.exchanges[index] for index in sorted(lane.used)]

    # Waiting ------------------------------------------------------------------

    def check_boundaries(self) -> None:
        if self.record_error is not None:
            raise LearnedRulesError(self.record_error)
        if self.model is not None and self.model.unexpected:
            raise LearnedRulesError(f"{len(self.model.unexpected)} unexpected model requests")
        if self.router is not None and self.router.unrouted:
            raise LearnedRulesError(f"{len(self.router.unrouted)} unrouted model requests")
        if self.stub.unknown_requests:
            raise LearnedRulesError("unsupported stub request")

    def wait(self, what: str, ready: Callable[[], Any]) -> Any:
        assert self.deadline is not None
        while time.monotonic() < self.deadline:
            self.check_boundaries()
            found = ready()
            if found:
                return found
            if self.args.record and self.record_start_remaining is not None:
                self.assert_record_budget(self.provider_remaining())
            time.sleep(POLL_SECONDS)
        raise LearnedRulesError(f"{what} did not happen inside the command bound")

    def final_notice(self, request_id: str) -> dict[str, Any] | None:
        marker = f"<!-- curie-execution-request:{request_id} -->"
        for comment in self.stub._rows("comment"):
            if marker in comment["body"] and fe._NOTICE_FINAL in comment["body"]:
                return dict(comment)
        return None

    # Sessions and the probe (decisions 11 and 12) ------------------------------

    def login(self) -> None:
        assert self.workdir is not None and self.dex_port is not None
        for role, email in USERS.items():
            try:
                session = pto.oidc_login(
                    self.opener,
                    api_origin=self.api_url(),
                    issuer_origin=self.issuer,
                    dex_local_origin=f"http://127.0.0.1:{self.dex_port}",
                    redirect_origin=self.redirect_uri,
                    email=email,
                    password=pto.DEX_PASSWORD,
                )
            except pto.LoginFailed as failure:
                raise LearnedRulesError(f"the {role} Dex login failed at {failure.step}") from None
            value = session.reveal()
            self.secrets.append(value)
            self.sessions[role] = value
            path = self.workdir / f"session-{role}"
            write_private(path, value + "\n")
            self.session_files[role] = path

    def probe(self) -> None:
        """Decision 12: two principals of the agent's tenant, before any model call."""

        agent = str(self.evidence["agent_id"])
        status, policy = self.api("GET", f"/agents/{agent}/access-policy")
        if status != 200 or not isinstance(policy, dict) or not policy.get("tenant_id"):
            raise LearnedRulesError(f"the agent access policy read returned HTTP {status}")
        self.agent_tenant = str(policy["tenant_id"])
        kinds: list[str] = []
        for role in USERS:
            status, session = self.api("GET", "/console/session", session=role)
            kind = session.get("kind") if isinstance(session, dict) else None
            if status != 200 or kind != "principal":
                raise LearnedRulesError(f"the {role} session is not a principal session ({kind})")
            kinds.append(str(kind))
            status, principal = self.api("GET", "/console/principal", session=role)
            if status != 200 or not isinstance(principal, dict):
                raise LearnedRulesError(f"the {role} principal read returned HTTP {status}")
            try:
                self.principals[role] = {
                    "id": str(uuid.UUID(str(principal.get("id")))),
                    "tenant_id": str(uuid.UUID(str(principal.get("tenant_id")))),
                }
            except ValueError:
                raise LearnedRulesError(f"the {role} principal has no readable id") from None
        ids = [self.principals[role]["id"] for role in USERS]
        tenants = [self.principals[role]["tenant_id"] for role in USERS]
        if ids[0] == ids[1]:
            raise LearnedRulesError("both sessions resolved one principal")
        if any(tenant != self.agent_tenant for tenant in tenants):
            raise LearnedRulesError("a principal is outside the agent's tenant")
        counts = self.model_requests()
        if any(counts.values()):
            raise LearnedRulesError(f"model requests were made before the session probe: {counts}")
        self.record_step(
            "0",
            session_kinds=kinds,
            principal_ids=ids,
            principal_tenants=tenants,
            agent_tenant=self.agent_tenant,
            model_requests=counts,
        )

    def link_accounts(self, github: Any) -> None:
        """Link the writer and reader principals to their stub accounts on the code host."""

        host = urlsplit(self.stub.base_url).netloc.lower()
        creator = self.principals["writer"]["id"]
        for role, account in (("writer", github.RULE_WRITER), ("reader", github.RULE_READER)):
            status, _ = self.api(
                "POST",
                f"/identity/admin/{self.agent_tenant}/code-host-links",
                body={
                    "principal_id": self.principals[role]["id"],
                    "code_host_kind": "github",
                    "host": host,
                    "account_id": str(account["id"]),
                    "created_by_principal_id": creator,
                },
            )
            if status != 201:
                raise LearnedRulesError(f"the {role} code host link returned HTTP {status}")

    def enable_proposals(self) -> None:
        """Decision 3: proposals on, read back from the agent."""

        self.command(
            *self.cluster_cli("overrides", AGENT, "--memory-rule-proposals", "on", *self.common()),
            timeout=300,
        )
        status, agent = self.api("GET", f"/agents/{self.evidence['agent_id']}")
        if (
            status != 200
            or not isinstance(agent, dict)
            or agent.get("memory_rule_proposals") is not True
        ):
            raise LearnedRulesError("the agent read does not show memory_rule_proposals on")
        self.proposals_on = True

    # The chain ------------------------------------------------------------------

    def list_rules(self) -> list[dict[str, Any]]:
        output = self.command(
            *self.cluster_cli("memory", AGENT, "--rules", *self.common()), timeout=120
        )
        data = json.loads(output)
        rules = data.get("rules") if isinstance(data, dict) else data
        if not isinstance(rules, list):
            raise LearnedRulesError("the rule list is not a list")
        return [rule_view(rule) for rule in rules]

    def decide(self, verb: str, rule_id: str, version: int, role: str) -> tuple[int, str]:
        return self.command_result(
            *self.cluster_cli(
                "memory",
                AGENT,
                f"--{verb}",
                rule_id,
                "--expected-version",
                str(version),
                "--session-file",
                str(self.session_files[role]),
                *self.common(),
            ),
            timeout=120,
        )

    def label_events(self, number: int) -> int:
        return sum(
            1
            for e in self.stub._rows("event")
            if e["issue_number"] == number
            and e["event"] == "labeled"
            and e["label"]["name"] == LABEL
        )

    def label_ticket(self, github: Any, number: int) -> None:
        """A person with write access labels the ticket, as ``one_ticket_run.label`` does."""

        text = ISSUE_FILES[number].read_text()
        title, _, body = text.partition("\n")
        issue = self.stub._issue(
            number, title.removeprefix("# ").strip(), body.lstrip("\n"), [LABEL]
        )
        issue["user"] = github.RULE_WRITER
        self.stub._put("issue", number, issue)
        self.stub._label_event(number, LABEL, "labeled", github.RULE_WRITER)

    def run_ticket(self, github: Any, number: int, rule_text: str | None) -> dict[str, Any]:
        """Label one ticket, wait for its pull request and final notice, and observe it."""

        baseline = [int(row["number"]) for row in self.stub._rows("pull")]
        lo = self.capture_count() + 1
        self.label_ticket(github, number)

        def ready() -> dict[str, Any] | None:
            pulls = [self.stub._fresh_pull(row) for row in self.stub._rows("pull")]
            opened = ots.closing_pulls(ots.new_pulls(pulls, baseline), number)
            requests = self.sql(requests_sql(number))
            if not opened or not requests:
                return None
            if any(r["status"] in ots.LIVE_STATUSES for r in requests):
                return None
            if not all(self.final_notice(str(r["id"])) for r in requests):
                return None
            return dict(opened[0])

        pull = self.wait(f"the pull request of #{number}", ready)
        window = (lo, self.capture_count())
        items = self.sql(work_items_sql(number))
        if len(items) != 1:
            raise LearnedRulesError(f"expected one work item for #{number}, found {len(items)}")
        status, item = self.api("GET", f"/work-items/{items[0]}")
        if status != 200 or not isinstance(item, dict):
            raise LearnedRulesError(f"the work item read of #{number} returned HTTP {status}")
        fresh = self.stub._fresh_pull(pull)
        diff = self.stub._run(
            "git",
            "--git-dir",
            str(self.stub._repository),
            "diff",
            f"{fresh['base']['sha']}...{fresh['head']['sha']}",
        ).decode(errors="replace")
        return {
            "labeller": github.RULE_WRITER["login"],
            "pull_request": int(pull["number"]),
            "closes": lrs.closes(pull.get("body")),
            "work_item_memory_rules": item.get("memory_rules"),
            "window": [window[0], window[1]],
            "captures": self.window_captures(
                window,
                number,
                rule_text,
                REQUESTER_FILE.read_text() if number == lrs.TICKET_B else None,
            ),
            "annotations": lrs.annotation_lines(pull.get("body")),
            "added_asserts": lrs.added_assert_lines(diff),
        }

    def run_chain(self, github: Any) -> None:
        pull_a = int(self.evidence["pull_request"]["number"])
        # The stub seeds one past label event on 3815; execute adds the live one.
        labelled = self.label_events(lrs.TICKET_A) > self.label_events_before
        self.record_step(
            "a",
            memory_rule_proposals=self.proposals_on,
            labelled=labelled,
            pull_request=pull_a,
            closes=lrs.closes(self.evidence["pull_request"].get("body")),
        )
        # Judge each step as it lands, so a broken link stops the run before the
        # next ticket's model calls are made; assert_chain still judges the whole.
        self.judge(lrs.assert_ticket_a, "a")
        raw_reply_url = self.revise(github, pull_a)
        # Steps are stored scrubbed, so later checks take the reply target the
        # judge read from step b; promote compares the raw URLs itself.
        reply_url = self.judge(lrs.assert_revision, "b", pull_a)
        rule = self.promote(pull_a, raw_reply_url)
        rule = self.judge(lrs.assert_promoted, "c", pull_a, reply_url)
        accepted = self.accept(rule)
        self.judge(lrs.assert_decided, "d", rule)
        self.ticket_b(github, rule)
        self.judge(lrs.assert_ticket_b, "e")
        self.ticket_c(github, accepted)
        self.judge(lrs.assert_ticket_c, "f", accepted)
        self.lanes_step()

    def judge(self, check: Callable[..., Any], name: str, *args: Any) -> Any:
        step = next(s for s in reversed(self.chain["steps"]) if s.get("step") == name)
        try:
            return check(step, *args)
        except lrs.LearnedRulesAssertionError as exc:
            raise LearnedRulesError(f"learned-rules: {exc}") from None

    def revise(self, github: Any, pull_a: int) -> str:
        """b: the writer's mentioned review comment admits a revision that proposes."""

        first = str(self.evidence["execution_request_id"])
        body = REVIEW_COMMENT.read_text().strip()
        lo = self.capture_count() + 1
        comment = self.stub.append_comment(
            pull_a, body, author=github.RULE_WRITER, author_association="COLLABORATOR"
        )

        def ready() -> dict[str, Any] | None:
            requests = [r for r in self.sql(requests_sql(lrs.TICKET_A)) if str(r["id"]) != first]
            if not requests or requests[-1]["status"] in ots.LIVE_STATUSES:
                return None
            notice = self.final_notice(str(requests[-1]["id"]))
            if notice is None:
                return None
            return {**requests[-1], "notice_on": notice["issue_number"]}

        revision = self.wait("the revision of ticket A", ready)
        window = (lo, self.capture_count())
        self.record_step(
            "b",
            comment={
                "id": comment["id"],
                "pull_request": pull_a,
                "author": github.RULE_WRITER["login"],
                "author_association": comment.get("author_association"),
                "body": body,
            },
            revision={
                "request_id": str(revision["id"]),
                "status": revision["status"],
                "reply_target_kind": revision.get("reply_target_kind"),
                "reply_target_url": revision.get("reply_target_url"),
                "reply_target_pr_number": revision.get("reply_target_pr_number"),
                "final_notice": True,
                "final_notice_on": revision["notice_on"],
            },
            propose_ordinals=lrs.propose_ordinals(self.ticket_exchanges("a")),
            revision_captures=[
                {key: c[key] for key in ("sequence", "issue", "ordinal", "mounted")}
                for c in self.window_captures(window, lrs.TICKET_A, None)
            ],
            rules_before_merge=self.list_rules(),
        )
        return str(revision.get("reply_target_url") or "")

    def promote(self, pull_a: int, reply_url: str) -> dict[str, Any]:
        """c: merging A's pull request makes one proposed rule; ``reply_url`` is
        the revision's unscrubbed reply target."""

        merged = self.stub.merge_pull(pull_a)
        rules = self.wait("the proposed rule", lambda: self.list_rules() or None)
        self.record_step(
            "c",
            merged=bool(merged.get("merged")),
            rules=rules,
            source_matches_reply_target=all(r.get("source_url") == reply_url for r in rules),
        )
        return dict(rules[0])

    def accept(self, rule: Mapping[str, Any]) -> dict[str, Any]:
        """d: the reader is refused write access; the writer accepts.

        Each lifecycle change bumps the rule's version, so the decision passes the
        version the list read, never a fixed number. Returns the rule as read after.
        """

        rule_id = str(rule["id"])
        version = int(rule["version"])
        reader_exit, reader_output = self.decide("accept", rule_id, version, "reader")
        writer_exit, writer_output = self.decide("accept", rule_id, version, "writer")
        after = next((r for r in self.list_rules() if r["id"] == rule_id), {})
        self.record_step(
            "d",
            rule_id=rule_id,
            expected_version=version,
            reader={"exit": reader_exit, "codes": lrs.error_codes(reader_output)},
            writer={
                "exit": writer_exit,
                "codes": lrs.error_codes(writer_output),
                "state": after.get("state"),
                "version": after.get("version"),
            },
        )
        return dict(after)

    def ticket_b(self, github: Any, rule: Mapping[str, Any]) -> None:
        """e: the writer's conflicting document, then ticket B under the accepted rule."""

        code, _ = self.command_result(
            self.curie,
            "--json",
            "cluster",
            "--context",
            self.context,
            "coding-instructions",
            "set",
            "--session-file",
            str(self.session_files["writer"]),
            "--file",
            str(REQUESTER_FILE),
            "--expected-revision",
            "0",
            *self.common(),
            timeout=120,
        )
        if code != 0:
            raise LearnedRulesError(f"coding-instructions set exited {code}")
        observed = self.run_ticket(github, lrs.TICKET_B, str(rule.get("text") or ""))
        self.record_step("e", coding_instructions_exit=code, **observed)

    def ticket_c(self, github: Any, rule: Mapping[str, Any]) -> None:
        """f: the writer removes R1; ticket C carries no learned rules."""

        rule_id = str(rule["id"])
        version = int(rule["version"])
        code, output = self.decide("remove", rule_id, version, "writer")
        state = next((r["state"] for r in self.list_rules() if r["id"] == rule_id), None)
        if code != 0 or state != "removed":
            # Stop before C's model calls; the judge names the failure.
            self.record_step(
                "f",
                remove_exit=code,
                remove_codes=lrs.error_codes(output),
                expected_version=version,
                state_after=state,
            )
            return
        observed = self.run_ticket(github, lrs.TICKET_C, str(rule.get("text") or ""))
        self.record_step(
            "f", remove_exit=code, expected_version=version, state_after=state, **observed
        )

    def lanes_step(self) -> None:
        """g: lanes complete, nothing unrouted, every capture on its ticket."""

        captures = [
            {
                "sequence": int(c["sequence"]),
                "issue": c.get("issue"),
                "ordinal": c.get("ordinal"),
                "key": key_digest(c.get("lane_key")),
            }
            for c in self.captures()
        ]
        if self.args.record:
            with self.model._lock:
                tags = list(self.model.recorded_tags)
                pairs = list(zip(self.model._exchanges, tags, strict=False))
            counts = {letter: tags.count(letter) for letter in lrs.TICKETS}
            untagged = sum(1 for tag in tags if tag not in lrs.TICKETS)
            served = [
                {
                    "lane": lrs.LANES[tag],
                    "ordinal": exchange["ordinal"],
                    "key": key_digest(exchange["lane_key"]),
                }
                for exchange, tag in pairs
                if tag in lrs.LANES
            ]
            self.record_step(
                "g",
                record=True,
                recorded_counts=counts,
                untagged=untagged,
                captures=captures,
                served=served,
            )
            return
        assert self.router is not None
        served = [{**row, "key": key_digest(row["key"])} for row in self.router.served_log]
        self.record_step(
            "g", record=False, lanes=self.router.evidence(), captures=captures, served=served
        )

    # Recording and teardown ---------------------------------------------------

    def write_recording(self) -> None:
        """Split the proxied exchanges by ticket, scrub them, and write the three files.

        ``ModelScript.close`` dumped every exchange to the run directory; a split
        that fails leaves the committed files untouched.
        """

        model_module = sk._module("learned_rules_record_model_script", MODEL_SCRIPT)
        split = lrl.split_recording(list(self.model._exchanges), list(self.model.recorded_tags))
        replacements = self.replacements()
        scrubbed = {
            letter: lrl.scrub_recording(exchanges, replacements)
            for letter, exchanges in split.items()
        }
        lrl.write_transcripts(model_module, scrubbed, self.transcripts)
        self.evidence["recorded_exchanges"] = {
            letter: len(items) for letter, items in scrubbed.items()
        }

    def cleanup(self) -> list[str]:
        errors: list[str] = []
        try:
            self.stop_dex_forward()
        except (OSError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
        errors.extend(super().cleanup())
        # Only a chain the judge passed may replace the committed fixtures: a failure
        # anywhere, including before the first model call, leaves all three as they were.
        if self.args.record and self.model is not None and self.chain.get("result") == "passed":
            try:
                self.write_recording()
            except (scripted_lanes.LaneError, OSError, ValueError) as exc:
                errors.append(f"recording split failed: {exc}")
        self.sessions.clear()
        self.evidence["cleanup_errors"] = errors
        self.evidence["cleanup_verified"] = not errors
        return errors
