"""ADR 0271 approver tickets through the real approval creation boundaries.

One file in two halves, so the case table the host judges and the requests the
pod sends cannot drift apart:

1. On the host, standard library only: the route registration, the derived
   serving bundles, the approver role filter, the creation case table and its
   judge, used by the ``approver-ticket-creation`` scenario in
   ``identity_scenarios.py``.
2. Inside the release's API pod, ``Target.pod_python`` runs
   :data:`POD_BOOTSTRAP`, which executes this same source (sent on stdin with
   the parameters) and prints :func:`dispatch`'s one JSON line for
   ``{"action": ..., ...}``. It reads a turn's ingress capsule and payload from the
   runs stream with the API's own settings, signs tickets with the frozen
   ``curie_internal.approver_ticket`` encoder, posts
   ``/v1/internal/identity/approvals`` with the API's worker token and
   ``/approvals`` with its platform key on loopback, and counts rows in the
   API's own Postgres.

Every product import is inside a function, so the host imports nothing but the
standard library. Nothing either half prints carries a key, token, capsule,
payload, nonce or ticket: results are status codes, refusal codes, ids, counts
and digest prefixes. The selector signing seed travels to the pod on stdin only.
"""

from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import io
import json
import re
import secrets
import tarfile
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

SCENARIO = "approver-ticket-creation"
# The evidence step ids, exactly as the 0271a task names them.
STEP_IDS = (
    "registration-refused-flag-off",
    "flag-disable-refused-while-registered",
    "concurrent-flag-and-registration",
    "registration-bundle-gate-refused",
    "registration-accepted",
    "deploy-breaking-gate-refused",
    "creation-valid",
    "creation-bad-signature",
    "creation-scope-change",
    "creation-requester-change",
    "creation-argument-change",
    "creation-expired",
    "creation-nonce-replay",
    "creation-conflicting-dedupe",
    "creation-identical-race",
    "legacy-new-policy-refused",
    "legacy-ordinary-unchanged",
    "route-bypass-refused",
    "cluster-message-negative-control",
)

# The hosted connector examples/weather declares, and two of its upstream tool
# names. Creation never calls the connector, so only the names matter.
CONNECTOR = "netpol-probe"
TOOL = "search_dashboards"
OTHER_TOOL = "list_datasources"
# The route that owns the registered pair, and a looser bound route.
TICKET_ROUTE = "ticket-review"
LOOSER_ROUTE = "ticket-open"
OWNED_ROUTES = (TICKET_ROUTE, LOOSER_ROUTE)
REQUESTING_SURFACE: dict[str, Any] = {"resolution": {"mode": "requesting_surface"}}
MANIFEST_PATH = ".claude-plugin/plugin.json"
# What `curie` never packs into a bundle (cli/src/bundle.rs EXCLUDED_NAMES).
EXCLUDED_NAMES = frozenset(
    {
        ".curieignore",
        ".curie",
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
    }
)

SUMMARY = "identity-e2e approver ticket request"
DEFAULT_ARGUMENTS: dict[str, Any] = {"query": "identity-e2e approver ticket"}
APPROVAL_TTL_S = 300
TICKET_LIFETIME_S = 240
# Tickets are issued a little in the pod's past, so `issued_at <= now` holds.
ISSUED_SKEW_S = 5
RACE_POSTS = 4
RACE_ROUNDS = 3
GATE_KINDS: tuple[str | None, ...] = (None, "policy", "permission")
RANDOM = "random"
HTTP_TIMEOUT_S = 60.0
BARRIER_TIMEOUT_S = 30.0

CREATION_PATH = "/v1/internal/identity/approvals"
LEGACY_PATH = "/approvals"
API_LOOPBACK = "http://127.0.0.1:8000"
CONTEXT_FIELD = "curie_identity_context_v1"

# Refusal codes, as plan decisions D2 to D14 name them.
FLAG_REQUIRED = "approver_ticket_requires_requester_identity"
SELECTOR_REGISTERED = "requester_identity_selector_registered"
ROUTES_VERSION_CONFLICT = "approval_routes.version_conflict"
FLAG_VERSION_CONFLICT = "agent.requester_identity_version_conflict"
BUNDLE_GATE_MISMATCH = "approver_ticket.bundle_gate_mismatch"
ROUTE_LOCKED = "approval_route_locked"
ROUTE_MISMATCH = "approval_ticket_route_mismatch"
TICKET_REQUIRED = "approval_ticket_required"
TICKET_UNREGISTERED = "approval_ticket_unregistered"
REQUIRES_HTTP_CREATION = "approval_ticket_requires_http_creation"
REPLAYED = "approval_ticket_replayed"
PROVENANCE_CONFLICT = "approval_provenance_conflict"
UNSUPPORTED = "unsupported_approval"
UNAVAILABLE = "authority_unavailable"


def ticket_refusal(code: str) -> str:
    """The creation detail for a ticket refusal: ``approval_ticket_<code>`` (plan D8)."""

    return f"approval_ticket_{code}"


# --- encodings --------------------------------------------------------------------------


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def live_tool(connector: str, tool: str) -> str:
    """A hosted connector tool's live name: ``mcp__<connector>__<tool>``."""

    return f"mcp__{connector}__{tool}"


def new_nonce() -> str:
    """32 random bytes, unpadded base64url, as a ticket nonce."""

    return b64url(secrets.token_bytes(32))


def nonce_forms(nonce: str) -> list[str]:
    """Every form the store might keep a spent nonce in: the text, or a digest of it."""

    return [
        nonce,
        hashlib.sha256(nonce.encode("ascii")).hexdigest(),
        hashlib.sha256(b64url_decode(nonce)).hexdigest(),
    ]


def selector_key(seed_b64: str, public_b64: str) -> dict[str, str]:
    """A standard-base64 Ed25519 seed and public key as the pod and the API take them."""

    seed = base64.b64decode(seed_b64, validate=True)
    public = base64.b64decode(public_b64, validate=True)
    if len(seed) != 32 or len(public) != 32:
        raise ValueError("an Ed25519 seed and public key are 32 bytes each")
    return {"seed_hex": seed.hex(), "public_key": b64url(public)}


def key_id_for(stamp: str) -> str:
    """A fresh selector key id per run."""

    return f"identity-e2e-selector-{stamp}"


_REVISION = re.compile(r"(?<![0-9a-f])([0-9a-f]{40}|[0-9a-f]{7,12})(?![0-9a-f])")


def revision_from_image(image: str) -> str | None:
    """The commit an image tag names (``sha-<hex>``, ``<hex>``), or None."""

    name = image.split("@", 1)[0]
    last = name.rsplit("/", 1)[-1]
    if ":" not in last:
        return None
    match = _REVISION.search(last.split(":", 1)[1])
    if match is None or not re.search(r"[a-f]", match.group(1)):
        return None
    return match.group(1)


# --- registration, routes and the approver role -------------------------------------------


def registration(
    *,
    key_id: str,
    public_key: str,
    role_version: int,
    connector: str = CONNECTOR,
    tool: str = TOOL,
) -> dict[str, Any]:
    """A route's ``approver_ticket`` write (plan D4): no users, default separation."""

    return {
        "connector": connector,
        "tool": tool,
        "key_id": key_id,
        "public_key": public_key,
        "ceiling": {"approver_role_version": role_version},
    }


def base_routes(stored: Any) -> dict[str, Any]:
    """An agent's stored routes without the two this scenario owns."""

    if not isinstance(stored, Mapping):
        return {}
    return {
        str(name): copy.deepcopy(binding)
        for name, binding in stored.items()
        if name not in OWNED_ROUTES
    }


def routes_with(
    base: Mapping[str, Any],
    registered: Mapping[str, Any] | None = None,
    *,
    route: str = TICKET_ROUTE,
) -> dict[str, Any]:
    """``base`` plus both owned routes as requesting-surface routes, ``registered`` on ``route``."""

    routes = base_routes(base)
    for name in OWNED_ROUTES:
        routes[name] = copy.deepcopy(REQUESTING_SURFACE)
    if registered is not None:
        routes[route]["approver_ticket"] = copy.deepcopy(dict(registered))
    return routes


def has_registration(routes: Any) -> bool:
    return isinstance(routes, Mapping) and any(
        isinstance(binding, Mapping) and binding.get("approver_ticket") is not None
        for binding in routes.values()
    )


def redacted(value: Any) -> Any:
    """``value`` without any ``public_key`` member, for evidence."""

    if isinstance(value, Mapping):
        return {key: redacted(item) for key, item in value.items() if key != "public_key"}
    if isinstance(value, list):
        return [redacted(item) for item in value]
    return value


def ceiling_subjects(roster: Mapping[str, Any]) -> list[str]:
    """The subjects of every active, resolved human in a harness roster."""

    subjects = []
    for entry in (roster.get("senders") or {}).values():
        if (
            entry.get("sender_kind", "human") == "human"
            and entry.get("account_status", "active") == "active"
            and entry.get("outcome") in (None, "resolved")
        ):
            subjects.append(str(entry["subject"]))
    return sorted(subjects)


def approver_filter(issuer: str, subjects: Iterable[str]) -> dict[str, Any]:
    """A restricted approver role: exactly these canonical identities."""

    return {
        "entries": [
            {"kind": "issuer_subject", "issuer": issuer, "subject": subject} for subject in subjects
        ]
    }


# --- the serving bundles ----------------------------------------------------------------------


def gated_manifest(manifest: Mapping[str, Any], gates: Sequence[tuple[str, str]]) -> dict[str, Any]:
    """``manifest`` with exactly these ``(live tool, route)`` approval gates."""

    derived = copy.deepcopy(dict(manifest))
    derived["approvalPolicy"] = {"gates": [{"gate": gate, "route": route} for gate, route in gates]}
    return derived


def bundle_archive(source: Path, manifest: Mapping[str, Any]) -> bytes:
    """A flat, reproducible tar.gz of ``source`` with ``manifest`` as its plugin manifest."""

    files: dict[str, bytes] = {}
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if any(part in EXCLUDED_NAMES for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"{relative} is a symlink; a bundle carries none")
        if path.is_file():
            files[relative.as_posix()] = path.read_bytes()
    files[MANIFEST_PATH] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name in sorted(files):
            info = tarfile.TarInfo(name)
            info.size = len(files[name])
            info.mode = 0o644
            info.mtime = 0
            tar.addfile(info, io.BytesIO(files[name]))
    return gzip.compress(raw.getvalue(), mtime=0)


def multipart(field: str, filename: str, data: bytes, content_type: str) -> tuple[bytes, str]:
    """One-file ``multipart/form-data`` body and its Content-Type header."""

    boundary = "identity-e2e-" + secrets.token_hex(16)
    head = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field}"; filename="{filename}"\r\n'
        f"Content-Type: {content_type}\r\n\r\n"
    ).encode()
    return head + data + f"\r\n--{boundary}--\r\n".encode(), (
        f"multipart/form-data; boundary={boundary}"
    )


# --- the creation case table -------------------------------------------------------------------


def expect(
    status: int | Sequence[int], detail: str | None = None, *, duplicate: bool | None = None
) -> dict[str, Any]:
    statuses = [status] if isinstance(status, int) else list(status)
    return {"status": statuses, "detail": detail, "duplicate": duplicate}


def ticket_spec(
    nonce: str,
    *,
    signer: str = "registered",
    key_id: str | None = None,
    tamper: bool = False,
    issued_offset: int = -ISSUED_SKEW_S,
    issued_at: int | None = None,
    lifetime: int = TICKET_LIFETIME_S,
    overrides: Mapping[str, Any] | None = None,
    signed_arguments: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """How the pod mints one ticket; ``nonce`` is a label the host maps to a nonce.

    ``signer`` is ``registered`` or ``other`` (a fresh key the API never saw).
    ``issued_at`` pins the issue time (an identical retry re-mints the same
    ticket: Ed25519 signatures are deterministic); otherwise it is the pod's
    clock plus ``issued_offset``. ``overrides`` replaces claims: ``tenant_id``
    or ``agent_id`` as ``random``, ``connector``, ``tool``, ``route``, or
    ``requester`` as ``{"issuer", "subject"}``. ``signed_arguments`` are the
    arguments the digest covers, when they differ from the request's.
    """

    return {
        "nonce": nonce,
        "signer": signer,
        "key_id": key_id,
        "tamper": tamper,
        "issued_offset": issued_offset,
        "issued_at": issued_at,
        "lifetime": lifetime,
        "overrides": dict(overrides or {}),
        "signed_arguments": None if signed_arguments is None else dict(signed_arguments),
    }


def case(
    name: str,
    *,
    expected: Mapping[str, Any],
    turn: str | None = None,
    endpoint: str = "http",
    route: str | None = TICKET_ROUTE,
    gate_kind: str | None = "permission",
    tool: str | None = None,
    arguments: Mapping[str, Any] | None = None,
    ticket: Mapping[str, Any] | None = None,
    summary: str = SUMMARY,
    dedupe_key: str | None = None,
    approval: Mapping[str, Any] | None = None,
    repeat: int = 1,
    concurrent: bool = False,
    reuse: str | None = None,
) -> dict[str, Any]:
    """One request the pod sends, ``repeat`` times, and what it must answer.

    ``endpoint`` is ``http`` (the internal creation boundary, worker token) or
    ``legacy`` (``POST /approvals``, platform key). The approval is built from
    ``turn``'s queued reply handle, with ``dedupe_key`` defaulting to the turn's
    event id, unless ``approval`` gives the whole body. ``reuse`` resends an
    earlier case's exact approval in the same call under a new request id.
    """

    return {
        "name": name,
        "endpoint": endpoint,
        "turn": turn,
        "route": route,
        "gate_kind": gate_kind,
        "tool": tool or live_tool(CONNECTOR, TOOL),
        "arguments": dict(DEFAULT_ARGUMENTS if arguments is None else arguments),
        "ticket": None if ticket is None else dict(ticket),
        "summary": summary,
        "dedupe_key": dedupe_key,
        "approval": None if approval is None else dict(approval),
        "repeat": repeat,
        "concurrent": concurrent,
        "reuse": reuse,
        "expected": dict(expected),
    }


def valid_cases() -> list[dict[str, Any]]:
    return [
        case(
            "valid",
            turn="valid",
            ticket=ticket_spec("valid"),
            expected=expect(201, duplicate=False),
        )
    ]


def bad_signature_cases(turn: str = "refuse") -> list[dict[str, Any]]:
    invalid = expect(422, ticket_refusal("invalid_ticket"))
    return [
        case(
            "signed-by-another-key",
            turn=turn,
            ticket=ticket_spec("bad-signer", signer="other"),
            expected=invalid,
        ),
        case(
            "tampered-signature",
            turn=turn,
            ticket=ticket_spec("bad-tamper", tamper=True),
            expected=invalid,
        ),
        case(
            "unregistered-key-id",
            turn=turn,
            ticket=ticket_spec("bad-key-id", key_id="identity-e2e-unregistered-key"),
            expected=expect(422, ticket_refusal("key_mismatch")),
        ),
    ]


def scope_change_cases(turn: str = "refuse") -> list[dict[str, Any]]:
    mismatch = expect(422, ticket_refusal("scope_mismatch"))
    changes: dict[str, dict[str, Any]] = {
        "tenant": {"tenant_id": RANDOM},
        "agent": {"agent_id": RANDOM},
        "connector": {"connector": f"{CONNECTOR}-elsewhere"},
        "tool": {"tool": OTHER_TOOL},
        "route": {"route": LOOSER_ROUTE},
    }
    return [
        case(
            f"{name}-changed",
            turn=turn,
            ticket=ticket_spec(f"scope-{name}", overrides=overrides),
            expected=mismatch,
        )
        for name, overrides in changes.items()
    ]


def requester_change_cases(
    others: Mapping[str, Mapping[str, str]], turn: str = "refuse"
) -> list[dict[str, Any]]:
    """The ticket names another verified human as requester than the turn's."""

    return [
        case(
            f"requester-{label}",
            turn=turn,
            ticket=ticket_spec(
                f"requester-{label}",
                overrides={"requester": {"issuer": who["issuer"], "subject": who["subject"]}},
            ),
            expected=expect(422, ticket_refusal("requester_mismatch")),
        )
        for label, who in others.items()
    ]


def argument_change_cases(turn: str = "refuse") -> list[dict[str, Any]]:
    mismatch = expect(422, ticket_refusal("arguments_mismatch"))
    signed = dict(DEFAULT_ARGUMENTS)
    return [
        case(
            "value-changed-after-signing",
            turn=turn,
            arguments={"query": "identity-e2e changed after signing"},
            ticket=ticket_spec("args-value", signed_arguments=signed),
            expected=mismatch,
        ),
        case(
            "argument-added-after-signing",
            turn=turn,
            arguments={**signed, "limit": 5},
            ticket=ticket_spec("args-added", signed_arguments=signed),
            expected=mismatch,
        ),
    ]


def expired_cases(turn: str = "refuse") -> list[dict[str, Any]]:
    expired = expect(422, ticket_refusal("preparation_expired"))
    return [
        case(
            "expired",
            turn=turn,
            ticket=ticket_spec("expired", issued_offset=-600, lifetime=200),
            expected=expired,
        ),
        case(
            "not-yet-valid",
            turn=turn,
            ticket=ticket_spec("not-yet-valid", issued_offset=120, lifetime=120),
            expected=expired,
        ),
    ]


def nonce_replay_cases(turn: str = "refuse") -> list[dict[str, Any]]:
    """The valid ticket's spent nonce on another dedupe key, then a fresh nonce there."""

    return [
        case(
            "spent-nonce-on-another-turn",
            turn=turn,
            ticket=ticket_spec("valid"),
            expected=expect(409, REPLAYED),
        ),
        case(
            "fresh-nonce-on-that-turn",
            turn=turn,
            ticket=ticket_spec("refuse-ok"),
            expected=expect(201, duplicate=False),
        ),
    ]


def conflicting_dedupe_cases(issued_at: int, turn: str = "valid") -> list[dict[str, Any]]:
    """Retries under the valid approval's dedupe key: identical, then conflicting."""

    return [
        case(
            "identical-retry",
            turn=turn,
            ticket=ticket_spec("valid", issued_at=issued_at),
            expected=expect(200, duplicate=True),
        ),
        case(
            "summary-changed",
            turn=turn,
            summary=SUMMARY + " (changed)",
            ticket=ticket_spec("valid", issued_at=issued_at),
            expected=expect(409, PROVENANCE_CONFLICT),
        ),
        case(
            "new-ticket-same-dedupe-key",
            turn=turn,
            ticket=ticket_spec("conflict"),
            expected=expect(409, PROVENANCE_CONFLICT),
        ),
    ]


def race_cases(turn: str = "race") -> list[dict[str, Any]]:
    """Identical creations at once, then one identical retry; judged by the host."""

    return [
        case(
            "identical-race",
            turn=turn,
            ticket=ticket_spec("race"),
            repeat=RACE_POSTS,
            concurrent=True,
            expected=expect((200, 201, 503)),
        ),
        case(
            "identical-retry-after-race",
            turn=turn,
            reuse="identical-race",
            expected=expect(200, duplicate=True),
        ),
    ]


def legacy_policy_cases(turn: str, dedupe: Mapping[str, str]) -> list[dict[str, Any]]:
    """``POST /approvals`` naming the registered pair: refused before the legacy flow."""

    return [
        case(
            "ticket-on-owning-route",
            endpoint="legacy",
            turn=turn,
            dedupe_key=dedupe["ticket-on-owning-route"],
            ticket=ticket_spec("legacy"),
            expected=expect(409, REQUIRES_HTTP_CREATION),
        ),
        case(
            "routeless",
            endpoint="legacy",
            turn=turn,
            route=None,
            dedupe_key=dedupe["routeless"],
            expected=expect(409, ROUTE_LOCKED),
        ),
        case(
            "looser-route-with-ticket",
            endpoint="legacy",
            turn=turn,
            route=LOOSER_ROUTE,
            dedupe_key=dedupe["looser-route-with-ticket"],
            ticket=ticket_spec("legacy"),
            expected=expect(409, ROUTE_LOCKED),
        ),
        case(
            "missing-ticket",
            endpoint="legacy",
            turn=turn,
            dedupe_key=dedupe["missing-ticket"],
            expected=expect(409, TICKET_REQUIRED),
        ),
    ]


LEGACY_POLICY_NAMES = (
    "ticket-on-owning-route",
    "routeless",
    "looser-route-with-ticket",
    "missing-ticket",
)


def slack_ordinary_approval(
    *, agent_id: str, channel: str, adapter: str | None, dedupe_key: str
) -> dict[str, Any]:
    """An ordinary routeless permission approval on a legacy-evaluable Slack binding."""

    return {
        "agent_id": agent_id,
        "conversation_id": f"identity-e2e-{dedupe_key}",
        "author": "U0IE2EAUTHOR",
        "summary": SUMMARY + " (ordinary)",
        "reply_kind": "slack",
        "reply_channel": channel,
        "reply_placeholder": None,
        "reply_endpoint": None,
        "reply_adapter": adapter,
        "reply_identity": None,
        "dedupe_key": dedupe_key,
        "route": None,
        "card_channel": channel,
        "gate_kind": "permission",
        "granted_tool": live_tool(CONNECTOR, OTHER_TOOL),
        "granted_arguments": {},
        "expires_in_seconds": APPROVAL_TTL_S,
    }


def legacy_ordinary_cases(
    *, turn: str, selected_dedupe: str, slack: Mapping[str, Any] | None
) -> list[dict[str, Any]]:
    """Ordinary legacy creation, which the ticket check must leave as it was.

    On the HTTP-selected binding the legacy boundary still answers its own
    ``unsupported_approval``; on a legacy-evaluable Slack binding it still
    creates (201) and is idempotent on the dedupe key (200).
    """

    cases = [
        case(
            "selected-binding-ordinary",
            endpoint="legacy",
            turn=turn,
            route=None,
            tool=live_tool(CONNECTOR, OTHER_TOOL),
            arguments={},
            dedupe_key=selected_dedupe,
            expected=expect(409, UNSUPPORTED),
        )
    ]
    if slack is not None:
        cases += [
            case(
                "legacy-binding-ordinary",
                endpoint="legacy",
                approval=slack,
                expected=expect(201),
            ),
            case(
                "legacy-binding-retry",
                endpoint="legacy",
                reuse="legacy-binding-ordinary",
                expected=expect(200),
            ),
        ]
    return cases


def route_bypass_cases(turn: str = "bypass") -> list[dict[str, Any]]:
    """Every way around the owning route, each with the gate kind absent, policy and permission."""

    owned, other = live_tool(CONNECTOR, TOOL), live_tool(CONNECTOR, OTHER_TOOL)
    cases = []
    for gate in GATE_KINDS:
        tag = gate or "absent"
        cases += [
            case(
                f"looser-route/{tag}",
                turn=turn,
                route=LOOSER_ROUTE,
                gate_kind=gate,
                tool=owned,
                expected=expect(409, ROUTE_LOCKED),
            ),
            case(
                f"routeless/{tag}",
                turn=turn,
                route=None,
                gate_kind=gate,
                tool=owned,
                expected=expect(409, ROUTE_LOCKED),
            ),
            case(
                f"empty-route/{tag}",
                turn=turn,
                route="",
                gate_kind=gate,
                tool=owned,
                expected=expect(409, ROUTE_LOCKED),
            ),
            case(
                f"ticket-on-unregistered-route/{tag}",
                turn=turn,
                route=LOOSER_ROUTE,
                gate_kind=gate,
                tool=other,
                ticket=ticket_spec("bypass-unregistered"),
                expected=expect(409, TICKET_UNREGISTERED),
            ),
            case(
                f"missing-ticket-on-owning-route/{tag}",
                turn=turn,
                route=TICKET_ROUTE,
                gate_kind=gate,
                tool=owned,
                expected=expect(409, TICKET_REQUIRED),
            ),
            case(
                f"other-tool-on-owning-route/{tag}",
                turn=turn,
                route=TICKET_ROUTE,
                gate_kind=gate,
                tool=other,
                ticket=ticket_spec("bypass-other-tool"),
                expected=expect(409, ROUTE_MISMATCH),
            ),
        ]
    cases += [
        case(
            "plugin-prefixed-name-on-owning-route",
            turn=turn,
            tool=f"mcp__plugin_weather_{CONNECTOR}__{TOOL}",
            ticket=ticket_spec("bypass-plugin"),
            expected=expect(409, ROUTE_MISMATCH),
        ),
        case(
            "malformed-name-on-owning-route",
            turn=turn,
            tool=f"mcp__{CONNECTOR}",
            ticket=ticket_spec("bypass-malformed"),
            expected=expect(409, ROUTE_MISMATCH),
        ),
    ]
    return cases


def route_bypass_liveness(turn: str = "bypass") -> list[dict[str, Any]]:
    """An ordinary unrelated tool on the looser route still creates (the lock is narrow)."""

    return [
        case(
            "ordinary-other-tool-on-looser-route",
            turn=turn,
            route=LOOSER_ROUTE,
            tool=live_tool(CONNECTOR, OTHER_TOOL),
            expected=expect(201, duplicate=False),
        )
    ]


def nonce_labels(cases: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted({str(item["ticket"]["nonce"]) for item in cases if item.get("ticket")})


def turn_labels(cases: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted({str(item["turn"]) for item in cases if item.get("turn")})


def case_mismatches(results: Iterable[Mapping[str, Any]]) -> list[str]:
    """Every response that is not what its case expected, as one line each."""

    problems = []
    for item in results:
        wanted = item.get("expected") or {}
        for index, answer in enumerate(item.get("results") or []):
            status, detail = answer.get("http"), answer.get("detail")
            wrong = (
                status not in wanted.get("status", ())
                or (wanted.get("detail") is not None and detail != wanted["detail"])
                or (
                    wanted.get("duplicate") is not None
                    and answer.get("duplicate") is not wanted["duplicate"]
                )
            )
            if wrong:
                problems.append(
                    f"{item.get('name')}[{index}] answered {status} {detail!r} "
                    f"(duplicate {answer.get('duplicate')!r}); expected {wanted}"
                )
    return problems


# --- responses ------------------------------------------------------------------------------

# 2xx fields an outcome may carry: never granted_arguments, which holds a ticket.
_SAFE_FIELDS = (
    "id",
    "duplicate",
    "agent_id",
    "route",
    "card_channel",
    "gate_kind",
    "granted_tool",
    "dedupe_key",
    "requested_by",
    "connector_requester_identity",
    "connector_requester_identity_version",
)


def detail_code(payload: Any) -> str | None:
    """The refusal code (or bounded message) of an error body, never its echoed input."""

    if not isinstance(payload, Mapping):
        return None if payload in (None, "") else "non_json_response"
    detail = payload.get("detail")
    if isinstance(detail, str):
        return detail[:300]
    if isinstance(detail, Mapping):
        code = detail.get("code")
        return str(code)[:300] if code else "detail_object"
    if isinstance(detail, list):
        first = detail[0] if detail and isinstance(detail[0], Mapping) else {}
        message = str(first.get("msg", ""))[:200]
        return f"validation_error: {message}" if message else "validation_error"
    return None


def outcome(status: int, payload: Any) -> dict[str, Any]:
    """A response as evidence: status, refusal code, and a few safe 2xx fields."""

    result: dict[str, Any] = {
        "http": status,
        "detail": None if 200 <= status < 300 else detail_code(payload),
    }
    if 200 <= status < 300 and isinstance(payload, Mapping):
        result.update({key: payload[key] for key in _SAFE_FIELDS if key in payload})
        if "status" in payload:
            result["approval_status"] = payload["status"]
    return result


# --- tickets and request bodies (shared by the pod and the unit tests) ------------------------


def ticket_claims(
    spec: Mapping[str, Any],
    *,
    tenant_id: str,
    agent_id: str,
    requester: Mapping[str, str],
    approvers: Sequence[Mapping[str, str]],
    registered: Mapping[str, str],
    key_id: str,
    nonce: str,
    now: int,
    arguments_sha256: str,
    new_uuid: Callable[[], str] = lambda: str(uuid.uuid4()),
) -> dict[str, Any]:
    """The ADR 0271 ticket claims ``spec`` describes, before signing.

    ``registered`` is the registration's ``connector``, ``tool`` and ``route``;
    ``requester`` the turn's verified canonical human; ``approvers`` the
    selected people, all in the claims' one tenant.
    """

    overrides = dict(spec.get("overrides") or {})
    tenant = new_uuid() if overrides.get("tenant_id") == RANDOM else tenant_id
    agent = new_uuid() if overrides.get("agent_id") == RANDOM else agent_id
    person = overrides.get("requester") or requester
    issued = spec.get("issued_at")
    issued_at = int(issued) if issued is not None else now + int(spec.get("issued_offset", 0))
    return {
        "version": 1,
        "purpose": "approval_request",
        "tenant_id": tenant,
        "agent_id": agent,
        "connector": overrides.get("connector", registered["connector"]),
        "tool": overrides.get("tool", registered["tool"]),
        "route": overrides.get("route", registered["route"]),
        "key_id": spec.get("key_id") or key_id,
        "requester": {
            "tenant_id": tenant,
            "issuer": person["issuer"],
            "subject": person["subject"],
        },
        "nonce": nonce,
        "issued_at": issued_at,
        "expires_at": issued_at + int(spec["lifetime"]),
        "arguments_sha256": arguments_sha256,
        "approvers": [
            {"tenant_id": tenant, "issuer": who["issuer"], "subject": who["subject"]}
            for who in approvers
        ],
    }


def tamper_signature(token: str) -> str:
    """Flip one middle character of the signature; the envelope stays canonical."""

    prefix, payload, signature = token.split(".")
    index = len(signature) // 2
    swapped = "B" if signature[index] != "B" else "C"
    return ".".join((prefix, payload, signature[:index] + swapped + signature[index + 1 :]))


def mint_ticket(
    item: Mapping[str, Any],
    *,
    nonces: Mapping[str, str],
    now: int,
    sign: Callable[..., str],
    digest: Callable[[dict[str, Any]], str],
    registered_key: Any,
    other_key: Callable[[], Any],
    **claims: Any,
) -> tuple[str, dict[str, Any]]:
    """Sign ``item``'s ticket with ``sign`` (``approver_ticket.sign_ticket``).

    ``digest`` is ``execution_arguments_sha256`` over the signed arguments;
    ``claims`` are :func:`ticket_claims`' context keywords. Returns the token
    and its claims.
    """

    spec = item["ticket"]
    signed = spec.get("signed_arguments")
    values = ticket_claims(
        spec,
        nonce=nonces[spec["nonce"]],
        now=now,
        arguments_sha256=digest(dict(item["arguments"] if signed is None else signed)),
        **claims,
    )
    key = registered_key if spec.get("signer", "registered") == "registered" else other_key()
    token = sign(values, signing_key=key)
    return (tamper_signature(token) if spec.get("tamper") else token), values


def approval_fields(
    item: Mapping[str, Any], *, turn: Any, agent_id: str, granted_arguments: Mapping[str, Any]
) -> dict[str, Any]:
    """The ``ApprovalRequest`` fields for ``item`` on a queued turn (``QueuedTurn``).

    The reply fields and the author are the turn's own, the dedupe key is its
    event id unless the case names one, and the card goes to the reply channel,
    which is where a routeless or requesting-surface card goes.
    """

    handle = turn.reply_handle
    return {
        "agent_id": agent_id,
        "conversation_id": turn.conversation_id,
        "author": turn.author,
        "summary": item["summary"],
        "reply_kind": handle.kind,
        "reply_channel": handle.channel,
        "reply_placeholder": handle.placeholder,
        "reply_endpoint": handle.endpoint,
        "reply_adapter": handle.adapter,
        "reply_identity": handle.identity,
        "dedupe_key": item.get("dedupe_key") or turn.event_id,
        "route": item["route"],
        "card_channel": handle.channel,
        "gate_kind": item["gate_kind"],
        "granted_tool": item["tool"],
        "granted_arguments": dict(granted_arguments),
        "expires_in_seconds": APPROVAL_TTL_S,
    }


def creation_request(
    approval: Mapping[str, Any], *, capsule: str, payload: str, scope: Any
) -> dict[str, Any]:
    """An ``ApprovalCreationRequestV1`` body carrying the turn's exact ingress bytes."""

    return {
        "schema_version": 1,
        "request_id": str(uuid.uuid4()),
        "approval": dict(approval),
        "identity": {
            "schema_version": 1,
            "context": capsule,
            "payload": payload,
            "scope": {
                "tenant_id": scope.tenant_id,
                "agent_id": scope.agent_id,
                "binding_id": scope.binding_id,
                "binding_generation": scope.binding_generation,
            },
        },
    }


# --- inside the API pod -------------------------------------------------------------------------


class DriverRefusal(Exception):
    """A refusal whose message is safe to print."""


class _Turn:
    def __init__(self, capsule: str, payload: str, context: Any, turn: Any) -> None:
        self.capsule = capsule
        self.payload = payload
        self.context = context
        self.turn = turn


def _settings() -> Any:
    from curie_api.config import get_settings

    return get_settings()


def _sql(body: Callable[[Any], Any]) -> Any:
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    async def main() -> Any:
        engine = create_async_engine(_settings().database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(main())


def _call(method: str, path: str, body: Any, headers: Mapping[str, str]) -> dict[str, Any]:
    import urllib.error
    import urllib.request

    data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(API_LOOPBACK + path, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:  # noqa: S310
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    except OSError as error:
        return {"http": 0, "detail": f"transport {type(error).__name__}"}
    try:
        payload = json.loads(raw.decode("utf-8")) if raw else None
    except ValueError:
        payload = None
    return outcome(status, payload)


def _send(
    requests: Sequence[tuple[str, str, Any, Mapping[str, str]]], *, concurrent: bool
) -> list[dict[str, Any]]:
    """Send ``(method, path, body, headers)`` requests; at once behind a barrier when concurrent."""

    if not concurrent or len(requests) < 2:
        return [_call(*request) for request in requests]
    barrier = threading.Barrier(len(requests))
    results: list[dict[str, Any] | None] = [None] * len(requests)

    def run(index: int) -> None:
        try:
            barrier.wait(timeout=BARRIER_TIMEOUT_S)
        except threading.BrokenBarrierError:
            results[index] = {"http": 0, "detail": "barrier_broken"}
            return
        results[index] = _call(*requests[index])

    threads = [threading.Thread(target=run, args=(index,)) for index in range(len(requests))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=HTTP_TIMEOUT_S * 2)
    return [result or {"http": 0, "detail": "no_response"} for result in results]


def _load_turns(refs: Mapping[str, Mapping[str, str]]) -> dict[str, _Turn]:
    """Each turn's stream entry, opened with the API's own runtime and checked."""

    if not refs:
        return {}
    import redis
    from aci_protocol import READER_CONTEXT, QueuedTurn
    from aci_protocol.service_config import STREAM_PAYLOAD_FIELD
    from curie_api.identity.http_initializer import configured_http_identity_runtime
    from curie_internal import identity_context as codec

    settings = _settings()
    runtime = configured_http_identity_runtime(settings)
    if runtime is None:
        raise DriverRefusal("the API has no HTTP identity runtime")
    client = redis.Redis.from_url(settings.valkey_dsn(), decode_responses=True)
    loaded: dict[str, _Turn] = {}
    try:
        for label, ref in refs.items():
            entries = client.xrange(
                settings.runs_stream, min=ref["stream_id"], max=ref["stream_id"]
            )
            if len(entries) != 1:
                raise DriverRefusal(
                    f"turn {label}: stream entry {ref['stream_id']} is not in the runs stream"
                )
            fields = entries[0][1]
            capsule, payload = fields.get(CONTEXT_FIELD), fields.get(STREAM_PAYLOAD_FIELD)
            if not capsule or not payload:
                raise DriverRefusal(f"turn {label}: the stream entry carries no identity capsule")
            context = codec.open_invocation_context(capsule, runtime._context_keys)
            if not isinstance(context, codec.InvocationContextV1):
                raise DriverRefusal(f"turn {label}: the capsule does not open with the API's keys")
            if (
                context.event_id != ref["event_id"]
                or hashlib.sha256(payload.encode("utf-8")).hexdigest() != context.payload_sha256
            ):
                raise DriverRefusal(f"turn {label}: the stream entry is not the sent turn")
            try:
                turn = QueuedTurn.model_validate(json.loads(payload), context=READER_CONTEXT)
            except ValueError:
                raise DriverRefusal(f"turn {label}: the queued payload does not parse") from None
            if turn.reply_handle is None:
                raise DriverRefusal(f"turn {label}: the queued turn has no reply handle")
            loaded[label] = _Turn(capsule, payload, context, turn)
    finally:
        client.close()
    return loaded


def _capture(params: Mapping[str, Any]) -> dict[str, Any]:
    summaries = {}
    for label, item in _load_turns(params["turns"]).items():
        context, scope = item.context, item.context.scope
        summaries[label] = {
            "event_id": context.event_id,
            "scope": {
                "tenant_id": scope.tenant_id,
                "agent_id": scope.agent_id,
                "binding_id": scope.binding_id,
                "binding_generation": scope.binding_generation,
                "provider": scope.provider,
            },
            "outcome": context.outcome,
            "canonical": None
            if context.canonical is None
            else {"issuer": context.canonical.issuer, "subject": context.canonical.subject},
            "principal_present": context.principal is not None,
            "role_policy_version": context.role_policy_version,
            "reply_kind": item.turn.reply_handle.kind,
        }
    return {"turns": summaries}


def _approval_for(
    item: Mapping[str, Any],
    turns: Mapping[str, _Turn],
    params: Mapping[str, Any],
    *,
    now: int,
    registered_key: Any,
) -> tuple[dict[str, Any], _Turn | None, int | None]:
    from aci_protocol import READER_CONTEXT, ApprovalRequest
    from curie_internal.approver_ticket import (
        TicketError,
        execution_arguments_sha256,
        sign_ticket,
    )
    from nacl.signing import SigningKey

    name = item["name"]
    issued_at: int | None = None
    turn: _Turn | None = None
    if item.get("approval") is not None:
        fields = dict(item["approval"])
    else:
        turn = turns[item["turn"]]
        context = turn.context
        if context.canonical is None:
            raise DriverRefusal(f"case {name}: the turn's sender is not a verified human")
        arguments = dict(item["arguments"])
        if item.get("ticket"):
            try:
                token, claims = mint_ticket(
                    item,
                    nonces=params["nonces"],
                    now=now,
                    sign=sign_ticket,
                    digest=execution_arguments_sha256,
                    registered_key=registered_key,
                    other_key=SigningKey.generate,
                    tenant_id=context.scope.tenant_id,
                    agent_id=context.scope.agent_id,
                    requester={
                        "issuer": context.canonical.issuer,
                        "subject": context.canonical.subject,
                    },
                    approvers=params["approvers"],
                    registered=params["registered"],
                    key_id=params["key"]["key_id"],
                )
            except TicketError as exc:
                raise DriverRefusal(
                    f"case {name}: the ticket does not encode ({exc.code})"
                ) from None
            issued_at = int(claims["issued_at"])
            arguments["approval_ticket"] = token
        fields = approval_fields(
            item, turn=turn.turn, agent_id=context.scope.agent_id, granted_arguments=arguments
        )
    try:
        approval = ApprovalRequest.model_validate(fields, context=READER_CONTEXT).model_dump(
            mode="json"
        )
    except ValueError:
        raise DriverRefusal(f"case {name}: the approval request does not validate") from None
    return approval, turn, issued_at


def _create(params: Mapping[str, Any]) -> dict[str, Any]:
    from nacl.signing import SigningKey

    settings = _settings()
    turns = _load_turns(params.get("turns") or {})
    registered_key = SigningKey(bytes.fromhex(params["key"]["seed_hex"]))
    now = int(time.time())
    built: dict[str, tuple[dict[str, Any], _Turn | None, int | None]] = {}
    reports = []
    for item in params["cases"]:
        name = item["name"]
        if item.get("reuse"):
            if item["reuse"] not in built:
                raise DriverRefusal(f"case {name}: nothing to reuse named {item['reuse']}")
            approval, turn, issued_at = built[item["reuse"]]
        else:
            approval, turn, issued_at = _approval_for(
                item, turns, params, now=now, registered_key=registered_key
            )
        built[name] = (approval, turn, issued_at)
        requests = []
        for _ in range(int(item.get("repeat") or 1)):
            if item["endpoint"] == "http":
                if turn is None:
                    raise DriverRefusal(f"case {name}: HTTP creation needs a turn's capsule")
                body = creation_request(
                    approval, capsule=turn.capsule, payload=turn.payload, scope=turn.context.scope
                )
                requests.append(
                    (
                        "POST",
                        CREATION_PATH,
                        body,
                        {"X-Curie-Worker-Token": settings.internal_worker_token},
                    )
                )
            else:
                requests.append(("POST", LEGACY_PATH, approval, {"X-API-Key": settings.api_key}))
        results = _send(requests, concurrent=bool(item.get("concurrent")))
        reports.append(
            {
                "name": name,
                "endpoint": item["endpoint"],
                "route": approval.get("route"),
                "gate_kind": approval.get("gate_kind"),
                "granted_tool": approval.get("granted_tool"),
                "ticket": isinstance(approval.get("granted_arguments"), dict)
                and "approval_ticket" in approval["granted_arguments"],
                "dedupe_key": approval.get("dedupe_key"),
                "issued_at": issued_at,
                "expected": item["expected"],
                "results": results,
            }
        )
    return {"now": now, "cases": reports, **_counts(params)}


def _counts(params: Mapping[str, Any]) -> dict[str, Any]:
    """Approval rows by dedupe key, spent nonces by label and row facts, from Postgres."""

    from curie_api.db import SCHEMA
    from sqlalchemy import text

    agent_id = str(params["agent_id"])
    dedupe = list(params.get("count_dedupe") or [])
    nonces = dict(params.get("count_nonces") or {})
    facts = list(params.get("facts") or [])
    table = f'"{SCHEMA}".approvals'

    async def body(session: Any) -> dict[str, Any]:
        rows = {}
        for key in dedupe:
            rows[key] = await session.scalar(
                text(f"SELECT count(*) FROM {table} WHERE dedupe_key = :d"), {"d": key}
            )
        spent = {}
        for label, nonce in nonces.items():
            spent[label] = await session.scalar(
                text(
                    f"SELECT count(*) FROM {table} WHERE agent_id::text = :a "
                    "AND selection_nonce = ANY(:forms)"
                ),
                {"a": agent_id, "forms": nonce_forms(nonce)},
            )
        found: dict[str, list[dict[str, Any]]] = {}
        for key in facts:
            result = await session.execute(
                text(
                    "SELECT id::text AS id, agent_id::text AS agent_id, status, route, "
                    "gate_kind, granted_tool, selection_mode, selection_connector, "
                    "selection_tool, selection_key_id, selection_arguments_sha256, "
                    "selection_requester, selected_approvers, "
                    "(selection_nonce IS NOT NULL) AS nonce_stored, "
                    "((granted_arguments ->> 'approval_ticket') IS NOT NULL) AS ticket_stored, "
                    f"(identity_context IS NOT NULL) AS sealed FROM {table} "
                    "WHERE dedupe_key = :d"
                ),
                {"d": key},
            )
            found[key] = [dict(row._mapping) for row in result]
        return {"rows": rows, "nonces": spent, "facts": found}

    return dict(_sql(body))


def _count_agent(params: Mapping[str, Any]) -> dict[str, Any]:
    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> int:
        return int(
            await session.scalar(
                text(f'SELECT count(*) FROM "{SCHEMA}".approvals WHERE agent_id::text = :a'),
                {"a": str(params["agent_id"])},
            )
        )

    return {"count": _sql(body)}


def _agent_routes(params: Mapping[str, Any]) -> dict[str, Any]:
    """The agent's stored route map as written, transport fields included."""

    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> Any:
        return await session.scalar(
            text(f'SELECT approval_routes FROM "{SCHEMA}".agents WHERE id::text = :a'),
            {"a": str(params["agent_id"])},
        )

    routes = _sql(body)
    if isinstance(routes, str):
        routes = json.loads(routes)
    return {"routes": routes}


def _principal(params: Mapping[str, Any]) -> dict[str, Any]:
    """The canonical principals rows for one (tenant, issuer, subject)."""

    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> list[dict[str, Any]]:
        result = await session.execute(
            text(
                f'SELECT type, status FROM "{SCHEMA}".principals WHERE tenant_id::text = :t '
                "AND idp_issuer = :i AND idp_subject = :s"
            ),
            {"t": params["tenant_id"], "i": params["issuer"], "s": params["subject"]},
        )
        return [dict(row._mapping) for row in result]

    return {"rows": _sql(body)}


def _approver_role(params: Mapping[str, Any]) -> dict[str, Any]:
    """Read the agent's approver role; set its filter when ``filter`` is given.

    ``version`` is the access-policy row's own version, the one a role change
    must name. A row whose role configuration is SQL NULL reads as no policy
    snapshot but still has a version (``up`` stores a teamless row), so the
    version never comes from the snapshot.

    With ``remove`` true the approver role key is taken out again: the restore
    for an agent that had none. The role-policy API has no operation for that
    (a ``filter: null`` role evaluates as unrestricted, an absent one as not
    configured), so the row is written here under the same lock order, with
    the version bump and audit event a role change records, and a configuration
    the agent stored as SQL NULL goes back to NULL once no role remains.
    """

    from curie_api import agent_role_policy, identity_audit
    from curie_api.schemas.agent_access import SetRoleFilterV1
    from sqlalchemy import func

    tenant, agent = uuid.UUID(params["tenant_id"]), uuid.UUID(params["agent_id"])

    async def locked(session: Any) -> tuple[Any, bool, Any]:
        found, policy, sql_null = await agent_role_policy.lock_policy_rows(
            session, tenant_id=tenant, agent_id=agent, require_active=False
        )
        if found is None:
            raise DriverRefusal("the agent does not exist")
        snapshot = agent_role_policy.stored_snapshot(
            policy, sql_null, tenant_id=tenant, agent_id=agent
        )
        return policy, sql_null, None if snapshot is None else snapshot.roles.approver

    def describe(policy: Any, sql_null: bool, role: Any) -> dict[str, Any]:
        return {
            "version": 0 if policy is None else policy.version,
            "row": policy is not None,
            "configured": policy is not None and not sql_null,
            "had_role": role is not None,
            "legacy_state": None if role is None else role.legacy_state,
            "filter": None
            if role is None or role.filter is None
            else role.filter.model_dump(mode="json"),
        }

    async def body(session: Any) -> dict[str, Any]:
        async with session.begin():
            state = describe(*await locked(session))
        if params.get("remove"):
            return await remove(session, state)
        if "filter" not in params:
            return state
        if state["had_role"] and state["filter"] == params["filter"]:
            return {**state, "action": "unchanged"}
        change = SetRoleFilterV1.model_validate(
            {
                "schema_version": 1,
                "tenant_id": str(tenant),
                "agent_id": str(agent),
                "expected_version": state["version"],
                "role": "approver",
                "operation": "set_filter",
                "filter": params["filter"],
            }
        )
        async with session.begin():
            result = await agent_role_policy.apply_role_change(session, change=change)
        return {**state, "action": "set", "version": result.version}

    async def remove(session: Any, state: dict[str, Any]) -> dict[str, Any]:
        if not state["had_role"]:
            return {**state, "action": "unchanged"}
        async with session.begin():
            policy, sql_null, role = await locked(session)
            if policy is None or role is None or policy.version != state["version"]:
                raise DriverRefusal("the approver role changed while it was being removed")
            roles = {
                name: value
                for name, value in dict(policy.role_configuration or {}).items()
                if name != "approver"
            }
            version = policy.version + 1
            policy.role_configuration = (
                None if not roles and params.get("configured") is False else roles
            )
            policy.version = version
            policy.updated_at = func.now()
            identity_audit.record(
                session,
                tenant_id=tenant,
                event_type="identity.agent_role_policy_changed",
                actor="identity-e2e",
                details={
                    "agent_id": str(agent),
                    "role": "approver",
                    "operation": "identity_e2e_restore_absent_role",
                    "previous_version": state["version"],
                    "version": version,
                    "previous_legacy_state": state["legacy_state"],
                    "legacy_state": None,
                },
            )
            await session.flush()
            after = describe(*await locked(session))
        return {**after, "action": "removed"}

    return dict(_sql(body))


def _race_flag(params: Mapping[str, Any]) -> dict[str, Any]:
    """A flag disable and a registration, both at one expected version, sent at once."""

    settings = _settings()
    path = f"/agents/{params['agent_id']}"
    headers = {"X-API-Key": settings.api_key}
    version = int(params["version"])
    calls = (
        (
            "flag_off",
            {
                "connector_requester_identity": False,
                "connector_requester_identity_expected_version": version,
            },
        ),
        (
            "register",
            {"approval_routes": params["routes"], "approval_routes_expected_version": version},
        ),
    )
    results = _send([("PATCH", path, body, headers) for _, body in calls], concurrent=True)
    return {"results": {name: result for (name, _), result in zip(calls, results, strict=True)}}


_ACTIONS: dict[str, Callable[[Mapping[str, Any]], dict[str, Any]]] = {
    "capture": _capture,
    "create": _create,
    "count_agent": _count_agent,
    "agent_routes": _agent_routes,
    "principal": _principal,
    "approver_role": _approver_role,
    "race_flag": _race_flag,
}


# A product refusal code (``RolePolicyRefusal.code`` and the like) is a fixed literal.
_REFUSAL_CODE = re.compile(r"[a-z][a-z0-9_.]{0,79}")


def _database_message(exc: Exception) -> str | None:
    """The driver's first line of a database error (a missing column, say), else None."""

    original = getattr(exc, "orig", None)
    if original is None:
        return None
    lines = str(original).strip().splitlines()
    return lines[0][:200] if lines else None


def dispatch(params: Any) -> dict[str, Any]:
    """Run one action; any failure becomes ``{"error": ...}`` without echoing inputs."""

    action = params.get("action") if isinstance(params, Mapping) else None
    handler = _ACTIONS.get(action) if isinstance(action, str) else None
    if handler is None:
        return {"error": f"unknown action {str(action)[:40]!r}"}
    try:
        return handler(params)
    except DriverRefusal as exc:
        return {"error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - product exception text may quote inputs.
        database = _database_message(exc)
        code = getattr(exc, "code", None)
        if isinstance(code, str) and _REFUSAL_CODE.fullmatch(code):
            database = database or code
        suffix = f": {database}" if database else ""
        return {"error": f"{action} failed with {type(exc).__name__}{suffix}"}


# This file's own text, sent to the pod on stdin beside the action's parameters.
# A kube exec carries its command line in the request URL, which is no place
# for this much source, so `python -c` runs only the bootstrap below. Executed
# from the bootstrap, the module has no __file__ and reads nothing.
SOURCE = Path(__file__).read_text(encoding="utf-8") if "__file__" in globals() else ""
SOURCE_PARAM = "driver_source"
POD_BOOTSTRAP = (
    "import json, sys\n"
    "P = json.load(sys.stdin)\n"
    "G = {'__name__': 'approver_ticket_driver_pod'}\n"
    f"exec(compile(P.pop({SOURCE_PARAM!r}), 'approver_ticket_driver.py', 'exec'), G)\n"
    "print(json.dumps(G['dispatch'](P), sort_keys=True, default=str))\n"
)


def pod_params(action: str, **params: Any) -> dict[str, Any]:
    """What ``Target.pod_python(POD_BOOTSTRAP, ...)`` sends on stdin for one action."""

    return {SOURCE_PARAM: SOURCE, "action": action, **params}
