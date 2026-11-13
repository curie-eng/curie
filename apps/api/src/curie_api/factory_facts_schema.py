"""The ticket facts exchange contract (ADR 0280), outside the ACI protocol."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal
from urllib.parse import quote

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, field_validator

REQUEST_MARKER = "<!-- curie-factory-facts-request:v1 -->"
Name = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")]
Digest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]


def _utc(value: datetime) -> datetime:
    if value.utcoffset() != timedelta(0):
        raise ValueError("UTC timestamp required")
    return value


UtcTime = Annotated[datetime, AfterValidator(_utc)]
Reason = Literal[
    "observation_missing",
    "observation_expired",
    "outside_catalog",
    "runner_unavailable",
    "provider_unsupported",
    "credential_rejected",
    "not_probed",
    "unreadable",
    "personal_owner_required",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    @field_validator("schema_version", mode="before", check_fields=False)
    @classmethod
    def integer_version(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("integer schema version required")
        return value


class FactsRequest(StrictModel):
    schema_version: Literal[1]
    trigger_label: Annotated[str, Field(min_length=1, max_length=100)]
    executables: Annotated[list[Name], Field(max_length=32)]
    services: Annotated[list[Name], Field(max_length=16)]

    def digest(self) -> str:
        canonical = self.model_dump()
        for key in ("executables", "services"):
            canonical[key] = sorted(set(canonical[key]))
        return digest(canonical)


def digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class InvalidFactsRequest(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def parse_request(body: str) -> FactsRequest:
    """Read the frozen layout; prose and field values never become instructions."""
    if not isinstance(body, str):
        raise InvalidFactsRequest("invalid_request")
    match = re.fullmatch(
        r"(?:[^\n]+\n(?:[ \t]*\n)?)?"
        + re.escape(REQUEST_MARKER)
        + r"\n```json\n(?P<json>.*?)\n```\s*",
        body.strip(),
        re.DOTALL,
    )
    if match is None:
        raise InvalidFactsRequest("invalid_layout")
    raw = match["json"]
    if len(("```json\n" + raw + "\n```").encode("utf-8")) > 4096:
        raise InvalidFactsRequest("request_too_large")
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or type(value.get("schema_version")) is not int:
            raise InvalidFactsRequest("invalid_request")
        return FactsRequest.model_validate(value)
    except (ValueError, ValidationError, RecursionError):
        raise InvalidFactsRequest("invalid_request") from None


class RequestIdentity(StrictModel):
    comment_id: str
    digest: Digest


class FactsSubject(StrictModel):
    repository_id: str
    issue_number: int | None
    trigger_label: str


class BindingFact(StrictModel):
    state: Literal["bound", "unbound", "label_not_served", "ambiguous"]
    reason: Literal["unbound", "label_not_served", "ambiguous"] | None


class LabelRoute(StrictModel):
    actor_id: str
    actor_kind: Literal["human"]
    route: Literal["github_user", "github_user_via_app"]
    state: Literal["eligible", "ineligible", "unknown"]
    reason: Reason | None


class ExecutableFact(StrictModel):
    name: Name
    state: Literal["present", "absent", "unknown"]
    basis: Literal["selected_image_probe"] | None
    reason: Reason | None
    observed_at: UtcTime | None


class ServiceFact(StrictModel):
    name: Name
    state: Literal["configured", "observed", "unknown"]
    reason: Reason | None
    observed_at: UtcTime | None


class CreditFact(StrictModel):
    state: Literal["sufficient", "insufficient", "unknown"]
    reason: Reason | None
    observed_at: UtcTime | None
    expires_at: UtcTime | None


class CredentialFact(StrictModel):
    state: Literal["healthy", "unhealthy", "unknown"]
    reason: Reason | None
    observed_at: UtcTime | None


class FactoryFact(StrictModel):
    enabled: bool
    kill_switch: Literal["clear", "engaged", "unknown"]
    upgrade_pause: Literal["clear", "paused", "unknown"]
    code_host_credential: CredentialFact


class FactoryFacts(StrictModel):
    schema_version: Literal[1]
    kind: Literal["factory_facts"]
    request: RequestIdentity | None
    subject: FactsSubject
    observed_at: UtcTime
    expires_at: UtcTime
    context_digest: Digest
    binding: BindingFact
    label_route: LabelRoute | None
    executables: list[ExecutableFact]
    services: list[ServiceFact]
    model_credit: CreditFact
    factory: FactoryFact
    admission: Literal["not_requested"]
    deployment_readiness: Literal["unvalidated"]


def answer_marker(label: str, comment_id: str, request_digest: str) -> str:
    return (
        f"curie-factory-facts:v1 label={quote(label, safe='')} "
        f"request={comment_id} digest={request_digest}"
    )


def marker_prefix(label: str) -> str:
    return f"<!-- curie-factory-facts:v1 label={quote(label, safe='')} "


def answer_body(facts: FactoryFacts, login: str) -> str:
    """Human groups first, machine contract in one fold, marker supplied by adapter."""
    label = facts.subject.trigger_label.replace("\n", " ").replace("\r", " ")
    span = "`" * (max((len(run) for run in re.findall(r"`+", label)), default=0) + 1)
    encoded = facts.model_dump_json(indent=2)
    fence = "`" * max(3, max((len(run) for run in re.findall(r"`+", encoded)), default=0) + 1)
    eligible = facts.label_route is not None and facts.label_route.state == "eligible"
    route = (
        "also through an app"
        if facts.label_route and facts.label_route.route == "github_user_via_app"
        else "directly"
    )
    lines = [
        "**Factory facts. No work was started.**",
        "",
        f"Repository: {facts.binding.state}, trigger label {span} {label} {span}",
        f"{login} may start work: {'yes' if eligible else 'no'}, {route}",
        f"Factory: {'enabled' if facts.factory.enabled else 'disabled'}, "
        f"kill switch {facts.factory.kill_switch}, upgrade pause {facts.factory.upgrade_pause}",
    ]
    for title, values in (("Sandbox tools", facts.executables), ("Services", facts.services)):
        if values:
            lines.append(f"{title}: " + " · ".join(f"{v.name} {v.state}" for v in values))
    lines.extend(
        [
            f"Model credit: {facts.model_credit.state}",
            f"Checked {facts.observed_at:%H:%M} UTC, valid until {facts.expires_at:%H:%M} UTC. "
            "These are facts about this factory, not a verdict on the ticket. "
            "Deployment readiness remains unvalidated.",
            "",
            "<details><summary>Details (JSON)</summary>",
            "",
            fence + "json",
            encoded,
            fence,
            "",
            "</details>",
        ]
    )
    return "\n".join(lines)
