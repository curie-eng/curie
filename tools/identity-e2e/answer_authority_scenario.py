"""The ``approver-ticket-answer`` scenario: ADR 0271 evidence items 3 and 4 on a live install.

It reuses the creation scenario's setup and restore (``TicketScenario``): a
gated examples/weather version serving alone, the requester flag on, a
selector registered on the owning route and a restricted approver role. On
top it chooses five roster people by part, sends real email turns whose
ingress capsules the creations use, changes the scripted directory through the
provider's roster override, and answers every approval the way the mail adapter
does: ``POST /approvals/{id}/resolve`` with an adapter principal, the sender as
the actor and a receiver-signed email answer proof over the exact body. No
answer goes through the ``curie`` CLI or an operator principal, except the
step that proves an operator principal is refused. Each step records HTTP
statuses, the row before and after, the new approval audit rows, identity audit
events and resume check decisions; never a key, token, proof, capsule, nonce or
ticket. See docs/testing/identity-e2e.md.

The worker is kept quiescent while steps run: every lookup the API applies
moves the tenant's lifecycle generation, and an answer's inline resume
publication refuses when that moved under it. So the run waits until the
worker has finished the turns it sent, and after each answer that publishes a
wake, until it has finished the resumed turn. Each wait is recorded under
``worker_waits``.
"""

from __future__ import annotations

import contextlib
import copy
import json
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

import answer_authority_driver as answers
import approver_ticket_driver as tickets
from fake_idp import ADAPTER_SECRET_HEADER
from identity_assets import SINK_PORT, ed25519_pair
from identity_e2e import Api, Harness, HarnessError
from identity_scenarios import (
    Evidence,
    StepFailed,
    TicketScenario,
    check,
    roster_subject,
    send_summary,
)
from identity_target import ADAPTER, http, write_private

KEEP = answers.KEEP
R, A, B, D, E = (
    answers.REQUESTER,
    answers.APPROVER,
    answers.SECOND,
    answers.OUTSIDER,
    answers.EXEMPT,
)


class AnswerScenario(TicketScenario):
    """One ``approver-ticket-answer`` run on one agent, and what it must put back."""

    def __init__(self, harness: Harness, args: Any, evidence: Evidence) -> None:
        super().__init__(harness, args, evidence)
        roster = harness.data.get("roster") or {}
        try:
            self.people = answers.choose_people(roster, args.sender, args.second_sender)
            self.namespace = answers.email_namespace(
                harness.data["registration"], self.people[D]
            )
        except ValueError as exc:
            raise HarnessError(str(exc)) from None
        self.roster_data = copy.deepcopy(dict(roster))
        self.subjects = {part: roster_subject(harness, who) for part, who in self.people.items()}
        self.alias = answers.alias_address(roster, self.people[R])
        # The approver role the run installs and returns to: everyone but the outsider.
        self.ceiling = sorted(self.subjects[part] for part in answers.BASE_ROLE)
        registration = harness.data["registration"]
        # Travels to the pod on stdin only, to sign answer proofs; never recorded.
        self.receiver = {
            "receiver_id": registration["receiver_id"],
            "generation": registration["generation"],
            "key": harness.data["keys"]["receiver"],
        }
        self.binding_id = str(harness.agent_entry(self.agent)["binding_id"])
        self.keys: dict[str, dict[str, str]] = {}
        self.free_turns: list[str] = []
        self.approvals: dict[str, str] = {}
        self.approved: set[str] = set()
        # Every wait for the worker, in run order: counts and seconds only.
        self.waits: list[dict[str, Any]] = []
        evidence.data["worker_waits"] = self.waits
        self.route_state: dict[str, Any] = {
            "registration": {"key": "k1", "users": None},
            "separation": None,
            "static": None,
        }
        self.changes["roster"] = False

    # the in-pod driver, the provider and readback ------------------------------------------

    def drive(self, action: str, **params: Any) -> dict[str, Any]:
        """One action of the in-pod answer driver; a refusal raises :class:`HarnessError`."""

        started = time.monotonic()
        print(f"{answers.SCENARIO}: driver {action} started", file=sys.stderr, flush=True)
        try:
            result = self.harness.target.pod_python(
                answers.POD_BOOTSTRAP, answers.pod_params(action, **params)
            )
        finally:
            print(
                f"{answers.SCENARIO}: driver {action} ended after "
                f"{time.monotonic() - started:.1f}s",
                file=sys.stderr,
                flush=True,
            )
        if not isinstance(result, dict) or "error" in result:
            error = result.get("error") if isinstance(result, dict) else result
            raise HarnessError(f"the in-pod answer driver ({action}) refused: {error}")
        return result

    def provider_roster(self) -> dict[str, Any]:
        """The provider's effective roster, overrides and change counter."""

        harness = self.harness
        with harness.target.forward(f"{harness.prefix}-idp", SINK_PORT, "/healthz") as base:
            status, payload = http("GET", base + "/roster")
        if status == 404:
            raise HarnessError(
                "the test provider has no /roster endpoint; rerun `up` so it serves the "
                "current fake_idp.py"
            )
        if status != 200 or not isinstance(payload, dict):
            raise HarnessError(f"the provider roster answered {status}")
        return payload

    def roster_change(self, change: Mapping[str, Any]) -> dict[str, Any]:
        """Apply one roster override; the result names what now differs from `up`'s roster."""

        harness = self.harness
        self.changes["roster"] = True
        secret = str(harness.data["keys"]["adapter_secret"])
        with harness.target.forward(f"{harness.prefix}-idp", SINK_PORT, "/healthz") as base:
            status, payload = http(
                "POST", base + "/roster", body=dict(change), headers={ADAPTER_SECRET_HEADER: secret}
            )
        if status != 200 or not isinstance(payload, dict):
            detail = payload.get("detail") if isinstance(payload, dict) else None
            raise HarnessError(f"the provider refused a roster change ({status}): {detail}")
        return {"overridden": payload.get("overridden"), "generation": payload.get("generation")}

    def entry(self, part: str) -> dict[str, Any]:
        return answers.entry_of(self.roster_data, self.people[part])

    def restore_people(self, *parts: str) -> dict[str, Any]:
        """These people's entries back as `up` installed them."""

        upsert = {self.people[part]: self.entry(part) for part in parts}
        return self.roster_change({"upsert": upsert})

    def stored_routes(self) -> dict[str, Any]:
        routes = self.drive("agent_routes", agent_id=self.agent_id)["routes"]
        return dict(routes) if isinstance(routes, dict) else {}

    def readback(self, api: Api, route: str = tickets.TICKET_ROUTE) -> dict[str, Any]:
        _, version, routes = self.configuration(api)
        return {"configuration_version": version, **answers.ticket_readback(routes, route)}

    def rows(self, *names: str) -> dict[str, Any]:
        ids = {name: self.approval(name) for name in names}
        found = self.drive("rows", ids=list(ids.values()))["rows"]
        return {name: found.get(approval_id) for name, approval_id in ids.items()}

    def approval(self, name: str) -> str:
        approval_id = self.approvals.get(name)
        if approval_id is None:
            raise StepFailed(f"approval {name} was never created (an earlier step failed)")
        return approval_id

    def identity_events(self, event_type: str) -> list[dict[str, Any]]:
        return list(
            self.drive("identity_audits", tenant_id=self.tenant_id, event_type=event_type)[
                "events"
            ]
        )

    # the worker's quiescence and people's principals -----------------------------------------

    def await_worker(self, what: str, events: Sequence[str], budget: float) -> dict[str, Any]:
        """Wait until the worker has finished every one of ``events``, for ``budget`` seconds.

        One in-pod call waits up to ``answers.SETTLE_CALL_S``; the host repeats
        it with what is still pending. Recorded in ``worker_waits``; a wait
        that runs out raises :class:`HarnessError` naming what is pending.
        """

        started = time.monotonic()
        pending, calls = list(events), 0
        while pending:
            remaining = budget - (time.monotonic() - started)
            calls += 1
            pending = list(
                self.drive(
                    "settled",
                    events=pending,
                    wait_s=max(0.0, min(answers.SETTLE_CALL_S, remaining)),
                )["pending"]
            )
            if time.monotonic() - started >= budget:
                break
        report = {
            "what": what,
            "events": len(events),
            "finished": len(events) - len(pending),
            "seconds": round(time.monotonic() - started, 1),
            "calls": calls,
        }
        self.waits.append(report)
        if pending:
            raise HarnessError(
                f"the worker did not finish {len(pending)} of {len(events)} events ({what}) "
                f"within {budget:.0f}s: {pending[:5]}"
            )
        return report

    def await_wake(self, name: str, published: bool) -> dict[str, Any]:
        """After an answer resolved ``name``: the worker's resumed turn, finished."""

        if not published:
            # The inline publication refused (a resume_refused row says why);
            # the wake stays owed to the reconciler, so the worker runs nothing.
            return {"published": False}
        event = answers.resume_event_id(self.approval(name))
        return {
            "published": True,
            **self.await_worker(f"the resumed turn of {name}", [event], answers.WAKE_SETTLE_S),
        }

    def principal_rows(self, *parts: str) -> dict[str, list[dict[str, Any]]]:
        """Each person's canonical ``principals`` rows (type and status), by part."""

        found = self.drive(
            "principals",
            tenant_id=self.tenant_id,
            issuer=self.issuer,
            subjects=[self.subjects[part] for part in parts],
        )["rows"]
        return {part: list(found.get(self.subjects[part]) or []) for part in parts}

    def reactivate(self, part: str) -> dict[str, Any]:
        """One fresh turn from ``part``, then the worker finishing it.

        The ingress applies the person's principal from the directory as it
        stands, so a principal an earlier lookup marked inactive is active again
        once the directory is.
        """

        sent = self.harness.send(
            self.agent, self.people[part], f"{answers.SCENARIO}: reactivate the {part}"
        )
        response = sent.get("response") if isinstance(sent.get("response"), dict) else {}
        if sent.get("http_status") != 200 or not response.get("event_id"):
            raise HarnessError(
                f"the {part}'s reactivation turn was not enqueued: {send_summary(sent)}"
            )
        waited = self.await_worker(
            f"the {part}'s reactivation turn", [str(response["event_id"])], answers.WAKE_SETTLE_S
        )
        return {"http": sent.get("http_status"), "worker": waited}

    def ensure_active(self, *parts: str) -> dict[str, Any]:
        """Each person's principal is an active human, as a creation selecting them needs.

        One that is not (an earlier lookup applied them inactive) is reactivated
        by a fresh turn; one still not active afterwards fails the step.
        """

        rows = self.principal_rows(*parts)
        report: dict[str, Any] = {}
        for part in parts:
            entry: dict[str, Any] = {"rows": rows[part]}
            report[self.people[part]] = entry
            if answers.active_human(rows[part]):
                continue
            entry["reactivation"] = self.reactivate(part)
            entry["rows_after"] = self.principal_rows(part)[part]
            if not answers.active_human(entry["rows_after"]):
                raise StepFailed(
                    f"{self.people[part]}'s principals rows are {entry['rows_after']}, not an "
                    "active human, even after a fresh turn from them (the directory serves "
                    f"them as {self.provider_entry(part)!r}), so a creation selecting them "
                    "would refuse approval_ticket_approver_inactive"
                )
        return report

    def provider_entry(self, part: str) -> Any:
        """The provider's current entry for ``part``, for a failure message."""

        try:
            return (self.provider_roster().get("senders") or {}).get(self.people[part])
        except HarnessError as exc:
            return f"unreadable: {exc}"

    def restore_active(self, part: str) -> dict[str, Any]:
        """``part``'s directory entry as `up` installed it, then their principal active again."""

        return {"roster": self.restore_people(part), "principal": self.ensure_active(part)}

    @contextlib.contextmanager
    def restoring(
        self, seen: dict[str, Any], key: str, action: Callable[[], Any]
    ) -> Iterator[None]:
        """Run ``action`` however the block ends and record its result under ``key``.

        After a failure inside the block the restore is still attempted, its own
        failure is recorded, and the block's failure propagates; after a clean
        block a failed restore fails the step.
        """

        try:
            yield
        except BaseException:
            try:
                seen[key] = action()
            except (HarnessError, StepFailed) as exc:
                seen[key] = {"failed": str(exc)[:400]}
            raise
        seen[key] = action()

    # route and role state -----------------------------------------------------------------

    def role_of(self, *parts: str, extra: tuple[str, ...] = (), accounts: tuple[Any, ...] = ()):
        subjects = [self.subjects[part] for part in parts] + list(extra)
        return answers.role_filter(self.issuer, subjects, accounts=accounts)

    def set_role(self, role_filter: Mapping[str, Any], *, migrate: bool = False) -> dict[str, Any]:
        self.changes["approver_role"] = True
        result = self.drive(
            "role",
            tenant_id=self.tenant_id,
            agent_id=self.agent_id,
            filter=dict(role_filter),
            migrate=migrate,
        )
        return {key: result.get(key) for key in ("action", "version", "legacy_state")}

    def describe_state(self) -> dict[str, Any]:
        state = self.route_state
        registration = state["registration"]
        return {
            "registration": None
            if registration is None
            else {
                "key_id": self.keys[registration["key"]]["key_id"],
                "ceiling_users": None
                if registration["users"] is None
                else [self.people[part] for part in registration["users"]],
            },
            "separation": state["separation"],
            "static_route": state["static"] is not None,
        }

    def write_state(self, api: Api, *, force: bool = False) -> dict[str, Any]:
        """Write the owned routes as ``route_state`` says, pinning the current approver role.

        Skipped when the stored map already means the same; ``force`` writes anyway.
        """

        _, version, _ = self.configuration(api)
        role_version = int(api.ok("GET", f"/agents/{self.agent_id}/access-policy")["version"])
        stored = self.stored_routes()
        state = self.route_state
        registration = None
        if state["registration"] is not None:
            key = self.keys[state["registration"]["key"]]
            users = state["registration"]["users"]
            registration = answers.ticket_registration(
                key_id=key["key_id"],
                public_key=key["public_key"],
                role_version=role_version,
                issuer=self.issuer,
                users=None if users is None else [self.subjects[part] for part in users],
            )
        proposed = answers.owned_routes(
            self.base,
            registration=registration,
            separation=state["separation"],
            static=state["static"],
        )
        described = self.describe_state()
        if not force and answers.owned_summary(stored) == answers.owned_summary(proposed):
            return {"written": False, "configuration_version": version, "state": described}
        body: dict[str, Any] = {"approval_routes": proposed}
        if answers.needs_version(stored, proposed):
            body["approval_routes_expected_version"] = version
        self.changes["routes"] = True
        status, payload = self.patch(api, body)
        if status != 200:
            raise StepFailed(
                f"writing the routes {described} answered {status}: {tickets.detail_code(payload)}"
            )
        _, after, _ = self.configuration(api)
        return {
            "written": True,
            "configuration_version": {"before": version, "after": after},
            "role_version": role_version,
            "state": described,
        }

    def settle(
        self,
        api: Api,
        *,
        role: Mapping[str, Any] | type[KEEP] = KEEP,
        registration: Mapping[str, Any] | None | type[KEEP] = KEEP,
        separation: Mapping[str, Any] | None | type[KEEP] = KEEP,
        static: Mapping[str, Any] | None | type[KEEP] = KEEP,
        force: bool = False,
    ) -> dict[str, Any]:
        """Bring the role and the owned routes to this state; the role first, so the
        registration pins it."""

        report: dict[str, Any] = {}
        if role is not KEEP:
            assert isinstance(role, Mapping)
            report["role"] = self.set_role(role["filter"], migrate=bool(role.get("migrate")))
        for name, value in (
            ("registration", registration),
            ("separation", separation),
            ("static", static),
        ):
            if value is not KEEP:
                self.route_state[name] = None if value is None else copy.deepcopy(dict(value))
        report["routes"] = self.write_state(api, force=force)
        return report

    def settle_base(self, api: Api, **changes: Any) -> dict[str, Any]:
        """The base state: the base role, the first key, no ceiling users, then ``changes``."""

        params: dict[str, Any] = {
            "role": {"filter": self.role_of(*answers.BASE_ROLE)},
            "registration": {"key": "k1", "users": None},
            "separation": None,
            "static": None,
        }
        params.update(changes)
        return self.settle(api, **params)

    def ensure_flag(self, api: Api) -> None:
        flag, version, _ = self.configuration(api)
        if flag is True:
            return
        self.changes["flag"] = True
        status, body = self.patch(
            api,
            {
                "connector_requester_identity": True,
                "connector_requester_identity_expected_version": version,
            },
        )
        if status != 200:
            raise HarnessError(f"enabling the flag answered {status}: {tickets.detail_code(body)}")

    # setup and restore -----------------------------------------------------------------------

    def routes(
        self, registered: Mapping[str, Any] | None = None, *, route: str = tickets.TICKET_ROUTE
    ) -> dict[str, Any]:
        """The owned routes with ``registered`` on the owning route (no exclusion route)."""

        return answers.owned_routes(self.base, registration=registered)

    def write_routes(self, api: Api, routes: Mapping[str, Any]) -> tuple[int, Any]:
        """A route write, versioned when either map names a selector or a separation."""

        body: dict[str, Any] = {"approval_routes": dict(routes)}
        _, version, stored = self.configuration(api)
        if answers.needs_version(stored, routes):
            body["approval_routes_expected_version"] = version
        return self.patch(api, body)

    def prepare(self) -> None:
        """The creation scenario's preconditions and snapshot, plus this run's own; changes
        nothing."""

        super().prepare()
        setup = self.evidence.data["setup"]
        stored = self.original.get("routes")
        check(
            not answers.has_separation(stored),
            f"{self.agent} already stores a route with separation; a previous run did not "
            "restore, so remove it with a versioned route write first",
        )
        self.base = answers.base_routes(stored)
        self.keys["k1"] = self.key
        seed, public = ed25519_pair(self.harness.state.directory)
        replacement = tickets.selector_key(seed, public)
        replacement["key_id"] = tickets.key_id_for(self.stamp) + "-replacement"
        write_private(
            self.harness.state.directory / "approver-answer-selector.json",
            json.dumps(replacement, sort_keys=True).encode(),
        )
        self.keys["k2"] = replacement
        raw = self.drive("role_raw", tenant_id=self.tenant_id, agent_id=self.agent_id)
        self.original["approver_role_raw"] = raw.get("raw")
        served = self.provider_roster()
        for part, address in self.people.items():
            entry = (served.get("senders") or {}).get(address) or {}
            check(
                entry.get("subject") == self.subjects[part],
                f"the provider serves {address} as {entry.get('subject')!r}, not the "
                f"{self.subjects[part]!r} `up` recorded; rerun `up`",
            )
        self.evidence.data["candidate_commit"] = tickets.revision_from_image(
            str(self.evidence.data.get("api_image") or "")
        )
        setup.update(
            people=dict(self.people),
            alias=self.alias,
            base_role=[self.people[part] for part in answers.BASE_ROLE],
            key_ids={name: key["key_id"] for name, key in self.keys.items()},
            static_route=answers.STATIC_ROUTE,
            exempt_group=answers.EXEMPT_GROUP,
            operator_user=answers.OPERATOR_USER,
            roster={"overridden": served.get("overridden"), "generation": served.get("generation")},
        )

    def install(self) -> None:
        """The creation scenario's install (with the base role), then the registration."""

        setup = self.evidence.data["setup"]
        leftover = self.provider_roster().get("overridden") or []
        if leftover:
            # Only this scenario overrides the roster; an interrupted run left these.
            setup["roster_reset_at_start"] = self.roster_change({"reset": True})
        super().install()
        with self.harness.api() as api:
            self.ensure_flag(api)
            setup["registration"] = self.write_state(api)

    def send_turns(self) -> None:
        """One approver turn (a canonical principals row), then one requester turn per approval.

        The requester's turns share one conversation, so the worker serves them
        from one sandbox, one after another; the creations read only each turn's
        ingress capsule. No step starts until the worker has finished all of
        them, since each turn's checks move the tenant's lifecycle generation.
        """

        sends = [("approver", A, None)] + [
            (f"r{index:02d}", R, "requester") for index in range(1, answers.TURN_COUNT + 1)
        ]
        conversation: str | None = None
        for label, part, thread in sends:
            sent = self.harness.send(
                self.agent,
                self.people[part],
                f"{answers.SCENARIO}: {label} turn",
                conversation=conversation if thread else None,
            )
            response = sent.get("response") if isinstance(sent.get("response"), dict) else {}
            if (
                sent.get("http_status") != 200
                or not response.get("event_id")
                or not response.get("stream_id")
            ):
                raise HarnessError(f"the {label} turn was not enqueued: {send_summary(sent)}")
            if thread:
                conversation = conversation or str(sent["conversation_id"])
            self.turns[label] = {
                "event_id": str(response["event_id"]),
                "stream_id": str(response["stream_id"]),
                "sender": self.people[part],
            }
        captured = self.drive(
            "capture",
            turns={
                label: {"stream_id": turn["stream_id"], "event_id": turn["event_id"]}
                for label, turn in self.turns.items()
            },
        )["turns"]
        for label, part, _ in sends:
            facts = captured[label]
            wanted = {"issuer": self.issuer, "subject": self.subjects[part]}
            if (
                facts.get("canonical") != wanted
                or not facts.get("principal_present")
                or (facts.get("scope") or {}).get("agent_id") != self.agent_id
            ):
                raise HarnessError(
                    f"the {label} turn's capsule does not name {self.people[part]} as a verified "
                    f"human on {self.agent}: {facts}"
                )
            self.turns[label]["capsule_facts"] = facts
        self.free_turns = [label for label, _, thread in sends if thread]
        principals = {}
        for part, rows in self.principal_rows(R, A).items():
            principals[self.people[part]] = rows
            if not answers.active_human(rows):
                raise HarnessError(f"{self.people[part]} has no active human principals row")
        self.evidence.data["turns"] = {
            label: {key: turn.get(key) for key in ("event_id", "sender", "capsule_facts")}
            for label, turn in self.turns.items()
        }
        self.evidence.data["principals"] = principals
        self.evidence.data["turns_settled"] = self.await_worker(
            "the run's turns",
            [turn["event_id"] for turn in self.turns.values()],
            answers.TURNS_SETTLE_S,
        )

    def restore(self) -> None:
        """Put back what ``changes`` records, roster first; a failure fails the run."""

        report: dict[str, Any] = {"done": [], "failed": []}
        self.evidence.data["restore"] = report

        def attempt(name: str, action: Callable[[], Any]) -> None:
            try:
                report["done"].append({"action": name, "result": action()})
            except (HarnessError, StepFailed, KeyError, TypeError, ValueError) as exc:
                report["failed"].append({"action": name, "error": str(exc)[:400]})

        changes = self.changes
        if changes["roster"]:
            attempt("roster", self._restore_roster)
        if changes["routes"] or changes["flag"] or changes["deployments"] or changes["ended"]:
            try:
                with self.harness.api() as api:
                    attempt(
                        "remove the registration and the exclusion routes",
                        lambda: self._drop_registration(api),
                    )
                    if changes["flag"]:
                        attempt("requester flag", lambda: self._restore_flag(api))
                    if changes["deployments"] or changes["ended"]:
                        attempt("deployments", lambda: self._restore_deployments(api))
                    if changes["routes"]:
                        attempt("approval routes", lambda: self._restore_routes(api))
                    attempt("verify", lambda: self._verify_restored(api))
            except HarnessError as exc:
                report["failed"].append({"action": "port-forward", "error": str(exc)[:400]})
        if changes["approver_role"]:
            attempt("approver role", self._restore_role)
        report["left"] = (
            f"approvals this run created stay pending until they expire "
            f"({answers.APPROVAL_TTL_S} s); answered ones stay resolved with their resume wakes "
            "in the runs stream; the run's agent versions stay, undeployed"
        )
        if report["failed"]:
            failed = "; ".join(f"{item['action']}: {item['error']}" for item in report["failed"])
            self.evidence.data.setdefault("aborted", f"restore incomplete: {failed}")

    def _restore_roster(self) -> dict[str, Any]:
        self.roster_change({"reset": True})
        view = self.provider_roster()
        if view.get("overridden"):
            raise HarnessError(f"the provider still overrides {view['overridden']}")
        return {
            "overridden": [],
            "generation": view.get("generation"),
            "matches_up_roster": view.get("senders") == (self.roster_data.get("senders") or {}),
        }

    def _drop_registration(self, api: Api) -> str:
        _, version, stored = self.configuration(api)
        if not (
            tickets.has_registration(stored)
            or answers.has_separation(stored)
            or answers.STATIC_ROUTE in stored
        ):
            return "no registration or exclusion route stored"
        status, body = self.patch(
            api, {"approval_routes": self.routes(), "approval_routes_expected_version": version}
        )
        if status != 200:
            raise HarnessError(f"answered {status}: {tickets.detail_code(body)}")
        return f"removed at version {version}"

    def _verify_restored(self, api: Api) -> dict[str, Any]:
        result = super()._verify_restored(api)
        stored = self.configuration(api)[2]
        if answers.has_separation(stored) or answers.STATIC_ROUTE in stored:
            raise HarnessError("not restored: a route still carries separation")
        return {**result, "no_separation": True}

    def _restore_role(self) -> dict[str, Any]:
        """The approver role exactly as stored before the run, legacy state included."""

        original = self.original["approver_role"]
        result = self.drive(
            "role_restore",
            tenant_id=self.tenant_id,
            agent_id=self.agent_id,
            raw=self.original.get("approver_role_raw"),
            configured=bool(original.get("configured")),
        )
        now = self.drive("approver_role", tenant_id=self.tenant_id, agent_id=self.agent_id)
        wrong = [
            key
            for key in ("configured", "had_role", "legacy_state", "filter")
            if now.get(key) != original.get(key)
        ]
        if wrong:
            raise HarnessError(f"the approver role differs from the original in {wrong}")
        return {"action": result.get("action"), "version": now.get("version")}

    # approvals, answers and resumes ------------------------------------------------------------

    def create(self, seen: dict[str, Any], step_id: str) -> dict[str, str]:
        """This step's approvals (``answers.APPROVALS``), one requester turn each.

        Every person a dynamic approval selects is first made sure to be an
        active human principal, since creation refuses anyone else. A creation
        that met a concurrent identity change (503) is sent again.
        """

        selected = sorted(
            {part for _, _, parts in answers.APPROVALS[step_id] for part in parts},
            key=answers.ROLES.index,
        )
        if selected:
            seen["principals"] = self.ensure_active(*selected)
        pending: dict[str, dict[str, Any]] = {}
        for name, kind, parts in answers.APPROVALS[step_id]:
            if not self.free_turns:
                raise HarnessError("no requester turn is left for another approval")
            pending[name] = {"kind": kind, "parts": parts, "turn": self.free_turns.pop(0)}
            self.nonces.setdefault(name, tickets.new_nonce())
        report: dict[str, Any] = {}
        seen["created"] = report
        made: dict[str, str] = {}
        todo = dict(pending)
        for attempt in range(1, answers.ATTEMPTS + 1):
            result = self.drive(
                "create",
                agent_id=self.agent_id,
                key={"seed_hex": self.keys["k1"]["seed_hex"], "key_id": self.keys["k1"]["key_id"]},
                registered={
                    "connector": tickets.CONNECTOR,
                    "tool": tickets.TOOL,
                    "route": tickets.TICKET_ROUTE,
                },
                turns={
                    spec["turn"]: {
                        "stream_id": self.turns[spec["turn"]]["stream_id"],
                        "event_id": self.turns[spec["turn"]]["event_id"],
                    }
                    for spec in todo.values()
                },
                batches=self.batches(todo),
                ttl_s=answers.APPROVAL_TTL_S,
            )
            answered = {item["name"]: item["results"][0] for item in result["cases"]}
            retry: dict[str, dict[str, Any]] = {}
            for name, spec in todo.items():
                answer = answered.get(name) or {}
                report[name] = {
                    "kind": spec["kind"],
                    "turn": spec["turn"],
                    "selected": [self.people[part] for part in spec["parts"]],
                    "http": answer.get("http"),
                    "detail": answer.get("detail"),
                    "attempts": attempt,
                }
                if answer.get("http") == 201 and answer.get("id"):
                    made[name] = str(answer["id"])
                    report[name].update(id=made[name], row=result["rows"].get(made[name]))
                elif answer.get("http") == 503 and answer.get("detail") == tickets.UNAVAILABLE:
                    retry[name] = spec
            todo = retry
            if not todo:
                break
        failed = {
            name: (item["http"], item["detail"])
            for name, item in report.items()
            if name not in made
        }
        check(not failed, f"creating {failed} did not answer 201")
        self.approvals.update(made)
        return made

    def batches(self, specs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
        """One creation batch per selected set; a static approval carries no ticket."""

        grouped: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}
        for name, spec in specs.items():
            batch = grouped.setdefault(
                (spec["kind"], tuple(spec["parts"])),
                {
                    "approvers": [
                        {"issuer": self.issuer, "subject": self.subjects[part]}
                        for part in spec["parts"]
                    ],
                    "nonces": {},
                    "cases": [],
                },
            )
            if spec["kind"] == answers.DYNAMIC:
                batch["nonces"][name] = self.nonces[name]
                batch["cases"].append(
                    tickets.case(
                        name,
                        turn=spec["turn"],
                        ticket=tickets.ticket_spec(name),
                        expected=tickets.expect(201, duplicate=False),
                    )
                )
            else:
                batch["cases"].append(
                    tickets.case(
                        name,
                        turn=spec["turn"],
                        route=answers.STATIC_ROUTE,
                        tool=tickets.live_tool(tickets.CONNECTOR, tickets.OTHER_TOOL),
                        expected=tickets.expect(201, duplicate=False),
                    )
                )
        return list(grouped.values())

    def answer(
        self,
        name: str,
        part: str,
        *,
        decision: str = "approved",
        address: str | None = None,
        retry: bool = True,
    ) -> dict[str, Any]:
        """One email answer from ``part`` (or ``address``), forwarded as the mail adapter does.

        An answer that resolved the row returns only once the worker has
        finished the turn it resumed from the published wake, so nothing after
        it meets that turn's identity checks.
        """

        approval_id = self.approval(name)
        sender = (address or self.people[part]).lower()
        result: dict[str, Any] = {}
        for attempt in range(1, answers.ATTEMPTS + 1):
            result = self.drive(
                "answer",
                approval_id=approval_id,
                binding_id=self.binding_id,
                receiver=self.receiver,
                sender=sender,
                decision=decision,
                note=answers.NOTE,
                adapter=ADAPTER,
            )
            result["attempts"] = attempt
            if not retry or not answers.transient(result):
                break
        if result.get("http") == 200 and decision == "approved":
            self.approved.add(name)
        if answers.publishes_wake(result):
            result["worker"] = self.await_wake(name, bool(result.get("resumed")))
        return {"approval": name, "actor": part, "address": sender, "decision": decision, **result}

    def operator_answer(self, name: str) -> dict[str, Any]:
        result = self.drive(
            "answer",
            approval_id=self.approval(name),
            operator=answers.OPERATOR_USER,
            decision="approved",
            note=answers.NOTE,
        )
        return {"approval": name, "actor": "operator", "subject": answers.OPERATOR_USER, **result}

    def resume(self, name: str, *, retry: bool = True) -> dict[str, Any]:
        """Both current checks of the wake the API published for ``name``.

        An ``unavailable`` decision is checked again (a concurrent identity
        change), unless ``retry`` is false: a check meant to meet an outage.
        """

        check(
            name in self.approved,
            f"{name} was never approved (an earlier step failed), so no wake exists to check",
        )
        result: dict[str, Any] = {}
        for attempt in range(1, answers.ATTEMPTS + 1):
            result = self.drive("resume", approval_id=self.approval(name))
            result["attempts"] = attempt
            if not retry or not answers.resume_transient(result):
                break
        return {"approval": name, **result}

    def expect_answer(self, seen: dict[str, Any], key: str, result: Mapping[str, Any], **want: Any):
        seen[key] = dict(result)
        problems = answers.judge_answer(result, **want)
        check(not problems, f"{key}: " + "; ".join(problems))
        action = want["action"]
        return answers.newest(result.get("audit") or [], action)

    def expect_resume(
        self, seen: dict[str, Any], key: str, name: str, decision: str, *, retry: bool = True
    ) -> None:
        result = self.resume(name, retry=retry)
        seen[key] = result
        problems = answers.judge_resume(result, decision=decision)
        check(not problems, f"{key}: " + "; ".join(problems))

    def refused(
        self, seen: dict[str, Any], key: str, result: Mapping[str, Any], code: str | None = None
    ) -> dict[str, Any] | None:
        """A refused answer: 403, the row still pending, one denied audit row naming ``code``."""

        return self.expect_answer(
            seen, key, result, http=403, row_after="pending", action=answers.DENIED, code=code
        )

    def allowed(
        self, seen: dict[str, Any], key: str, result: Mapping[str, Any], row_after: str = "approved"
    ) -> dict[str, Any] | None:
        return self.expect_answer(
            seen, key, result, http=200, row_after=row_after, action=answers.RESOLVED
        )

    def fact(self, row: Mapping[str, Any] | None, name: str, wanted: Any, what: str) -> None:
        found = answers.evidence_fact(row, name)
        check(
            found is wanted,
            f"{what}: the audit row's selection evidence reads {name}={found!r}, not {wanted!r}",
        )

    # item 3: decision 5 ----------------------------------------------------------------------

    def answer_selected_approver_allowed(self) -> None:
        step = "answer-selected-approver-allowed"
        with (
            self.evidence.step(
                step,
                "the selected approver approves: 200, the row approved, the resolved audit "
                "carries the selection, and the published resume passes both current checks",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            resolved = self.allowed(seen, "approver", self.answer("x1", A))
            seen["selection"] = answers.selection_facts(resolved)
            check(bool(seen["selection"]), "the resolved audit row carries no selection evidence")
            self.fact(resolved, "equality", False, "the approver is not the requester")
            self.expect_resume(seen, "resume", "x1", "continue")
            check(seen["resume"].get("wake_status") == "approved", "the wake is not approved")

    def answer_unselected_refused(self) -> None:
        step = "answer-unselected-refused"
        with (
            self.evidence.step(
                step,
                "a second approver inside the ceiling but not selected is refused 403 "
                f"{answers.NOT_SELECTED}; the row stays pending",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            self.refused(seen, "second", self.answer("x2", B), answers.NOT_SELECTED)

    def selection_fixed_after_source_change(self) -> None:
        with (
            self.evidence.step(
                "selection-fixed-after-source-change",
                "after the role, the registration ceiling and the roster change, the stored "
                "selection is unchanged: the unselected approver is still refused and the "
                "selected one still approves",
            ) as seen,
            self.harness.api() as api,
        ):
            before = self.rows("x2")["x2"]
            seen["row_before"] = before
            check(before is not None and before["status"] == "pending", "x2 is not pending")
            pinned = self.readback(api)
            try:
                seen["source_change"] = self.settle(
                    api,
                    role={
                        "filter": self.role_of(
                            *answers.BASE_ROLE, extra=(answers.UNROSTERED_SUBJECT,)
                        )
                    },
                    registration={"key": "k1", "users": (A, B)},
                )
                seen["roster"] = self.roster_change(
                    {
                        "upsert": {
                            self.people[part]: answers.with_groups(
                                self.entry(part), add=[answers.CHANGED_GROUP]
                            )
                            for part in (A, B)
                        }
                    }
                )
                rewritten = self.readback(api)
                seen["readback"] = {"before": pinned, "after": rewritten}
                check(
                    rewritten["approver_role_version"] != pinned["approver_role_version"]
                    and answers.UNROSTERED_SUBJECT in rewritten["pinned_subjects"]
                    and sorted(rewritten["users"]) == sorted([self.subjects[A], self.subjects[B]]),
                    "the registration was not re-pinned to the rewritten role with its new users",
                )
                after = self.rows("x2")["x2"]
                seen["row_after_change"] = after
                check(
                    after["selected_sha256"] == before["selected_sha256"]
                    and after["snapshot_sha256"] == before["snapshot_sha256"],
                    "the stored selection changed with its sources",
                )
                self.refused(seen, "second", self.answer("x2", B), answers.NOT_SELECTED)
                self.allowed(seen, "approver", self.answer("x2", A))
            finally:
                seen["roster_restored"] = self.restore_people(A, B)

    def ceiling_member_revoked(self) -> None:
        step = "ceiling-member-revoked"
        with (
            self.evidence.step(
                step,
                "with the selected approver dropped from ceiling.users, their answer is refused "
                f"{answers.OUTSIDE_CEILING} and an earlier approval of theirs refuses at resume",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            seen["revoke"] = self.settle(api, registration={"key": "k1", "users": (R, B, E)})
            check(self.subjects[A] not in self.readback(api)["users"], "A is still in the ceiling")
            self.refused(seen, "approver", self.answer("x3", A), answers.OUTSIDE_CEILING)
            self.expect_resume(seen, "resume", "x1", "refuse")

    def key_revoked(self) -> None:
        step = "key-revoked"
        with (
            self.evidence.step(
                step,
                "with the registration replaced under a new key id, the answer is refused "
                f"{answers.KEY_REVOKED} and an earlier approval refuses at resume",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            seen["replace"] = self.settle(api, registration={"key": "k2", "users": None})
            check(
                self.readback(api)["key_id"] == self.keys["k2"]["key_id"],
                "the registration does not name the replacement key id",
            )
            self.refused(seen, "approver", self.answer("x4", A), answers.KEY_REVOKED)
            self.expect_resume(seen, "resume", "x1", "refuse")

    def registration_removed(self) -> None:
        step = "registration-removed"
        with (
            self.evidence.step(
                step,
                "with the registration removed from the route, the answer is refused and an "
                "earlier approval refuses at resume",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            seen["remove"] = self.settle(api, registration=None, separation=None)
            readback = self.readback(api)
            check(
                readback["bound"] and not readback["registered"],
                "the route is not bound without a registration",
            )
            denied = self.refused(seen, "approver", self.answer("x5", A))
            seen["refusal_code"] = answers.refusal_code(denied)
            self.expect_resume(seen, "resume", "x1", "refuse")

    def canonical_inactive(self) -> None:
        step = "canonical-inactive"
        with (
            self.evidence.step(
                step,
                "with the selected approver deactivated in the directory, their answer is "
                "refused and an earlier approval refuses at resume; reactivated, it continues",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            # The refused answer applies A's principal as disabled, and nothing
            # but a fresh lookup of A applies it active again; the creations
            # after this step select A. So however the block ends, A's entry is
            # restored and a fresh turn from A reactivates the principal.
            with self.restoring(seen, "restored", lambda: self.restore_active(A)):
                seen["roster"] = self.roster_change(
                    {"upsert": {self.people[A]: answers.deactivated(self.entry(A))}}
                )
                denied = self.refused(seen, "approver", self.answer("x6", A))
                seen["refusal_code"] = answers.refusal_code(denied)
                seen["approver_principal_inactive"] = self.principal_rows(A)[A]
                self.expect_resume(seen, "resume", "x1", "refuse")
            self.expect_resume(seen, "resume_after_reactivation", "x1", "continue")

    def migrated_role_no_legacy_block(self) -> None:
        step = "migrated-role-no-legacy-block"
        with (
            self.evidence.step(
                step,
                "with the approver role migrated and no approvers block on the route, the "
                "selected approver approves and the unselected one is refused",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(
                api, role={"filter": self.role_of(*answers.BASE_ROLE), "migrate": True}
            )
            role = self.drive("approver_role", tenant_id=self.tenant_id, agent_id=self.agent_id)
            readback = self.readback(api)
            seen["role"] = {key: role.get(key) for key in ("version", "legacy_state")}
            seen["readback"] = readback
            check(role.get("legacy_state") == "migrated", "the approver role is not migrated")
            check(not readback["has_approvers_block"], "the route carries an approvers block")
            self.create(seen, step)
            self.refused(seen, "second", self.answer("x7", B), answers.NOT_SELECTED)
            self.allowed(seen, "approver", self.answer("x7", A))

    def raw_account_not_selected(self) -> None:
        step = "raw-account-not-selected"
        with (
            self.evidence.step(
                step,
                "a raw surface-account role entry admits a resolved, unselected person, who is "
                f"still refused {answers.NOT_SELECTED}",
            ) as seen,
            self.harness.api() as api,
        ):
            account = answers.surface_account(self.namespace, self.people[D])
            seen["settle"] = self.settle_base(
                api,
                role={"filter": self.role_of(*answers.BASE_ROLE, accounts=(account,))},
            )
            readback = self.readback(api)
            seen["readback"] = readback
            check(
                self.subjects[D] not in readback["pinned_subjects"]
                and readback["pinned_accounts"] == 1,
                "the role does not admit the outsider by a raw account entry alone",
            )
            self.create(seen, step)
            denied = self.refused(seen, "outsider", self.answer("x8", D))
            # Which check refuses first is the API's choice; either names the stored
            # selection or the canonical ceiling, never the raw account.
            seen["refusal_code"] = answers.refusal_code(denied)
            check(
                any(
                    answers.names_code(denied, code)
                    for code in (answers.NOT_SELECTED, answers.OUTSIDE_CEILING)
                ),
                f"the refusal names {seen['refusal_code']!r}, not {answers.NOT_SELECTED} or "
                f"{answers.OUTSIDE_CEILING}",
            )

    def ceiling_widened(self) -> None:
        step = "ceiling-widened"
        with (
            self.evidence.step(
                step,
                "with the role re-pinned to include an outsider, the outsider is refused and the "
                "selected approver approves; revoking the selected approver then still refuses",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            seen["widen"] = self.settle(
                api,
                role={"filter": self.role_of(*answers.BASE_ROLE, D)},
                registration={"key": "k1", "users": None},
            )
            check(
                self.subjects[D] in self.readback(api)["pinned_subjects"],
                "the re-pinned ceiling does not name the outsider",
            )
            self.refused(seen, "outsider", self.answer("x9", D), answers.NOT_SELECTED)
            self.allowed(seen, "approver", self.answer("x9", A))
            seen["revoke"] = self.settle(api, registration={"key": "k1", "users": (R, B, D, E)})
            self.refused(seen, "revoked", self.answer("x10", A), answers.OUTSIDE_CEILING)

    # item 4: decision 6 ------------------------------------------------------------------------

    def separation_default_on_omission(self) -> None:
        with (
            self.evidence.step(
                "separation-default-on-omission",
                "a registration written without separation reads back exclude_requester true",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api, force=True)
            written = seen["settle"]["routes"]
            check(written.get("written") is True, "the registration was not written again")
            readback = self.readback(api)
            stored = (self.stored_routes().get(tickets.TICKET_ROUTE) or {}).get("separation")
            seen["readback"] = readback["separation"]
            seen["stored"] = stored
            for name, value in (("read back", readback["separation"]), ("stored", stored)):
                check(
                    isinstance(value, dict)
                    and value.get("exclude_requester") is True
                    and not value.get("exempt_group"),
                    f"the {name} separation is {value!r}, not exclude_requester true",
                )

    def separation_requester_excluded_alias(self) -> None:
        step = "separation-requester-excluded-alias"
        with (
            self.evidence.step(
                step,
                "the requester, selected, is refused from their own address and from a second "
                f"address of the same person ({answers.REQUESTER_EXCLUDED}); the other approves",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            try:
                seen["roster"] = self.roster_change({"upsert": {self.alias: self.entry(R)}})
                self.create(seen, step)
                for key, address in (("requester", None), ("requester_alias", self.alias)):
                    denied = self.refused(
                        seen, key, self.answer("y1", R, address=address), answers.REQUESTER_EXCLUDED
                    )
                    self.fact(denied, "equality", True, f"{key} is the requester")
                resolved = self.allowed(seen, "approver", self.answer("y1", A))
                self.fact(resolved, "equality", False, "the approver is not the requester")
            finally:
                seen["roster_restored"] = self.roster_change({"delete": [self.alias]})

    def separation_requester_rejection(self) -> None:
        step = "separation-requester-rejection"
        with (
            self.evidence.step(
                step,
                "on the exclusion route the requester may reject their own request: 200 rejected",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            self.create(seen, step)
            resolved = self.allowed(
                seen, "requester", self.answer("y2", R, decision="rejected"), row_after="rejected"
            )
            check(
                (resolved or {}).get("decision") == "rejected",
                "the resolved audit row does not record a rejection",
            )

    def new_separation_events(self, before_ids: set[str]) -> list[dict[str, Any]]:
        """The ``approval_separation.changed`` events for the owning route since ``before_ids``."""

        return [
            event
            for event in self.identity_events(answers.SEPARATION_CHANGED)
            if event["id"] not in before_ids
            and (event.get("details") or {}).get("route") == tickets.TICKET_ROUTE
        ]

    def separation_change_event(
        self, seen: dict[str, Any], before_ids: set[str], versions: tuple[Any, Any]
    ) -> dict[str, Any]:
        """The one new ``approval_separation.changed`` event for the owning route."""

        events = self.new_separation_events(before_ids)
        seen["identity_audit"] = events
        check(len(events) == 1, f"{len(events)} new {answers.SEPARATION_CHANGED} events")
        change = answers.separation_change(events[0]["details"])
        seen["change"] = change
        check(events[0].get("actor") == "operator", "the change is not audited as the operator's")
        check(
            (change["old_configuration_version"], change["new_configuration_version"]) == versions,
            f"the event names versions {change['old_configuration_version']} -> "
            f"{change['new_configuration_version']}, not {versions[0]} -> {versions[1]}",
        )
        return {"event": events[0], **change}

    def exempt_separation(self) -> dict[str, Any]:
        return answers.separation(True, exempt_group=answers.EXEMPT_GROUP, issuer=self.issuer)

    def separation_exemption_member(self) -> None:
        step = "separation-exemption-member"
        with (
            self.evidence.step(
                step,
                "the operator writes an exempt group that reads back; the requester in it "
                "approves their own request with exemption_used, and the resume passes both "
                "checks",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api)
            before = {event["id"] for event in self.identity_events(answers.SEPARATION_CHANGED)}
            version = self.configuration(api)[1]
            seen["exempt"] = self.settle_base(api, separation=self.exempt_separation())
            after = self.configuration(api)[1]
            # Recorded, not required: decision 6 pins the audit at opt-out (the
            # operator opt-out step); whether adding an exemption alone is audited
            # is the API's call.
            seen["identity_audit"] = {
                "configuration_version": {"before": version, "after": after},
                "events": self.new_separation_events(before),
            }
            readback = self.readback(api)["separation"] or {}
            seen["readback"] = readback
            check(
                (readback.get("exempt_group") or {}).get("id") == answers.EXEMPT_GROUP,
                "the exempt group does not read back",
            )
            seen["roster"] = self.roster_change(
                {
                    "upsert": {
                        self.people[R]: answers.with_groups(
                            self.entry(R), add=[answers.EXEMPT_GROUP]
                        )
                    }
                }
            )
            self.create(seen, step)
            resolved = self.allowed(seen, "requester", self.answer("y3", R))
            self.fact(resolved, "exemption_used", True, "the requester is exempt")
            self.fact(resolved, "equality", True, "the requester answered their own request")
            self.expect_resume(seen, "resume", "y3", "continue")

    def separation_exemption_removed(self) -> None:
        step = "separation-exemption-removed"
        with (
            self.evidence.step(
                step,
                "with the requester out of the exempt group, the exempt approval refuses at "
                f"resume and a new self-answer is refused {answers.REQUESTER_EXCLUDED}",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api, separation=self.exempt_separation())
            seen["roster"] = self.restore_people(R)
            self.expect_resume(seen, "resume", "y3", "refuse")
            self.create(seen, step)
            self.refused(seen, "requester", self.answer("y4", R), answers.REQUESTER_EXCLUDED)

    def separation_exemption_unavailable(self) -> None:
        step = "separation-exemption-unavailable"
        with (
            self.evidence.step(
                step,
                "with the requester back in the exempt group and their lookup unavailable, their "
                "self-answer is 503 and audited denied with status 503, and both current checks "
                "of the exempt approval's resume answer unavailable with resume_refused audit "
                "rows; with the lookup restored both continue",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api, separation=self.exempt_separation())
            exempt = {
                self.people[R]: answers.with_groups(self.entry(R), add=[answers.EXEMPT_GROUP])
            }
            with self.restoring(seen, "roster_restored", lambda: self.restore_people(R)):
                seen["roster_exempt"] = self.roster_change({"upsert": exempt})
                self.create(seen, step)
                with self.restoring(
                    seen, "lookup_restored", lambda: self.roster_change({"upsert": exempt})
                ):
                    seen["roster_unavailable"] = self.roster_change(
                        {"upsert": {self.people[R]: answers.unavailable(self.entry(R))}}
                    )
                    # Neither is retried: the outage is the point.
                    self.expect_answer(
                        seen,
                        "requester",
                        self.answer("y5", R, retry=False),
                        http=503,
                        row_after="pending",
                        action=answers.DENIED,
                        status=503,
                    )
                    self.expect_resume(seen, "resume", "y3", "unavailable", retry=False)
                # Exempt and resolvable again, the step 14 self-approval continues.
                self.expect_resume(seen, "resume_restored", "y3", "continue")

    def separation_exempt_unselected(self) -> None:
        step = "separation-exempt-unselected"
        with (
            self.evidence.step(
                step,
                f"an exempt group member who is not selected is refused {answers.NOT_SELECTED}",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api, separation=self.exempt_separation())
            try:
                seen["roster"] = self.roster_change(
                    {
                        "upsert": {
                            self.people[E]: answers.with_groups(
                                self.entry(E), add=[answers.EXEMPT_GROUP]
                            )
                        }
                    }
                )
                self.create(seen, step)
                self.refused(seen, "exempt", self.answer("y6", E), answers.NOT_SELECTED)
            finally:
                seen["roster_restored"] = self.restore_people(E)

    def separation_operator_opt_out(self) -> None:
        step = "separation-operator-opt-out"
        with (
            self.evidence.step(
                step,
                "an operator write of exclude_requester false is audited with both values and "
                "versions; the requester's self-approval then passes, recorded as such",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api, separation=self.exempt_separation())
            before = {event["id"] for event in self.identity_events(answers.SEPARATION_CHANGED)}
            version = self.configuration(api)[1]
            seen["opt_out"] = self.settle_base(api, separation=answers.separation(False))
            after = self.configuration(api)[1]
            change = self.separation_change_event(seen, before, (version, after))
            check(
                change["old_exclude_requester"] is True
                and change["new_exclude_requester"] is False,
                f"the event records {change['old_exclude_requester']!r} -> "
                f"{change['new_exclude_requester']!r}, not true -> false",
            )
            readback = self.readback(api)["separation"] or {}
            seen["readback"] = readback
            check(readback.get("exclude_requester") is False, "the opt-out does not read back")
            self.create(seen, step)
            resolved = self.allowed(seen, "requester", self.answer("y7", R))
            self.fact(resolved, "exclusion", False, "exclusion is off")
            self.fact(resolved, "equality", True, "the requester answered their own request")

    def separation_agent_owned_opt_out_refused(self) -> None:
        with (
            self.evidence.step(
                "separation-agent-owned-opt-out-refused",
                "an agent-owned access-policy write carrying separation is refused 422 and the "
                "stored separation is unchanged",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(api, separation=answers.separation(True))
            stored = (self.stored_routes().get(tickets.TICKET_ROUTE) or {}).get("separation")
            policy = api.ok("GET", f"/agents/{self.agent_id}/access-policy")
            base = {
                "invocation_team_id": policy.get("invocation_team_id"),
                "approver_team_id": policy.get("approver_team_id"),
                "expected_version": policy.get("version"),
            }
            variants = {
                "separation": {**base, "separation": {"exclude_requester": False}},
                "approval_routes": {
                    **base,
                    "approval_routes": {
                        tickets.TICKET_ROUTE: {"separation": {"exclude_requester": False}}
                    },
                },
            }
            writes = {}
            for name, body in variants.items():
                status, payload = api.call("PUT", f"/agents/{self.agent_id}/access-policy", body)
                writes[name] = {"status": status, "detail": tickets.detail_code(payload)}
            seen["writes"] = writes
            after = (self.stored_routes().get(tickets.TICKET_ROUTE) or {}).get("separation")
            policy_after = api.ok("GET", f"/agents/{self.agent_id}/access-policy")
            seen["separation"] = {"before": stored, "after": after}
            seen["policy_version"] = {
                "before": policy.get("version"),
                "after": policy_after.get("version"),
            }
            check(
                all(item["status"] == 422 for item in writes.values()),
                f"an agent-owned write was not refused 422: {writes}",
            )
            check(
                after == stored
                and isinstance(after, dict)
                and after.get("exclude_requester") is True,
                "the stored separation changed",
            )
            check(
                policy_after.get("version") == policy.get("version"),
                "the refused write changed the access policy version",
            )

    def static_binding(self) -> dict[str, Any]:
        return answers.static_binding(
            emails=[self.people[R], self.people[A]], operators=[answers.OPERATOR_USER]
        )

    def static_exclusion_route(self) -> None:
        step = "static-exclusion-route"
        with (
            self.evidence.step(
                step,
                "on a route without a selector that excludes the requester, the requester is "
                f"refused {answers.REQUESTER_EXCLUDED} on an ordinary approval and another "
                "listed approver approves",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(
                api, separation=answers.separation(True), static=self.static_binding()
            )
            readback = self.readback(api, answers.STATIC_ROUTE)
            seen["readback"] = readback
            check(
                readback["bound"]
                and not readback["registered"]
                and (readback["separation"] or {}).get("exclude_requester") is True,
                "the static route does not read back bound, without a selector, excluding the "
                "requester",
            )
            made = self.create(seen, step)
            row = self.rows("z1")["z1"]
            check(
                (row or {}).get("selection_mode") == "ordinary"
                and (row or {}).get("route") == answers.STATIC_ROUTE,
                f"z1 is not an ordinary approval on the static route: {row}",
            )
            seen["approval"] = made
            denied = self.refused(
                seen, "requester", self.answer("z1", R), answers.REQUESTER_EXCLUDED
            )
            self.fact(denied, "equality", True, "the requester answered their own request")
            self.allowed(seen, "approver", self.answer("z1", A))

    def operator_principal_refused(self) -> None:
        step = "operator-principal-refused"
        with (
            self.evidence.step(
                step,
                "an operator principal is refused 403 on a dynamic approval and on a static "
                "exclusion approval whose route lists that operator's user; the rows stay pending",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["settle"] = self.settle_base(
                api, separation=answers.separation(True), static=self.static_binding()
            )
            self.create(seen, step)
            for key, name in (("dynamic", "w1"), ("static", "z2")):
                self.expect_answer(
                    seen,
                    key,
                    self.operator_answer(name),
                    http=403,
                    row_after="pending",
                    action=answers.DENIED,
                    code=answers.OPERATOR_REFUSED,
                    principal_kind="operator",
                )


def run(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0271 evidence items 3 and 4 for slice 0271b, on one agent of a live install.

    Setup, changing nothing: the creation scenario's preconditions and snapshot,
    the five people, a second selector key and the stored approver role. Then
    the creation scenario's install with the base approver role, the
    registration, one approver turn and one requester turn per approval, and a
    wait until the worker has finished them all. Every step settles the role and
    the owned routes it needs, creates its approvals through
    ``/v1/internal/identity/approvals`` once each selected person is an active
    human principal, changes the directory through the provider's roster
    override, and answers through ``/approvals/{id}/resolve``, waiting after a
    resolving answer for the worker's resumed turn. ``finally`` resets the
    roster, removes the registration and the exclusion route, and restores the
    flag, the deployments, the routes and the approver role, then checks them;
    a failed restore fails the run.
    """

    scenario = AnswerScenario(harness, args, evidence)
    evidence.data.update(
        agent=scenario.agent,
        requester=scenario.people[R],
        approver=scenario.people[A],
        steps_required=list(answers.STEP_IDS),
    )
    scenario.prepare()
    try:
        scenario.install()
        scenario.send_turns()
        scenario.answer_selected_approver_allowed()
        scenario.answer_unselected_refused()
        scenario.selection_fixed_after_source_change()
        scenario.ceiling_member_revoked()
        scenario.key_revoked()
        scenario.registration_removed()
        scenario.canonical_inactive()
        scenario.migrated_role_no_legacy_block()
        scenario.raw_account_not_selected()
        scenario.ceiling_widened()
        scenario.separation_default_on_omission()
        scenario.separation_requester_excluded_alias()
        scenario.separation_requester_rejection()
        scenario.separation_exemption_member()
        scenario.separation_exemption_removed()
        scenario.separation_exemption_unavailable()
        scenario.separation_exempt_unselected()
        scenario.separation_operator_opt_out()
        scenario.separation_agent_owned_opt_out_refused()
        scenario.static_exclusion_route()
        scenario.operator_principal_refused()
    finally:
        scenario.restore()
