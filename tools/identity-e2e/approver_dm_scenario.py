"""The ``approver-dm`` scenario: ADR 0272 evidence items 1, 3 and 4 on a live install.

It reuses the answer scenario's setup and restore (``AnswerScenario``, itself
built on the creation scenario's ``TicketScenario``): a gated examples/weather
version serving alone, the requester flag on, a selector registered on the
owning route, a restricted approver role, the roster people and one turn per
approval. On top it inserts the DM bot's workspace namespace row (no product
surface writes one on an HTTP-selected tenant), maps two synthetic Slack user
ids to roster people through the provider's roster override, and writes the
DM resolution on the ticket route.

Every write and answer goes through a real API boundary: operator route
writes through ``PATCH /agents/{id}``; creations through
``POST /v1/internal/identity/approvals`` from a turn's ingress capsule;
registrations through ``POST /v1/internal/identity/approvals/{id}/card`` with
the worker token; answers through ``POST /approvals/{id}/resolve`` with a chat
principal and a Slack receiver proof the harness signs, as the dispatcher
forwards a click. The worker's post is scripted: no live turn creates a DM
approval (the fake model cannot call a gated tool with a ticket), so the
harness registers the DM channel and, for retention, remembers the card with
the worker's own card store in the worker pod. Each step records statuses,
refusal codes, row facts, scrubbed audit rows and Valkey TTLs; never a key,
token, proof, capsule, nonce or ticket. See docs/testing/identity-e2e.md.
"""

from __future__ import annotations

import copy
import json
import sys
import time
from collections.abc import Mapping, Sequence
from typing import Any

import answer_authority_driver as answers
import approver_dm_driver as dm
import approver_ticket_driver as tickets
from answer_authority_scenario import AnswerScenario
from identity_assets import (
    DM_ACCOUNT,
    DM_FOREIGN_ACCOUNT,
    DM_FOREIGN_IDENTITY,
    DM_IDENTITY,
    PENDING_RETENTION_ENV,
    SLACK_APPROVER,
    SLACK_OTHER,
    SLACK_UNKNOWN,
    slack_namespace,
)
from identity_e2e import Api, Harness, HarnessError
from identity_scenarios import Evidence, StepFailed, check, require_passed

R, A, B = answers.REQUESTER, answers.APPROVER, answers.SECOND
# The requester turns the run sends, one per creation dedupe key it needs.
TURNS = {"refuse": "r01", "d3": "r02", "d4": "r03", "d5": "r04", "d6": "r05"}
SUMMARY = "identity-e2e approver-dm card"
# Polls of the worker's card reference after settlement, each up to the
# driver's in-pod wait.
RETENTION_CALLS = 3


class DmScenario(AnswerScenario):
    """One ``approver-dm`` run on one agent, and what it must put back."""

    def __init__(self, harness: Harness, args: Any, evidence: Evidence) -> None:
        super().__init__(harness, args, evidence)
        slack = harness.data.get("slack") or {}
        if not isinstance(slack.get("dm"), dict) or not isinstance(slack.get("foreign"), dict):
            raise HarnessError(
                "the harness state has no Slack receiver registrations; rerun `up` so it "
                "registers the approver DM receivers"
            )
        dm_state, foreign = slack["dm"], slack["foreign"]
        self.dm_namespace = slack_namespace(dm_state["namespace_id"], DM_ACCOUNT)
        self.foreign_namespace = slack_namespace(foreign["namespace_id"], DM_FOREIGN_ACCOUNT)
        self.dm_installation = str(dm_state["installation_id"])
        self.foreign_installation = str(foreign["installation_id"])
        self.dm_identity_id = str(dm_state["identity_id"])
        # Travel to the API pod on stdin only, to sign answer proofs; never recorded.
        self.receivers = {
            "dm": {
                "receiver_id": dm_state["receiver_id"],
                "key": dm_state["key"],
                "identity_id": dm_state["identity_id"],
                "installation_id": dm_state["installation_id"],
                "account": DM_ACCOUNT,
            },
            "foreign": {
                "receiver_id": foreign["receiver_id"],
                "key": foreign["key"],
                "identity_id": foreign["identity_id"],
                "installation_id": foreign["installation_id"],
                "account": DM_FOREIGN_ACCOUNT,
            },
        }
        self.slack_people = {A: SLACK_APPROVER, B: SLACK_OTHER}
        self.changes["namespace"] = False

    # pods -------------------------------------------------------------------------------

    def ddrive(self, action: str, **params: Any) -> dict[str, Any]:
        """One action of the in-pod DM driver; a refusal raises :class:`HarnessError`."""

        started = time.monotonic()
        print(f"{dm.SCENARIO}: driver {action} started", file=sys.stderr, flush=True)
        try:
            result = self.harness.target.pod_python(
                dm.POD_BOOTSTRAP, dm.pod_params(action, **params)
            )
        finally:
            print(
                f"{dm.SCENARIO}: driver {action} ended after {time.monotonic() - started:.1f}s",
                file=sys.stderr,
                flush=True,
            )
        if not isinstance(result, dict) or "error" in result:
            error = result.get("error") if isinstance(result, dict) else result
            raise HarnessError(f"the in-pod DM driver ({action}) refused: {error}")
        return result

    def worker_card(self, approval_id: str, **params: Any) -> dict[str, Any]:
        """The worker pod's view of one approval's card reference (``dm._worker_card``)."""

        result = self.harness.target.worker_python(
            dm.WORKER_BOOTSTRAP, dm.pod_params("card", approval_id=approval_id, **params)
        )
        if not isinstance(result, dict) or "error" in result:
            error = result.get("error") if isinstance(result, dict) else result
            raise HarnessError(f"the worker card read refused: {error}")
        return result

    def dm_row(self, name: str) -> dict[str, Any]:
        approval_id = self.approval(name)
        row = self.ddrive("dm_rows", ids=[approval_id])["rows"].get(approval_id)
        if row is None:
            raise StepFailed(f"approval {name} is gone")
        return dict(row)

    # routes ------------------------------------------------------------------------------

    def registration_now(self, api: Api) -> dict[str, Any]:
        """The run's selector, pinning the approver role version as it stands."""

        role_version = int(api.ok("GET", f"/agents/{self.agent_id}/access-policy")["version"])
        return answers.ticket_registration(
            key_id=self.keys["k1"]["key_id"],
            public_key=self.keys["k1"]["public_key"],
            role_version=role_version,
            issuer=self.issuer,
        )

    def write_dm(
        self, api: Api, label: str, code: str | None = None, **options: Any
    ) -> dict[str, Any]:
        """One versioned DM route write, recorded with the stored state before and after."""

        registration = None if options.pop("unregistered", False) else self.registration_now(api)
        routes = dm.dm_routes(self.base, registration, identity=DM_IDENTITY, **options)
        before = (self.stored_routes(), self.configuration(api)[1])
        self.changes["routes"] = True
        status, body = self.write_routes(api, routes)
        after = (self.stored_routes(), self.configuration(api)[1])
        return dm.route_write(label, code, status=status, body=body, before=before, after=after)

    def ensure_dm_route(self, api: Api, **options: Any) -> dict[str, Any]:
        write = self.write_dm(api, "dm-route", **options)
        if write["http"] != 200:
            raise StepFailed(f"writing the DM route answered {write['http']} {write['detail']!r}")
        return write

    # setup and restore -------------------------------------------------------------------

    def prepare(self) -> None:
        """The answer scenario's preconditions, plus the DM bot's rows and the worker setting."""

        super().prepare()
        setup = self.evidence.data["setup"]
        rows = self.harness.target.sql(
            (
                "SELECT status, provider_installation_id::text AS installation "
                "FROM curie.channel_identities WHERE id::text = :i AND name = :n",
                {"i": self.dm_identity_id, "n": DM_IDENTITY},
            ),
            (
                "SELECT status FROM curie.provider_installations WHERE id::text = :i",
                {"i": self.dm_installation},
            ),
        )
        identity, installation = rows
        check(
            len(identity) == 1
            and identity[0]["status"] == "active"
            and identity[0]["installation"] == self.dm_installation,
            f"the channel identity {DM_IDENTITY} `up` created is missing or not active; rerun `up`",
        )
        check(
            len(installation) == 1 and installation[0]["status"] == "connected",
            f"the Slack installation {DM_ACCOUNT} `up` created is missing; rerun `up`",
        )
        stored = self.original.get("routes") or {}
        check(
            not any(
                isinstance(binding, Mapping)
                and (binding.get("resolution") or {}).get("mode") == dm.DM_MODE
                for binding in (stored.values() if isinstance(stored, Mapping) else [])
            ),
            f"{self.agent} already stores an approver_dm route; a previous run did not restore",
        )
        nil = "00000000-0000-0000-0000-000000000000"
        worker = self.worker_card(nil)
        enabled = worker.get("pending_retention_enabled") is True
        setup["dm"] = {
            "identity": DM_IDENTITY,
            "identity_id": self.dm_identity_id,
            "installation_id": self.dm_installation,
            "namespace": dict(self.dm_namespace),
            "foreign_identity": DM_FOREIGN_IDENTITY,
            "foreign_installation_id": self.foreign_installation,
            "slack_people": {
                self.people[part]: native for part, native in self.slack_people.items()
            },
            "unresolved_slack_user": SLACK_UNKNOWN,
            "worker_pending_retention": {"env": PENDING_RETENTION_ENV, "enabled": enabled},
        }
        check(
            enabled,
            f"the worker runs with {PENDING_RETENTION_ENV} off; rerun `up`, which sets it",
        )

    def install(self) -> None:
        """The answer scenario's install, then the DM namespace row and the Slack people."""

        super().install()
        setup = self.evidence.data["setup"]
        self.changes["namespace"] = True
        inserted = self.harness.target.sql(
            (
                "INSERT INTO curie.identity_namespaces "
                "(id, tenant_id, provider, authority, kind, key, status) "
                "VALUES (CAST(:i AS uuid), CAST(:t AS uuid), 'slack', '', 'slack_workspace', "
                ":k, 'active') ON CONFLICT (id) DO NOTHING",
                {"i": self.dm_namespace["id"], "t": self.tenant_id, "k": DM_ACCOUNT},
            )
        )[0]
        # 0 when an interrupted run left the row; restore deletes it either way.
        setup["namespace_row"] = {"inserted": inserted, "id": self.dm_namespace["id"]}
        setup["slack_roster"] = self.roster_change(
            {
                "upsert": {
                    native.lower(): self.entry(part) for part, native in self.slack_people.items()
                }
            }
        )

    def restore(self) -> None:
        """The answer scenario's restore, then the namespace row; a failure fails the run."""

        super().restore()
        report = self.evidence.data["restore"]
        if self.changes["namespace"]:
            try:
                deleted = self.harness.target.sql(
                    (
                        "DELETE FROM curie.identity_namespaces WHERE id::text = :i",
                        {"i": self.dm_namespace["id"]},
                    )
                )[0]
                report["done"].append({"action": "DM namespace row", "result": deleted})
            except HarnessError as exc:
                report["failed"].append({"action": "DM namespace row", "error": str(exc)[:400]})
                self.evidence.data.setdefault(
                    "aborted", f"restore incomplete: DM namespace row: {exc}"
                )
        report["left"] = (
            f"approvals this run created stay pending until they expire "
            f"({answers.APPROVAL_TTL_S} s), answered ones stay resolved; the DM card "
            "references the run remembered carry the worker's bounded terminal TTL; the "
            "run's agent versions stay, undeployed"
        )

    # creation, registration and answers ---------------------------------------------------

    def turn_ref(self, name: str) -> dict[str, str]:
        turn = self.turns[TURNS[name]]
        return {"stream_id": turn["stream_id"], "event_id": turn["event_id"]}

    def account(self, native: str, *, foreign: bool = False) -> dict[str, Any]:
        return dm.recipient_account(
            installation_id=self.foreign_installation if foreign else self.dm_installation,
            namespace=self.foreign_namespace if foreign else self.dm_namespace,
            account_id=native,
        )

    def person(self, part: str, *accounts: Mapping[str, Any]) -> dict[str, Any]:
        return dm.selected(self.issuer, self.subjects[part], accounts)

    def dm_create(
        self,
        seen: dict[str, Any],
        batches: Sequence[dict[str, Any]],
        *,
        count_turns: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Send creation batches from the API pod; record answers, rows and spent nonces."""

        names = []
        for item in batches:
            for case in item["cases"]:
                self.nonces.setdefault(case["name"], tickets.new_nonce())
                item["nonces"][case["name"]] = self.nonces[case["name"]]
                names.append(case["name"])
        labels = sorted({case["turn"] for item in batches for case in item["cases"]})
        result = self.ddrive(
            "dm_create",
            agent_id=self.agent_id,
            key={"seed_hex": self.keys["k1"]["seed_hex"], "key_id": self.keys["k1"]["key_id"]},
            registered={
                "connector": tickets.CONNECTOR,
                "tool": tickets.TOOL,
                "route": tickets.TICKET_ROUTE,
            },
            turns={
                label: {
                    "stream_id": self.turns[label]["stream_id"],
                    "event_id": self.turns[label]["event_id"],
                }
                for label in labels
            },
            batches=list(batches),
            count_dedupe=[self.turns[TURNS[name]]["event_id"] for name in count_turns],
            count_nonces=names,
            ttl_s=answers.APPROVAL_TTL_S,
        )
        seen["cases"] = [
            {key: item.get(key) for key in ("name", "route", "expected", "results")}
            for item in result["cases"]
        ]
        seen["rows_for_turns"] = dict(result.get("rows") or {})
        seen["spent_nonces"] = dict(result.get("nonces") or {})
        return result

    def create_dm(self, seen: dict[str, Any], name: str) -> dict[str, Any]:
        """One valid DM approval selecting the approver with their one Slack account."""

        seen["principals"] = self.ensure_active(A)
        result = self.dm_create(
            seen,
            [
                dm.batch(
                    [self.person(A, self.account(SLACK_APPROVER))],
                    dm.creation_case(name, TURNS[name], expected=tickets.expect(201)),
                )
            ],
        )
        answered = result["cases"][0]["results"][0]
        problems = dm.delivery_problems(
            answered,
            dm.expected_delivery(
                identity=DM_IDENTITY,
                installation_id=self.dm_installation,
                recipient=SLACK_APPROVER,
            ),
        )
        check(not problems, f"creating {name}: " + "; ".join(problems))
        approval_id = str(answered["id"])
        self.approvals[name] = approval_id
        row = result["created"].get(approval_id) or {}
        seen["created"] = {
            "id": approval_id,
            "delivery": answered.get("delivery"),
            "row": scrubbed_row(row),
        }
        check(
            row.get("card_channel") is None and not row.get("card_audits"),
            f"{name} was created with card_channel {row.get('card_channel')!r} or a card audit",
        )
        return row

    def register(self, name: str, *requests: tuple[str, str]) -> dict[str, Any]:
        """Register ``(label, channel)`` DM cards for ``name`` in order, playing the worker."""

        return self.ddrive(
            "register_card",
            approval_id=self.approval(name),
            turn=self.turn_ref(name),
            dedupe_key=self.turns[TURNS[name]]["event_id"],
            requests=[
                {
                    "label": label,
                    "card_identity": DM_IDENTITY,
                    "recipient": SLACK_APPROVER,
                    "channel": channel,
                }
                for label, channel in requests
            ],
        )

    def click(
        self, name: str, *specs: Mapping[str, Any], concurrent: bool = False
    ) -> dict[str, Any]:
        """Scripted Slack clicks on ``name``; a run that met an identity change is resent."""

        result: dict[str, Any] = {}
        for attempt in range(1, answers.ATTEMPTS + 1):
            result = self.ddrive(
                "slack_answers",
                approval_id=self.approval(name),
                answers=[dict(spec) for spec in specs],
                concurrent=concurrent,
            )
            result["attempts"] = attempt
            unchanged = result.get("status_after") == result.get("status_before")
            if not (unchanged and all(item.get("http") == 503 for item in result["results"])):
                break
        return result

    def spec(
        self,
        label: str,
        *,
        native: str = SLACK_APPROVER,
        channel: str,
        receiver: str = "dm",
        decision: str = "approved",
    ) -> dict[str, Any]:
        return dm.answer_spec(
            label,
            native=native,
            channel=channel,
            receiver=self.receivers[receiver],
            decision=decision,
        )

    def refused_click(
        self,
        name: str,
        spec: Mapping[str, Any],
        statuses: Sequence[int],
        card_channel: str | None,
    ) -> tuple[dict[str, Any], list[str]]:
        """One click that must have no effect; its evidence and problems."""

        result = self.click(name, spec)
        answered = result["results"][0]
        flat = {
            **answered,
            "status_after": result.get("status_after"),
            "audit": result.get("audit"),
            "card_channel": result.get("card_channel"),
            "attempts": result.get("attempts"),
        }
        return flat, dm.unanswered_problems(flat, statuses=statuses, card_channel=card_channel)

    def refused_email(
        self, name: str, card_channel: str | None
    ) -> tuple[dict[str, Any], list[str]]:
        """The selected approver's email reply forwarded on the origin thread: no effect."""

        result = self.answer(name, A, retry=False)
        result.update(label="email-reply", card_channel=self.dm_row(name).get("card_channel"))
        return result, dm.unanswered_problems(
            result, statuses=dm.EMAIL_REFUSED, card_channel=card_channel
        )

    def quiesce(self, what: str) -> dict[str, Any]:
        """The worker idle: every turn sent and every resumed wake finished.

        Every HTTP identity lookup advances the tenant lifecycle generation,
        and a settling answer's inline resume publication refuses when it
        moved under it, so no settling answer is sent while a turn runs.
        """

        events = [turn["event_id"] for turn in self.turns.values()]
        events += [answers.resume_event_id(self.approval(name)) for name in sorted(self.approved)]
        return self.await_worker(what, events, answers.WAKE_SETTLE_S)

    # step 1 ------------------------------------------------------------------------------

    def route_write_refusals(self) -> None:
        with (
            self.evidence.step(
                "dm-route-write-refusals",
                "operator route writes refuse a DM without a selector, a DM with a "
                "notification and an identity with no registered receiver, each 422 with its "
                "code and nothing stored; a DM on the ticket route is accepted and reads back "
                "exactly",
            ) as seen,
            self.harness.api() as api,
        ):
            writes = [
                self.write_dm(api, "dm-without-selector", dm.STATIC_ROUTE, unregistered=True),
                self.write_dm(
                    api, "dm-on-unregistered-route", dm.STATIC_ROUTE, route=tickets.LOOSER_ROUTE
                ),
                self.write_dm(
                    api, "dm-with-notification", dm.WITH_NOTIFICATION, notification=dm.NOTIFY
                ),
                self.write_dm(
                    api,
                    "identity-without-receiver",
                    dm.RECEIVER_MISSING,
                    resolution=dm.dm_resolution(dm.UNREGISTERED_IDENTITY),
                ),
            ]
            seen["refusals"] = writes
            problems = [problem for write in writes for problem in dm.refused_write(write)]
            accepted = self.write_dm(api, "dm-on-ticket-route")
            seen["accepted"] = accepted
            if accepted["http"] != 200:
                problems.append(
                    f"the DM route write answered {accepted['http']} {accepted['detail']!r}"
                )
            out = api.ok("GET", f"/agents/{self.agent_id}")
            readback = (out.get("approval_routes") or {}).get(tickets.TICKET_ROUTE) or {}
            stored = self.stored_routes().get(tickets.TICKET_ROUTE) or {}
            wanted = dm.dm_resolution(DM_IDENTITY)
            seen["readback"] = {
                "api": readback.get("resolution"),
                "stored": stored.get("resolution"),
                "notification": readback.get("notification"),
            }
            if readback.get("resolution") != wanted or stored.get("resolution") != wanted:
                problems.append(f"the DM route reads back {seen['readback']}, not {wanted}")
            if readback.get("notification") is not None:
                problems.append("the DM route reads back a notification")
            check(not problems, "; ".join(problems))

    # step 2 ------------------------------------------------------------------------------

    def creation_refusals(self) -> None:
        with self.evidence.step(
            "dm-creation-refusals",
            "creation through the internal boundary refuses two selected approvers, no "
            "recipient account, an account on another installation, an account the directory "
            "resolves to another person or not at all, a worker-chosen card channel and an "
            "identity without a registered receiver; no row is stored and no nonce spent",
        ) as seen:
            seen["principals"] = self.ensure_active(A, B)
            turn = TURNS["refuse"]

            def case(name: str, status: int, code: str, dm_card: bool = True) -> dict[str, Any]:
                return dm.creation_case(
                    name, turn, expected=tickets.expect(status, code), dm=dm_card
                )

            mine = self.account(SLACK_APPROVER)
            batches = [
                dm.batch(
                    [self.person(A, mine), self.person(B, self.account(SLACK_OTHER))],
                    case("two-selected", 422, dm.RECIPIENT_COUNT),
                ),
                dm.batch([self.person(A)], case("no-account", 422, dm.ACCOUNT_MISSING)),
                dm.batch(
                    [self.person(A, self.account(SLACK_APPROVER, foreign=True))],
                    case("other-installation", 422, dm.WRONG_INSTALLATION),
                ),
                dm.batch(
                    [self.person(A, self.account(SLACK_OTHER))],
                    case("account-of-another-person", 422, dm.ACCOUNT_UNRESOLVED),
                ),
                dm.batch(
                    [self.person(A, self.account(SLACK_UNKNOWN))],
                    case("account-unresolved", 422, dm.ACCOUNT_UNRESOLVED),
                ),
                dm.batch(
                    [self.person(A, mine)],
                    case("card-channel-named", 409, tickets.UNSUPPORTED, dm_card=False),
                ),
            ]
            result = self.dm_create(seen, batches, count_turns=["refuse"])
            problems = dm.creation_problems(
                result["cases"], result.get("rows") or {}, result.get("nonces") or {}
            )
            seen["receiver_missing"] = oob = {}
            stored = self.stored_routes()
            changed = copy.deepcopy(stored)
            changed[tickets.TICKET_ROUTE]["resolution"] = dm.dm_resolution(dm.UNREGISTERED_IDENTITY)
            # No operator write can store this (step 1 refuses it), so the
            # route is changed out of band and put back exactly however the
            # creation ends.
            try:
                self.store_routes(changed)
                oob["stored_identity"] = dm.UNREGISTERED_IDENTITY
                missing = self.dm_create(
                    oob,
                    [
                        dm.batch(
                            [self.person(A, mine)],
                            case("receiver-missing", 409, dm.RECEIVER_MISSING),
                        )
                    ],
                    count_turns=["refuse"],
                )
                problems += dm.creation_problems(
                    missing["cases"], missing.get("rows") or {}, missing.get("nonces") or {}
                )
            finally:
                self.store_routes(stored)
                oob["routes_restored"] = self.stored_routes() == stored
            check(oob["routes_restored"], "the out-of-band route change was not put back")
            check(not problems, "; ".join(problems))

    def store_routes(self, routes: Mapping[str, Any]) -> None:
        self.harness.target.sql(
            (
                "UPDATE curie.agents SET approval_routes = CAST(:r AS jsonb) WHERE id::text = :a",
                {"r": json.dumps(routes, sort_keys=True), "a": self.agent_id},
            )
        )

    # step 3 ------------------------------------------------------------------------------

    def unregistered_unanswerable(self) -> None:
        with self.evidence.step(
            "dm-unregistered-unanswerable",
            "a valid DM creation answers its verified recipient and stays unregistered; the "
            "selected approver's email reply on the origin thread and a Slack click on the "
            "origin channel are refused and the row stays pending with no card",
        ) as seen:
            row = self.create_dm(seen, "d3")
            problems: list[str] = []
            email, found = self.refused_email("d3", None)
            seen["email_reply"], problems = email, problems + found
            click, found = self.refused_click(
                "d3",
                self.spec("origin-channel", channel=str(row.get("reply_channel"))),
                dm.UNAUTHENTICATED,
                None,
            )
            seen["origin_click"], problems = click, problems + found
            click, found = self.refused_click(
                "d3",
                self.spec("unregistered-dm", channel=dm.DM_UNREGISTERED),
                dm.UNAUTHENTICATED,
                None,
            )
            seen["unregistered_dm_click"], problems = click, problems + found
            after = self.dm_row("d3")
            seen["row_after"] = scrubbed_row(after)
            if after.get("status") != "pending" or after.get("card_channel") is not None:
                problems.append(f"d3 ends {after.get('status')!r} on {after.get('card_channel')!r}")
            seen["worker_post"] = {"carried": dm.CARRIED["worker-dm-post-failure-log"]}
            check(not problems, "; ".join(problems))

    # step 4 ------------------------------------------------------------------------------

    def card_registration(self) -> None:
        with (
            self.evidence.step(
                "dm-card-registration",
                "the worker-token registration stores the DM channel once, an identical retry "
                "is a duplicate, a conflicting channel is 409, and after the route is "
                "repointed both are still judged against the committed registration",
            ) as seen,
            self.harness.api() as api,
        ):
            self.create_dm(seen, "d4")
            seen["scripted"] = "registration plays the worker's post (no Slack in the install)"
            first = self.register(
                "d4",
                ("first", dm.DM_REGISTERED),
                ("identical-retry", dm.DM_REGISTERED),
                ("conflicting-channel", dm.DM_CONFLICT),
            )
            first_results = first["results"]
            problems = dm.registration_problems(
                first_results[0], http=200, channel=dm.DM_REGISTERED, duplicate=False
            )
            problems += dm.registration_problems(
                first_results[1], http=200, channel=dm.DM_REGISTERED, duplicate=True
            )
            problems += dm.registration_problems(
                first_results[2], http=409, detail=dm.CARD_CONFLICT
            )
            seen["registrations"] = first_results
            try:
                seen["repoint"] = self.write_dm(
                    api,
                    "repoint-requesting-surface",
                    resolution=dict(tickets.REQUESTING_SURFACE["resolution"]),
                )
                if seen["repoint"]["http"] != 200:
                    problems.append(f"repointing the route answered {seen['repoint']['http']}")
                again = self.register(
                    "d4",
                    ("identical-after-repoint", dm.DM_REGISTERED),
                    ("conflicting-after-repoint", dm.DM_CONFLICT),
                )["results"]
                seen["after_repoint"] = again
                problems += dm.registration_problems(
                    again[0], http=200, channel=dm.DM_REGISTERED, duplicate=True
                )
                problems += dm.registration_problems(again[1], http=409, detail=dm.CARD_CONFLICT)
            finally:
                seen["route_restored"] = self.ensure_dm_route(api)
            row = self.dm_row("d4")
            seen["row_after"] = scrubbed_row(row)
            if row.get("card_channel") != dm.DM_REGISTERED or row.get("status") != "pending":
                problems.append(
                    f"d4 is {row.get('status')!r} on {row.get('card_channel')!r} after the "
                    "registrations"
                )
            problems += dm.card_audit_problems(
                row.get("card_audits") or [],
                channel=dm.DM_REGISTERED,
                recipient=SLACK_APPROVER,
                identity=DM_IDENTITY,
            )
            check(not problems, "; ".join(problems))

    # step 5 ------------------------------------------------------------------------------

    def answer_authority(self) -> None:
        with (
            self.evidence.step(
                "dm-answer-authority",
                "scripted Slack clicks from another DM, another actor, another installation's "
                "receiver and a copied card, an operator principal the route lists and an "
                "email reply are all refused with the row pending; two concurrent valid "
                "clicks give one decision by the selected person, whose resume continues",
            ) as seen,
            self.harness.api() as api,
        ):
            seen["route"] = self.ensure_dm_route(
                api, approvers={"users": [answers.OPERATOR_USER, SLACK_APPROVER]}
            )
            try:
                row = self.create_dm(seen, "d5")
                registered = self.register("d5", ("first", dm.DM_ANSWER))["results"][0]
                seen["registration"] = registered
                problems = dm.registration_problems(
                    registered, http=200, channel=dm.DM_ANSWER, duplicate=False
                )
                check(not problems, "; ".join(problems))
                refusals = (
                    (self.spec("other-dm", channel=dm.DM_ELSEWHERE), dm.UNAUTHENTICATED),
                    (
                        self.spec("other-actor", native=SLACK_OTHER, channel=dm.DM_ANSWER),
                        dm.REFUSED,
                    ),
                    (
                        self.spec("other-installation", channel=dm.DM_ANSWER, receiver="foreign"),
                        dm.UNAUTHENTICATED,
                    ),
                    (
                        self.spec(
                            "copied-card-origin-channel", channel=str(row.get("reply_channel"))
                        ),
                        dm.UNAUTHENTICATED,
                    ),
                    (
                        self.spec("copied-card-public-channel", channel=dm.COPY_CHANNEL),
                        dm.UNAUTHENTICATED,
                    ),
                )
                seen["refused"] = []
                for spec, statuses in refusals:
                    result, found = self.refused_click("d5", spec, statuses, dm.DM_ANSWER)
                    seen["refused"].append(result)
                    problems += found
                operator = self.operator_answer("d5")
                seen["operator"] = operator
                problems += answers.judge_answer(
                    operator,
                    http=403,
                    row_after="pending",
                    action=answers.DENIED,
                    code=answers.OPERATOR_REFUSED,
                    principal_kind="operator",
                )
                email, found = self.refused_email("d5", dm.DM_ANSWER)
                seen["email_reply"], problems = email, problems + found
            finally:
                seen["route_restored"] = self.ensure_dm_route(api)
            seen["idle"] = self.quiesce("idle before the concurrent DM answers")
            race = self.click(
                "d5",
                self.spec("valid-1", channel=dm.DM_ANSWER),
                self.spec("valid-2", channel=dm.DM_ANSWER),
                concurrent=True,
            )
            seen["concurrent"] = race
            problems += dm.race_problems(race, actor=SLACK_APPROVER)
            if race.get("status_after") == "approved":
                self.approved.add("d5")
                seen["worker"] = self.await_wake("d5", bool(race.get("resumed")))
                resume = self.resume("d5")
                seen["resume"] = resume
                problems += answers.judge_resume(resume, decision="continue")
            check(not problems, "; ".join(problems))

    # step 6 ------------------------------------------------------------------------------

    def pending_retention(self) -> None:
        with self.evidence.step(
            "dm-pending-retention",
            "the worker's card reference for a registered pending DM approval has no TTL and "
            "is in the retention index; after the selected person approves it carries a "
            "bounded TTL (or is deleted exactly with a bounded retention marker)",
        ) as seen:
            self.create_dm(seen, "d6")
            registered = self.register("d6", ("first", dm.DM_RETENTION))["results"][0]
            seen["registration"] = registered
            problems = dm.registration_problems(
                registered, http=200, channel=dm.DM_RETENTION, duplicate=False
            )
            check(not problems, "; ".join(problems))
            approval_id = self.approval("d6")
            seen["scripted"] = (
                "the card reference is written by the worker's own ApprovalCardStore in the "
                "worker pod, as the worker's DM post records it"
            )
            pending = self.worker_card(
                approval_id,
                remember={"channel": dm.DM_RETENTION, "summary": SUMMARY, "adapter": DM_IDENTITY},
            )
            seen["pending"] = pending
            seen["idle"] = self.quiesce("idle before the retention approval")
            answer = self.click("d6", self.spec("selected", channel=dm.DM_RETENTION))
            seen["answer"] = answer
            if answer["results"][0].get("http") != 200 or answer.get("status_after") != "approved":
                problems.append(
                    f"the selected approver's answer was {answer['results'][0].get('http')} "
                    f"and the row is {answer.get('status_after')!r}"
                )
            else:
                self.approved.add("d6")
                seen["worker"] = self.await_wake("d6", bool(answer.get("resumed")))
            settled: dict[str, Any] = {}
            for _ in range(RETENTION_CALLS):
                settled = self.worker_card(
                    approval_id, until_settled=True, wait_s=dm.RETENTION_WAIT_S
                )
                if dm.settled_reference(settled):
                    break
            seen["settled"] = settled
            problems += dm.retention_problems(pending, settled, channel=dm.DM_RETENTION)
            check(not problems, "; ".join(problems))


def scrubbed_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """An approval's DM facts as evidence."""

    keep = ("id", "status", "route", "card_channel", "reply_kind", "selection_mode", "resolved_by")
    found = {key: row.get(key) for key in keep}
    found["card_audits"] = [answers.scrub(item) for item in row.get("card_audits") or []]
    return found


def run(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0272 evidence items 1, 3 and 4 on one agent of a live install.

    Setup, changing nothing: the answer scenario's preconditions, the DM bot's
    identity and installation rows ``up`` created, and the worker's pending
    retention setting. Then the answer scenario's install, the DM workspace
    namespace row and the Slack people, an approver turn and five requester
    turns, and a wait until the worker has finished them. ``finally`` restores
    everything the answer scenario restores and deletes the namespace row; a
    failed restore fails the run.
    """

    scenario = DmScenario(harness, args, evidence)
    evidence.data.update(
        agent=scenario.agent,
        requester=scenario.people[R],
        approver=scenario.people[A],
        steps_required=list(dm.STEP_IDS),
        carried=dict(dm.CARRIED),
    )
    scenario.prepare()
    try:
        scenario.install()
        scenario.send_turns(len(TURNS))
        scenario.route_write_refusals()
        require_passed(evidence)
        scenario.creation_refusals()
        scenario.unregistered_unanswerable()
        scenario.card_registration()
        scenario.answer_authority()
        scenario.pending_retention()
    finally:
        scenario.restore()
