"""The activation transition, @spec PROTECTED-COMMISSIONING-6 (design decision 4).

Activation changes only broker authorization state that G already measured,
in the order clause 6 fixes, each write by its own principal:

1. commissioning control moves the journal from ``closing`` to ``closed``,
   after the campaign's settlement (ingress closed, deliveries settled,
   sessions terminated, workloads and targets absent) is confirmed;
2. the provisioner disables the candidate ACL users and kills their
   sessions, leaving their rules as instantiated, and the design decision 7
   observer confirms the candidates disabled and every other principal at its
   baseline;
3. the ADR 0236 verifier decides, and the commissioning issuer writes the
   ``commissioning_acceptance`` record for that verdict;
4. the verifier constructs the Qualification and Readiness publishers and
   both write their unchanged v1 records;
5. the provisioner compares and sets the selection from ``admission_open:
   false`` to ``true``, every other member byte equal, after reading back the
   journal ``closed`` and the stored Qualification and Readiness that the
   unchanged readers will accept.

After step 5 the verifier observes B, G, M, the execution digest and the rest
of its tuple again. A difference closes admission again by the reverse
compare and set and refuses. No pod template, image, mount, Secret reference,
ACL rule, guard revision, measurement or bootstrap file is written here.

Every failure refuses with the step it stopped at and leaves the earlier
writes in place: a journal still ``closing`` blocks acceptance, and the
selection opens only after the Qualification and Readiness exist. Activation
starts only from ``closing``, so after any failure, restart or interruption
past step 1 the ``closed`` journal refuses every later activation: no new
verdict is ever tried for the same campaign. Steps 2 and 5 run only on a
session whose ``ACL WHOAMI`` is the trusted provisioner on M's epoch. Once the
acceptance record is written the remaining steps, including the re-read and
any reverse compare and set, run to completion however often the caller is
cancelled; the cancellation is raised afterwards.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, NoReturn

from redis import Redis

from .admission_records import _encode, parse_selection
from .authority_records import (
    Manifest,
    _scalar,
    parse_qualification,
    parse_readiness,
    validate_authority,
)
from .commissioning_acceptance_writer import _closed, issue_acceptance
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_campaign_records import (
    CommissioningAcceptance,
    parse_commissioning_acceptance,
)
from .commissioning_journal import campaign_keys
from .commissioning_producers import PhaseExpectation, _usernames
from .commissioning_records import CommissioningPermit
from .commissioning_verifier import Publication

__all__ = [
    "STEPS",
    "ActivationRefused",
    "ActivationReport",
    "CommissioningActivation",
    "close_admission",
    "disable_candidates",
    "open_admission",
    "opened_selection",
]

# Clause 6, in order; the verifier's re-read of the tuple comes last.
STEPS = (
    "journal-closed",
    "candidates-disabled",
    "accepted",
    "published",
    "admission-open",
    "tuple-unchanged",
)
_SELECTION = "protected:control:selection:"
# One compare and set on the provisioner's own rules: ``EVAL`` may declare the
# selection, and the script's ``GET`` and ``SET`` are each checked again.
_SELECTION_CAS = """
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then
  return 'refused'
end
redis.call('SET', KEYS[1], ARGV[2])
return 'set'
"""
_CANDIDATES = ("candidate_enqueue", "candidate_worker")
# Design decision 7: after step 2 the candidates are expected disabled.
ACTIVATED = PhaseExpectation("activated", {name: False for name in _CANDIDATES})


class ActivationRefused(RuntimeError):
    """Redacted refusal naming only the clause 6 step it stopped at."""

    def __init__(self, step: str = "journal-closed") -> None:
        if step not in STEPS:
            raise ValueError("unknown activation step")
        super().__init__("Commissioning activation refused at " + step)
        self.step = step


def _refuse(step: str = "journal-closed") -> NoReturn:
    """Raise a detached refusal, even inside a caller's active exception."""
    error = ActivationRefused(step)
    try:
        raise error from None
    finally:
        error.__context__ = None


def _require(condition: bool, step: str = "journal-closed") -> None:
    if not condition:
        _refuse(step)


def opened_selection(raw: bytes) -> bytes:
    """The canonical selection with ``admission_open`` true; every other byte equal.

    Only a canonical, closed selection opens. @spec PROTECTED-COMMISSIONING-6.
    """
    try:
        _require(type(raw) is bytes, "admission-open")
        closed = parse_selection(raw)
        _require(closed["admission_open"] is False and _encode(closed) == raw, "admission-open")
        opened = _encode(dict(closed, admission_open=True))
        _require(parse_selection(opened) == dict(closed, admission_open=True), "admission-open")
        _require(
            opened.replace(b'"admission_open":true', b'"admission_open":false') == raw,
            "admission-open",
        )
        return opened
    except ActivationRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse("admission-open")


def _text(value: Any) -> str:
    if type(value) is bytes:
        value = value.decode("ascii")
    _require(type(value) is str, "admission-open")
    return str(value)


def _pinned(session: Redis, manifest: Manifest, step: str, username: str) -> dict[str, Any]:
    """M as a dict, after the session proves it is the trusted provisioner on M's epoch.

    ``ACL WHOAMI`` must name the provisioner and ``INFO server`` must show M's
    live ``run_id``.
    """
    _require(type(manifest) is Manifest and type(username) is str and bool(username), step)
    whoami = session.execute_command("ACL", "WHOAMI")  # type: ignore[no-untyped-call]
    _require(whoami in (username, username.encode("ascii")), step)
    m = manifest.as_dict()
    _require(session.info("server").get("run_id") == m["broker_identity"]["run_id"], step)
    return m


def _ok(reply: Any) -> bool:
    return reply in (b"OK", "OK", True)


def disable_candidates(
    session: Redis,
    bootstrap: CommissioningBootstrap,
    *,
    manifest: Manifest,
    provisioner_username: str,
) -> None:
    """Clause 6 step 2 on a provisioner session: each candidate ``off``, sessions killed.

    ``off`` changes only the enabled state; the instantiated rules G's template
    measured stay as they are. The usernames are B's candidate credential IDs,
    never caller supplied. The session must be the trusted provisioner on M's
    epoch. Repeating the step is harmless.
    @spec PROTECTED-COMMISSIONING-6.
    """
    try:
        _require(type(bootstrap) is CommissioningBootstrap, "candidates-disabled")
        _pinned(session, manifest, "candidates-disabled", provisioner_username)
        credentials = bootstrap.as_dict()["candidate_credentials"]
        for name in ("enqueue", "worker"):
            username = credentials[name]["id"]
            reply = session.execute_command(  # type: ignore[no-untyped-call]
                "ACL", "SETUSER", username, "off"
            )
            _require(_ok(reply), "candidates-disabled")
            killed = session.execute_command(  # type: ignore[no-untyped-call]
                "CLIENT", "KILL", "USER", username
            )
            _require(type(killed) is int and killed >= 0, "candidates-disabled")
    except ActivationRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse("candidates-disabled")


def open_admission(
    session: Redis,
    *,
    manifest: Manifest,
    permit: CommissioningPermit,
    qualification_generation: str,
    trusted_max_readiness_ms: int,
    provisioner_username: str,
) -> tuple[bytes, bytes]:
    """Clause 6 step 5 on a provisioner session: the selection's one predeclared switch.

    Before the compare and set the provisioner reads back, with its own read
    rights, that M is stored unchanged, the journal is ``closed`` and the
    stored Qualification and Readiness are the ones the selection names and
    that the unchanged ``validate_authority`` accepts at broker time. The
    selection must be exactly the closed record the provisioner installed;
    One script replaces it only while it is byte equal. An already opened
    selection is a replay. Returns the closed and opened bytes.
    @spec PROTECTED-COMMISSIONING-6.
    """
    step = "admission-open"
    try:
        _require(type(permit) is CommissioningPermit, step)
        _scalar(qualification_generation, "generation")
        _require(type(trusted_max_readiness_ms) is int and trusted_max_readiness_ms > 0, step)
        m = _pinned(session, manifest, step, provisioner_username)
        key = _SELECTION + m["runtime_id"]
        _require(
            session.get("protected:control:manifest:" + manifest.digest)
            == manifest.canonical_bytes,
            step,
        )
        journal = session.get(campaign_keys(permit).journal)
        _require(journal is not None and _closed(_text(journal), permit), step)
        q_raw = session.get(
            "protected:control:qualification:"
            + m["qualification_id"]
            + ":"
            + qualification_generation
        )
        r_raw = session.get(
            "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
        )
        _require(type(q_raw) is bytes and type(r_raw) is bytes, step)
        assert isinstance(q_raw, bytes) and isinstance(r_raw, bytes)
        qualification, readiness = parse_qualification(q_raw), parse_readiness(r_raw)
        seconds, micros = session.time()
        validate_authority(
            manifest,
            qualification,
            readiness,
            broker_identity=m["broker_identity"],
            broker_now_ms=int(seconds) * 1000 + int(micros) // 1000,
            trusted_max_readiness_ms=trusted_max_readiness_ms,
        )
        _require(
            qualification.as_dict()["qualification_generation"] == qualification_generation,
            step,
        )
        stored = session.get(key)
        _require(type(stored) is bytes, step)
        assert isinstance(stored, bytes)
        expected = {
            "schema_version": 1,
            "runtime_id": m["runtime_id"],
            "runtime_generation": m["runtime_generation"],
            "manifest_digest": manifest.digest,
            "qualification_id": m["qualification_id"],
            "qualification_generation": qualification_generation,
            "broker_run_id": m["broker_identity"]["run_id"],
        }
        closed = _encode(dict(expected, admission_open=False))
        opened = opened_selection(closed)
        if stored == opened:
            return closed, opened
        _require(stored == closed, step)
        reply = session.execute_command(  # type: ignore[no-untyped-call]
            "EVAL", _SELECTION_CAS, 1, key, closed, opened
        )
        _require(reply in (b"set", "set") and session.get(key) == opened, step)
        return closed, opened
    except ActivationRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse(step)


def close_admission(
    session: Redis,
    *,
    manifest: Manifest,
    closed: bytes,
    opened: bytes,
    provisioner_username: str,
) -> None:
    """The reverse compare and set, only from the exact bytes this activation wrote.

    @spec PROTECTED-COMMISSIONING-6.
    """
    step = "tuple-unchanged"
    try:
        _require(opened_selection(closed) == opened, step)
        m = _pinned(session, manifest, step, provisioner_username)
        key = _SELECTION + m["runtime_id"]
        reply = session.execute_command(  # type: ignore[no-untyped-call]
            "EVAL", _SELECTION_CAS, 1, key, opened, closed
        )
        _require(reply in (b"set", "set") or session.get(key) == closed, step)
    except ActivationRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse(step)


@dataclass(frozen=True, slots=True, repr=False)
class ActivationReport:
    """What activation wrote and observed; a report, never a capability."""

    selection_before: bytes
    selection_after: bytes
    acceptance: CommissioningAcceptance
    publication: Publication

    def __repr__(self) -> str:
        return "ActivationReport()"


def _acceptance(
    verifier: Any, evidence: str, manifest: Manifest, generation: str, now_ms: int
) -> CommissioningAcceptance:
    t, m = verifier.installation, manifest.as_dict()
    value = {
        "schema_version": 1,
        "record_type": "commissioning_acceptance",
        "campaign": {
            "campaign_id": t.campaign_id,
            "campaign_generation": t.campaign_generation,
            "permit_digest": t.permit,
            "bootstrap_digest": t.bootstrap,
            "manifest_digest": t.manifest,
            "runtime_id": m["runtime_id"],
            "runtime_generation": m["runtime_generation"],
            "broker_run_id": t.broker_run_id,
        },
        "evidence_digest": evidence,
        "qualification_id": m["qualification_id"],
        "qualification_generation": generation,
        "accepted_at_ms": str(now_ms),
    }
    return parse_commissioning_acceptance(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    )


class CommissioningActivation:
    """One activation of one accepted campaign, run once.

    The collaborators are each principal's own handle: ``journal`` the
    control's ``CommissioningJournal``, ``provisioner`` a pinned provisioner
    session, ``acl`` the design decision 7 ``AclPrincipalStateProducer``,
    ``issuer`` the issuer's ``CommissioningSession`` and ``verifier`` the
    campaign's ``CommissioningVerifier``. ``settled`` raises unless the
    campaign's settlement and absence are confirmed now.
    """

    def __init__(
        self,
        *,
        permit: CommissioningPermit,
        manifest: Manifest,
        bootstrap: CommissioningBootstrap,
        trusted_issuer: dict[str, Any],
        trusted_control: dict[str, Any],
        max_permit_ms: int,
        trusted_max_readiness_ms: int,
        qualification_generation: str,
        journal: Any,
        settled: Callable[[], Awaitable[None]],
        provisioner: Redis,
        trusted_provisioner: str,
        acl: Any,
        issuer: Any,
        verifier: Any,
    ) -> None:
        try:
            _require(type(permit) is CommissioningPermit and type(manifest) is Manifest)
            _require(type(bootstrap) is CommissioningBootstrap)
            _require(type(max_permit_ms) is int and max_permit_ms > 0)
            _require(type(trusted_max_readiness_ms) is int and trusted_max_readiness_ms > 0)
            _scalar(qualification_generation, "generation")
            _require(type(trusted_issuer) is dict and type(trusted_control) is dict)
            # B names no provisioner credential: the administrator's grant does.
            # It is never one of the principals the observer checks.
            _require(type(trusted_provisioner) is str and bool(trusted_provisioner))
            _require(trusted_provisioner not in _usernames(bootstrap).values())
            journal_permit = getattr(journal, "permit", None)
            _require(type(journal_permit) is CommissioningPermit)
            assert isinstance(journal_permit, CommissioningPermit)
            _require(journal_permit.digest == permit.digest)
            _require(callable(settled))
            for handle, names in (
                (journal, ("status", "confirm_closed")),
                (acl, ("require",)),
                (issuer, ("broker_time_ms",)),
                (verifier, ("decide", "publish", "confirm_unchanged")),
            ):
                _require(all(callable(getattr(handle, name, None)) for name in names))
            _require(verifier.installation.permit == permit.digest)
        except ActivationRefused:
            raise
        except Exception:  # noqa: BLE001  One redacted refusal.
            _refuse()
        self._permit, self._manifest, self._bootstrap = permit, manifest, bootstrap
        self._trusted: dict[str, Any] = {
            "permit": permit,
            "manifest": manifest,
            "bootstrap": bootstrap,
            "trusted_issuer": trusted_issuer,
            "trusted_control": trusted_control,
            "max_permit_ms": max_permit_ms,
        }
        self._max_readiness_ms = trusted_max_readiness_ms
        self._generation = qualification_generation
        self._journal, self._settled = journal, settled
        self._provisioner, self._acl = provisioner, acl
        self._provisioner_username = trusted_provisioner
        self._issuer, self._verifier = issuer, verifier
        self._started = False

    def __repr__(self) -> str:
        return "CommissioningActivation()"

    async def activate(self) -> ActivationReport:
        """Run clause 6 once; refuses with the step it stopped at."""
        _require(not self._started, "journal-closed")
        self._started = True
        await self._close_journal()
        await self._disable_candidates()
        verdict, acceptance = await self._accept()
        # From here the writes run to completion even if the caller is
        # cancelled, however often: a compare and set already sent to the broker
        # must still be followed by the tuple re-read and, if needed, its reverse.
        task = asyncio.create_task(self._complete(verdict, acceptance))
        cancelled = False
        while True:
            try:
                report = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                if task.done():
                    raise
                cancelled = True
            except ActivationRefused:
                if cancelled:
                    raise asyncio.CancelledError() from None
                raise
        if cancelled:
            raise asyncio.CancelledError()
        return report

    async def _step(self, step: str, call: Awaitable[Any]) -> Any:
        try:
            return await call
        except ActivationRefused as refused:
            _refuse(refused.step if refused.step == step else step)
        except Exception:  # noqa: BLE001  Collaborator diagnostics never escape.
            _refuse(step)

    async def _close_journal(self) -> None:
        """Step 1, only from ``closing``.

        Activation never begins a close; the supervisor does. A journal already
        ``closed`` means an earlier activation passed step 1 and then failed or
        was interrupted, so it refuses: clause 6 blocks activation after any
        restart, and a retry would otherwise let new verdicts be tried until one
        passes.
        """
        step = "journal-closed"
        state = await self._step(step, self._journal.status())
        _require(state.state == "closing", step)
        await self._step(step, self._settled())
        state = await self._step(step, self._journal.confirm_closed(state.generation))
        _require(state.state == "closed", step)

    async def _disable_candidates(self) -> None:
        """Step 2, then the decision 7 observation of the expected phase."""
        step = "candidates-disabled"
        await self._step(
            step,
            asyncio.to_thread(
                disable_candidates,
                self._provisioner,
                self._bootstrap,
                manifest=self._manifest,
                provisioner_username=self._provisioner_username,
            ),
        )
        await self._step(step, self._acl.require(ACTIVATED))

    async def _accept(self) -> tuple[Any, CommissioningAcceptance]:
        """Step 3: the verifier decides, then the issuer writes the acceptance."""
        step = "accepted"
        verdict = await self._step(step, self._verifier.decide())
        evidence = getattr(verdict, "evidence_digest", None)
        _require(getattr(verdict, "accepted", False) is True and type(evidence) is str, step)
        assert isinstance(evidence, str)
        now_ms = await self._step(step, self._issuer.broker_time_ms())
        try:
            acceptance = _acceptance(
                self._verifier, evidence, self._manifest, self._generation, now_ms
            )
        except Exception:  # noqa: BLE001
            _refuse(step)
        await self._step(step, issue_acceptance(self._issuer, acceptance, **self._trusted))
        return verdict, acceptance

    async def _complete(
        self, verdict: Any, acceptance: CommissioningAcceptance
    ) -> ActivationReport:
        """Steps 4 and 5, then the verifier's re-read of the tuple."""
        publication = await self._step("published", self._verifier.publish(verdict, acceptance))
        _require(
            type(publication) is Publication and publication.outcome == "published", "published"
        )
        closed, opened = await self._step(
            "admission-open",
            asyncio.to_thread(
                open_admission,
                self._provisioner,
                manifest=self._manifest,
                permit=self._permit,
                qualification_generation=self._generation,
                trusted_max_readiness_ms=self._max_readiness_ms,
                provisioner_username=self._provisioner_username,
            ),
        )
        try:
            reason = await self._verifier.confirm_unchanged()
        except Exception:  # noqa: BLE001  An unavailable re-read never confirms the tuple.
            reason = "unavailable"
        if reason is not None:
            await self._step(
                "tuple-unchanged",
                asyncio.to_thread(
                    close_admission,
                    self._provisioner,
                    manifest=self._manifest,
                    closed=closed,
                    opened=opened,
                    provisioner_username=self._provisioner_username,
                ),
            )
            _refuse("tuple-unchanged")
        return ActivationReport(closed, opened, acceptance, publication)
