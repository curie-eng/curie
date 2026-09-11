"""``python -m curie_test_support.interaction``: the harness over a pipe.

NDJSON in, NDJSON out, one object per verb, in order, flushed as it goes. Line
per verb rather than one document at the end because an agent streaming the
harness needs the result of step 1 before it writes step 2, and a buffered
single document is unusable for that.

Why a ``python -m`` entry point and not a ``curie`` subcommand: a ``curie`` verb
would make this change CLI-surface-bearing and pull the ``local`` and
``local-release`` E2E tiers into a stage that alters no runtime behavior. The
module entry point gives an agent the same scriptable surface with no
released-binary identity, and is reachable only from a source checkout with the
dev dependency group installed.

Failure framing is the load-bearing part. An unknown verb exits non-zero, names
itself on stderr, and emits NO object claiming success -- an agent that reads a
partial success and believes an approval was sent when it was not is exactly the
class of false proof this stage exists to prevent. A ``HarnessTimeout`` is
reported as a structured object naming the verb and the bound, not as a killed
process with a traceback the caller has to regex.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from typing import Any

from .faults import SUPPORTED_FAULTS
from .harness import HarnessTimeout, InteractionHarness, UncapturedAction
from .results import FaultResult, Snapshot

# Named predicates: a scripted ``await_outcome`` cannot send a Python callable
# over a pipe, and accepting a source string to ``eval`` would make the entry
# point an arbitrary-code surface for the sake of a test helper. The vocabulary
# is closed for the same reason the fault set is -- a typo must fail loudly
# rather than wait for a predicate that can never be true.
_PREDICATES: dict[str, Callable[[Snapshot], bool]] = {
    "never": lambda snapshot: False,
    "always": lambda snapshot: True,
    "any_message": lambda snapshot: bool(snapshot.messages),
    "any_action": lambda snapshot: any(message.actions for message in snapshot.messages),
    "any_stream_entry": lambda snapshot: snapshot.stream_entries > 0,
    "approval_settled": lambda snapshot: snapshot.approval_status not in (None, "pending"),
}

_VERBS = (
    "send",
    "messages",
    "act",
    "await_outcome",
    "reset",
    "create_approval",
    "approval",
    "audit",
    "resume_turns",
    "arm_fault",
    "disarm_fault",
)

_EXIT_UNKNOWN_VERB = 2
_EXIT_VERB_FAILED = 1


def _emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def _dispatch(harness: InteractionHarness, verb: str, request: dict[str, Any]) -> Any:
    if verb == "send":
        return harness.send(
            str(request.get("text", "")),
            thread=request.get("thread"),
            deadline_s=float(request.get("deadline_s", 60.0)),
        )
    if verb == "arm_fault":
        # The pipe's form of ``inject_fault``. A context manager cannot cross a
        # line-per-verb boundary, so without this pair an agent scripting the
        # harness could arm no fault at all and every failure case the harness
        # can express was in-process-only. ``disarm_fault`` is the other half,
        # and ``__exit__`` still reverts anything a script leaves armed.
        name = str(request["fault"])
        harness.arm_fault(name, **dict(request.get("options") or {}))
        return FaultResult(fault=name, armed=True, armed_faults=harness.armed_faults)
    if verb == "disarm_fault":
        return harness.disarm_fault(str(request["fault"]))
    if verb == "messages":
        return harness.messages()
    if verb == "reset":
        return harness.reset(deadline_s=float(request.get("deadline_s", 30.0)))
    if verb == "resume_turns":
        return harness.resume_turns()
    if verb == "create_approval":
        return harness.create_approval(**dict(request.get("body") or {}))
    if verb == "approval":
        return harness.approval(str(request["approval_id"]))
    if verb == "audit":
        return harness.audit(str(request["approval_id"]))
    if verb == "await_outcome":
        name = str(request.get("predicate", "always"))
        predicate = _PREDICATES.get(name)
        if predicate is None:
            raise ValueError(
                f"unknown predicate {name!r}; supported predicates are: "
                f"{', '.join(sorted(_PREDICATES))}"
            )
        return harness.await_outcome(
            predicate=predicate, deadline_s=float(request.get("deadline_s", 30.0))
        )
    if verb == "act":
        # The captured action is looked up by its HANDLE -- the unguessable
        # token the harness minted for that one action when it captured it off
        # the real render, and which only a prior ``messages`` call can have
        # told the caller.
        #
        # This is the identity rule expressed over a pipe. In process, ``act``
        # matches on object identity, so a caller who hand-builds a
        # field-identical ``CapturedAction`` is refused. Looking the stored
        # object up from the request's OTHER fields reopened exactly that hole
        # at the process boundary: the identity check was then handed the real
        # stored object and could not fail, while every field it was found by
        # is reconstructible without a card ever rendering -- the action id is
        # an importable renderer constant, the message id follows the harness's
        # ``1700.%04d`` scheme, and ``value`` was optional. A caller who never
        # called ``messages`` could click. The handle is the one field that
        # cannot be reconstructed, so it is the only one the lookup trusts.
        #
        # ``message`` is still required and still checked, against the handle's
        # own message: a request that names the wrong card is a scripting bug
        # worth reporting, not something to silently correct.
        handle = request.get("handle")
        if not isinstance(handle, str) or not handle:
            raise UncapturedAction(
                "act() refuses a request with no 'handle': an action is "
                "identified over the pipe by the unguessable handle that "
                "messages() emitted for it, never by its action id, message id "
                "or value -- all three are reconstructible without the card "
                "ever rendering, which is the shortcut act() exists to refuse. "
                "Call the 'messages' verb and pass the action's 'handle'."
            )
        message_id = str(request["message"])
        captured = next(
            (
                action
                for message in harness.messages().messages
                for action in message.actions
                if action.handle == handle
            ),
            None,
        )
        if captured is None:
            raise UncapturedAction(
                f"act() refuses the handle {handle!r} on message {message_id!r}: "
                "no action captured on this harness carries it. A handle is "
                "minted per captured action, so one that was fabricated, or "
                "taken from a different harness process, has nothing to name."
            )
        return harness.act(
            message=message_id,
            action=captured,
            actor=str(request["actor"]),
            note=request.get("note"),
            deadline_s=float(request.get("deadline_s", 30.0)),
        )
    raise AssertionError(f"verb {verb!r} passed validation but has no handler")


def _error_payload(exc: BaseException) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": type(exc).__name__, "message": str(exc)}
    if isinstance(exc, HarnessTimeout):
        payload["verb"] = exc.verb
        payload["what"] = exc.what
        payload["deadline_s"] = exc.deadline_s
        payload["elapsed_s"] = exc.elapsed_s
    return payload


def main(argv: list[str] | None = None) -> int:
    """Read, execute and answer ONE line at a time, never the whole script first.

    Line-at-a-time is load bearing, not a micro-optimisation. ``act`` now takes
    the unguessable handle ``messages`` minted, and a handle is minted per
    captured action inside THIS process -- so a caller has to read the
    ``messages`` answer before it can write the ``act`` line. Reading all of
    stdin up front made that impossible and would have left the pipe's only
    honest ``act`` route unreachable, which is the pressure that pushes a
    provenance rule back into a guessable one. The answer to each verb is
    flushed as it goes, so a caller holding the other end of the pipe has it
    before it writes the next line.
    """

    harness: InteractionHarness | None = None
    try:
        for number, line in enumerate(sys.stdin, start=1):
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
            except ValueError as exc:
                sys.stderr.write(f"line {number} is not valid JSON: {exc}\n")
                return _EXIT_UNKNOWN_VERB
            if not isinstance(parsed, dict) or "verb" not in parsed:
                sys.stderr.write(f"line {number} has no 'verb' key: {line}")
                return _EXIT_UNKNOWN_VERB
            verb = str(parsed["verb"])
            if verb not in _VERBS:
                # Reported BEFORE anything is emitted for this line: no object
                # may claim success for a verb that was rejected.
                sys.stderr.write(
                    f"unknown verb {verb!r}; supported verbs are: {', '.join(_VERBS)}\n"
                    f"(supported faults are: {', '.join(SUPPORTED_FAULTS)})\n"
                )
                return _EXIT_UNKNOWN_VERB
            if harness is None:
                harness = InteractionHarness()
                harness.__enter__()
            try:
                result = _dispatch(harness, verb, parsed)
            except Exception as exc:  # noqa: BLE001 - reported, not swallowed
                _emit({"verb": verb, "ok": False, "error": _error_payload(exc)})
                return _EXIT_VERB_FAILED
            _emit({"verb": verb, "ok": True, "result": result.to_dict()})
    finally:
        if harness is not None:
            harness.__exit__(None, None, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
