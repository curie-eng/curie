"""Report each Slack identity's installation to the platform API (#3039, ADR 0198 decision 4).

The API cannot call Slack for an identity: it does not hold the token. So each
identity's own ``auth.test`` answer, made with that identity's bot token, is
reported to ``POST /identity/slack-reports``, and the API attaches the identity
to its installation and records the non-Grid evidence the mention path needs.

This runs in the background, one daemon thread per identity, retrying with
backoff until the API takes the report, refuses it for good (401, 403 or
422, logged at ERROR once), or the dispatcher stops. It is
independent of preflight, which calls ``auth.test`` only when several
identities are declared, and it never blocks startup or Slack traffic. A
restart re-reports, which is how a changed token's evidence comes back.

Only ids are logged, never a token: an exception is logged by its type alone.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from typing import Any, Literal

import httpx
from curie_telemetry import inject_trace_context
from slack_sdk.web import WebClient

report_logger = logging.getLogger("curie_dispatcher.installation_report")

# Every phase named, as in admission. Off every Slack path, so this only bounds
# how long one reporter thread waits before its next retry.
_REPORT_TIMEOUT = httpx.Timeout(connect=0.5, read=1.5, write=0.5, pool=0.5)

ReportOutcome = Literal["ok", "retry", "permanent"]
# The same report with the same key cannot succeed: no auth, or a body the API
# will always refuse.
_PERMANENT_STATUSES = frozenset({401, 403, 422})


def _response_data(response: object) -> Mapping[str, Any] | None:
    # A ``SlackResponse`` keeps the parsed body on ``data``; a plain mapping is
    # accepted too.
    data = getattr(response, "data", response)
    return data if isinstance(data, Mapping) else None


def auth_test_report(identity_name: str, response: object) -> dict[str, Any] | None:
    """The report body for one identity's ``auth.test`` answer, or None.

    None when the answer is not ok, or is malformed: no team, or a present
    ``enterprise_id`` that is neither a string nor null, which is neither Grid
    nor non-Grid and so is not reported. It carries exactly what Slack said.
    ``enterprise_id_present`` records whether ``enterprise_id`` was sent (an
    ordinary workspace omits it); ``is_enterprise_install`` is kept only as a
    strict bool, so a string or a number is no evidence at all (None).
    """
    data = _response_data(response)
    if data is None or data.get("ok") is not True:
        return None
    team_id = data.get("team_id")
    if not isinstance(team_id, str) or not team_id:
        return None
    enterprise_id = data.get("enterprise_id")
    if enterprise_id is not None and not isinstance(enterprise_id, str):
        return None
    is_enterprise_install = data.get("is_enterprise_install")
    return {
        "name": identity_name,
        "team_id": team_id,
        "enterprise_id": enterprise_id,
        "enterprise_id_present": "enterprise_id" in data,
        "is_enterprise_install": (
            is_enterprise_install if isinstance(is_enterprise_install, bool) else None
        ),
    }


class InstallationReportClient:
    """Thin client for the API's ``POST /identity/slack-reports``."""

    def __init__(self, api_base_url: str, api_key: str, client: httpx.Client | None = None) -> None:
        self._url = f"{api_base_url.rstrip('/')}/identity/slack-reports"
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client or httpx.Client(timeout=_REPORT_TIMEOUT)

    def report(self, payload: dict[str, Any]) -> ReportOutcome:
        """POST one report and say what to do next. Never raises.

        ``"ok"`` when the API took it. ``"retry"`` for a 404 (the identity is
        not declared yet), a 5xx, any other unexpected status, or a transport
        error. ``"permanent"`` for a 401, 403 or 422: retrying the same report
        with the same key cannot succeed, so it is logged at ERROR once.

        A mismatch (the identity is attached to another installation) is an
        answer, not a failure: it is logged at WARNING and not retried.
        """
        return self._report(payload)[0]

    def _report(self, payload: dict[str, Any]) -> tuple[ReportOutcome, int | None]:
        """:meth:`report`, with the HTTP status (None on a transport error)."""
        name = payload.get("name")
        team_id = payload.get("team_id")
        headers = dict(self._headers)
        try:
            inject_trace_context(headers)
            response = self._client.post(self._url, json=payload, headers=headers)
            status = response.status_code
            answer = response.json() if status == 200 else None
        except Exception as exc:  # noqa: BLE001 - this seam promises an outcome
            # Not only HTTPError: a closed client or a non-JSON body can raise
            # anything, and this seam promises an outcome.
            report_logger.warning(
                "slack installation report failed identity=%s error=%s", name, type(exc).__name__
            )
            return "retry", None
        if status in _PERMANENT_STATUSES:
            report_logger.error(
                "slack installation report refused identity=%s status=%s: not retried",
                name,
                status,
            )
            return "permanent", status
        if status != 200:
            report_logger.warning(
                "slack installation report failed identity=%s status=%s", name, status
            )
            return "retry", status
        if isinstance(answer, dict) and answer.get("installation_mismatch") is True:
            report_logger.warning(
                "slack installation report installation_mismatch identity=%s reported_team=%s "
                "provider_installation_id=%s: the identity is attached to another installation",
                name,
                team_id,
                answer.get("provider_installation_id"),
            )
        else:
            report_logger.info("slack installation reported identity=%s team=%s", name, team_id)
        return "ok", status


def _report_until_taken(
    name: str,
    web_client: WebClient,
    client: InstallationReportClient,
    stop_event: threading.Event,
    initial_backoff_s: float,
    max_backoff_s: float,
) -> None:
    backoff = initial_backoff_s
    while not stop_event.is_set():
        try:
            response = web_client.auth_test()
        except Exception as exc:  # noqa: BLE001 - any failure is a retry
            report_logger.warning(
                "slack installation auth.test failed identity=%s error=%s",
                name,
                type(exc).__name__,
            )
        else:
            payload = auth_test_report(name, response)
            if payload is None:
                data = _response_data(response)
                if data is not None and data.get("ok") is True:
                    # Ok but unusable will not fix itself on retry, so it is
                    # terminal for this identity. The shape is logged, never the
                    # values.
                    report_logger.error(
                        "slack installation auth.test malformed identity=%s "
                        "team_id_type=%s enterprise_id_type=%s; not reporting",
                        name,
                        type(data.get("team_id")).__name__,
                        type(data.get("enterprise_id")).__name__,
                    )
                    return
                else:
                    report_logger.warning(
                        "slack installation auth.test not ok identity=%s error=%s",
                        name,
                        data.get("error") if data is not None else None,
                    )
            else:
                outcome, status = client._report(payload)
                if outcome != "retry":
                    return
                if status == 404:
                    # The identity is not declared yet: wait the longest.
                    if stop_event.wait(max_backoff_s):
                        return
                    continue
        if stop_event.wait(backoff):
            return
        backoff = min(backoff * 2, max_backoff_s)


def start_installation_reports(
    identities: Sequence[tuple[str, WebClient]],
    client: InstallationReportClient,
    *,
    stop_event: threading.Event,
    initial_backoff_s: float = 2.0,
    max_backoff_s: float = 60.0,
) -> list[threading.Thread]:
    """Start one daemon reporter thread per ``(identity name, its WebClient)``.

    Each thread retries (``initial_backoff_s`` doubling to ``max_backoff_s``;
    a 404 waits ``max_backoff_s``) until the API takes its report or refuses
    it for good; setting ``stop_event`` ends every thread,
    including one waiting out its backoff.
    """
    threads: list[threading.Thread] = []
    for name, web_client in identities:
        thread = threading.Thread(
            target=_report_until_taken,
            args=(name, web_client, client, stop_event, initial_backoff_s, max_backoff_s),
            name=f"installation-report-{name}",
            daemon=True,
        )
        thread.start()
        threads.append(thread)
    return threads
