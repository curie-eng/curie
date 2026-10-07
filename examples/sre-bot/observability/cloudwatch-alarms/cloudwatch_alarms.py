"""Optional CloudWatch source; @spec SRE-CW-1 through SRE-CW-6.

Contract: examples/sre-bot/docs/CLOUDWATCH-ALARMS.md.
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import hmac
import math
import os
import pathlib
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FORM = "application/x-www-form-urlencoded; charset=utf-8"
STS_NS = {"sts": "https://sts.amazonaws.com/doc/2011-06-15/"}
MONITORING_NS = {"cw": "http://monitoring.amazonaws.com/doc/2010-08-01/"}
SESSION_NAME = "curie-cloudwatch-alarms"
# Assume again once fewer than this many seconds of the credentials remain.
REFRESH_MARGIN = datetime.timedelta(seconds=300)
HTTP_TIMEOUT = 20

DEFAULT_METRIC_PREFIX = "curie_cloudwatch"
METRIC_PREFIX_PATTERN = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")

Http = Callable[[str, str, dict[str, str], bytes], tuple[int, bytes]]


@dataclasses.dataclass(frozen=True)
# @spec SRE-CW-2 SRE-CW-5
class Credentials:
    access_key: str
    secret_key: str = dataclasses.field(repr=False)
    session_token: str = dataclasses.field(repr=False)
    expiration: datetime.datetime


# @spec SRE-CW-5
class Refused(Exception):
    """A non-200 answer. It carries the status only: an error body can quote the request."""

    # @spec SRE-CW-1 SRE-CW-5
    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = status


# --- signing ------------------------------------------------------------------------


# @spec SRE-CW-2
def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="-_.~")


# @spec SRE-CW-2
def _hmac(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def sigv4_headers(
    method: str,
    url: str,
    body: bytes,
    region: str,
    service: str,
    access_key: str,
    secret_key: str,
    session_token: str | None,
    now: datetime.datetime,
) -> dict[str, str]:
    """@spec SRE-CW-2."""
    parts = urllib.parse.urlsplit(url)
    amz_date = now.astimezone(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    headers = {"Content-Type": FORM, "Host": parts.netloc, "X-Amz-Date": amz_date}
    if session_token is not None:
        headers["X-Amz-Security-Token"] = session_token
    signed = {name.lower(): " ".join(value.split()) for name, value in headers.items()}
    names = sorted(signed)
    query = "&".join(
        f"{_quote(k)}={_quote(v)}"
        for k, v in sorted(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    )
    canonical = "\n".join(
        [
            method,
            parts.path or "/",
            query,
            "".join(f"{n}:{signed[n]}\n" for n in names),
            ";".join(names),
            hashlib.sha256(body).hexdigest(),
        ]
    )
    scope = f"{amz_date[:8]}/{region}/{service}/aws4_request"
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    key = ("AWS4" + secret_key).encode()
    for part in (amz_date[:8], region, service, "aws4_request"):
        key = _hmac(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={';'.join(names)}, Signature={signature}"
    )
    return headers


# --- reading AWS's answers ----------------------------------------------------------


# @spec SRE-CW-2
def parse_credentials(xml: bytes) -> Credentials:
    result = ET.fromstring(xml).find("sts:AssumeRoleWithWebIdentityResult/sts:Credentials", STS_NS)
    if result is None:
        raise ValueError("no Credentials in the AssumeRoleWithWebIdentity answer")
    fields = {
        tag: result.findtext(f"sts:{tag}", "", STS_NS)
        for tag in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
    }
    if not all(fields.values()):
        raise ValueError("Credentials without all of its fields")
    expiration = datetime.datetime.fromisoformat(fields["Expiration"].replace("Z", "+00:00"))
    if expiration.tzinfo is None:
        raise ValueError("an Expiration with no time zone")
    return Credentials(
        fields["AccessKeyId"], fields["SecretAccessKey"], fields["SessionToken"], expiration
    )


def parse_alarms(xml: bytes) -> tuple[list[tuple[str, str, str]], str | None]:
    """@spec SRE-CW-2 SRE-CW-4."""
    result = ET.fromstring(xml).find("cw:DescribeAlarmsResult", MONITORING_NS)
    if result is None:
        # Not an empty page: read as one, it would resolve every alert.
        raise ValueError("no DescribeAlarmsResult in the DescribeAlarms answer")
    # Direct members only: an alarm's own Dimensions, Metrics and actions hold members too.
    members = result.findall("cw:MetricAlarms/cw:member", MONITORING_NS) + result.findall(
        "cw:CompositeAlarms/cw:member", MONITORING_NS
    )
    # `disable-alarm-actions` is how on-call stops an alarm's email; the bot's copy stops with it.
    alarms = [
        (
            m.findtext("cw:AlarmName", "", MONITORING_NS),
            m.findtext("cw:AlarmDescription", "", MONITORING_NS),
            m.findtext("cw:StateTransitionedTimestamp", "", MONITORING_NS),
        )
        for m in members
        if (m.findtext("cw:ActionsEnabled", "", MONITORING_NS) or "").strip().lower() != "false"
    ]
    return alarms, result.findtext("cw:NextToken", None, MONITORING_NS) or None


# --- the metrics --------------------------------------------------------------------


def _validate_prefix(metric_prefix: str) -> None:
    """@spec SRE-CW-1."""
    if not METRIC_PREFIX_PATTERN.fullmatch(metric_prefix):
        raise ValueError("METRIC_PREFIX")


# @spec SRE-CW-4
def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_metrics(
    alarms: list[tuple[str, str, str]],
    poll_ok: bool,
    last_success: float | None,
    metric_prefix: str = DEFAULT_METRIC_PREFIX,
) -> str:
    """@spec SRE-CW-1 SRE-CW-3 SRE-CW-4."""
    _validate_prefix(metric_prefix)
    in_alarm = f"{metric_prefix}_alarm_in_alarm"
    poll = f"{metric_prefix}_alarm_poll_ok"
    last = f"{metric_prefix}_alarm_last_success_timestamp_seconds"
    lines = [
        f"# HELP {in_alarm} 1 for each CloudWatch alarm in ALARM "
        "that publishes to the alert topic, "
        "as of the last successful poll.",
        f"# TYPE {in_alarm} gauge",
    ]
    # A description is a label, so it is one line.
    normalized = {
        (name, " ".join(description.split()), state_transitioned_at.strip())
        for name, description, state_transitioned_at in alarms
    }
    for name, description, state_transitioned_at in sorted(normalized):
        lines.append(
            f'{in_alarm}{{alarm="{_label(name)}",description="{_label(description)}",'
            f'state_transitioned_at="{_label(state_transitioned_at)}"}} 1'
        )
    lines += [
        f"# HELP {poll} 1 if the last poll of CloudWatch succeeded, "
        "0 if it failed or none has finished.",
        f"# TYPE {poll} gauge",
        f"{poll} {1 if poll_ok else 0}",
        f"# HELP {last} Unix time of the last successful poll of CloudWatch.",
        f"# TYPE {last} gauge",
    ]
    if last_success is not None:
        lines.append(f"{last} {float(last_success)!r}")
    return "\n".join(lines) + "\n"


# --- the poll -----------------------------------------------------------------------


# @spec SRE-CW-2 SRE-CW-3
class Poller:
    # @spec SRE-CW-1 SRE-CW-5
    def __init__(
        self,
        topic_arn: str,
        region: str,
        role_arn: str,
        token_file: str,
        http: Http,
        clock: Callable[[], datetime.datetime],
        metric_prefix: str = DEFAULT_METRIC_PREFIX,
    ):
        _validate_prefix(metric_prefix)
        self.metric_prefix = metric_prefix
        self.topic_arn, self.region, self.role_arn = topic_arn, region, role_arn
        self.token_file = pathlib.Path(token_file)
        self.http, self.clock = http, clock
        self._credentials: Credentials | None = None
        # (alarms, poll_ok, last success), replaced whole, so the server never reads half a poll.
        self._state: tuple[list[tuple[str, str, str]], bool, float | None] = ([], False, None)

    def poll_once(self) -> None:
        """@spec SRE-CW-3 SRE-CW-5."""
        step = "assume"
        try:
            credentials = self._current_credentials()
            step = "describe"
            alarms = self._describe(credentials)
        except Exception as exc:  # noqa: BLE001 -- the reader serves on whatever failed
            detail = f"HTTP {exc.status}" if isinstance(exc, Refused) else type(exc).__name__
            print(
                f"cloudwatch-alarms: poll failed at {step}: {detail}", file=sys.stderr, flush=True
            )
            self._state = (self._state[0], False, self._state[2])
            return
        self._state = (alarms, True, self.clock().timestamp())

    # @spec SRE-CW-3 SRE-CW-4
    def metrics(self) -> str:
        return render_metrics(*self._state, metric_prefix=self.metric_prefix)

    # @spec SRE-CW-2
    def _current_credentials(self) -> Credentials:
        if (
            self._credentials is None
            or self._credentials.expiration - self.clock() < REFRESH_MARGIN
        ):
            self._credentials = self._assume()
        return self._credentials

    # @spec SRE-CW-2
    def _assume(self) -> Credentials:
        # The kubelet rotates this token. The STS web-identity call is unsigned.
        body = urllib.parse.urlencode(
            {
                "Action": "AssumeRoleWithWebIdentity",
                "Version": "2011-06-15",
                "RoleArn": self.role_arn,
                "RoleSessionName": SESSION_NAME,
                "WebIdentityToken": self.token_file.read_text().strip(),
            }
        ).encode()
        status, answer = self.http(
            "POST", f"https://sts.{self.region}.amazonaws.com/", {"Content-Type": FORM}, body
        )
        if status != 200:
            raise Refused(status)
        return parse_credentials(answer)

    # @spec SRE-CW-2 SRE-CW-3
    def _describe(self, credentials: Credentials) -> list[tuple[str, str, str]]:
        url = f"https://monitoring.{self.region}.amazonaws.com/"
        alarms, next_token = [], None
        while True:
            fields = [
                ("Action", "DescribeAlarms"),
                ("Version", "2010-08-01"),
                ("StateValue", "ALARM"),
                ("ActionPrefix", self.topic_arn),
                ("AlarmTypes.member.1", "MetricAlarm"),
                ("AlarmTypes.member.2", "CompositeAlarm"),
                ("MaxRecords", "100"),
            ]
            if next_token is not None:
                fields.append(("NextToken", next_token))
            body = urllib.parse.urlencode(fields).encode()
            headers = sigv4_headers(
                "POST",
                url,
                body,
                self.region,
                "monitoring",
                credentials.access_key,
                credentials.secret_key,
                credentials.session_token,
                self.clock(),
            )
            status, answer = self.http("POST", url, headers, body)
            if status != 200:
                raise Refused(status)
            page, next_token = parse_alarms(answer)
            alarms += page
            if next_token is None:
                return alarms


# --- serving ------------------------------------------------------------------------


def make_server(listen_addr: str, poller: Poller) -> ThreadingHTTPServer:
    """@spec SRE-CW-6."""

    # @spec SRE-CW-6
    class Handler(BaseHTTPRequestHandler):
        # Without one, each idle connection holds a server thread for good.
        timeout = 10

        # @spec SRE-CW-5
        def log_message(self, format: str, *args: object) -> None:
            return

        # @spec SRE-CW-6
        def do_GET(self) -> None:  # noqa: N802
            if urllib.parse.urlsplit(self.path).path != "/metrics":
                self.send_error(404)
                return
            body = poller.metrics().encode()
            self.send_response(200)
            # Prometheus 3 refuses a scrape whose Content-Type it cannot read.
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    host, port = listen_addr.rsplit(":", 1)
    return ThreadingHTTPServer((host, int(port)), Handler)


# @spec SRE-CW-2 SRE-CW-5
def _http(method: str, url: str, headers: dict[str, str], body: bytes) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, exc.read()


def install_signal_handlers() -> None:
    """@spec SRE-CW-5.

    Python is PID 1 in the container, and a PID namespace's init is never sent
    a signal left at its default disposition, so without this a rollout waits
    out the grace period and ends in SIGKILL. SystemExit is not an Exception,
    so a poll in flight does not swallow it.
    """
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))


# @spec SRE-CW-1 SRE-CW-5 SRE-CW-6
def main() -> int:
    install_signal_handlers()
    required = ("TOPIC_ARN", "AWS_REGION", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        print(f"cloudwatch-alarms: {', '.join(missing)} not set", file=sys.stderr)
        return 2
    try:
        interval = float(os.environ.get("POLL_SECONDS", "60"))
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("POLL_SECONDS")
        metric_prefix = os.environ.get("METRIC_PREFIX", DEFAULT_METRIC_PREFIX)
        _validate_prefix(metric_prefix)
        listen_addr = os.environ.get("LISTEN_ADDR", "0.0.0.0:9108")
        host, port = listen_addr.rsplit(":", 1)
        if not host or not 0 <= int(port) <= 65535:
            raise ValueError("LISTEN_ADDR")
    except ValueError:
        print("cloudwatch-alarms: invalid configuration", file=sys.stderr)
        return 2
    poller = Poller(
        topic_arn=os.environ["TOPIC_ARN"],
        region=os.environ["AWS_REGION"],
        role_arn=os.environ["AWS_ROLE_ARN"],
        token_file=os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"],
        http=_http,
        clock=lambda: datetime.datetime.now(datetime.UTC),
        metric_prefix=metric_prefix,
    )
    server = make_server(listen_addr, poller)
    # Serving before the first poll, so poll_ok 0 is what a reader that never reads shows.
    threading.Thread(target=server.serve_forever, daemon=True).start()
    while True:
        poller.poll_once()
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
