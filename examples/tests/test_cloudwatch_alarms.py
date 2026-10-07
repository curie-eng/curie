"""Behavioral contract for the optional CloudWatch source (#4253).

Fake AWS answers prove the reader's request and state handling, not live AWS
qualification. See examples/sre-bot/docs/CLOUDWATCH-ALARMS.md for real tiers.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from xml.sax.saxutils import escape

import pytest
import yaml

OBS = Path(__file__).resolve().parents[1] / "sre-bot" / "observability"
PROGRAM = OBS / "cloudwatch-alarms" / "cloudwatch_alarms.py"
UTC = dt.UTC
NOW = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
REGION = "us-east-1"
TOPIC = "arn:aws:sns:us-east-1:000000000000:example-alert-topic"
ROLE = "arn:aws:iam::000000000000:role/example-read-only-role"
TOKEN = "EXAMPLE-PROJECTED-WEB-TOKEN-ONE"
SECRET = "EXAMPLE-SECRET-KEY-DO-NOT-LOG"
SESSION = "EXAMPLE-SESSION-TOKEN-DO-NOT-LOG"
NEXT = "example+next/page=="
TRANSITION = "2026-10-07T11:59:00Z"


@pytest.fixture
def reader():
    assert PROGRAM.is_file(), "SRE-CW-1: optional generic CloudWatch reader is missing"
    spec = importlib.util.spec_from_file_location("curie_example_cloudwatch", PROGRAM)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def page(
    names=(),
    *,
    token=None,
    disabled=(),
    composite=(),
    description="Example alarm",
    state="ALARM",
    alarm_actions=(TOPIC,),
    ok_actions=(),
    insufficient_actions=(),
):
    """AWS Query XML fixture; nested members deliberately contain no alarms."""

    def action_members(tag, actions):
        return f"<{tag}>" + "".join(f"<member>{escape(a)}</member>" for a in actions) + f"</{tag}>"

    def members(selected):
        return "".join(
            f"<member><AlarmName>{escape(name)}</AlarmName>"
            f"<ActionsEnabled>{str(name not in disabled).lower()}</ActionsEnabled>"
            f"<StateValue>{escape(state)}</StateValue>"
            + action_members("AlarmActions", alarm_actions)
            + action_members("OKActions", ok_actions)
            + action_members("InsufficientDataActions", insufficient_actions)
            + f"<AlarmDescription>{escape(description)}</AlarmDescription>"
            f"<StateTransitionedTimestamp>{TRANSITION}</StateTransitionedTimestamp>"
            "<Dimensions><member><Name>nested-not-an-alarm</Name></member></Dimensions>"
            "<StateReason>Changing reason</StateReason>"
            "<StateUpdatedTimestamp>2026-10-07T12:00:00Z</StateUpdatedTimestamp>"
            "</member>"
            for name in selected
        )

    next_token = f"<NextToken>{escape(token)}</NextToken>" if token else ""
    return (
        '<DescribeAlarmsResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">'
        f"<DescribeAlarmsResult><MetricAlarms>{members(names)}</MetricAlarms>"
        f"<CompositeAlarms>{members(composite)}</CompositeAlarms>{next_token}"
        "</DescribeAlarmsResult></DescribeAlarmsResponse>"
    ).encode()


class Aws:
    """Independent request recorder for only STS and DescribeAlarms."""

    def __init__(self):
        self.now = NOW
        self.requests = []
        self.assumes = []
        self.pages = {None: (200, page(["example-alarm"]))}
        self.assume_failure = None

    def clock(self):
        return self.now

    def __call__(self, method, url, headers, body):
        fields = dict(urllib.parse.parse_qsl(body.decode(), keep_blank_values=True))
        self.requests.append((method, url, headers, fields))
        assert method == "POST"
        if fields.get("Action") == "AssumeRoleWithWebIdentity":
            assert url == f"https://sts.{REGION}.amazonaws.com/"
            assert fields["RoleArn"] == ROLE
            assert fields["Version"] == "2011-06-15"
            assert fields["RoleSessionName"] == "curie-cloudwatch-alarms"
            self.assumes.append(fields)
            if self.assume_failure:
                if isinstance(self.assume_failure, Exception):
                    raise self.assume_failure
                return self.assume_failure
            key = f"AKIDEXAMPLE{len(self.assumes)}"
            expires = (self.now + dt.timedelta(hours=1)).isoformat()
            return 200, (
                '<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                "<AssumeRoleWithWebIdentityResult><Credentials>"
                f"<AccessKeyId>{key}</AccessKeyId><SecretAccessKey>{SECRET}</SecretAccessKey>"
                f"<SessionToken>{SESSION}</SessionToken><Expiration>{expires}</Expiration>"
                "</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
            ).encode()
        assert url == f"https://monitoring.{REGION}.amazonaws.com/"
        assert fields == {
            "Action": "DescribeAlarms",
            "Version": "2010-08-01",
            "StateValue": "ALARM",
            "ActionPrefix": TOPIC,
            "AlarmTypes.member.1": "MetricAlarm",
            "AlarmTypes.member.2": "CompositeAlarm",
            "MaxRecords": "100",
            **({"NextToken": fields["NextToken"]} if "NextToken" in fields else {}),
        }
        lowered = {key.lower(): value for key, value in headers.items()}
        assert lowered["x-amz-security-token"] == SESSION
        auth = lowered["authorization"]
        assert f"Credential=AKIDEXAMPLE{len(self.assumes)}/" in auth
        assert "/us-east-1/monitoring/aws4_request" in auth
        assert "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token" in auth
        result = self.pages[fields.get("NextToken")]
        if isinstance(result, Exception):
            raise result
        return result


def poller(reader, tmp_path, *, metric_prefix="curie_cloudwatch"):
    token_file = tmp_path / "token"
    token_file.write_text(TOKEN + "\n")
    aws = Aws()
    source = reader.Poller(
        topic_arn=TOPIC,
        region=REGION,
        role_arn=ROLE,
        token_file=str(token_file),
        http=aws,
        clock=aws.clock,
        metric_prefix=metric_prefix,
    )
    return source, aws, token_file


def test_sigv4_matches_independent_aws_example(reader):
    # @spec SRE-CW-2. AWS's published IAM ListUsers vector, independently
    # checked with botocore 1.34.137; AWS contract:
    # https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    args = dict(
        method="GET",
        url="https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08",
        body=b"",
        region=REGION,
        service="iam",
        access_key="AKIDEXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        session_token=None,
        now=dt.datetime(2015, 8, 30, 12, 36, tzinfo=UTC),
    )
    headers = reader.sigv4_headers(**args)
    assert (
        "Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        in headers["Authorization"]
    )
    assert "X-Amz-Security-Token" not in headers
    changed = reader.sigv4_headers(**{**args, "body": b"changed", "session_token": SESSION})
    assert changed["Authorization"] != headers["Authorization"]
    assert changed["X-Amz-Security-Token"] == SESSION
    assert "x-amz-security-token" in changed["Authorization"]


def test_complete_pages_filter_disabled_alarms_and_preserve_episode(reader, tmp_path):
    # @spec SRE-CW-2 SRE-CW-3 SRE-CW-4. Query API shape measured by prior
    # reader fixtures; provider contract API_DescribeAlarms.html (spec link).
    source, aws, _ = poller(reader, tmp_path)
    assert "curie_cloudwatch_alarm_poll_ok 0\n" in source.metrics()
    assert not any(
        line.startswith("curie_cloudwatch_alarm_last_success_timestamp_seconds ")
        for line in source.metrics().splitlines()
    )
    assert aws.requests == []
    aws.pages = {
        None: (200, page(["example-alarm", "disabled"], disabled=["disabled"], token=NEXT)),
        NEXT: (200, page(composite=["example-composite"], description="")),
    }
    source.poll_once()
    text = source.metrics()
    assert 'alarm="example-alarm"' in text and 'alarm="example-composite",description=""' in text
    assert "disabled" not in text and "nested-not-an-alarm" not in text
    assert "curie_cloudwatch_alarm_poll_ok 1\n" in text
    assert f"curie_cloudwatch_alarm_last_success_timestamp_seconds {NOW.timestamp()}\n" in text
    assert [fields.get("NextToken") for _, _, _, fields in aws.requests[1:]] == [None, NEXT]
    first = page(["example-alarm"])
    refreshed = first.replace(b"Changing reason", b"New reason").replace(b"12:00:00Z", b"12:01:00Z")
    assert reader.parse_alarms(first, TOPIC) == reader.parse_alarms(refreshed, TOPIC)


@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize(
    "fields, expected",
    [
        ({"alarm_actions": (TOPIC, "arn:aws:sns:us-east-1:000000000000:other")}, True),
        ({"alarm_actions": (), "ok_actions": (TOPIC,)}, False),
        ({"alarm_actions": (), "insufficient_actions": (TOPIC,)}, False),
        ({"alarm_actions": (TOPIC + "-other",)}, False),
        ({"state": "OK"}, False),
    ],
)
def test_provider_candidates_require_exact_alarm_action_and_current_alarm(
    reader, tmp_path, composite, fields, expected
):
    # @spec SRE-CW-2 SRE-CW-3. Exercise the signed poll consumer, not just parser output.
    source, aws, _ = poller(reader, tmp_path)
    names = {"composite": ["candidate"]} if composite else {"names": ["candidate"]}
    aws.pages = {None: (200, page(**names, **fields))}
    source.poll_once()
    assert ('alarm="candidate"' in source.metrics()) == expected
    assert "alarm_poll_ok 1\n" in source.metrics()


def test_missing_description_and_disabled_composite_are_handled(reader):
    # @spec SRE-CW-2 SRE-CW-4.
    response = page(
        ["example-metric"], composite=["example-composite", "disabled"], disabled=["disabled"]
    ).replace(b"<AlarmDescription>Example alarm</AlarmDescription>", b"")
    alarms, token = reader.parse_alarms(response, TOPIC)
    assert sorted(alarms) == [
        ("example-composite", "", TRANSITION),
        ("example-metric", "", TRANSITION),
    ]
    assert token is None


@pytest.mark.parametrize(
    "failure",
    [
        (503, b"provider error body"),
        (200, b"<bad"),
        (200, b"<root/>"),
        TimeoutError("private message"),
    ],
)
def test_failed_later_page_keeps_snapshot_then_recovers(reader, tmp_path, capsys, failure):
    # @spec SRE-CW-3 SRE-CW-5.
    source, aws, _ = poller(reader, tmp_path)
    source.poll_once()
    before = source.metrics()
    aws.now += dt.timedelta(seconds=60)
    aws.pages = {None: (200, page(["partial-new-alarm"], token=NEXT)), NEXT: failure}
    source.poll_once()
    assert source.metrics() == before.replace("alarm_poll_ok 1\n", "alarm_poll_ok 0\n")
    stderr = capsys.readouterr().err
    assert len(stderr.splitlines()) == 1 and "describe" in stderr
    assert "private message" not in stderr and "provider error body" not in stderr
    aws.pages = {None: (200, page())}
    source.poll_once()
    assert 'alarm="' not in source.metrics()
    assert "alarm_poll_ok 1\n" in source.metrics()
    assert str(aws.now.timestamp()) in source.metrics()


def test_rotated_token_and_credentials_are_used_only_near_expiry(reader, tmp_path):
    # @spec SRE-CW-2.
    source, aws, token_file = poller(reader, tmp_path)
    source.poll_once()
    token_file.write_text("EXAMPLE-ROTATED-TOKEN\n")
    aws.now += dt.timedelta(seconds=3300)
    source.poll_once()
    assert len(aws.assumes) == 1  # Exactly five minutes remaining is reusable.
    aws.now += dt.timedelta(seconds=1)
    source.poll_once()
    assert [item["WebIdentityToken"] for item in aws.assumes] == [TOKEN, "EXAMPLE-ROTATED-TOKEN"]
    assert "AKIDEXAMPLE2/" in aws.requests[-1][2]["Authorization"]


@pytest.mark.parametrize("step", ["assume", "describe"])
def test_secret_echoes_are_redacted_and_failures_keep_serving(reader, tmp_path, capsys, step):
    # @spec SRE-CW-3 SRE-CW-5.
    source, aws, token_file = poller(reader, tmp_path)
    source.poll_once()
    prior = source.metrics()
    leak = " ".join([TOKEN, SECRET, SESSION])
    if step == "assume":
        aws.now += dt.timedelta(hours=1)
        aws.assume_failure = urllib.error.URLError(leak)
    else:
        aws.pages = {None: (403, leak.encode())}
    source.poll_once()
    output = capsys.readouterr()
    assert output.out == ""
    assert len(output.err.splitlines()) == 1 and step in output.err
    for secret in (TOKEN, SECRET, SESSION):
        assert secret not in output.err + source.metrics()
    assert source.metrics() == prior.replace("alarm_poll_ok 1\n", "alarm_poll_ok 0\n")
    credential = reader.Credentials("AKIDEXAMPLE", SECRET, SESSION, NOW)
    assert SECRET not in repr(credential) and SESSION not in repr(credential)
    token_file.unlink()
    aws.now += dt.timedelta(hours=1)
    source.poll_once()
    assert "FileNotFoundError" in capsys.readouterr().err


def test_metrics_escape_deduplicate_and_allow_compatible_prefix(reader, tmp_path):
    # @spec SRE-CW-1 SRE-CW-4.
    alarm = (
        'example"alarm\\path\nnext',
        '  a "quoted"  description\nwith\t whitespace  ',
        TRANSITION,
    )
    output = reader.render_metrics([alarm, alarm], True, 1.0)
    assert output.count("} 1\n") == 1
    assert 'alarm="example\\"alarm\\\\path\\nnext"' in output
    assert 'description="a \\"quoted\\" description with whitespace"' in output
    assert output.endswith("\n")
    assert "# TYPE curie_cloudwatch_alarm_in_alarm gauge\n" in output
    compatible = reader.render_metrics([alarm, alarm], True, 1.0, metric_prefix="legacy_cloudwatch")
    assert compatible == output.replace("curie_cloudwatch", "legacy_cloudwatch")
    source, _, _ = poller(reader, tmp_path, metric_prefix="legacy_cloudwatch")
    assert "legacy_cloudwatch_alarm_poll_ok 0\n" in source.metrics()


def test_http_reads_current_snapshot_and_other_paths_are_refused(reader, tmp_path):
    # @spec SRE-CW-6.
    source, aws, _ = poller(reader, tmp_path)
    server = reader.make_server("127.0.0.1:0", source)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        with urllib.request.urlopen(base + "/metrics", timeout=3) as response:
            assert response.status == 200
            assert "version=0.0.4" in response.headers["Content-Type"]
            assert response.read().decode() == source.metrics()
        source.poll_once()
        aws.pages = {None: TimeoutError("failed read")}
        source.poll_once()
        with urllib.request.urlopen(base + "/metrics", timeout=3) as response:
            assert response.read().decode() == source.metrics()
        for path in ["/", "/metrics/", "/healthz"]:
            with pytest.raises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(base + path, timeout=3)
            assert error.value.code == 404
        # Exercise the timeout over the real connection rather than inspecting
        # the handler's class or mirroring its configuration.
        with socket.create_connection(server.server_address, timeout=3) as idle:
            idle.settimeout(31)
            start = time.monotonic()
            assert idle.recv(1) == b""
            assert time.monotonic() - start <= 30
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)
        assert not worker.is_alive()


@pytest.mark.parametrize("field", ["AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"])
def test_credentials_fail_closed_on_missing_field(reader, field):
    # @spec SRE-CW-2. STS API contract linked in specification.
    fields = {
        "AccessKeyId": "AKIDEXAMPLE",
        "SecretAccessKey": SECRET,
        "SessionToken": SESSION,
        "Expiration": NOW.isoformat(),
    }
    del fields[field]
    body = '<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials>'
    body += "".join(f"<{name}>{value}</{name}>" for name, value in fields.items())
    body += "</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
    with pytest.raises(ValueError):
        reader.parse_credentials(body.encode())


def test_naive_expiry_is_refused_and_assume_errors_keep_last_good_snapshot(
    reader, tmp_path, capsys
):
    # @spec SRE-CW-2 SRE-CW-3 SRE-CW-5.
    body = (
        '<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<AssumeRoleWithWebIdentityResult><Credentials>"
        "<AccessKeyId>AKIDEXAMPLE</AccessKeyId>"
        f"<SecretAccessKey>{SECRET}</SecretAccessKey><SessionToken>{SESSION}</SessionToken>"
        "<Expiration>2026-10-07T12:00:00</Expiration>"
        "</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
    )
    with pytest.raises(ValueError):
        reader.parse_credentials(body.encode())
    source, aws, _ = poller(reader, tmp_path)
    source.poll_once()
    before = source.metrics()
    aws.now += dt.timedelta(hours=1)
    for failure in [(403, b"private provider body"), (200, b"<bad"), (200, b"<root/>")]:
        aws.assume_failure = failure
        source.poll_once()
        assert source.metrics() == before.replace("alarm_poll_ok 1\n", "alarm_poll_ok 0\n")
        stderr = capsys.readouterr().err
        assert len(stderr.splitlines()) == 1 and "assume" in stderr
        assert "private provider body" not in stderr


@pytest.mark.parametrize(
    "environment",
    [{}, {"METRIC_PREFIX": "bad-prefix"}, {"POLL_SECONDS": "0"}, {"POLL_SECONDS": "nan"}],
)
def test_startup_refuses_invalid_configuration_without_credentials_or_listener(reader, environment):
    # @spec SRE-CW-1. Subprocess crosses main's real environment boundary.
    required = {
        "TOPIC_ARN": TOPIC,
        "AWS_REGION": REGION,
        "AWS_ROLE_ARN": ROLE,
        "AWS_WEB_IDENTITY_TOKEN_FILE": "/does-not-exist",
    }
    env = dict(environment) if not environment else {**required, **environment}
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(PROGRAM)],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 2
    assert "Traceback" not in result.stderr
    assert TOPIC not in result.stderr and ROLE not in result.stderr


def test_occupied_listener_exits_with_safe_configuration_error(reader):
    # @spec SRE-CW-1 SRE-CW-5. A real occupied socket crosses main's bind boundary.
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        env = {
            "TOPIC_ARN": TOPIC,
            "AWS_REGION": REGION,
            "AWS_ROLE_ARN": ROLE,
            "AWS_WEB_IDENTITY_TOKEN_FILE": "/does-not-exist",
            "LISTEN_ADDR": f"127.0.0.1:{occupied.getsockname()[1]}",
        }
        result = subprocess.run(
            [sys.executable, "-I", "-S", str(PROGRAM)],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "cloudwatch-alarms: invalid configuration\n"
    assert "Traceback" not in result.stderr and ROLE not in result.stderr


def test_program_imports_with_stdlib_and_sigterm_exits_zero(reader):
    # @spec SRE-CW-1 SRE-CW-5.
    probe = (
        "import importlib.util, os, signal, sys\n"
        f"s=importlib.util.spec_from_file_location('cw', {str(PROGRAM)!r})\n"
        "m=importlib.util.module_from_spec(s);sys.modules[s.name]=m;s.loader.exec_module(m)\n"
        "m.install_signal_handlers();os.kill(os.getpid(),signal.SIGTERM)\n"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", probe], capture_output=True, timeout=5
    )
    assert result.returncode == 0 and result.stdout == result.stderr == b""


def test_optional_prometheus_rules_execute_real_consumer_vectors(tmp_path):
    # @spec SRE-CW-7. This executes production PromQL; cluster qualification
    # separately verifies chart rendering, live scraping and rule loading.
    overlay_path = OBS / "prometheus-cloudwatch.yaml"
    assert overlay_path.is_file(), "SRE-CW-7: optional CloudWatch metric consumer is missing"
    overlay = yaml.safe_load(overlay_path.read_text())
    rules = overlay["serverFiles"]["cloudwatch_rules.yml"]
    (tmp_path / "cloudwatch_rules.yml").write_text(yaml.safe_dump(rules))
    vector = Path(__file__).with_name("cloudwatch-alarms.test.yaml")
    (tmp_path / vector.name).write_text(vector.read_text())
    executable = os.environ.get("PROMTOOL") or shutil.which("promtool")
    assert executable, "promtool is required to prove production CloudWatch rule behavior"
    result = subprocess.run(
        [executable, "test", "rules", vector.name],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_optional_manifest_keeps_pod_and_credential_security_boundary():
    # @spec SRE-CW-8. Security configuration text pin permitted by public
    # AGENTS.md test policy item 3; issue #4253. Runtime cluster validation is
    # separately required and this assertion does not substitute for it.
    manifest = OBS / "cloudwatch-alarms.yaml"
    assert manifest.is_file(), "SRE-CW-8: optional CloudWatch deployment is missing"
    documents = [item for item in yaml.safe_load_all(manifest.read_text()) if item]
    deployment = next(item for item in documents if item["kind"] == "Deployment")
    spec = deployment["spec"]["template"]["spec"]
    container = spec["containers"][0]
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert spec["securityContext"]["runAsUser"] != 0
    assert spec["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert container["volumeMounts"] and all(
        mount["readOnly"] for mount in container["volumeMounts"]
    )
    assert "@sha256:" in container["image"]
    serialized = yaml.safe_dump(documents)
    assert "arn:aws:" not in serialized
    assert "SecretAccessKey" not in serialized and "SessionToken" not in serialized
