"""The Docker-substrate backend's pure helpers, judged by what consumes their output.

The isolation env is checked against the names the CLI's isolation contract
reads, the worker rebinding against the needles that contract validates, the
seeded files against the API's own operator-file reader, the runner view
against a token the worker's own signer mints, and the worker overlay against
the shipped overlay Dockerfile.
"""

from __future__ import annotations

import base64
import io
import json
import re
import stat
import tarfile
from pathlib import Path
from typing import Any

import identity_compose as compose
import identity_scenarios
import identity_scenarios_docker as docker_scenarios
import pytest
from curie_internal import operator_file
from curie_worker import caller_token
from identity_e2e import caller_fingerprint, decode_caller_token
from identity_target import HarnessError
from nacl.signing import SigningKey

PROJECT = "ie2e-test"
TENANT = "00000000-0000-0000-0000-000000000001"


def _ports() -> compose.Ports:
    return compose.Ports(27100)


def _files(tmp_path: Path, count: int = 2) -> list[Path]:
    return [compose.BASE_COMPOSE, *(tmp_path / f"f{i}.yaml" for i in range(1, count))]


def _cli_isolation_names() -> set[str]:
    source = (compose.REPO / "cli" / "src" / "local.rs").read_text()
    block = re.search(r"const ISOLATION_ENV: &\[&str\] = &\[(.*?)\];", source, re.S)
    assert block is not None, "cli/src/local.rs no longer declares ISOLATION_ENV"
    return set(re.findall(r'"([A-Z0-9_]+)"', block.group(1)))


def _base_config() -> dict[str, Any]:
    def port(target: int, published: int) -> dict[str, Any]:
        return {
            "mode": "ingress",
            "host_ip": "127.0.0.1",
            "target": target,
            "published": str(published),
            "protocol": "tcp",
        }

    return {
        "services": {
            "curie-api": {"ports": [port(8000, 28000)]},
            "postgres": {"ports": [port(5432, 25432)]},
            "valkey": {"ports": [port(6379, 26379)]},
            "rustfs": {"ports": [port(9000, 29000), port(9001, 29001)]},
            "otel-collector": {"ports": [port(4317, 24317), port(4318, 24318)]},
            "curie-worker": {},
        }
    }


def _override(tmp_path: Path) -> dict[str, Any]:
    images = compose.resolve_images(project=PROJECT, tag="cand1", prefix="example-cand")
    return compose.override_document(
        project=PROJECT,
        ports=_ports(),
        published=compose.port_overrides(_base_config(), _ports()),
        images=images,
        overlay_dockerfile=compose.worker_dockerfile(
            compose.WORKER_OVERLAY_DOCKERFILE.read_text(), images.worker
        ),
        overlay_context=compose.WORKER_OVERLAY_DOCKERFILE.parent,
    )


# --- the isolation contract --------------------------------------------------------


def test_isolation_env_sets_every_name_the_cli_contract_requires(tmp_path: Path) -> None:
    env = compose.isolation_env(
        project=PROJECT, files=_files(tmp_path), ports=_ports(), image_tag="cand1"
    )
    assert _cli_isolation_names() <= set(env)
    assert env["COMPOSE_PROJECT_NAME"] == PROJECT
    assert env["COMPOSE_FILE"].split(":")[0] == str(compose.BASE_COMPOSE)
    assert env["CURIE_DOCKER_NETWORK"] == f"{PROJECT}_runner"
    # The CLI refuses `localhost` for a host-network worker's endpoints.
    assert "localhost" not in json.dumps(env)


def test_isolation_env_refuses_a_single_or_relative_file(tmp_path: Path) -> None:
    with pytest.raises(HarnessError):
        compose.isolation_env(
            project=PROJECT, files=[compose.BASE_COMPOSE], ports=_ports(), image_tag="t"
        )
    with pytest.raises(HarnessError):
        compose.isolation_env(
            project=PROJECT,
            files=[compose.BASE_COMPOSE, Path("rel.yaml")],
            ports=_ports(),
            image_tag="t",
        )


def test_worker_environment_rebinds_what_the_cli_validates(tmp_path: Path) -> None:
    """The needles cli/src/local.rs validate_isolated_override checks, per worker key."""

    ports = _ports()
    env = compose.isolation_env(project=PROJECT, files=_files(tmp_path), ports=ports, image_tag="t")
    worker = compose.worker_environment(ports, PROJECT, "example-cand/curie-runner:cand1")
    needles = {
        "DATABASE_URL": f"{env['CURIE_LOCAL_POSTGRES_HOST']}:{env['CURIE_LOCAL_POSTGRES_PORT']}",
        "VALKEY_HOST": env["VALKEY_HOST"],
        "VALKEY_PORT": env["VALKEY_PORT"],
        "S3_ENDPOINT_URL": env["S3_ENDPOINT_URL"],
        "CURIE_API_URL": env["CURIE_API_URL"],
        "CURIE_DOCKER_NETWORK": env["CURIE_DOCKER_NETWORK"],
        "SLACK_API_BASE_URL": f":{env['CURIE_LOCAL_STUB_PORT']}",
    }
    for key, needle in needles.items():
        assert needle in worker[key], key
    # Secrets arrive only by interpolation from the harness's environment.
    assert worker["CURIE_CONNECTOR_CALLER_SIGNING_KEY"] == "${CURIE_CONNECTOR_CALLER_SIGNING_KEY:-}"
    assert worker["CURIE_FAKE_MODEL"] == "1"


def test_port_overrides_keep_every_published_port_in_the_block() -> None:
    ports = _ports()
    mapped = compose.port_overrides(_base_config(), ports)
    assert mapped["curie-api"] == [f"127.0.0.1:{ports.api}:8000"]
    assert mapped["postgres"] == [f"127.0.0.1:{ports.postgres}:5432"]
    hosts = []
    for entries in mapped.values():
        for entry in entries:
            ip, host, _target = entry.split(":")
            assert ip == "127.0.0.1"
            hosts.append(int(host))
    assert len(hosts) == len(set(hosts))
    assert all(ports.base <= host <= ports.last < 32768 for host in hosts)
    assert ports.sink not in hosts and ports.stub not in hosts


def test_port_overrides_refuse_a_base_that_moved_a_reached_service() -> None:
    config = _base_config()
    del config["services"]["valkey"]
    with pytest.raises(HarnessError, match="valkey"):
        compose.port_overrides(config, _ports())


def test_ports_refuse_a_block_past_the_ephemeral_range() -> None:
    with pytest.raises(HarnessError):
        compose.Ports(32700)


# --- the override files ------------------------------------------------------------


def _loader() -> Any:
    yaml = pytest.importorskip("yaml")

    class Loader(yaml.SafeLoader):
        pass

    def override(loader: Any, node: Any) -> Any:
        if isinstance(node, yaml.SequenceNode):
            return {"!override": loader.construct_sequence(node, deep=True)}
        return {"!override": loader.construct_mapping(node, deep=True)}

    Loader.add_constructor("!override", override)
    return yaml, Loader


def test_yaml_dump_round_trips_the_override_with_its_tags(tmp_path: Path) -> None:
    yaml, loader = _loader()
    document = _override(tmp_path)
    parsed = yaml.load(compose.yaml_dump(document), Loader=loader)  # noqa: S506 - SafeLoader subclass
    assert parsed["networks"] == {"curie_runner": {"name": f"{PROJECT}_runner"}}
    assert parsed["services"]["postgres"]["ports"] == {
        "!override": [f"127.0.0.1:{_ports().postgres}:5432"]
    }
    build = parsed["services"]["curie-worker"]["build"]["!override"]
    assert set(build) == {"context", "dockerfile_inline"}
    assert parsed["services"]["identity-e2e-idp"]["profiles"] == ["identity-e2e"]
    assert parsed["services"]["identity-e2e-seed"]["profiles"] == ["identity-e2e"]
    assert parsed["volumes"] == {compose.HTTP_IDENTITY_VOLUME: {}, compose.IDP_VOLUME: {}}
    identity = yaml.load(  # noqa: S506
        compose.yaml_dump(compose.identity_overlay_document()), Loader=loader
    )
    api = identity["services"]["curie-api"]
    assert api["environment"]["CURIE_HTTP_IDENTITY_DIR"] == compose.HTTP_IDENTITY_MOUNT
    assert api["volumes"] == [f"{compose.HTTP_IDENTITY_VOLUME}:{compose.HTTP_IDENTITY_MOUNT}:ro"]


def test_yaml_dump_quotes_strings_compose_could_misread() -> None:
    yaml, loader = _loader()
    document = {"a": {"b": "yes", "c": "0755", "d": "x: y", "e": "multi\nline", "f": []}}
    assert yaml.load(compose.yaml_dump(document), Loader=loader) == document  # noqa: S506


def test_the_provider_alias_is_the_certificate_and_lookup_host(tmp_path: Path) -> None:
    idp = _override(tmp_path)["services"]["identity-e2e-idp"]
    assert idp["networks"]["default"]["aliases"] == [compose.provider_host(PROJECT)]
    assert idp["ports"] == [f"127.0.0.1:{_ports().sink}:{compose.SINK_PORT}"]
    assert idp["volumes"] == [f"{compose.IDP_VOLUME}:{compose.IDP_MOUNT}:ro"]
    assert idp["read_only"] is True


def test_worker_overlay_builds_from_the_candidate_and_escapes_interpolation() -> None:
    shipped = compose.WORKER_OVERLAY_DOCKERFILE.read_text()
    rebuilt = compose.worker_dockerfile(shipped, "example-cand/curie-worker:cand1")
    assert "FROM example-cand/curie-worker:cand1" in rebuilt.splitlines()
    assert "ghcr.io/curie-eng/curie-worker" not in rebuilt
    # Everything else is the shipped overlay, byte for byte.
    assert rebuilt.replace("FROM example-cand/curie-worker:cand1", "") == shipped.replace(
        "FROM ghcr.io/curie-eng/curie-worker:${BASE_TAG}", ""
    )
    escaped = compose.compose_escape(rebuilt)
    assert "${" not in escaped.replace("$${", "")
    assert escaped.replace("$$", "$") == rebuilt


def test_worker_overlay_refuses_a_dockerfile_it_does_not_recognise() -> None:
    with pytest.raises(HarnessError):
        compose.worker_dockerfile("FROM python:3.13\n", "example-cand/curie-worker:cand1")


def test_resolve_images_names_candidates_and_the_overlay_under_the_project() -> None:
    images = compose.resolve_images(project=PROJECT, tag="cand1", prefix="example-cand/")
    assert images.api == "example-cand/curie-api:cand1"
    assert images.runner == "example-cand/curie-runner:cand1"
    assert images.worker_local == f"{PROJECT}/curie-worker-local:cand1"
    pinned = compose.resolve_images(project=PROJECT, tag="latest", api_repo="other/api")
    assert pinned.api == "other/api:latest"
    assert pinned.worker == "ghcr.io/curie-eng/curie-worker:latest"
    with pytest.raises(HarnessError):
        compose.resolve_images(project=PROJECT, tag="bad tag")


# --- seeded files --------------------------------------------------------------------


def test_seed_archive_files_pass_the_api_operator_file_reader(tmp_path: Path) -> None:
    document = {"schema_version": 1, "providers": []}
    raw = compose.seed_archive(
        {"http-identity/api-runtime.json": json.dumps(document).encode(), "idp/bearer": b"x"}
    )
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        members = {member.name: member for member in archive.getmembers()}
        assert set(members) == {"http-identity/api-runtime.json", "idp/bearer"}
        for member in members.values():
            assert (member.uid, member.gid, member.mode) == (0, compose.APP_GID, 0o440)
            # 0440 has no group write and nothing for others: the reader's rule.
            forbidden = stat.S_IWGRP | stat.S_IRWXO
            assert member.mode & forbidden == 0
        # Extracted unprivileged, the files are owned by this user, which the
        # reader also accepts (it allows root or its own uid).
        archive.extractall(tmp_path, filter="data")
    target = tmp_path / "http-identity"
    for path in target.iterdir():
        path.chmod(0o440)
    assert operator_file.read_operator_json(str(target), "api-runtime.json") == document


def test_seed_archive_refuses_paths_outside_the_volume_root() -> None:
    with pytest.raises(HarnessError):
        compose.seed_archive({"../etc/passwd": b"x"})


# --- runner containers ------------------------------------------------------------------


def _inspected(token: str | None, created: str) -> dict[str, Any]:
    env = ["PATH=/usr/bin", "CURIE_RUNNER_PORT=8080", "NO_EQUALS_ENTRY"]
    if token is not None:
        env.append(f"CURIE_CONNECTOR_CALLER_TOKEN={token}")
    return {
        "Name": "/curie-thread-abc-123456",
        "Created": created,
        "State": {"Status": "running"},
        "Config": {
            "Env": env,
            "Labels": {compose.AGENT_LABEL: "agent-a", compose.STACK_LABEL: f"{PROJECT}_runner"},
        },
    }


def test_runner_view_reads_the_token_the_worker_signs() -> None:
    seed = base64.b64encode(bytes(SigningKey.generate())).decode()
    principal = caller_token.CallerPrincipal(
        tenant_id=TENANT, issuer="https://issuer.example.test", subject="alice"
    )
    token = caller_token.mint(seed, agent="agent-a", exp=2_000_000_000, principal=principal)
    view = compose.runner_view(_inspected(token, "2026-11-08T08:27:40.123456789Z"))
    assert view["name"] == "curie-thread-abc-123456"
    assert view["agent"] == "agent-a"
    assert view["created_epoch"] == pytest.approx(1_794_126_460.123456, abs=1e-5)
    assert view["token"] == token
    decoded = decode_caller_token(view["token"])
    assert decoded["payload"]["principal"] == {
        "issuer": "https://issuer.example.test",
        "subject": "alice",
        "tenant_id": TENANT,
    }
    assert len(caller_fingerprint(token)) == 16


def test_runner_view_without_a_token_or_a_readable_stamp() -> None:
    view = compose.runner_view(_inspected(None, "not a time"))
    assert view["token"] is None
    assert view["created_epoch"] is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-11-08T08:27:40Z", 1_794_126_460.0),
        ("2026-11-08T08:27:40.5+00:00", 1_794_126_460.5),
        ("2026-11-08T10:27:40.000000001+02:00", 1_794_126_460.0),
        ("", None),
    ],
)
def test_parse_created(raw: str, expected: float | None) -> None:
    value = compose.parse_created(raw)
    assert value == (None if expected is None else pytest.approx(expected, abs=1e-5))


def test_log_entries_read_docker_timestamps_and_worker_records() -> None:
    record = json.dumps(
        {"message": "requester handoff pending for event e2 on thread t", "exception": None}
    )
    failing = json.dumps({"message": "turn failed", "exception": "Traceback\nValueError: boom"})
    text = "\n".join(
        [
            f"2026-11-08T08:27:39.900000000Z {record}",
            f"2026-11-08T08:27:41.000000000Z {record}",
            f"2026-11-08T08:27:42.000000000Z {failing}",
            "a line with no stamp",
            "",
        ]
    )
    since = 1_794_126_460.0
    entries = compose.log_entries(text, since, "curie-worker")
    assert [entry["message"] for entry in entries] == [
        "requester handoff pending for event e2 on thread t",
        "turn failed | ValueError: boom",
        "a line with no stamp",
    ]
    assert entries[-1]["at"] == since


# --- environment hygiene ---------------------------------------------------------


def test_context_excluded_keeps_secrets_and_outputs_out_of_a_build_context() -> None:
    for path in (
        "./.git",
        "./.git/config",
        "./cli/target/debug/curie",
        "./.env",
        "./apps/api/.env.local",
        "./x/node_modules/y",
        "./a/__pycache__/b.pyc",
        "./keys/server.key",
        "./.venv/bin/python",
    ):
        assert compose.context_excluded(path), path
    for path in (
        ".",
        "./apps/api/Dockerfile",
        "./uv.lock",
        "./x/.env.example",
        "./cli/src/main.rs",
        "./runner/pip.conf",
    ):
        assert not compose.context_excluded(path), path


def test_context_member_drops_the_local_owner() -> None:
    info = tarfile.TarInfo("./apps/api/Dockerfile")
    info.uid, info.gid, info.uname, info.gname = 1234, 1234, "someone", "group"
    kept = compose.context_member(info)
    assert kept is not None and (kept.uid, kept.gid, kept.uname, kept.gname) == (0, 0, "", "")
    assert compose.context_member(tarfile.TarInfo("./.env")) is None


def test_child_env_strips_credentials_and_stack_steering_names() -> None:
    base = {
        "PATH": "/usr/bin",
        "HOME": "/srv/x",
        "DOCKER_HOST": "ssh://example",
        "ANTHROPIC_API_KEY": "secret",
        "CLAUDE_CODE_OAUTH_TOKEN": "secret",
        "CURIE_CREDENTIALS": "secret",
        "CURIE_API_KEY": "other-stack",
        "COMPOSE_FILE": "/other.yaml",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://x",
        "DATABASE_URL": "postgres://x",
    }
    env = compose.child_env(base, {"CURIE_FAKE_MODEL": "1"})
    assert env == {
        "PATH": "/usr/bin",
        "HOME": "/srv/x",
        "DOCKER_HOST": "ssh://example",
        "CURIE_FAKE_MODEL": "1",
    }


def test_redact_removes_secret_assignments_the_cli_prints() -> None:
    line = (
        "COMPOSE_PROJECT_NAME=p 'CURIE_LOCAL_API_KEY=2d88f4ff***' "
        'CURIE_LOCAL_POSTGRES_PASSWORD=abc CURIE_ADAPTER_CREDENTIALS={"a":"b"} docker compose up'
    )
    redacted = compose.redact(line)
    assert "2d88f4ff" not in redacted and "abc" not in redacted and '"b"' not in redacted
    assert "COMPOSE_PROJECT_NAME=p" in redacted
    assert "CURIE_LOCAL_API_KEY=<redacted>" in redacted


# --- scenario helpers ------------------------------------------------------------


def test_the_docker_scenario_is_registered() -> None:
    assert docker_scenarios.SCENARIO in identity_scenarios.SCENARIOS


def test_deferral_facts_report_the_durable_wait_without_requiring_it() -> None:
    messages = [
        "requester handoff pending for event ev-b on thread t: a different requester's turn",
        "capacity wait parked event_id=ev-b cause=requester_handoff",
    ]
    facts = docker_scenarios.deferral_facts(
        messages,
        second_event="ev-b",
        a_reply_at=100.0,
        b_claim_created=101.0,
        expect_durable_wait=True,
    )
    assert facts["durable_wait"]["seen"] is True
    assert facts["pending_names_second_event"] is True
    older = docker_scenarios.deferral_facts(
        messages[:1],
        second_event="ev-b",
        a_reply_at=100.0,
        b_claim_created=101.0,
        expect_durable_wait=False,
    )
    assert older["durable_wait"] == {
        "expected": False,
        "seen": False,
        "line": "capacity wait parked event_id=ev-b cause=requester_handoff",
        "matches": [],
    }
    with pytest.raises(identity_scenarios.StepFailed, match="durable wait"):
        docker_scenarios.deferral_facts(
            messages[:1],
            second_event="ev-b",
            a_reply_at=100.0,
            b_claim_created=101.0,
            expect_durable_wait=True,
        )


def test_deferral_facts_fail_when_nothing_was_deferred_or_b_ran_early() -> None:
    with pytest.raises(identity_scenarios.StepFailed, match="deferral not exercised"):
        docker_scenarios.deferral_facts(
            ["unrelated"],
            second_event="e",
            a_reply_at=1.0,
            b_claim_created=2.0,
            expect_durable_wait=False,
        )
    with pytest.raises(identity_scenarios.StepFailed, match="before the first human's reply"):
        docker_scenarios.deferral_facts(
            ["requester handoff pending for event e"],
            second_event="e",
            a_reply_at=100.0,
            b_claim_created=90.0,
            expect_durable_wait=False,
        )
    with pytest.raises(identity_scenarios.StepFailed, match="interrupted"):
        docker_scenarios.deferral_facts(
            ["requester handoff pending", "interrupting before retry"],
            second_event="e",
            a_reply_at=1.0,
            b_claim_created=2.0,
            expect_durable_wait=False,
        )


@pytest.mark.parametrize(
    ("texts", "expected"),
    [
        (["working", "all done"], True),
        (["all done.", "  "], True),
        (["all done", "a folded notice"], False),
        ([], False),
    ],
)
def test_ends_with_answer(texts: list[str], expected: bool) -> None:
    assert docker_scenarios.ends_with_answer(texts) is expected


def test_require_container_token_judges_principal_and_creation() -> None:
    principal = {"issuer": "i", "subject": "alice", "tenant_id": TENANT}
    observed = {
        "caller_token_present": True,
        "claim": "c1",
        "claim_created_epoch": 100.0,
        "caller_fingerprint": "0123456789abcdef",
        "claims": {"agent": "agent-a", "exp": 5, "principal": principal},
    }
    seen = docker_scenarios.require_container_token(
        observed, agent="agent-a", principal=principal, sent_at=101.0
    )
    assert seen["principal"] == principal and seen["container"] == "c1"
    with pytest.raises(identity_scenarios.StepFailed, match="created at"):
        docker_scenarios.require_container_token(
            observed, agent="agent-a", principal=principal, sent_at=110.0
        )
    with pytest.raises(identity_scenarios.StepFailed, match="carries"):
        docker_scenarios.require_container_token(
            observed, agent="agent-a", principal=None, sent_at=None
        )


def test_default_agents_follow_the_registration_order_not_the_sorted_state() -> None:
    data = {
        "agents": {"a-control": {}, "z-flag": {}},
        "agent_order": ["z-flag", "a-control"],
    }
    assert compose.default_agents(data, None, None) == ("z-flag", "a-control")
    assert compose.default_agents(data, "a-control", None) == ("a-control", "z-flag")
    assert compose.default_agents({"agents": {}}, None, None) == (None, None)
