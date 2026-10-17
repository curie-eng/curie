"""The shared owner's pure contracts and real argument-validation boundary."""

from __future__ import annotations

import importlib.util
import ipaddress
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from urllib.parse import unquote, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
HELPER = Path(__file__).with_name("test_env.py")


def load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("curie_test_env_test_subject", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        ["--"],
        ["--profile", "core", "--"],
        ["--profile", "full", "--"],
        ["--profile", "invalid", "--", "python3", "-c", "pass"],
        ["--unknown-option", "--", "python3", "-c", "pass"],
    ],
)
def test_invalid_arguments_return_usage_and_one_final_object(arguments: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, str(HELPER), *arguments],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 2, result.stderr
    report = json.loads(result.stdout)
    assert isinstance(report, dict)
    assert isinstance(report.get("error"), str) and report["error"]
    assert "Traceback" not in result.stderr


def test_environment_replaces_every_consumer_endpoint_without_mutating_the_caller() -> None:
    original = {
        "COMPOSE_PROJECT_NAME": "curie-test-env-example",
        "COMPOSE_FILE": "/source/compose.dev.yaml:/source/.projects/override.yaml",
        "DATABASE_URL": "postgresql://unowned.example.com:1/example",
        "TEST_DATABASE_URL": "postgresql://unowned.example.com:2/example",
        "VALKEY_HOST": "unowned.example.com",
        "VALKEY_PORT": "1",
        "TEST_VALKEY_HOST": "unowned.example.com",
        "TEST_VALKEY_PORT": "2",
        "TEST_VALKEY_PW": "example password/@",
        "TEST_VALKEY_URL": "redis://unowned.example.com:9/4",
        "S3_ENDPOINT_URL": "http://unowned.example.com:3",
        "TEST_S3_ENDPOINT_URL": "http://unowned.example.com:4",
        "TEST_OTEL_COLLECTOR_ENDPOINT": "http://unowned.example.com:5/v1/traces",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://unowned.example.com:6",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
        "CURIE_PRIMARY_SOURCE_RESOURCES": "0",
        "CURIE_SETTLEMENT_TEST_REDIS_URL": "redis://unowned.example.com:7/0",
        "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT": "unowned-project",
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES": "/source/unowned.yaml",
        "CURIE_RELEASED_UPGRADE_POSTGRES_HOST": "unowned.example.com",
        "CURIE_RELEASED_UPGRADE_POSTGRES_PORT": "8",
        "DOCKER_CONTEXT": "example-caller-context",
        "EXAMPLE_PASSTHROUGH": "retained",
    }
    before = dict(original)
    result = load_helper().private_service_environment(
        original, postgres=35432, valkey=36379, s3=39000, otel=34318
    )
    assert original == before
    database = "postgresql+asyncpg://postgres:postgres@127.0.0.1:35432/postgres"
    assert result["DATABASE_URL"] == result["TEST_DATABASE_URL"] == database
    assert result["VALKEY_HOST"] == result["TEST_VALKEY_HOST"] == "127.0.0.1"
    assert result["VALKEY_PORT"] == result["TEST_VALKEY_PORT"] == "36379"
    assert result["S3_ENDPOINT_URL"] == result["TEST_S3_ENDPOINT_URL"] == "http://127.0.0.1:39000"
    assert result["TEST_OTEL_COLLECTOR_ENDPOINT"] == "http://127.0.0.1:34318/v1/traces"
    assert result["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://127.0.0.1:34318"
    assert result["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"
    assert result["CURIE_PRIMARY_SOURCE_RESOURCES"] == "1"
    assert result["TEST_VALKEY_URL"] == result["CURIE_SETTLEMENT_TEST_REDIS_URL"]
    redis = urlsplit(result["TEST_VALKEY_URL"])
    assert redis.scheme == "redis" and redis.path == "/0"
    assert redis.hostname == "127.0.0.1" and redis.port == 36379
    assert unquote(redis.password or "") == original["TEST_VALKEY_PW"]
    assert result["CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT"] == original["COMPOSE_PROJECT_NAME"]
    assert result["CURIE_RELEASED_UPGRADE_COMPOSE_FILES"] == original["COMPOSE_FILE"]
    assert result["CURIE_RELEASED_UPGRADE_POSTGRES_HOST"] == "127.0.0.1"
    assert result["CURIE_RELEASED_UPGRADE_POSTGRES_PORT"] == "35432"
    assert result["CURIE_RELEASED_UPGRADE_INTEGRATION"] == "1"
    assert result["DOCKER_CONTEXT"] == original["DOCKER_CONTEXT"]
    assert result["EXAMPLE_PASSTHROUGH"] == "retained"


def test_released_upgrade_environment_is_an_explicit_isolated_contract() -> None:
    result = load_helper().released_upgrade_environment(
        "curie-test-env-example", "/source/base.yaml:/source/override.yaml", 35432
    )
    assert result == {
        "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT": "curie-test-env-example",
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES": "/source/base.yaml:/source/override.yaml",
        "CURIE_RELEASED_UPGRADE_POSTGRES_HOST": "127.0.0.1",
        "CURIE_RELEASED_UPGRADE_POSTGRES_PORT": "35432",
        "CURIE_RELEASED_UPGRADE_INTEGRATION": "1",
    }


def test_subnet_allocation_is_repeatable_and_does_not_mutate_inventory() -> None:
    helper = load_helper()
    occupied = ["10.240.0.0/23", "172.31.240.0/22", "192.168.240.0/24"]
    before = list(occupied)
    first = helper.private_network_subnets("curie-test-env-example", occupied)
    assert first == helper.private_network_subnets("curie-test-env-example", occupied)
    assert occupied == before
    selected = [ipaddress.ip_network(value) for value in first]
    assert len(selected) == 2 and selected[0] != selected[1]
    for network in selected:
        assert network.prefixlen == 24
        assert not any(network.overlaps(ipaddress.ip_network(value)) for value in occupied)
    second = helper.private_network_subnets("curie-test-env-example", [*occupied, *first])
    assert not any(
        ipaddress.ip_network(left).overlaps(ipaddress.ip_network(right))
        for left in first
        for right in second
    )


def test_exhausted_subnet_pool_is_transient() -> None:
    helper = load_helper()
    with pytest.raises(helper.TransientError):
        helper.private_network_subnets(
            "curie-test-env-example",
            ["10.240.0.0/20", "172.31.240.0/20", "192.168.240.0/20"],
        )


def test_malformed_subnet_inventory_fails_closed() -> None:
    with pytest.raises(ValueError):
        load_helper().private_network_subnets("curie-test-env-example", ["invalid-subnet"])
