"""Per-agent runner resource overrides round-trip through the API (#3209).

Same three-way PATCH semantics as `model` and `thinking`: omitted leaves the
stored value, explicit JSON null clears it, and an object sets it. Null means
the chart block. A set value is the whole requests and limits block. Quota
env is read from Settings on the PATCH that writes the field.
"""

import hashlib
import json
import re
from typing import Any

import pytest
from curie_api.config import get_settings

# The chart block the operator sends. Every key is required when the field is set.
_VALID: dict[str, Any] = {
    "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "1Gi"},
    "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
}

_QUOTA_VARS: tuple[str, ...] = (
    "CURIE_SANDBOX_QUOTA_REQUESTS_CPU",
    "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY",
    "CURIE_SANDBOX_QUOTA_LIMITS_CPU",
    "CURIE_SANDBOX_QUOTA_LIMITS_MEMORY",
)

# requests.cpu=1, limits.cpu=2, requests.memory=2Gi, limits.memory=4Gi.
_FULL_QUOTA: dict[str, str] = {
    "CURIE_SANDBOX_QUOTA_REQUESTS_CPU": "1",
    "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY": "2Gi",
    "CURIE_SANDBOX_QUOTA_LIMITS_CPU": "2",
    "CURIE_SANDBOX_QUOTA_LIMITS_MEMORY": "4Gi",
}


def _resources() -> dict[str, Any]:
    return {
        "requests": dict(_VALID["requests"]),
        "limits": dict(_VALID["limits"]),
    }


def _channel(name: str) -> str:
    digest = hashlib.sha256(name.encode()).hexdigest()[:10].upper()
    return "C" + digest


def _create_agent(client: Any, auth_headers: dict[str, str], name: str) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": _channel(name)},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _patch(client: Any, auth_headers: dict[str, str], agent_id: str, body: dict[str, Any]) -> Any:
    return client.patch(f"/agents/{agent_id}", json=body, headers=auth_headers)


def _read(client: Any, auth_headers: dict[str, str], agent_id: str) -> dict[str, Any]:
    resp = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _set_quota(monkeypatch: pytest.MonkeyPatch, values: dict[str, str | None]) -> None:
    # test_config.py overrides Settings by writing the environment and clearing
    # the lru_cache on get_settings, so the next request builds a new Settings.
    # A name left out, or mapped to None, is unset. "" stays set and blank.
    for name in _QUOTA_VARS:
        value = values.get(name)
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    get_settings.cache_clear()


def _detail_text(resp: Any) -> str:
    # The refusal is the detail string, or the validation `msg`. The echoed
    # request body is not part of that sentence (it already contains "1" and "cpu").
    detail = resp.json()["detail"]
    if isinstance(detail, str):
        return detail
    if isinstance(detail, list):
        parts: list[str] = []
        for item in detail:
            if isinstance(item, dict):
                parts.append(str(item.get("msg", "")))
            else:
                parts.append(str(item))
        return " ".join(parts)
    raise AssertionError(resp.text)


def _shape_body(slug: str) -> dict[str, Any]:
    if slug == "missing-ephemeral":
        body = _resources()
        del body["requests"]["ephemeral-storage"]
        return {"runner_resources": body}
    if slug == "cpu-above-limit":
        # "1" is 1000 millicores and "500m" is 500. Lexical order would call
        # "1" the smaller string, so a refusal here is the quantity compare.
        body = _resources()
        body["requests"]["cpu"] = "1"
        body["limits"]["cpu"] = "500m"
        return {"runner_resources": body}
    if slug == "blank-cpu":
        body = _resources()
        body["requests"]["cpu"] = ""
        return {"runner_resources": body}
    if slug == "json-string":
        return {"runner_resources": json.dumps(_resources())}
    raise AssertionError(slug)


def test_created_agent_runner_resources_is_null(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "rr-null")
    assert agent["runner_resources"] is None
    assert _read(client, auth_headers, agent["id"])["runner_resources"] is None


def test_patch_sets_runner_resources_and_a_model_patch_leaves_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "rr-set")
    resources = _resources()
    resp = _patch(client, auth_headers, agent["id"], {"runner_resources": resources})
    assert resp.status_code == 200, resp.text
    assert resp.json()["runner_resources"] == resources
    assert _read(client, auth_headers, agent["id"])["runner_resources"] == resources

    resp = _patch(client, auth_headers, agent["id"], {"model": "claude-sonnet-5"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "claude-sonnet-5"
    assert resp.json()["runner_resources"] == resources
    stored = _read(client, auth_headers, agent["id"])
    assert stored["model"] == "claude-sonnet-5"
    assert stored["runner_resources"] == resources


def test_patch_null_clears_runner_resources(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "rr-clear")
    assert (
        _patch(
            client, auth_headers, agent["id"], {"runner_resources": _resources()}
        ).status_code
        == 200
    )
    resp = _patch(client, auth_headers, agent["id"], {"runner_resources": None})
    assert resp.status_code == 200, resp.text
    assert resp.json()["runner_resources"] is None
    assert _read(client, auth_headers, agent["id"])["runner_resources"] is None


@pytest.mark.parametrize(
    "slug",
    ["missing-ephemeral", "cpu-above-limit", "blank-cpu", "json-string"],
)
def test_shape_refusal_does_not_store(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    slug: str,
) -> None:
    try:
        # Shape is checked with quota env unset, so a 422 is the shape and not the quota.
        _set_quota(monkeypatch, {})
        agent = _create_agent(client, auth_headers, f"rr-shape-{slug}")
        resources = _resources()
        seeded = _patch(client, auth_headers, agent["id"], {"runner_resources": resources})
        assert seeded.status_code == 200, seeded.text
        refused = _patch(client, auth_headers, agent["id"], _shape_body(slug))
        assert refused.status_code == 422, refused.text
        assert _read(client, auth_headers, agent["id"])["runner_resources"] == resources
    finally:
        get_settings.cache_clear()


def test_quota_refuses_over_cpu_and_stores_within_quota(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    try:
        _set_quota(monkeypatch, dict(_FULL_QUOTA))
        agent = _create_agent(client, auth_headers, "rr-quota-over")
        over = _resources()
        over["requests"]["cpu"] = "1500m"
        # 1500m is 1.5 cores. The block's limit must stay above that or the
        # shape check refuses it before the quota check. The hard requests.cpu
        # quota below is 1 core, which is what this case is over.
        over["limits"]["cpu"] = "2"
        refused = _patch(client, auth_headers, agent["id"], {"runner_resources": over})
        assert refused.status_code == 422, refused.text
        detail = _detail_text(refused)
        assert "cpu" in detail
        assert "1500m" in detail
        assert "quota" in detail.casefold()
        # "1" is the requests.cpu hard quota. It must appear outside the "1500m" token.
        assert re.search(r"(?<![0-9])1(?![0-9])", detail.replace("1500m", "")), detail
        assert _read(client, auth_headers, agent["id"])["runner_resources"] is None

        within = _resources()
        stored = _patch(client, auth_headers, agent["id"], {"runner_resources": within})
        assert stored.status_code == 200, stored.text
        assert stored.json()["runner_resources"] == within
        assert _read(client, auth_headers, agent["id"])["runner_resources"] == within
    finally:
        get_settings.cache_clear()


def test_quota_refusal_does_not_save_other_fields_in_the_same_request(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    try:
        _set_quota(monkeypatch, dict(_FULL_QUOTA))
        agent = _create_agent(client, auth_headers, "rr-quota-sibling")
        over = _resources()
        over["requests"]["cpu"] = "1500m"
        over["limits"]["cpu"] = "2"
        refused = _patch(
            client,
            auth_headers,
            agent["id"],
            {"model": "claude-sonnet-5", "runner_resources": over},
        )
        assert refused.status_code == 422, refused.text
        stored = _read(client, auth_headers, agent["id"])
        assert stored["model"] is None
        assert stored["runner_resources"] is None
    finally:
        get_settings.cache_clear()


def test_unset_quota_accepts_over_cpu_when_shape_is_valid(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    try:
        _set_quota(monkeypatch, {})
        agent = _create_agent(client, auth_headers, "rr-quota-unset")
        over = _resources()
        over["requests"]["cpu"] = "1500m"
        over["limits"]["cpu"] = "2"
        resp = _patch(client, auth_headers, agent["id"], {"runner_resources": over})
        assert resp.status_code == 200, resp.text
        assert resp.json()["runner_resources"] == over
        assert _read(client, auth_headers, agent["id"])["runner_resources"] == over
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize(
    "variable",
    _QUOTA_VARS,
    ids=["requests-cpu", "requests-memory", "limits-cpu", "limits-memory"],
)
@pytest.mark.parametrize("blank", [False, True], ids=["missing", "blank"])
def test_incomplete_quota_configuration_does_not_store(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    blank: bool,
) -> None:
    short = variable.removeprefix("CURIE_SANDBOX_QUOTA_").lower().replace("_", "-")
    kind = "blank" if blank else "miss"
    try:
        _set_quota(monkeypatch, dict(_FULL_QUOTA))
        agent = _create_agent(client, auth_headers, f"rr-quota-{kind}-{short}")
        resources = _resources()
        seeded = _patch(client, auth_headers, agent["id"], {"runner_resources": resources})
        assert seeded.status_code == 200, seeded.text

        partial: dict[str, str | None] = dict(_FULL_QUOTA)
        partial[variable] = "" if blank else None
        _set_quota(monkeypatch, partial)
        # 200m is inside every hard limit that is still set, so a check that
        # skips a missing or blank variable would store this. It must not.
        changed = _resources()
        changed["requests"]["cpu"] = "200m"
        refused = _patch(client, auth_headers, agent["id"], {"runner_resources": changed})
        assert refused.status_code == 422, refused.text
        assert "quota configuration is incomplete" in _detail_text(refused).casefold()
        assert _read(client, auth_headers, agent["id"])["runner_resources"] == resources
    finally:
        get_settings.cache_clear()
