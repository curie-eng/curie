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
from curie_api.runner_resources import (
    RunnerResourcesError,
    quota_refusal,
    validate_runner_resources,
)

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

# The same quota with a decimal core value (#3719). `resourceQuota.hard.requestsCpu:
# "2.5"` is a valid Kubernetes quantity the chart renders into the env setting
# unchanged, and it means the same as 2500m.
_FRACTIONAL_QUOTA: dict[str, str] = {
    "CURIE_SANDBOX_QUOTA_REQUESTS_CPU": "2.5",
    "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY": "2Gi",
    "CURIE_SANDBOX_QUOTA_LIMITS_CPU": "4",
    "CURIE_SANDBOX_QUOTA_LIMITS_MEMORY": "4Gi",
}

# Same shape as _FRACTIONAL_QUOTA for quota_refusal's keyword arguments.
_FRACTIONAL_QUOTA_ARGS: dict[str, str] = {
    "requests_cpu": "2.5",
    "requests_memory": "2Gi",
    "limits_cpu": "4",
    "limits_memory": "4Gi",
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


def test_validate_runner_resources_stores_stripped_quantities() -> None:
    raw = _resources()
    raw["requests"]["cpu"] = " 500m "
    stored = validate_runner_resources(raw)
    assert stored is not None
    assert stored["requests"]["cpu"] == "500m"
    assert stored["limits"]["memory"] == "2Gi"


def test_quota_refusal_accepts_decimal_cpu_quota_for_a_fitting_override() -> None:
    # AC #3719: a decimal core quota (2.5, and the smaller 0.5) is a valid
    # Kubernetes quantity and compares in millicores, so it behaves exactly
    # like its millicore spelling.
    fitting = _resources()  # 500m requested of the cpu quota.
    assert quota_refusal(fitting, **_FRACTIONAL_QUOTA_ARGS) is None
    # The decimal core compares equal to its millicore spelling.
    millicore_quota = dict(_FRACTIONAL_QUOTA_ARGS, requests_cpu="2500m")
    assert quota_refusal(fitting, **millicore_quota) is None
    half_core: dict[str, str] = dict(_FRACTIONAL_QUOTA_ARGS, requests_cpu="0.5", limits_cpu="1")
    under_half = _resources()
    under_half["requests"]["cpu"] = "500m"
    assert quota_refusal(under_half, **half_core) is None


def test_quota_refusal_refuses_an_oversized_override_against_a_decimal_quota() -> None:
    over = _resources()
    over["requests"]["cpu"] = "3000m"
    # The block's own limit must stay above the request or the shape check
    # refuses first; 3000m requested of a 2.5-core quota is the oversized case.
    over["limits"]["cpu"] = "4"
    refusal = quota_refusal(over, **_FRACTIONAL_QUOTA_ARGS)
    assert refusal is not None
    assert "cpu request 3000m cannot fit sandbox quota hard 2.5" in refusal
    assert "lower the override or raise resourceQuota.hard" in refusal


def test_quota_refusal_names_the_setting_for_an_unparseable_quota_value() -> None:
    # An invalid quota value is an operator configuration error, so the refusal
    # names the quota setting and never reads as a problem with the request.
    with pytest.raises(RunnerResourcesError) as exc:
        quota_refusal(_resources(), **dict(_FRACTIONAL_QUOTA_ARGS, requests_cpu="2x"))
    message = str(exc.value)
    assert "CURIE_SANDBOX_QUOTA_REQUESTS_CPU" in message
    assert "'2x'" in message
    with pytest.raises(RunnerResourcesError) as exc:
        quota_refusal(_resources(), **dict(_FRACTIONAL_QUOTA_ARGS, requests_memory="2Xi"))
    assert "CURIE_SANDBOX_QUOTA_REQUESTS_MEMORY" in str(exc.value)


def test_quota_refusal_accepts_the_bare_point_decimal_forms() -> None:
    # ".5" and "2." are valid Kubernetes quantities, so the quota side accepts
    # them; the override grammar keeps refusing its own stricter spellings.
    point_five: dict[str, str] = dict(_FRACTIONAL_QUOTA_ARGS, requests_cpu=".5", limits_cpu="2")
    half = _resources()  # requests.cpu 500m, exactly the .5-core quota.
    assert quota_refusal(half, **point_five) is None
    over_half = _resources()
    over_half["requests"]["cpu"] = "600m"
    refusal = quota_refusal(over_half, **point_five)
    assert refusal is not None
    assert "cpu request 600m cannot fit sandbox quota hard .5" in refusal

    trailing_point: dict[str, str] = dict(_FRACTIONAL_QUOTA_ARGS, requests_cpu="2.")
    under_two = _resources()
    under_two["requests"]["cpu"] = "1500m"
    assert quota_refusal(under_two, **trailing_point) is None


def test_quota_refusal_compares_a_huge_millicore_quota_exactly() -> None:
    # 9007199254740993 is the first integer float cannot hold, so this pins
    # the quota comparison to exact integer arithmetic: an override equal to
    # its quota fits, and one millicore above it is refused.
    huge = "9007199254740993"
    quota: dict[str, str] = dict(_FRACTIONAL_QUOTA_ARGS, requests_cpu=f"{huge}m")
    equal = _resources()
    equal["requests"]["cpu"] = f"{huge}m"
    assert quota_refusal(equal, **quota) is None
    above = _resources()
    above["requests"]["cpu"] = f"{int(huge) + 1}m"
    refusal = quota_refusal(above, **quota)
    assert refusal is not None
    assert f"cpu request {int(huge) + 1}m cannot fit sandbox quota hard {huge}m" in refusal


def test_override_cpu_grammar_still_refuses_decimal_values() -> None:
    # Only the quota side widened (#3719). An override cpu like "2.5" in the
    # request body is refused as before, through both the shape check and the
    # quota preflight.
    fractional = _resources()
    fractional["requests"]["cpu"] = "2.5"
    fractional["limits"]["cpu"] = "4"
    with pytest.raises(RunnerResourcesError) as exc:
        validate_runner_resources(fractional)
    assert "cpu quantity '2.5' is not a whole number of cores or millicores" in str(exc.value)
    with pytest.raises(RunnerResourcesError) as exc:
        quota_refusal(fractional, **_FRACTIONAL_QUOTA_ARGS)
    assert "cpu quantity '2.5' is not a whole number of cores or millicores" in str(exc.value)


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


def test_decimal_quota_stores_a_fitting_override_and_refuses_an_oversized_one(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    try:
        # #3719: `resourceQuota.hard.requestsCpu: "2.5"` is a valid Kubernetes
        # quantity, so an override inside it (500m of 2500m) must be stored,
        # and only an override that exceeds it refused.
        _set_quota(monkeypatch, dict(_FRACTIONAL_QUOTA))
        agent = _create_agent(client, auth_headers, "rr-quota-decimal")
        within = _resources()
        stored = _patch(client, auth_headers, agent["id"], {"runner_resources": within})
        assert stored.status_code == 200, stored.text
        assert stored.json()["runner_resources"] == within
        assert _read(client, auth_headers, agent["id"])["runner_resources"] == within

        over = _resources()
        over["requests"]["cpu"] = "3000m"
        over["limits"]["cpu"] = "4"
        refused = _patch(client, auth_headers, agent["id"], {"runner_resources": over})
        assert refused.status_code == 422, refused.text
        detail = _detail_text(refused)
        assert "cpu request 3000m cannot fit sandbox quota hard 2.5" in detail
        assert _read(client, auth_headers, agent["id"])["runner_resources"] == within
    finally:
        get_settings.cache_clear()


def test_decimal_quota_still_refuses_a_decimal_override_value(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    try:
        # The quota side widened, not the override grammar: a cpu of "2.5" in
        # the request body is refused as before, even under a decimal quota.
        _set_quota(monkeypatch, dict(_FRACTIONAL_QUOTA))
        agent = _create_agent(client, auth_headers, "rr-quota-decimal-override")
        fractional = _resources()
        fractional["requests"]["cpu"] = "2.5"
        fractional["limits"]["cpu"] = "4"
        refused = _patch(client, auth_headers, agent["id"], {"runner_resources": fractional})
        assert refused.status_code == 422, refused.text
        detail = _detail_text(refused)
        assert "cpu quantity '2.5' is not a whole number of cores or millicores" in detail
        assert _read(client, auth_headers, agent["id"])["runner_resources"] is None
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
