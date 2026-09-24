"""Guards for the opt-in alert path heartbeat (issue #3059).

A broken alert path looks exactly like a quiet cluster. The heartbeat overlay
adds an always-firing rule and routes it to a dead man's switch outside the
cluster, so that "no alert firing" can be trusted while the heartbeat keeps
arriving. It is applied after alertmanager-webhook.yaml, and Helm replaces
lists, so it restates what it keeps: the curie-sre receiver and the chart's
default rule files. The default install must carry no heartbeat at all, or
every existing Alertmanager overlay starts sending an always-firing alert to
the bot.

These are structural checks on the shipped files. Alertmanager's own router
and promtool run on the rendered config in
charts/curie/ci/observability-stack-assertions.sh.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY = REPO_ROOT / "examples" / "sre-bot" / "observability"
HEARTBEAT_OVERLAY = OBSERVABILITY / "alertmanager-heartbeat.yaml"
WEBHOOK_OVERLAY = OBSERVABILITY / "alertmanager-webhook.yaml"
PROMETHEUS_VALUES = OBSERVABILITY / "prometheus-values.yaml"

HEARTBEAT_ALERT = "CurieAlertPathHeartbeat"
HEARTBEAT_RECEIVER = "heartbeat"
BOT_RECEIVER = "curie-sre"
HEARTBEAT_SECRET = "alertmanager-heartbeat"
HEARTBEAT_RULE_FILE = "/etc/config/heartbeat_rules.yml"

# serverFiles."prometheus.yml".rule_files as prometheus-community/prometheus
# 29.27.0 (PROMETHEUS_CHART_VERSION in observability-stack-assertions.sh)
# ships it. An overlay that sets rule_files replaces this list, so leaving one
# out stops Prometheus loading it.
CHART_DEFAULT_RULE_FILES = [
    "/etc/config/recording_rules.yml",
    "/etc/config/alerting_rules.yml",
    "/etc/config/rules",
    "/etc/config/alerts",
]


def _load(path: Path) -> dict:
    assert path.is_file(), f"missing observability overlay {path}"
    payload = yaml.safe_load(path.read_text())
    assert isinstance(payload, dict), f"{path} is not a YAML mapping"
    return payload


def _config(values: dict) -> dict:
    return (values.get("alertmanager") or {}).get("config") or {}


def _receivers(values: dict) -> dict[str, dict]:
    receivers = _config(values).get("receivers") or []
    names = [receiver["name"] for receiver in receivers]
    assert len(names) == len(set(names)), f"duplicate receiver names: {names}"
    return {receiver["name"]: receiver for receiver in receivers}


def _routed_receivers(route: dict) -> set[str]:
    found = {route["receiver"]} if route.get("receiver") else set()
    for child in route.get("routes") or []:
        found |= _routed_receivers(child)
    return found


def _rules(rule_file: dict) -> list[dict]:
    rules: list[dict] = []
    for group in (rule_file or {}).get("groups") or []:
        rules.extend(group.get("rules") or [])
    return rules


def _heartbeat_volume(overlay: dict) -> dict:
    volumes = [
        volume
        for volume in overlay["alertmanager"].get("extraVolumes") or []
        if (volume.get("secret") or {}).get("secretName") == HEARTBEAT_SECRET
    ]
    assert len(volumes) == 1, (
        f"expected one extraVolumes entry for Secret {HEARTBEAT_SECRET}, got {volumes}"
    )
    return volumes[0]


def _normalized_matchers(route: dict) -> list[str]:
    return ["".join(matcher.split()) for matcher in route.get("matchers") or []]


def test_heartbeat_overlay_enables_alertmanager() -> None:
    overlay = _load(HEARTBEAT_OVERLAY)
    assert overlay["alertmanager"]["enabled"] is True


def test_heartbeat_overlay_keeps_the_base_default_route() -> None:
    route = _config(_load(HEARTBEAT_OVERLAY))["route"]
    base_route = _config(_load(WEBHOOK_OVERLAY))["route"]
    assert route["receiver"] == BOT_RECEIVER, (
        "every alert other than the heartbeat must still reach the bot"
    )
    assert route["group_by"] == base_route["group_by"]


def test_only_the_heartbeat_routes_away_from_the_bot() -> None:
    route = _config(_load(HEARTBEAT_OVERLAY))["route"]
    children = route.get("routes") or []
    assert len(children) == 1, (
        f"expected exactly one child route, for the heartbeat; got {children}"
    )
    child = children[0]
    assert child["receiver"] == HEARTBEAT_RECEIVER
    assert _normalized_matchers(child) == [f'alertname="{HEARTBEAT_ALERT}"'], (
        f"the heartbeat route must match alertname={HEARTBEAT_ALERT} and "
        f"nothing else; got {child.get('matchers')}"
    )
    assert "match" not in child and "match_re" not in child


def test_heartbeat_route_repeats_on_the_measured_cadence() -> None:
    child = _config(_load(HEARTBEAT_OVERLAY))["route"]["routes"][0]
    assert child["group_wait"] == "0s"
    assert child["group_interval"] == "1m"
    assert child["repeat_interval"] == "1m"


def test_heartbeat_receiver_reads_its_url_from_the_mounted_secret() -> None:
    overlay = _load(HEARTBEAT_OVERLAY)
    receivers = _receivers(overlay)
    assert set(receivers) == {BOT_RECEIVER, HEARTBEAT_RECEIVER}
    webhooks = receivers[HEARTBEAT_RECEIVER].get("webhook_configs") or []
    assert len(webhooks) == 1, f"expected one heartbeat webhook, got {webhooks}"
    webhook = webhooks[0]
    assert "url" not in webhook, "the heartbeat URL must come from the Secret"
    assert webhook["send_resolved"] is False

    volume = _heartbeat_volume(overlay)
    assert volume["secret"].get("optional") is True, (
        "a missing heartbeat Secret must fail only the heartbeat posts; a "
        "required one keeps Alertmanager from starting and takes the bot's "
        "alerts down with it"
    )
    mounts = [
        mount
        for mount in overlay["alertmanager"].get("extraVolumeMounts") or []
        if mount.get("name") == volume["name"]
    ]
    assert len(mounts) == 1, (
        f"expected one extraVolumeMounts entry for volume {volume['name']}"
    )
    mount = mounts[0]
    assert mount.get("readOnly") is True
    url_file = PurePosixPath(webhook["url_file"])
    mount_path = PurePosixPath(mount["mountPath"])
    assert url_file.is_absolute()
    assert url_file != mount_path and url_file.is_relative_to(mount_path), (
        f"url_file {url_file} is not a file under the Secret mount {mount_path}"
    )


def test_heartbeat_overlay_leaves_extra_secret_mounts_to_the_operator() -> None:
    alertmanager = _load(HEARTBEAT_OVERLAY)["alertmanager"]
    assert "extraSecretMounts" not in alertmanager, (
        "the operator's alert-signer token mount lives in extraSecretMounts, "
        "and Helm replaces the list, so setting it here drops that mount or "
        "loses the heartbeat's, whichever overlay is applied last"
    )


def test_heartbeat_overlay_restates_curie_sre_exactly() -> None:
    restated = _receivers(_load(HEARTBEAT_OVERLAY))[BOT_RECEIVER]
    base = _receivers(_load(WEBHOOK_OVERLAY))[BOT_RECEIVER]
    assert restated == base, (
        "Helm replaces the receivers list, so the heartbeat overlay must carry "
        "curie-sre exactly as alertmanager-webhook.yaml has it"
    )


def test_heartbeat_overlay_keeps_the_chart_default_rule_files() -> None:
    server_files = _load(HEARTBEAT_OVERLAY)["serverFiles"]
    assert server_files["prometheus.yml"]["rule_files"] == [
        *CHART_DEFAULT_RULE_FILES,
        HEARTBEAT_RULE_FILE,
    ]
    assert "alerting_rules.yml" not in server_files, (
        "an overlay alerting_rules.yml replaces the reliability rule groups"
    )


def test_heartbeat_rule_always_fires_and_never_pages() -> None:
    server_files = _load(HEARTBEAT_OVERLAY)["serverFiles"]
    rules = _rules(server_files["heartbeat_rules.yml"])
    assert [rule.get("alert") for rule in rules] == [HEARTBEAT_ALERT]
    rule = rules[0]
    assert str(rule["expr"]).strip() == "vector(1)"
    assert rule["labels"]["severity"] == "none"
    assert rule["labels"]["component"] == "curie-alert-path"
    annotations = rule.get("annotations") or {}
    assert annotations.get("summary") and annotations.get("description")


def test_default_install_carries_no_heartbeat() -> None:
    for path in (WEBHOOK_OVERLAY, PROMETHEUS_VALUES):
        assert HEARTBEAT_ALERT not in path.read_text(), (
            f"{path.name} must not carry {HEARTBEAT_ALERT}; it is opt-in"
        )

    webhook = _load(WEBHOOK_OVERLAY)
    assert HEARTBEAT_RECEIVER not in _receivers(webhook)
    assert HEARTBEAT_RECEIVER not in _routed_receivers(_config(webhook)["route"])

    values = _load(PROMETHEUS_VALUES)
    assert HEARTBEAT_RECEIVER not in _receivers(values)
    server_files = values.get("serverFiles") or {}
    assert "heartbeat_rules.yml" not in server_files
    rule_files = (server_files.get("prometheus.yml") or {}).get("rule_files") or []
    assert HEARTBEAT_RULE_FILE not in rule_files
    always_firing = [
        rule.get("alert")
        for rule in _rules(server_files.get("alerting_rules.yml"))
        if str(rule.get("expr", "")).strip() == "vector(1)"
    ]
    assert not always_firing, f"default rules carry always-firing {always_firing}"
