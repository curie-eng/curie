"""The SRE bot looks up an alert where its rule actually lives.

Every alert this bundle pages on is a Prometheus rule: it is loaded from
``serverFiles`` in ``observability/prometheus-values.yaml`` and evaluated by
that Prometheus, so Grafana lists it as datasource-managed, never as a
Grafana-managed rule.

The failure this exists to stop, observed on a live install during a drill of
eight alerts: the bot searched ``alerting_manage_rules`` by name, found no
Grafana-managed match, and wrote that no matching rule exists in Grafana. It
said this about several alerts, ``CuriePersistentVolumeSpaceLow`` among them.
Its conclusions happened to be right, but "the rule does not exist" was false,
and a person reading it would doubt the alert that had just paged them. The
one time it looked at datasource-managed rules, it found the rule.

These tests read the skill's prose with HTML comments removed: the operator
catalogue comment in SKILL.md talks about alert rules too, and a comment is
not something the model is told.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "sre-bot"
SKILL = BUNDLE / "skills" / "sre-bot" / "SKILL.md"
PROMETHEUS_VALUES = BUNDLE / "observability" / "prometheus-values.yaml"
GRAFANA_VALUES = BUNDLE / "observability" / "grafana-values.yaml"

GRAFANA_HEADING = "## If Grafana tools are present"

PROMETHEUS_RULES = re.compile(r"\bPrometheus (?:alert(?:ing)? )?rules?\b", re.IGNORECASE)
ALERTS_QUERY = re.compile(r"\bALERTS\{\s*alertname\s*=")
GRAFANA_MANAGED = re.compile(r"\bGrafana[- ]managed\b", re.IGNORECASE)
CALLED_MISSING = re.compile(
    r"\bmissing\b|\bdoes(?: not|n't) exist\b|\babsent\b|\bno (?:such|matching) rule\b",
    re.IGNORECASE,
)
PROHIBITION = re.compile(
    r"\bnever\b"
    r"|\b(?:do not|don't|must not)"
    r" (?:say|report|call|write|claim|conclude|treat|describe|state)\b",
    re.IGNORECASE,
)


def _skill_prose() -> str:
    return re.sub(r"<!--.*?-->", "", SKILL.read_text(encoding="utf-8"), flags=re.DOTALL)


def _grafana_section() -> str:
    prose = _skill_prose()
    start = prose.find(f"\n{GRAFANA_HEADING}\n")
    assert start != -1, f"SKILL.md must keep its '{GRAFANA_HEADING}' section"
    body = prose[start + 1 :]
    following = re.search(r"^## ", body[len(GRAFANA_HEADING) :], flags=re.MULTILINE)
    return body if following is None else body[: len(GRAFANA_HEADING) + following.start()]


def _flat(text: str) -> str:
    return " ".join(text.split())


def _items(section: str) -> list[str]:
    """Each bullet, heading or paragraph of ``section``, flattened to one line.

    A fenced block stays with the bullet it sits in, so a query written as a
    code block still counts as part of the instruction around it.
    """

    items: list[str] = []
    current: list[str] = []
    fenced = False
    after_blank = False
    for line in section.splitlines():
        if re.match(r"^\s*(?:`{3,}|~{3,})", line):
            fenced = not fenced
            current.append(line)
            after_blank = False
            continue
        if fenced:
            current.append(line)
            continue
        if not line.strip():
            after_blank = True
            continue
        starts_item = line.startswith(("- ", "* ", "#")) or (after_blank and not line[0].isspace())
        if starts_item and current:
            items.append(_flat("\n".join(current)))
            current = []
        current.append(line)
        after_blank = False
    if current:
        items.append(_flat("\n".join(current)))
    return items


def _sentences(section: str) -> list[str]:
    return [sentence for item in _items(section) for sentence in re.split(r"(?<=[.!?])\s+", item)]


def test_skill_says_the_bundles_alerts_are_prometheus_rules() -> None:
    assert [
        sentence
        for sentence in _sentences(_grafana_section())
        if re.search(r"\bbundle\b", sentence, re.IGNORECASE)
        and re.search(r"\balerts?\b", sentence, re.IGNORECASE)
        and PROMETHEUS_RULES.search(sentence)
    ], (
        "The Grafana section must say the alerts this bundle pages on are "
        "Prometheus rules, so the bot does not look for them among "
        "Grafana-managed rules alone."
    )


def test_skill_gives_the_alerts_series_fallback_through_query_prometheus() -> None:
    assert [
        item
        for item in _items(_grafana_section())
        if ALERTS_QUERY.search(item) and "`query_prometheus`" in item
    ], (
        "The Grafana section must give the fallback of querying "
        'ALERTS{alertname="<name>"} through `query_prometheus` when a name '
        "search of Grafana-managed rules finds nothing."
    )


def test_skill_forbids_calling_a_rule_missing_because_grafana_managed_rules_lack_it() -> None:
    assert [
        sentence
        for sentence in _sentences(_grafana_section())
        if GRAFANA_MANAGED.search(sentence)
        and CALLED_MISSING.search(sentence)
        and PROHIBITION.search(sentence)
    ], (
        "The Grafana section must forbid reporting a rule as missing because "
        "Grafana-managed rules do not list it."
    )


def test_the_claim_holds_the_bundle_provisions_no_grafana_managed_alerts() -> None:
    # The skill's claim is a fact about these two files. If the bundle ever
    # provisions Grafana-managed rules, the sentence goes false with it.
    grafana = yaml.safe_load(GRAFANA_VALUES.read_text(encoding="utf-8")) or {}
    assert "alerting" not in grafana
    assert "alerts" not in (grafana.get("sidecar") or {})
    prometheus = yaml.safe_load(PROMETHEUS_VALUES.read_text(encoding="utf-8"))
    rules = [
        rule
        for document in (prometheus.get("serverFiles") or {}).values()
        for group in (document or {}).get("groups") or []
        for rule in group.get("rules") or []
        if "alert" in rule
    ]
    assert rules, "prometheus-values.yaml serverFiles must carry the bundle's alert rules"
