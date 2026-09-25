#!/usr/bin/env bash
# Exercise dispatcher.slack.identities (ADR-0168 decision 1, #3102): a stock
# install renders the objects it always did, a list renders each identity by
# Secret reference only into the dispatcher, worker and API from one helper,
# every malformed shape fails the render, extraEnv cannot shadow a rendered
# name, and the EXACT rendered declaration parses through the real Settings of
# all three services while the worker's sandbox filter drops every token.
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

python3 - "$CHART" "$WORK" <<'PY'
import json
import pathlib
import shutil
import subprocess
import sys

import yaml

chart = pathlib.Path(sys.argv[1])
work = pathlib.Path(sys.argv[2])

SECRET = "acme-curie-secrets"
DECLARATION = "CURIE_SLACK_IDENTITIES"
INDEXED = ("CURIE_SLACK_APP_TOKEN__", "CURIE_SLACK_BOT_TOKEN__", "CURIE_SLACK_SIGNING_SECRET__")
# Generated credentials differ on every render; compare these by key set only.
RANDOM_DOCS = {
    ("Secret", SECRET),
    ("Job", "acme-curie-upgrade-drain"),
    ("Job", "acme-curie-upgrade-drain-release"),
}
PLAIN = {"dispatcher": {"slack": {"appToken": "xapp-example", "botToken": "xoxb-example"}}}

count = 0


def render(values, *args, source=None, ok=True):
    """One `helm template` of `values` into its own output dir."""
    global count
    count += 1
    values_file = work / f"values-{count}.yaml"
    values_file.write_text(yaml.safe_dump(values))
    output = work / str(count)
    result = subprocess.run(
        ["helm", "template", "acme", str(source or chart), "-f", str(values_file),
         "--output-dir", str(output), *args],
        text=True,
        capture_output=True,
    )
    if ok:
        assert result.returncode == 0, f"expected a successful render, got:\n{result.stderr}"
    return result, output


def documents(output):
    found = {}
    for path in sorted(output.rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc:
                found[(doc["kind"], doc["metadata"]["name"])] = doc
    return found


def env(output, filename, container):
    path = output / "curie/templates" / filename
    if not path.exists():
        return None
    for doc in yaml.safe_load_all(path.read_text()):
        for spec in [((doc or {}).get("spec") or {}).get("template", {}).get("spec", {})]:
            for entry in spec.get("containers", []):
                if entry["name"] == container:
                    return entry.get("env", [])
    return None


def workloads(output):
    return {
        "dispatcher": env(output, "dispatcher.yaml", "dispatcher"),
        "worker": env(output, "worker.yaml", "worker"),
        "api": env(output, "api.yaml", "api"),
    }


def named(entries, *names):
    return [entry for entry in entries if entry["name"] in names]


def ref(name, key):
    return {"secretKeyRef": {"name": name, "key": key}}


def entry(name, secret, key):
    return {"name": name, "valueFrom": ref(secret, key)}


def identity_env_names(output):
    """Every rendered env name that belongs to the identities feature, anywhere."""
    names = []
    for doc in documents(output).values():
        stack = [doc]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                if "env" in value and isinstance(value["env"], list):
                    names += [
                        e["name"] for e in value["env"]
                        if e["name"] == DECLARATION or e["name"].startswith(INDEXED)
                    ]
                stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)
    return names


def same_objects(label, left, right, ignore=()):
    """Every deterministic object equal, and the random ones equal by key set."""
    a, b = documents(left), documents(right)
    assert a.keys() == b.keys(), f"{label}: object sets differ: {sorted(set(a) ^ set(b))}"
    for key in a:
        if key in ignore:
            continue
        if key in RANDOM_DOCS:
            assert (a[key].get("stringData") or {}).keys() == (b[key].get("stringData") or {}).keys(), (
                f"{label}: {key} carries a different set of keys"
            )
            continue
        assert a[key] == b[key], f"{label}: {key} differs from the reference render"


def refuse(label, values, *expected, args=()):
    """A render that must FAIL with the given message fragments."""
    result, output = render(values, *args, ok=False)
    if result.returncode == 0:
        raise AssertionError(
            f"{label}: helm template SUCCEEDED but must be refused. Rendered identity env: "
            f"{identity_env_names(output)!r}"
        )
    for text in expected:
        assert text in result.stderr, (
            f"{label}: expected stderr to contain {text!r}\n--- actual stderr ---\n{result.stderr}"
        )


def slack(**fields):
    return {"dispatcher": {"slack": fields}}


def with_block(*identities, **extra):
    return slack(appToken="xapp-example", botToken="xoxb-example", identities=list(identities), **extra)


SECOND = {
    "name": "second",
    "appTokenExistingSecret": "slack-second",
    "botTokenExistingSecret": "slack-second",
    "signingSecretExistingSecret": "slack-second",
}
THIRD = {
    "name": "third",
    "appTokenExistingSecret": "slack-third",
    "appTokenExistingSecretKey": "app",
    "botTokenExistingSecret": "slack-third",
    "botTokenExistingSecretKey": "bot",
}
LEGACY_DISPATCHER = [
    entry("SLACK_APP_TOKEN", SECRET, "slackAppToken"),
    entry("SLACK_BOT_TOKEN", SECRET, "slackBotToken"),
    entry("SLACK_SIGNING_SECRET", SECRET, "slackSigningSecret"),
]
LEGACY_BOT = [entry("SLACK_BOT_TOKEN", SECRET, "slackBotToken")]
LEGACY_NAMES = ("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN", "SLACK_SIGNING_SECRET")

# --- S1: a stock install with no Slack at all -------------------------------
_, stock = render({})
stock_env = workloads(stock)
assert stock_env["dispatcher"] is None, "S1: a token-less install must not render the dispatcher"
for workload in ("worker", "api"):
    assert named(stock_env[workload], *LEGACY_NAMES) == LEGACY_BOT, (
        f"S1: {workload} must render exactly today's SLACK_BOT_TOKEN; got "
        f"{named(stock_env[workload], *LEGACY_NAMES)!r}"
    )
assert identity_env_names(stock) == [], f"S1: stock render carries {identity_env_names(stock)!r}"

# --- S2: the single block, plain values -------------------------------------
_, plain = render(PLAIN)
plain_env = workloads(plain)
assert named(plain_env["dispatcher"], *LEGACY_NAMES) == LEGACY_DISPATCHER, (
    f"S2: dispatcher Slack env changed: {named(plain_env['dispatcher'], *LEGACY_NAMES)!r}"
)
for workload in ("worker", "api"):
    assert named(plain_env[workload], *LEGACY_NAMES) == LEGACY_BOT, f"S2: {workload} changed"
assert identity_env_names(plain) == [], f"S2: plain render carries {identity_env_names(plain)!r}"

# --- S3: the single block by existingSecret, with a custom key --------------
_, byo = render(slack(
    appTokenExistingSecret="slack-main", appTokenExistingSecretKey="app",
    botTokenExistingSecret="slack-main", signingSecretExistingSecret="slack-sign",
))
byo_env = workloads(byo)
assert named(byo_env["dispatcher"], *LEGACY_NAMES) == [
    entry("SLACK_APP_TOKEN", "slack-main", "app"),
    entry("SLACK_BOT_TOKEN", "slack-main", "slackBotToken"),
    entry("SLACK_SIGNING_SECRET", "slack-sign", "slackSigningSecret"),
], f"S3: dispatcher BYO env changed: {named(byo_env['dispatcher'], *LEGACY_NAMES)!r}"
for workload in ("worker", "api"):
    assert named(byo_env[workload], *LEGACY_NAMES) == [
        entry("SLACK_BOT_TOKEN", "slack-main", "slackBotToken")
    ], f"S3: {workload} BYO env changed"
assert identity_env_names(byo) == []

# --- S4: an empty or null list is the stock shape, fresh and on upgrade -----
for label, identities in (("S4 empty", []), ("S4 null", None)):
    _, output = render({"dispatcher": {"slack": {**PLAIN["dispatcher"]["slack"], "identities": identities}}})
    same_objects(label, plain, output)
_, plain_upgrade = render(PLAIN, "--is-upgrade")
_, empty_upgrade = render(with_block(), "--is-upgrade")
same_objects("S4 upgrade", plain_upgrade, empty_upgrade)

# --- T1: the block plus two listed identities -------------------------------
_, two = render(with_block(SECOND, THIRD))
two_env = workloads(two)
expected_declaration = [
    {"name": "default", "app_token_env": "SLACK_APP_TOKEN", "bot_token_env": "SLACK_BOT_TOKEN",
     "signing_secret_env": "SLACK_SIGNING_SECRET"},
    {"name": "second", "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
     "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
     "signing_secret_env": "CURIE_SLACK_SIGNING_SECRET__0"},
    {"name": "third", "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
     "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1", "signing_secret_env": None},
]
indexed_dispatcher = [
    entry("CURIE_SLACK_APP_TOKEN__0", "slack-second", "slackAppToken"),
    entry("CURIE_SLACK_BOT_TOKEN__0", "slack-second", "slackBotToken"),
    entry("CURIE_SLACK_SIGNING_SECRET__0", "slack-second", "slackSigningSecret"),
    entry("CURIE_SLACK_APP_TOKEN__1", "slack-third", "app"),
    entry("CURIE_SLACK_BOT_TOKEN__1", "slack-third", "bot"),
]
indexed_bot = [
    entry("CURIE_SLACK_BOT_TOKEN__0", "slack-second", "slackBotToken"),
    entry("CURIE_SLACK_BOT_TOKEN__1", "slack-third", "bot"),
]
assert two_env["dispatcher"] is not None, "T1: the dispatcher must render"
assert named(two_env["dispatcher"], *LEGACY_NAMES) == LEGACY_DISPATCHER, "T1: default moved"
assert [e for e in two_env["dispatcher"] if e["name"].startswith(INDEXED)] == indexed_dispatcher, (
    f"T1: dispatcher indexed env: {[e for e in two_env['dispatcher'] if e['name'].startswith(INDEXED)]!r}"
)
declarations = {}
for workload in ("worker", "api"):
    assert named(two_env[workload], *LEGACY_NAMES) == LEGACY_BOT, f"T1: {workload} default moved"
    rendered = [e for e in two_env[workload] if e["name"].startswith(INDEXED)]
    assert rendered == indexed_bot, f"T1: {workload} must get bot tokens only; got {rendered!r}"
for workload, entries in two_env.items():
    values = [e["value"] for e in entries if e["name"] == DECLARATION]
    assert len(values) == 1, f"T1: {workload} must carry exactly one {DECLARATION}; got {values!r}"
    declarations[workload] = values[0]
assert len(set(declarations.values())) == 1, f"T1: the three workloads disagree: {declarations!r}"
assert json.loads(declarations["worker"]) == expected_declaration, declarations["worker"]
# Secret references only: nothing listed lands in the chart's own Secret.
for entries in two_env.values():
    for e in entries:
        if e["name"].startswith(INDEXED):
            assert e["valueFrom"]["secretKeyRef"]["name"] != SECRET, f"T1: {e!r} reads the chart Secret"
same_objects(
    "T1 everything else",
    plain,
    two,
    ignore={("Deployment", "acme-curie-dispatcher"), ("Deployment", "acme-curie-worker"),
            ("Deployment", "acme-curie-api")},
)
(work / "rendered-declaration.txt").write_text(declarations["worker"])
(work / "expected-declaration.json").write_text(json.dumps(expected_declaration))
(work / "worker-env-names.json").write_text(json.dumps([e["name"] for e in two_env["worker"]]))

# --- T2: no block, `default` listed; the dispatcher still deploys -----------
listed_default = {
    "name": "default",
    "appTokenExistingSecret": "slack-main",
    "botTokenExistingSecret": "slack-main",
}
_, only_list = render(slack(identities=[SECOND, listed_default]))
only_env = workloads(only_list)
assert only_env["dispatcher"] is not None, "T2: a list naming default must deploy the dispatcher"
assert named(only_env["dispatcher"], *LEGACY_NAMES) == [
    entry("SLACK_APP_TOKEN", "slack-main", "slackAppToken"),
    entry("SLACK_BOT_TOKEN", "slack-main", "slackBotToken"),
], f"T2: a listed default takes the legacy names: {named(only_env['dispatcher'], *LEGACY_NAMES)!r}"
for workload in ("worker", "api"):
    assert named(only_env[workload], *LEGACY_NAMES) == [
        entry("SLACK_BOT_TOKEN", "slack-main", "slackBotToken")
    ], f"T2: {workload} default bot token must follow the listed default"
# The one shape where `default` takes the legacy names from a list entry and
# has no signing secret; the real parser reads it below, beside T1's.
t2_expected = [
    {"name": "default", "app_token_env": "SLACK_APP_TOKEN", "bot_token_env": "SLACK_BOT_TOKEN",
     "signing_secret_env": None},
    {"name": "second", "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
     "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
     "signing_secret_env": "CURIE_SLACK_SIGNING_SECRET__0"},
]
t2_declarations = {
    workload: [e["value"] for e in entries if e["name"] == DECLARATION]
    for workload, entries in only_env.items()
}
assert all(len(values) == 1 for values in t2_declarations.values()), (
    f"T2: every workload must carry exactly one {DECLARATION}; got {t2_declarations!r}"
)
assert len({values[0] for values in t2_declarations.values()}) == 1, (
    f"T2: the three workloads disagree: {t2_declarations!r}"
)
t2_rendered = t2_declarations["worker"][0]
assert json.loads(t2_rendered) == t2_expected, f"T2: default first, no signing secret: {t2_rendered}"
(work / "t2-rendered-declaration.txt").write_text(t2_rendered)
(work / "t2-expected-declaration.json").write_text(json.dumps(t2_expected))

# --- T3: no dispatcher here; the API and worker still read the list ---------
headless_values = with_block(SECOND)
headless_values["dispatcher"]["deploy"] = False
_, headless = render(headless_values)
headless_env = workloads(headless)
assert headless_env["dispatcher"] is None, "T3: dispatcher.deploy=false must not render it"
for workload in ("worker", "api"):
    assert len(named(headless_env[workload], DECLARATION)) == 1, f"T3: {workload} lost the list"

# --- N: every malformed shape fails the render -------------------------------
W0 = "dispatcher.slack.identities[0]"
refuse("N1 duplicate name", with_block(SECOND, {**THIRD, "name": "second"}),
       "dispatcher.slack.identities[1].name", "repeats")
refuse("N2 uppercase name", with_block({**SECOND, "name": "Second"}), f"{W0}.name", "must match")
refuse("N3 leading hyphen", with_block({**SECOND, "name": "-second"}), f"{W0}.name", "must match")
refuse("N4 41 characters", with_block({**SECOND, "name": "a" * 41}), "at most 40 characters")
render(with_block({**SECOND, "name": "a" * 40}))  # N4 control: exactly 40 renders
refuse("N5 missing name", with_block({k: v for k, v in SECOND.items() if k != "name"}),
       f"{W0}.name")
refuse("N6 non-string name", with_block({**SECOND, "name": 7}), f"{W0}.name must be a string")
refuse("N7 missing app token ref",
       with_block({k: v for k, v in SECOND.items() if k != "appTokenExistingSecret"}),
       f"{W0}.appTokenExistingSecret")
refuse("N8 missing bot token ref",
       with_block({k: v for k, v in SECOND.items() if k != "botTokenExistingSecret"}),
       f"{W0}.botTokenExistingSecret")
refuse("N9 plain token value", with_block({**SECOND, "botToken": "xoxb-example"}),
       f"{W0}.botToken is a plain secret value", "#1759")
refuse("N10 unknown key", with_block({**SECOND, "note": "x"}), f"{W0} has unknown key \"note\"")
refuse("N11 default twice", with_block({**listed_default}), f"{W0} names the identity \"default\"")
refuse("N12 no default", slack(identities=[SECOND]), "declares no identity named \"default\"")
refuse("N13 half-configured block", slack(appToken="xapp-example", identities=[SECOND]),
       "dispatcher.slack is half-configured")
refuse("N14 a map, not a list", slack(appToken="xapp-example", botToken="xoxb-example",
                                      identities={"second": SECOND}),
       "dispatcher.slack.identities must be a list")
refuse("N15 a scalar entry", with_block("second"), f"{W0} must be a mapping")
refuse("N16 malformed second entry", with_block(SECOND, {**THIRD, "name": "Third"}),
       "dispatcher.slack.identities[1].name")
refuse("N17 refused on upgrade too", with_block({**SECOND, "name": "Second"}), f"{W0}.name",
       args=("--is-upgrade",))
refuse("N18 non-string signing ref", with_block({**SECOND, "signingSecretExistingSecret": True}),
       f"{W0}.signingSecretExistingSecret must name the Secret holding this identity's signing secret")
refuse("N19 empty signing ref", with_block({**SECOND, "signingSecretExistingSecret": ""}),
       f"{W0}.signingSecretExistingSecret must name the Secret holding this identity's signing secret")
refuse("N20 signing key without its Secret",
       with_block({**THIRD, "signingSecretExistingSecretKey": "signing"}),
       f"{W0}.signingSecretExistingSecretKey is set without {W0}.signingSecretExistingSecret")
refuse("N21 an empty map, not a list", slack(appToken="xapp-example", botToken="xoxb-example",
                                            identities={}),
       "dispatcher.slack.identities must be a list")
refuse("N22 the reserved delivery selector", with_block({**SECOND, "name": "curie-cluster-message"}),
       f"{W0}.name", "curie-cluster-message", "reserved")

# --- X: extraEnv cannot shadow a name this chart owns -----------------------
def with_extra_env(values, workload, name):
    """`values` plus one `<workload>.extraEnv` entry named `name`."""
    values = json.loads(json.dumps(values))
    values.setdefault(workload, {})["extraEnv"] = [{"name": name, "value": "shadow"}]
    return values


# X1: reserved even with no list, like every optional branch (files/reserved-env.yaml).
for workload in ("worker", "api", "dispatcher"):
    refuse(f"X1 {workload} declaration", with_extra_env(PLAIN, workload, DECLARATION),
           f"{workload}.extraEnv", DECLARATION, "dispatcher.slack.identities")
# X2: an indexed name is reserved wherever the list renders it.
for workload, name in (("worker", "CURIE_SLACK_BOT_TOKEN__0"), ("api", "CURIE_SLACK_BOT_TOKEN__0"),
                       ("dispatcher", "CURIE_SLACK_APP_TOKEN__0"),
                       ("dispatcher", "CURIE_SLACK_SIGNING_SECRET__0")):
    refuse(f"X2 {workload} {name}", with_extra_env(with_block(SECOND), workload, name),
           f"{workload}.extraEnv", name, W0)

# --- M: the reservation is what keeps one entry per name --------------------
mutant = work / "mutant"
shutil.copytree(chart, mutant)
helper = mutant / "templates/_slack-identities.tpl"
text = helper.read_text()
reservation = "{{- toJson $reserved -}}"
assert text.count(reservation) == 1, f"M: expected one {reservation!r} in _slack-identities.tpl"
helper.write_text(text.replace(reservation, "{}"))
_, mutant_output = render(
    with_extra_env(with_block(SECOND), "worker", "CURIE_SLACK_BOT_TOKEN__0"), source=mutant
)
duplicates = named(env(mutant_output, "worker.yaml", "worker"), "CURIE_SLACK_BOT_TOKEN__0")
assert len(duplicates) == 2, f"M: without the reservation the mutant must emit two; got {duplicates!r}"

print(f"OK: {count} Helm renders; stock, plain, BYO, empty and null lists render today's objects; "
      "two identities render by Secret reference into all three workloads from one declaration; "
      "every malformed shape and every extraEnv shadow refused; removed-reservation control "
      "reproduced two entries")
PY

# Round-trip the EXACT rendered declaration through the REAL Settings of all
# three services, and the worker's rendered env names through the REAL sandbox
# filter. A copy of either rule here would prove nothing about drift. `uv run`
# starts from the repo root: the apps are workspace members.
(
  cd "$REPO_ROOT"
  uv run --python 3.13 python - "$WORK/rendered-declaration.txt" "$WORK/expected-declaration.json" "$WORK/worker-env-names.json" \
    "$WORK/t2-rendered-declaration.txt" "$WORK/t2-expected-declaration.json" <<'PY'
import json
import os
import sys

# Ambient CURIE_* or SLACK_* on a developer box or runner would otherwise decide this gate.
for key in [key for key in os.environ if key.startswith(("CURIE_", "SLACK_"))]:
    del os.environ[key]

rendered = open(sys.argv[1]).read()
expected = json.load(open(sys.argv[2]))
worker_env_names = json.load(open(sys.argv[3]))
t2_rendered = open(sys.argv[4]).read()
t2_expected = json.load(open(sys.argv[5]))

os.environ["CURIE_API_KEY"] = "example-api-key"
os.environ["CURIE_APPROVAL_CHAT_ATTESTER_SECRET"] = "example-attester-secret"

from curie_api.config import Settings
from curie_dispatcher.config import DispatcherConfig
from curie_worker.config import WorkerConfig
from curie_worker.sandbox.types import filter_agent_child_env
from pydantic import ValidationError


def parse_everywhere(label, declaration, want):
    """The declaration as all three services read it, and read alike."""
    os.environ["CURIE_SLACK_IDENTITIES"] = declaration
    parsed = {
        "api": Settings().slack_identities,
        "worker": WorkerConfig().slack_identities,
        "dispatcher": DispatcherConfig().slack_identities,
    }
    assert len(set(parsed.values())) == 1, f"{label}: the three services disagree: {parsed!r}"
    assert [identity.model_dump() for identity in parsed["api"]] == want, (label, parsed["api"])
    return parsed


t2_parsed = parse_everywhere("T2", t2_rendered, t2_expected)
parsed = parse_everywhere("T1", rendered, expected)

token_names = {
    name for name in worker_env_names
    if name == "SLACK_BOT_TOKEN" or name.startswith("CURIE_SLACK_BOT_TOKEN__")
}
assert len(token_names) == 3, f"expected the worker's three bot tokens, got {token_names!r}"
child = filter_agent_child_env({name: "placeholder" for name in worker_env_names})
assert token_names.isdisjoint(child), f"the sandbox filter let {token_names & child.keys()!r} through"

# The negative direction: an env name outside the filtered shapes must be
# refused by the same parser, or the filter is no longer complete.
unfiltered = json.loads(rendered)
unfiltered[1]["bot_token_env"] = "PATH"
os.environ["CURIE_SLACK_IDENTITIES"] = json.dumps(unfiltered)
try:
    WorkerConfig()
except ValidationError:
    pass
else:
    raise AssertionError("the real parser ACCEPTED a bot token env name the sandbox filter misses")

print(f"OK: the rendered declarations parsed identically in the API, worker and dispatcher "
      f"({[identity.name for identity in parsed['api']]} and, list-only, "
      f"{[identity.name for identity in t2_parsed['api']]}); the sandbox filter dropped "
      f"{sorted(token_names)}; an unfiltered env name was refused")
PY
)
