#!/usr/bin/env bash
# Exercise the first-class dispatcher.threadedBotAllowlist value (#2437):
# render controls, render-time refusals, a removed-guard duplicate control, the
# fresh and --is-upgrade paths, and a round trip of the EXACT rendered string
# through the real dispatcher parser.
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

NAME = "CURIE_SLACK_THREADED_BOT_ALLOWLIST"
# Fixture ids use the sanctioned placeholder shape (apps/dispatcher/README.md:72).
# This repo's gitleaks configuration flags realistic-looking Slack ids, so never
# substitute a plausible one here or in a comment.
CHANNEL, CHANNEL_2 = "C0EXAMPLE1", "C0EXAMPLE2"
BOT, BOT_2 = "B0EXAMPLE1", "B0EXAMPLE2"
GROUP = "G0EXAMPLE1"
DM = "D0EXAMPLE1"

# A bare `helm template charts/curie` renders almost nothing: the dispatcher is
# gated behind curie.dispatcher.enabled. Same base shape as
# ci/reserved-env-assertions.sh.
base = {
    "dispatcher": {"slack": {"appToken": "xapp-example", "botToken": "xoxb-example"}},
    "agentSandbox": {"runner": {"credentials": "example", "workspace": {"enabled": True}}},
}
values = work / "values.yaml"
values.write_text(yaml.safe_dump(base))


def dispatcher_values(**overrides):
    """base values with extra dispatcher keys merged in, without mutating base."""
    return {**base, "dispatcher": {**base["dispatcher"], **overrides}}


def helm_escape(value):
    """helm's --set/--set-string data parser splits on unescaped commas."""
    return value.replace(",", "\\,")


# An operator already on the OLD shape: dispatcher.extraEnv carries the variable
# and there is NO threadedBotAllowlist key at all. This is what `helm get values`
# returns for such a release, and it is the input for the --is-upgrade path.
OPERATOR_VALUE = json.dumps([{"channel_id": CHANNEL, "bot_id": BOT}], separators=(",", ":"))
# helm's --set/--set-string parser splits its data on UNESCAPED commas, so a
# JSON value like OPERATOR_VALUE (which contains one) blows up argument
# parsing before the chart ever renders. Escape the comma(s) ONLY in what we
# hand to helm on the command line; the expected/asserted value stays the
# real, unescaped JSON string, since the whole point of these cases is that
# the operator's string reaches the rendered env verbatim.
OPERATOR_SET = helm_escape(OPERATOR_VALUE)
legacy = dispatcher_values(extraEnv=[{"name": NAME, "value": OPERATOR_VALUE}])
legacy_values = work / "legacy-retained.yaml"
legacy_values.write_text(yaml.safe_dump(legacy))

count = 0


def render(*args, source=None, values_file=None, ok=True):
    """One `helm template` into its own output dir. Mirrors reserved-env-assertions.sh."""
    global count
    count += 1
    output = work / str(count)
    result = subprocess.run(
        [
            "helm", "template", "acme", str(source or chart),
            "-f", str(values_file or values),
            "--output-dir", str(output),
            *args,
        ],
        text=True,
        capture_output=True,
    )
    if ok:
        assert result.returncode == 0, f"expected a successful render, got:\n{result.stderr}"
    return result, output


def envs(output):
    """The dispatcher container's env list out of the rendered Deployment."""
    docs = yaml.safe_load_all((output / "curie/templates/dispatcher.yaml").read_text())

    def walk(value):
        if isinstance(value, dict):
            if value.get("name") == "dispatcher" and "env" in value:
                yield value["env"]
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)

    return next(env for doc in docs for env in walk(doc))


def allowlist_entries(output):
    return [entry for entry in envs(output) if entry["name"] == NAME]


def others(output):
    """Every env entry that is NOT the allowlist, for byte-identity comparisons."""
    return [entry for entry in envs(output) if entry["name"] != NAME]


def refuse(label, *args, expected, values_file=None):
    """A render that must FAIL, and must fail with the plan's message text.

    Asserting the message, not just the exit code, is what keeps these controls
    non-vacuous: against a chart with no validation the render SUCCEEDS and the
    first assertion fires with the entries it wrongly produced.
    """
    result, output = render(*args, values_file=values_file, ok=False)
    if result.returncode == 0:
        rendered = allowlist_entries(output)
        raise AssertionError(
            f"{label}: helm template SUCCEEDED but the entry is malformed and must be "
            f"refused at render. Rendered {NAME} entries: {rendered!r}"
        )
    for text in expected:
        assert text in result.stderr, (
            f"{label}: expected stderr to contain {text!r}\n--- actual stderr ---\n{result.stderr}"
        )
    return result


# --- T1: empty default omits everything (AC 1, D1) --------------------------
_, baseline = render()
baseline_env = envs(baseline)
assert allowlist_entries(baseline) == [], (
    f"T1: default render must emit no {NAME} entry; got {allowlist_entries(baseline)!r}"
)
baseline_names = [entry["name"] for entry in baseline_env]
assert NAME not in baseline_names, f"T1: {NAME} present in {baseline_names!r}"

# --- D2: an explicit null takes the same omit branch (nil-safety) -----------
_, nil_render = render("--set", "dispatcher.threadedBotAllowlist=null")
assert envs(nil_render) == baseline_env, (
    "D2: --set dispatcher.threadedBotAllowlist=null must render byte-identically to the "
    f"default.\nexpected: {baseline_env!r}\nrendered: {envs(nil_render)!r}"
)

# --- T2: a configured pair renders parseable JSON (AC 2, D3) ----------------
_, configured = render(
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
)
entries = allowlist_entries(configured)
assert len(entries) == 1, f"T2: expected exactly one {NAME} entry, got {entries!r}"
rendered_value = entries[0]["value"]
# Compare DECODED objects: the template's toJson emits Go's alphabetical key
# order, which the parser does not care about and a string compare would.
assert json.loads(rendered_value) == [{"channel_id": CHANNEL, "bot_id": BOT}], (
    f"T2: expected [{{'channel_id': {CHANNEL!r}, 'bot_id': {BOT!r}}}], "
    f"rendered {rendered_value!r}"
)
assert others(configured) == baseline_env, (
    "T2: configuring the allowlist must not disturb any other dispatcher env entry.\n"
    f"expected: {baseline_env!r}\nrendered: {others(configured)!r}"
)
# Hand the EXACT rendered string, never a reconstruction, to the bash half for
# the real-parser round trip (D4).
(work / "rendered-value.txt").write_text(rendered_value)
(work / "expected-pairs.json").write_text(json.dumps([{"channel_id": CHANNEL, "bot_id": BOT}]))

# --- T2b: two pairs, order preserved ----------------------------------------
_, two = render(
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    "--set-string", f"dispatcher.threadedBotAllowlist[1].channel_id={CHANNEL_2}",
    "--set-string", f"dispatcher.threadedBotAllowlist[1].bot_id={BOT_2}",
)
two_entries = allowlist_entries(two)
assert len(two_entries) == 1, f"T2b: expected exactly one {NAME} entry, got {two_entries!r}"
assert json.loads(two_entries[0]["value"]) == [
    {"channel_id": CHANNEL, "bot_id": BOT},
    {"channel_id": CHANNEL_2, "bot_id": BOT_2},
], f"T2b: both pairs must survive in values order; rendered {two_entries[0]['value']!r}"

# --- T2c: a G-prefixed (group/MPIM) channel_id is accepted (regex `[CG]`) ---
_, grouped = render(
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={GROUP}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
)
grouped_entries = allowlist_entries(grouped)
assert len(grouped_entries) == 1, f"T2c: expected exactly one {NAME} entry, got {grouped_entries!r}"
assert json.loads(grouped_entries[0]["value"]) == [{"channel_id": GROUP, "bot_id": BOT}], (
    f"T2c: a G-prefixed channel_id must render and decode; rendered {grouped_entries[0]['value']!r}"
)

# --- T3: extraEnv-only still works, verbatim (AC 4, D8) ---------------------
_, passthrough = render(
    "--set", f"dispatcher.extraEnv[0].name={NAME}",
    "--set-string", f"dispatcher.extraEnv[0].value={OPERATOR_SET}",
)
assert allowlist_entries(passthrough) == [{"name": NAME, "value": OPERATOR_VALUE}], (
    "T3: an existing dispatcher.extraEnv entry must pass through verbatim while the "
    f"typed value is empty; got {allowlist_entries(passthrough)!r}"
)
assert others(passthrough) == baseline_env, "T3: extraEnv must not disturb the rest of env"

# --- T3b: extraEnv passthrough is not retroactively validated ---------------
_, unvalidated = render(
    "--set", f"dispatcher.extraEnv[0].name={NAME}",
    "--set-string", "dispatcher.extraEnv[0].value=not-json",
)
assert allowlist_entries(unvalidated) == [{"name": NAME, "value": "not-json"}], (
    "T3b: the chart must not police a value it does not own; got "
    f"{allowlist_entries(unvalidated)!r}"
)

# --- N1..N9: every malformed shape fails the RENDER (AC 3, D6) --------------
WHERE = "dispatcher.threadedBotAllowlist[0]"
refuse(
    "N1 missing key",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    expected=[WHERE, "must have exactly the keys channel_id and bot_id"],
)
refuse(
    "N2 extra key",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    "--set-string", "dispatcher.threadedBotAllowlist[0].note=x",
    expected=[WHERE, "must have exactly the keys channel_id and bot_id", "note"],
)
refuse(
    "N3 channel_id regex miss",
    "--set-string", "dispatcher.threadedBotAllowlist[0].channel_id=lowercase",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    expected=[f"{WHERE}.channel_id", "does not match ^[CG][A-Z0-9]+$"],
)
refuse(
    "N4 bot_id regex miss",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={CHANNEL}",
    expected=[f"{WHERE}.bot_id", "does not match ^B[A-Z0-9]+$"],
)
refuse(
    "N5 non-mapping entry",
    "--set-string", "dispatcher.threadedBotAllowlist[0]=oops",
    expected=[WHERE, "must be a mapping"],
)
# N6: a non-string scalar. This is why the kindIs "string" checks exist and are
# not redundant with the regexes -- regexMatch on a non-string is not safe.
refuse(
    "N6 non-string scalar",
    "--set", "dispatcher.threadedBotAllowlist[0].channel_id=123",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    expected=[f"{WHERE}.channel_id", "must be a string"],
)
refuse(
    "N7 empty-string value (CLI form lands on the exactly-keys branch, not the "
    "regex branch -- helm drops a key --set-string'd to empty, per the comment "
    "in the template)",
    "--set-string", "dispatcher.threadedBotAllowlist[0].channel_id=",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    expected=[WHERE, "must have exactly the keys channel_id and bot_id", "got [bot_id]"],
)
# N7b: the SAME empty channel_id, but supplied through a values file, where
# helm does NOT drop the key -- this is the case that actually pins the regex
# branch against an empty string, rather than re-landing on N7's exactly-keys
# branch by accident.
empty_channel_values = work / "empty-channel.yaml"
empty_channel_values.write_text(
    yaml.safe_dump(
        dispatcher_values(threadedBotAllowlist=[{"channel_id": "", "bot_id": BOT}])
    )
)
refuse(
    "N7b empty-string channel_id via values file reaches the regex branch",
    values_file=empty_channel_values,
    expected=[f'{WHERE}.channel_id ""', "does not match ^[CG][A-Z0-9]+$"],
)
# N8: both paths naming the variable -> refusal, for an extraEnv value both
# EQUAL to and DIFFERENT from the rendered JSON. The #2297 lesson is that equal
# values also break strategic merge patches, so equality is not an escape hatch.
for label, operator_value in (("equal", OPERATOR_VALUE), ("different", "[]")):
    # Escape commas the same way as OPERATOR_SET above -- "different" ([]) has
    # none, so this is a no-op there, but "equal" carries the same comma bug.
    operator_set = helm_escape(operator_value)
    refuse(
        f"N8 both paths set ({label} value)",
        "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
        "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
        "--set", f"dispatcher.extraEnv[0].name={NAME}",
        "--set-string", f"dispatcher.extraEnv[0].value={operator_set}",
        expected=["dispatcher.extraEnv", NAME, "dispatcher.threadedBotAllowlist"],
    )
# N9: the malformed entry sits at index [1] behind a valid [0]. Guards against a
# validator that only ever inspects the first entry.
refuse(
    "N9 malformed second index",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    "--set-string", "dispatcher.threadedBotAllowlist[1].channel_id=lowercase",
    "--set-string", f"dispatcher.threadedBotAllowlist[1].bot_id={BOT_2}",
    expected=["dispatcher.threadedBotAllowlist[1].channel_id", "does not match ^[CG][A-Z0-9]+$"],
)
# N10/N11: a bare one-character id. Pins `+` (one-or-more) against a `*`
# (zero-or-more) mutant of either regex -- no other fixture uses a
# single-character id, so `*` would otherwise still pass every case here.
refuse(
    "N10 bare single-character channel_id",
    "--set-string", "dispatcher.threadedBotAllowlist[0].channel_id=C",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    expected=[f"{WHERE}.channel_id", "does not match ^[CG][A-Z0-9]+$"],
)
refuse(
    "N11 bare single-character bot_id",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", "dispatcher.threadedBotAllowlist[0].bot_id=B",
    expected=[f"{WHERE}.bot_id", "does not match ^B[A-Z0-9]+$"],
)
# N12: a D-prefixed id (Slack DM) must be refused. Pins the channel class
# against a broadened `^[A-Z][A-Z0-9]+$` mutant -- every existing fixture only
# ever exercises a lowercase failure, which a broadened class still catches,
# so this is the only case that pins the class down to exactly `[CG]`.
refuse(
    "N12 D-prefixed (DM) channel_id",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={DM}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    expected=[f"{WHERE}.channel_id", "does not match ^[CG][A-Z0-9]+$"],
)
# N13: the wrong-type top-level case for a MAP instead of a list. `range`
# yields the map KEY as the loop index, so this also pins the %v verb fix --
# a %d verb here mangles the message instead of naming the key cleanly.
wrong_type_values = work / "wrong-type.yaml"
wrong_type_values.write_text(
    yaml.safe_dump(dispatcher_values(threadedBotAllowlist={"oops": "value"}))
)
refuse(
    "N13 threadedBotAllowlist supplied as a map, not a list",
    values_file=wrong_type_values,
    expected=["dispatcher.threadedBotAllowlist[oops]", "must be a mapping"],
)

# --- D9: the upgrade path for an operator still on the old shape (AC 4) -----
_, upgraded = render("--is-upgrade", values_file=legacy_values)
assert allowlist_entries(upgraded) == [{"name": NAME, "value": OPERATOR_VALUE}], (
    "D9: a retained legacy values file with no threadedBotAllowlist key must render the "
    f"operator's single verbatim entry under --is-upgrade; got {allowlist_entries(upgraded)!r}"
)
# D7: the refusals are not fresh-install-only.
refuse(
    "D7 upgrade refuses a malformed entry",
    "--is-upgrade",
    "--set-string", "dispatcher.threadedBotAllowlist[0].channel_id=lowercase",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    values_file=legacy_values,
    expected=[f"{WHERE}.channel_id", "does not match ^[CG][A-Z0-9]+$"],
)
refuse(
    "D7 upgrade refuses both paths set",
    "--is-upgrade",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    values_file=legacy_values,
    expected=["dispatcher.extraEnv", NAME, "dispatcher.threadedBotAllowlist"],
)

# --- D12: removed-guard mutation control ------------------------------------
# Force the gate true while leaving the reservation empty. If the reservation is
# what makes one entry structurally guaranteed -- rather than luck -- the mutant
# renders TWO entries for the name.
mutant = work / "mutant"
shutil.copytree(chart, mutant)
template = mutant / "templates/dispatcher.yaml"
text = template.read_text()
gate = "{{- if .Values.dispatcher.threadedBotAllowlist }}"
assert text.count(gate) == 1, (
    "D12: expected exactly one occurrence of the gate\n"
    f"  {gate}\nin templates/dispatcher.yaml; found {text.count(gate)}"
)
template.write_text(text.replace(gate, "{{- if true }}"))
reservation = '{{- $threadedReserved = dict "CURIE_SLACK_THREADED_BOT_ALLOWLIST" "dispatcher.threadedBotAllowlist" -}}'
mutant_text = template.read_text()
assert mutant_text.count(reservation) == 1, (
    "D12: expected exactly one reservation assignment\n"
    f"  {reservation}\nin templates/dispatcher.yaml; found {mutant_text.count(reservation)}"
)
template.write_text(mutant_text.replace(reservation, ""))
_, mutant_output = render(
    "--set-string", f"dispatcher.threadedBotAllowlist[0].channel_id={CHANNEL}",
    "--set-string", f"dispatcher.threadedBotAllowlist[0].bot_id={BOT}",
    "--set", f"dispatcher.extraEnv[0].name={NAME}",
    "--set-string", "dispatcher.extraEnv[0].value=[]",
    source=mutant,
)
duplicates = allowlist_entries(mutant_output)
assert len(duplicates) == 2 and duplicates[1]["value"] == "[]", (
    "D12: with the reservation removed the mutant must emit TWO entries for "
    f"{NAME} (that is what the reservation prevents); got {duplicates!r}"
)

print(f"OK: {count} Helm renders; empty/null omit, configured pairs encode, extraEnv passes "
      "through verbatim, every malformed shape and both-paths-set refused on fresh and "
      "--is-upgrade renders, removed-reservation control reproduced two entries")
PY

# Round-trip the EXACT rendered string through the REAL dispatcher parser (D4).
# Re-implementing ThreadedBotAdmission's regexes here and checking the template
# against our own copy would prove nothing about drift; running the value through
# curie_dispatcher.config.DispatcherConfig means a change to the model or to the
# NoDecode/_decode_threaded_bot_allowlist env decoding breaks this chart gate.
# `uv run` must start from the repo root: apps/dispatcher is a workspace member,
# while this script's own paths are relative to $CHART.
(
  cd "$REPO_ROOT"
  uv run --python 3.13 python - "$WORK/rendered-value.txt" "$WORK/expected-pairs.json" <<'PY'
import json
import os
import sys

# Any ambient CURIE_* on a developer box or CI runner would otherwise decide
# this gate instead of the chart.
for key in [key for key in os.environ if key.startswith("CURIE_")]:
    del os.environ[key]

NAME = "CURIE_SLACK_THREADED_BOT_ALLOWLIST"
rendered = open(sys.argv[1]).read()
expected_pairs = json.load(open(sys.argv[2]))

# DispatcherConfig._require_independent_chat_attester rejects a blank attester
# secret and one equal to the API key, so without these two distinct non-empty
# placeholders every run here would fail for a reason unrelated to the allowlist.
os.environ["CURIE_API_KEY"] = "example-api-key"
os.environ["CURIE_APPROVAL_CHAT_ATTESTER_SECRET"] = "example-attester-secret"

from pydantic import ValidationError

from curie_dispatcher.config import DispatcherConfig, ThreadedBotAdmission

os.environ[NAME] = rendered
config = DispatcherConfig()
expected = tuple(ThreadedBotAdmission(**pair) for pair in expected_pairs)
assert config.slack_threaded_bot_allowlist == expected, (
    f"the rendered string {rendered!r} parsed to "
    f"{config.slack_threaded_bot_allowlist!r}, expected {expected!r}"
)

# The negative direction: the template's exact-keys rule and the parser's
# extra="forbid" must be the SAME rule, so a payload the template would never
# emit has to be rejected here. If someone relaxes extra="forbid", this fails.
unknown_key = json.dumps([dict(expected_pairs[0], note="x")])
os.environ[NAME] = unknown_key
try:
    DispatcherConfig()
except ValidationError:
    pass
else:
    raise AssertionError(
        f"the real parser ACCEPTED {unknown_key!r}; ThreadedBotAdmission must stay "
        'extra="forbid" or the template\'s exact-keys validation is no longer the same rule'
    )

print(f"OK: rendered {rendered!r} parsed by the real DispatcherConfig into {expected!r}; "
      "unknown-key payload rejected by that same parser")
PY
)
