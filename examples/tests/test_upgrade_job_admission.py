"""The #2163 escalation stays disclosed until ADR-0141 is Accepted and applied.

The failure this exists to stop is a sketch that claims to close the hole
while (a) only allowlisting the privileged ServiceAccount, which preserves
the escalation, (b) getting applied by the example installer from a Draft
ADR, or (c) letting the permission map say the path is already mitigated.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "sre-bot"
ADMISSION = BUNDLE / "manifests" / "upgrade-job-admission.yaml"
UPGRADE_ROLE = BUNDLE / "manifests" / "upgrade-role.yaml"
PERMISSION_MAP = BUNDLE / "docs" / "PERMISSION-MAP.md"
ADR = REPO / "docs" / "adr" / "0141-admission-pins-jobs-a-connector-token-may-create.md"
EXAMPLES_RS = REPO / "cli" / "src" / "examples.rs"
PLATFORM_CRONJOB = BUNDLE / "platform-upgrade" / "cronjob.yaml"
SELF_CRONJOB = BUNDLE / "self-upgrade" / "cronjob.yaml"


def _docs(path: Path) -> list[object]:
    loaded = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    return [doc for doc in loaded if doc is not None]


def _kinds(path: Path, kind: str) -> list[dict]:
    return [doc for doc in _docs(path) if isinstance(doc, dict) and doc.get("kind") == kind]


def _cronjob(path: Path) -> dict:
    docs = _kinds(path, "CronJob")
    assert len(docs) == 1, f"{path} must declare exactly one CronJob"
    return docs[0]


def _cel(policy: dict) -> str:
    spec = policy.get("spec") or {}
    chunks: list[str] = []
    for condition in spec.get("matchConditions") or []:
        chunks.append(str(condition.get("expression") or ""))
    for variable in spec.get("variables") or []:
        chunks.append(str(variable.get("expression") or ""))
    for validation in spec.get("validations") or []:
        chunks.append(str(validation.get("expression") or ""))
        chunks.append(str(validation.get("message") or ""))
    return "\n".join(chunks)


def test_draft_adr_0141_names_the_attack_the_control_and_the_exclusions() -> None:
    text = ADR.read_text(encoding="utf-8")
    assert "\nStatus: Draft\n" in text, (
        "ADR-0141 must stay Draft; acceptance is what would authorize applying "
        "the sketch"
    )
    assert "does not authorize implementation" in text
    for required in (
        "sre-bot-upgrader",
        "curie-platform-upgrader",
        "spec.template.spec.serviceAccountName",
        "ValidatingAdmissionPolicy",
        "verbatim instantiation",
        "ServiceAccount-choice-only is not this control",
        "What this deliberately does not cover",
        "upgrade-job-admission.yaml",
        "#2163",
        "#2175",
        "#2122",
    ):
        assert required in text, f"ADR-0141 must name {required!r}"


def test_admission_sketch_pins_live_cronjob_params_not_an_sa_allowlist() -> None:
    policies = _kinds(ADMISSION, "ValidatingAdmissionPolicy")
    names = {doc["metadata"]["name"] for doc in policies}
    assert names == {
        "curie-upgrade-job-template-pin",
        "curie-upgrade-job-label-required",
    }
    pin = next(
        doc
        for doc in policies
        if doc["metadata"]["name"] == "curie-upgrade-job-template-pin"
    )
    param = (pin.get("spec") or {}).get("paramKind") or {}
    assert param.get("kind") == "CronJob", (
        "the pin must fetch the live CronJob as params; an inlined SA "
        "allowlist is the false mitigation ADR-0141 rejects"
    )
    cel = _cel(pin)
    assert "params.spec.jobTemplate" in cel
    assert "variables.podSA == variables.templateSA" in cel
    assert "t.image == c.image" in cel
    assert "t.command == c.command" in cel
    assert "c.env == t.env" in cel
    assert "!has(v.hostPath)" in cel
    assert "system:serviceaccount:curie:sre-bot-upgrader" in cel
    # An allowlist that names the privileged SA would preserve the escalation.
    assert "curie-platform-upgrader" not in cel


def test_admission_sketch_denies_jobs_that_are_not_labeled_as_a_named_cronjob() -> None:
    deny = next(
        doc
        for doc in _kinds(ADMISSION, "ValidatingAdmissionPolicy")
        if doc["metadata"]["name"] == "curie-upgrade-job-label-required"
    )
    cel = _cel(deny)
    platform = _cronjob(PLATFORM_CRONJOB)["metadata"]["name"]
    self_upgrade = _cronjob(SELF_CRONJOB)["metadata"]["name"]
    assert platform in cel
    assert self_upgrade in cel
    assert "expression: false" in yaml.dump(deny, sort_keys=False) or any(
        (v.get("expression") or "").strip().strip('"') == "false"
        for v in (deny.get("spec") or {}).get("validations") or []
    )


def test_admission_bindings_point_at_the_shipped_cronjobs_and_deny() -> None:
    bindings = _kinds(ADMISSION, "ValidatingAdmissionPolicyBinding")
    by_name = {doc["metadata"]["name"]: doc for doc in bindings}
    assert "curie-upgrade-job-template-pin-platform" in by_name
    assert "curie-upgrade-job-template-pin-self" in by_name
    assert "curie-upgrade-job-label-required" in by_name
    platform = by_name["curie-upgrade-job-template-pin-platform"]
    self_upgrade = by_name["curie-upgrade-job-template-pin-self"]
    assert platform["spec"]["paramRef"]["name"] == _cronjob(PLATFORM_CRONJOB)["metadata"]["name"]
    assert self_upgrade["spec"]["paramRef"]["name"] == _cronjob(SELF_CRONJOB)["metadata"]["name"]
    for binding in (platform, self_upgrade):
        assert binding["spec"]["validationActions"] == ["Deny"]
        assert binding["spec"]["paramRef"]["parameterNotFoundAction"] == "Deny"
        assert binding["spec"]["policyName"] == "curie-upgrade-job-template-pin"


def test_cronjob_templates_still_carry_the_fields_the_attack_abuses() -> None:
    """Positive: the legitimate platform Job does select the privileged SA.

    Negative: the admission sketch does not hard-code that SA as allowed, so
    selecting it with a different command is not a policy-shaped success.
    """

    platform = _cronjob(PLATFORM_CRONJOB)
    pod = platform["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "curie-platform-upgrader"
    assert pod["containers"][0]["command"] == ["sh", "/opt/platform-upgrade/upgrade.sh"]
    self_upgrade = _cronjob(SELF_CRONJOB)
    self_pod = self_upgrade["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert "serviceAccountName" not in self_pod
    assert self_pod["containers"][0]["command"] == [
        "python3",
        "/opt/self-upgrade/redeploy.py",
    ]
    pin = next(
        doc
        for doc in _kinds(ADMISSION, "ValidatingAdmissionPolicy")
        if doc["metadata"]["name"] == "curie-upgrade-job-template-pin"
    )
    assert "curie-platform-upgrader" not in _cel(pin)


def test_installer_does_not_apply_the_draft_admission_sketch() -> None:
    source = EXAMPLES_RS.read_text(encoding="utf-8")
    assert "upgrade-job-admission.yaml" not in source, (
        "wiring the sketch into cli/src/examples.rs would apply a Draft "
        "control the next time someone runs --platform-upgrade"
    )
    include = (
        'include_bytes!("../../examples/sre-bot/manifests/'
        'upgrade-job-admission.yaml")'
    )
    assert include not in source


def test_permission_map_still_discloses_the_hole_and_names_the_draft() -> None:
    text = PERMISSION_MAP.read_text(encoding="utf-8")
    assert "leaked `sre-bot-upgrader` token" in text
    assert "curie-platform-upgrader" in text
    assert "ADR-0141" in text
    assert "Draft" in text
    assert "does not apply" in text or "not applied" in text.lower() or "not authorize" in text
    for closed in (
        "the escalation is closed",
        "this hole is closed",
        "admission now prevents",
        "RBAC now restricts spec.serviceAccountName",
    ):
        assert closed not in text, (
            f"PERMISSION-MAP.md must not claim {closed!r} while the control "
            "is a Draft sketch the installer does not apply"
        )


def test_upgrade_role_points_at_the_draft_without_claiming_enforcement() -> None:
    text = UPGRADE_ROLE.read_text(encoding="utf-8")
    assert "curie-platform-upgrader" in text
    assert "ADR-0141" in text
    assert "upgrade-job-admission.yaml" in text
    assert "Do not kubectl apply" in text or "does not authorize" in text
