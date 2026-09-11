"""Declared-connector validation (ADR-0086, #1063).

Every case here is one an author would otherwise discover as an opaque
Kubernetes apply failure -- or worse, as a connector that comes up and quietly
does the wrong thing. Catching them at validation is the point of the file
existing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from plugin_format import validate_bundle
from plugin_format.connectors import validate_connectors


def _bundle(root: Path, connectors_yaml: str | None) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "b", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (root / "skills" / "b").mkdir(parents=True, exist_ok=True)
    (root / "skills" / "b" / "SKILL.md").write_text(
        "---\nname: b\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    if connectors_yaml is not None:
        (root / "connectors.yaml").write_text(connectors_yaml, encoding="utf-8")
    return root


def _codes(data: object) -> list[str]:
    _, errors = validate_connectors(data)
    return [c for c, _ in errors]


# The cross-language corpora. Both live at the repository root because the Rust
# CLI reads the same bytes; see each file's own `comment` for why the seam needs
# freezing rather than a shared import.
_VECTORS = Path(__file__).parents[3] / "tests" / "vectors"


def _vector_file(name: str) -> dict:
    return json.loads((_VECTORS / name).read_text(encoding="utf-8"))


_BUILD_DECL = _vector_file("connector-build-decl.json")

# Every key a vector may carry. Asserted, so a key added for the Rust lane alone
# cannot pass vacuously here -- the failure mode the model-credential-forwarding
# and approval-action-id vectors both guard the same way.
_BUILD_DECL_KEYS = {
    "name",
    "why",
    "document",
    "expect",
    "codes",
    "resolved_dockerfile",
    "fixture",
}


# --------------------------------------------------------------------------- #
# Accepted shapes
# --------------------------------------------------------------------------- #
def test_hosted_connector_is_accepted() -> None:
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "grafana": {
                    "image": "grafana/mcp-grafana:0.17.2",
                    "args": ["-t", "streamable-http"],
                    "env": {"GRAFANA_URL": "https://g.example.com"},
                    "secrets": ["GRAFANA_TOKEN"],
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None
    assert parsed.connectors["grafana"].is_hosted


def test_remote_connector_is_accepted() -> None:
    parsed, errors = validate_connectors(
        {"connectors": {"internal": {"url": "https://mcp.internal/mcp", "secrets": ["T"]}}}
    )
    assert errors == []
    assert parsed is not None
    assert not parsed.connectors["internal"].is_hosted


def test_absent_file_is_fine(tmp_path: Path) -> None:
    # A bundle with no hosted connectors simply omits the file.
    assert validate_bundle(str(_bundle(tmp_path, None))).valid


# --------------------------------------------------------------------------- #
# Rejected shapes -- each would otherwise fail late and obscurely
# --------------------------------------------------------------------------- #
def test_both_image_and_url_is_ambiguous() -> None:
    # Who owns the process? Guessing either way silently ignores half the spec.
    assert "connectors.ambiguous" in _codes(
        {"connectors": {"g": {"image": "x:1", "url": "https://y/mcp"}}}
    )


def test_neither_image_nor_url_is_underspecified() -> None:
    assert "connectors.underspecified" in _codes({"connectors": {"g": {"secrets": ["T"]}}})


def test_runtime_config_on_a_remote_connector_is_rejected() -> None:
    # args/env configure a process Curie starts. On a url connector they would
    # be accepted and then silently do nothing -- the worst kind of no-op.
    assert "connectors.remote_has_runtime" in _codes(
        {"connectors": {"g": {"url": "https://y/mcp", "env": {"A": "b"}}}}
    )


def test_headers_on_a_hosted_connector_are_rejected() -> None:
    assert "connectors.hosted_has_headers" in _codes(
        {"connectors": {"g": {"image": "x:1", "headers": {"Authorization": "Bearer x"}}}}
    )


@pytest.mark.parametrize(
    "name",
    [
        "Grafana",  # uppercase -- not a DNS label
        "grafana_mcp",  # underscore -- not a DNS label
        "-grafana",  # leading dash
        "grafana-",  # trailing dash
        "g" * 41,  # over the cap
        "",  # empty
    ],
)
def test_name_must_be_a_dns_label(name: str) -> None:
    # The name becomes a Kubernetes object name and a Service DNS label. A bad
    # one fails at apply time with a message about the object, not the bundle.
    assert "connectors.bad_name" in _codes({"connectors": {name: {"image": "x:1"}}})


def test_unknown_key_is_rejected_not_ignored() -> None:
    # This package is lenient elsewhere because real Claude Code bundles carry
    # keys it does not model. connectors.yaml is Curie's own file with no
    # external producer, so an unrecognised key is a typo -- and `secretz`
    # silently ignored means a connector that starts without its credential.
    assert _codes({"connectors": {"g": {"image": "x:1", "secretz": ["T"]}}}) != []


def test_port_out_of_range_is_rejected() -> None:
    assert "connectors.bad_port" in _codes({"connectors": {"g": {"image": "x:1", "port": 99999}}})


def test_non_mapping_file_is_rejected() -> None:
    assert "connectors.not_object" in _codes(["not", "a", "mapping"])


# --------------------------------------------------------------------------- #
# Reaching it through validate_bundle
# --------------------------------------------------------------------------- #
def test_bundle_surfaces_a_connector_error(tmp_path: Path) -> None:
    root = _bundle(tmp_path, "connectors:\n  Bad_Name:\n    image: x:1\n")
    result = validate_bundle(str(root))
    assert not result.valid
    assert any(e.code == "connectors.bad_name" for e in result.errors)


def test_bundle_surfaces_unparseable_yaml(tmp_path: Path) -> None:
    root = _bundle(tmp_path, "connectors:\n  g:\n   image: [unclosed\n")
    result = validate_bundle(str(root))
    assert not result.valid
    assert any(e.code == "connectors.unreadable" for e in result.errors)


def test_bundle_rejects_a_duplicate_connector_name(tmp_path: Path) -> None:
    root = _bundle(
        tmp_path,
        "connectors:\n"
        "  grafana:\n"
        "    url: https://first.example.com/mcp\n"
        "  grafana:\n"
        "    url: https://second.example.com/mcp\n",
    )
    result = validate_bundle(str(root))
    assert not result.valid
    issue = next(e for e in result.errors if e.code == "connectors.duplicate_connector")
    assert "grafana" in issue.message


def test_bundle_with_a_valid_connector_passes(tmp_path: Path) -> None:
    root = _bundle(
        tmp_path,
        "connectors:\n  grafana:\n    image: grafana/mcp-grafana:0.17.2\n"
        "    secrets: [GRAFANA_TOKEN]\n",
    )
    assert validate_bundle(str(root)).valid


# --------------------------------------------------------------------------- #
# One name, one owner (#1118)
# --------------------------------------------------------------------------- #
def test_a_name_in_both_connectors_yaml_and_mcp_json_is_rejected(tmp_path: Path) -> None:
    # Curie injects the connector's entry alongside whatever the bundle declares.
    # With both naming `grafana`, which one the agent talks to is decided
    # downstream and the loser is overridden with no diagnostic -- either the
    # author's committed entry is ignored, or it silently wins over the objects
    # Curie actually created. Caught here, the fix is a one-line edit.
    root = _bundle(tmp_path, "connectors:\n  grafana:\n    image: x:1\n")
    (root / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"grafana": {"type": "http", "url": "http://hand-written/mcp"}}}),
        encoding="utf-8",
    )
    result = validate_bundle(str(root))
    assert not result.valid
    assert any(e.code == "connectors.duplicate_server" for e in result.errors)


def test_distinct_names_across_the_two_files_are_fine(tmp_path: Path) -> None:
    # The files are complementary by design: connectors.yaml for what Curie
    # hosts, .mcp.json for anything else (a stdio server, say).
    root = _bundle(tmp_path, "connectors:\n  grafana:\n    image: x:1\n")
    (root / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"local-tool": {"command": "./bin/tool"}}}),
        encoding="utf-8",
    )
    assert validate_bundle(str(root)).valid


def test_an_unreadable_mcp_config_does_not_add_a_confusing_second_error(tmp_path: Path) -> None:
    # `_validate_mcp` already errors on the unreadable declaration. Cross-checking
    # a partial set would name a collision we cannot actually confirm.
    root = _bundle(tmp_path, "connectors:\n  grafana:\n    image: x:1\n")
    (root / ".mcp.json").write_text("{not json", encoding="utf-8")
    result = validate_bundle(str(root))
    assert not result.valid
    assert not any(e.code == "connectors.duplicate_server" for e in result.errors)


def test_a_typo_in_a_placeholder_is_rejected() -> None:
    # Unsubstituted text reaches the container verbatim, so the connector starts
    # and rejects every call. Nothing catches that at runtime.
    codes = _codes({"connectors": {"g": {"image": "x:1", "args": ["-h", "${CURIE_ALOWED_HOSTS}"]}}})
    assert "connectors.unknown_placeholder" in codes


def test_known_placeholders_are_accepted_in_args_and_env() -> None:
    _, errors = validate_connectors(
        {
            "connectors": {
                "g": {
                    "image": "x:1",
                    "args": ["-h", "${CURIE_ALLOWED_HOSTS}", "-p", "${CURIE_CONNECTOR_PORT}"],
                    "env": {"SELF": "${CURIE_CONNECTOR_URL}", "H": "${CURIE_CONNECTOR_HOST}"},
                }
            }
        }
    )
    assert errors == []


def test_the_validator_and_the_renderer_share_one_placeholder_list() -> None:
    # Two lists would drift: the renderer would substitute something the
    # validator rejects, or accept something that reaches the container raw.
    from plugin_format.connector_render import PLACEHOLDERS

    for name in PLACEHOLDERS:
        _, errors = validate_connectors(
            {"connectors": {"g": {"image": "x:1", "args": ["${" + name + "}"]}}}
        )
        assert errors == [], f"renderer substitutes ${{{name}}} but the validator rejects it"


# --------------------------------------------------------------------------- #
# The hosted form's escape hatch for tiers that cannot host -- #1160
# --------------------------------------------------------------------------- #
def test_a_hosted_connector_may_declare_where_to_reach_it_when_unhosted() -> None:
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "grafana": {
                    "image": "grafana/mcp-grafana:0.17.2",
                    "unhosted_url": "${GRAFANA_MCP_URL}",
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None
    assert parsed.connectors["grafana"].is_hosted, "still hosted where Curie can host"


def test_unhosted_url_on_a_remote_connector_is_rejected() -> None:
    # A `url` connector is already reachable on every tier, so the fallback
    # could never apply -- accepting it would be a silent no-op.
    assert "connectors.remote_has_unhosted_url" in _codes(
        {"connectors": {"g": {"url": "https://y/mcp", "unhosted_url": "http://z/mcp"}}}
    )


# --------------------------------------------------------------------------- #
# Referencing a Secret Curie did not create -- #1163
# --------------------------------------------------------------------------- #
def test_a_connector_may_reference_a_secret_provisioned_out_of_band() -> None:
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "grafana": {
                    "image": "x:1",
                    "secrets": [{"name": "TOKEN", "from_secret": "grafana-mcp"}],
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None
    spec = parsed.connectors["grafana"]
    assert spec.secret_names() == ["TOKEN"]
    # The property the whole change exists for: nothing in the deploy path
    # needs to hold this credential, which is what lets a reconciler apply a
    # connector without holding every agent's secrets (ADR-0090).
    assert spec.resolved_secrets() == []


def test_the_literal_form_still_needs_resolving() -> None:
    parsed, _ = validate_connectors({"connectors": {"g": {"image": "x:1", "secrets": ["TOKEN"]}}})
    assert parsed is not None
    assert parsed.connectors["g"].resolved_secrets() == ["TOKEN"]


def test_both_forms_can_be_mixed_on_one_connector() -> None:
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "g": {
                    "image": "x:1",
                    "secrets": ["OWNED", {"name": "REFD", "from_secret": "s"}],
                    "bearer_secret": "OWNED",
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None
    assert parsed.connectors["g"].secret_names() == ["OWNED", "REFD"]
    assert parsed.connectors["g"].resolved_secrets() == ["OWNED"]


# --------------------------------------------------------------------------- #
# A connector-declared secret NAME gets the manifest `secrets` policy -- #457
# --------------------------------------------------------------------------- #
def test_a_reserved_name_cannot_be_declared_as_a_connector_secret() -> None:
    # The hole: the fence lived only on plugin.json `secrets`, so moving the
    # name here bought a connector the operator's own model credential --
    # resolved from their environment or vault and handed to the bundle's
    # container before any validator runs at the skill tier.
    assert "connectors.secret_name_reserved" in _codes(
        {"connectors": {"g": {"image": "x:1", "secrets": ["ANTHROPIC_API_KEY"]}}}
    )


def test_a_reserved_name_cannot_be_declared_as_a_secret_file_key() -> None:
    # Same name, same resolution, same per-agent Secret -- only the delivery
    # differs, so the fence has to cover this key too.
    assert "connectors.secret_name_reserved" in _codes(
        {
            "connectors": {
                "g": {"image": "x:1", "secret_files": {"HTTPS_PROXY": "/secrets/proxy"}}
            }
        }
    )


def test_a_reserved_name_is_refused_through_validate_bundle(tmp_path: Path) -> None:
    # The consumer path that matters: the API validates an uploaded bundle
    # through validate_bundle, and the runner validates the packed snapshot
    # through the same parser -- so a hostile connectors.yaml is refused at
    # upload rather than discovered at apply.
    root = _bundle(
        tmp_path,
        "connectors:\n  g:\n    image: x:1\n    secrets: [CLAUDE_CODE_OAUTH_TOKEN]\n",
    )
    result = validate_bundle(str(root))
    assert not result.valid
    assert any(e.code == "connectors.secret_name_reserved" for e in result.errors)


def test_the_curie_prefix_is_fenced_for_a_connector_secret() -> None:
    # The forward-safe half of the rule: a boot key nobody remembered to
    # enumerate is still reserved because it carries the prefix.
    assert "connectors.secret_name_reserved" in _codes(
        {"connectors": {"g": {"image": "x:1", "secrets": ["CURIE_RUNNER_TOKEN"]}}}
    )


def test_a_malformed_secret_name_is_refused() -> None:
    # A lowercase name cannot be delivered as an env var and consumed by
    # `${VAR}` expansion, so it would bind to nothing at runtime.
    codes = _codes({"connectors": {"g": {"image": "x:1", "secrets": ["grafana-token"]}}})
    assert "connectors.secret_name_invalid" in codes
    assert "connectors.secret_name_reserved" not in codes, "shape is reported once, not twice"


def test_a_hosted_connector_with_several_secrets_must_name_the_bearer() -> None:
    # #2559: picking secrets[0] as the Authorization header is positional.
    # Two names and no bearer_secret is refused rather than silently binding
    # both into the sandbox (and using the first as the Bearer).
    assert "connectors.bearer_secret_required" in _codes(
        {"connectors": {"g": {"image": "x:1", "secrets": ["POD_ONLY", "PAT"]}}}
    )


def test_a_named_bearer_secret_must_be_one_of_the_declared_secrets() -> None:
    assert "connectors.bearer_secret_unknown" in _codes(
        {
            "connectors": {
                "g": {
                    "image": "x:1",
                    "secrets": ["POD_ONLY", "PAT"],
                    "bearer_secret": "NOT_DECLARED",
                }
            }
        }
    )


def test_a_named_bearer_secret_is_accepted_on_a_multi_secret_hosted_connector() -> None:
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "g": {
                    "image": "x:1",
                    "secrets": ["POD_ONLY", "PAT"],
                    "bearer_secret": "PAT",
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None
    assert parsed.connectors["g"].bearer_secret == "PAT"


def test_a_single_secret_hosted_connector_does_not_require_bearer_secret() -> None:
    # Compat for the github-mcp-server shape: one name, that name is the Bearer.
    parsed, errors = validate_connectors(
        {"connectors": {"g": {"image": "x:1", "secrets": ["PAT"]}}}
    )
    assert errors == []
    assert parsed is not None
    assert parsed.connectors["g"].bearer_secret is None


def test_a_remote_connector_does_not_require_bearer_secret_for_several_secrets() -> None:
    # Derivation is hosted-only. A url: connector's headers are author input
    # (ADR-0009 ${VAR} in .mcp.json / headers), so several secrets without
    # bearer_secret stay legal.
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "r": {
                    "url": "https://mcp.example.com/mcp",
                    "secrets": ["A", "B"],
                    "headers": {"Authorization": "Bearer ${A}"},
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None


def test_an_ordinary_connector_secret_name_still_validates() -> None:
    # The over-refusal control: the names the shipped examples declare must
    # keep validating clean, in both delivery forms.
    _, errors = validate_connectors(
        {
            "connectors": {
                "grafana": {"image": "x:1", "secrets": ["GRAFANA_SERVICE_ACCOUNT_TOKEN"]},
                "kubernetes": {
                    "image": "y:1",
                    "secret_files": {"K8S_READONLY_KUBECONFIG": "/secrets/kubeconfig"},
                },
            }
        }
    )
    assert errors == []


def test_the_secret_ref_form_is_held_to_the_same_name_policy() -> None:
    # `from_secret` moves who holds the VALUE, not which env var the connector
    # container ends up reading -- a secretKeyRef named ANTHROPIC_BASE_URL
    # redirects the session exactly the same way.
    assert "connectors.secret_name_reserved" in _codes(
        {
            "connectors": {
                "g": {
                    "image": "x:1",
                    "secrets": [{"name": "ANTHROPIC_BASE_URL", "from_secret": "s"}],
                }
            }
        }
    )


def test_an_empty_from_secret_is_rejected() -> None:
    # It renders a secretKeyRef at a Secret named '', which the API server
    # rejects at APPLY -- long after the deploy looked like it worked.
    assert "connectors.empty_secret_ref" in _codes(
        {"connectors": {"g": {"image": "x:1", "secrets": [{"name": "T", "from_secret": ""}]}}}
    )


def test_the_same_env_name_declared_twice_is_rejected() -> None:
    # Two env entries with one name means the container silently gets whichever
    # the renderer emitted last -- possibly pointed at the wrong Secret.
    assert "connectors.duplicate_secret" in _codes(
        {"connectors": {"g": {"image": "x:1", "secrets": ["T", {"name": "T", "from_secret": "s"}]}}}
    )


def test_key_defaults_to_the_env_var_name() -> None:
    parsed, _ = validate_connectors(
        {"connectors": {"g": {"image": "x:1", "secrets": [{"name": "T", "from_secret": "s"}]}}}
    )
    assert parsed is not None
    assert parsed.connectors["g"].secrets[0].secret_key() == "T"  # type: ignore[union-attr]


# --------------------------------------------------------------------------- #
# Names Curie's own platform MCP servers occupy -- #1200
# --------------------------------------------------------------------------- #
RESERVED_NAMES = ["curie", "curie-state"]


@pytest.mark.parametrize("name", RESERVED_NAMES)
def test_a_reserved_platform_server_name_is_rejected(name: str) -> None:
    # `curie` is the approval server and `curie-state` the durable state server.
    # Both ride the same mcp_servers map a declared connector rides, so a
    # connector claiming the name replaces the platform server in the agent's
    # session -- the agent quietly loses request_approval or the state tools,
    # with nothing logged and nothing failing until a skill calls one.
    assert "connectors.reserved_name" in _codes({"connectors": {name: {"image": "x:1"}}})


@pytest.mark.parametrize("name", RESERVED_NAMES)
def test_bundle_rejects_a_reserved_connector_name(tmp_path: Path, name: str) -> None:
    # The shape the issue reports as validating clean today: connectors.yaml
    # declares the name, no .mcp.json exists, so the #1118 cross-check has
    # nothing to compare against and the bundle sails through deploy.
    root = _bundle(tmp_path, f"connectors:\n  {name}:\n    image: x:1\n")
    result = validate_bundle(str(root))
    assert not result.valid
    offending = [e for e in result.errors if e.code == "connectors.reserved_name"]
    assert offending, [e.code for e in result.errors]
    assert name in offending[0].message


def test_a_name_merely_starting_with_curie_is_accepted() -> None:
    # The fence is the two exact names, not the `curie-` prefix. Fencing the
    # prefix would reject a legitimate `curie-docs` connector, which is a worse
    # accidental collision than the one being prevented.
    assert _codes({"connectors": {"curie-docs": {"image": "x:1"}}}) == []


def test_a_reserved_name_and_a_bad_shape_report_both() -> None:
    # Every other check in this loop accumulates, so the author sees the whole
    # file's problems in one pass rather than one rename per deploy attempt.
    codes = _codes({"connectors": {"curie": {"secrets": ["T"]}}})
    assert "connectors.reserved_name" in codes
    assert "connectors.underspecified" in codes


# --------------------------------------------------------------------------- #
# The `build:` form: a bundle declares source, not a hand-pasted image -- ADR 0113
# --------------------------------------------------------------------------- #
def test_a_build_only_connector_is_accepted_and_is_hosted() -> None:
    # Hosted-ness is about WHO RUNS THE PROCESS, not about whether the image ref
    # has been resolved yet. Keeping `is_hosted` keyed to `image` would make a
    # source-built connector look remote to `render`, `mcp_entry` and the
    # runner all at once: no objects rendered, and `unhosted_mcp_entry` handing
    # back a remote-form entry built from a `url` that is None.
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "k8s-write": {
                    "build": {
                        "context": "connectors/k8s-write",
                        "dockerfile": "Dockerfile",
                        "platforms": ["linux/amd64", "linux/arm64"],
                    },
                    "env": {"K8S_WRITE_ALLOWLIST": "acme-ns/acme-api"},
                    "secret_files": {"K8S_WRITE_KUBECONFIG": "/secrets/kubeconfig"},
                }
            }
        }
    )
    assert errors == []
    assert parsed is not None
    spec = parsed.connectors["k8s-write"]
    assert spec.is_hosted
    assert spec.image is None, "the digest lives in connectors.lock.yaml, never in the declaration"
    assert spec.build is not None
    assert spec.build.context == "connectors/k8s-write"
    assert spec.build.platforms == ["linux/amd64", "linux/arm64"]


def test_dockerfile_defaults_to_Dockerfile() -> None:
    # The common case omits it. A reader that leaves the resolved path empty
    # builds nothing, or builds whatever the daemon's own default happens to
    # be, which is a different file in a different place.
    parsed, errors = validate_connectors(
        {
            "connectors": {
                "tempo": {"build": {"context": "connectors/tempo", "platforms": ["linux/amd64"]}}
            }
        }
    )
    assert errors == []
    assert parsed is not None
    assert parsed.connectors["tempo"].build is not None
    assert parsed.connectors["tempo"].build.dockerfile == "Dockerfile"


def test_platforms_has_no_default() -> None:
    # Required rather than defaulted, because a silently single-arch build is
    # the exact failure ADR 0113 names: it passes every declaration check and
    # fails after apply as "no matching manifest for linux/arm64". A default
    # would pick one arch on the author's behalf and never say so.
    assert _codes({"connectors": {"tempo": {"build": {"context": "connectors/tempo"}}}}) != []


@pytest.mark.parametrize("second", ["image", "url"])
def test_build_beside_another_form_is_ambiguous(second: str) -> None:
    # Two image sources, or an image source plus a claim that the process is
    # already running elsewhere. Picking either silently ignores the other, and
    # the one ignored is the one the author edited last.
    value = "ghcr.io/acme-corp/acme-bot-k8s-write-mcp:v1" if second == "image" else "https://mcp.acme.example.com/mcp"
    codes = _codes(
        {
            "connectors": {
                "k8s-write": {
                    second: value,
                    "build": {"context": "connectors/k8s-write", "platforms": ["linux/amd64"]},
                }
            }
        }
    )
    assert "connectors.ambiguous" in codes


def test_the_underspecified_message_names_the_build_form() -> None:
    # An author who meant to declare source and mistyped the key is told the
    # form exists. A message that still names only `image` and `url` sends them
    # back to hand-building and pasting a digest, which is the workflow ADR 0113
    # exists to delete.
    _, errors = validate_connectors({"connectors": {"k8s-write": {"secrets": ["K8S_WRITE_TOKEN"]}}})
    message = next(m for code, m in errors if code == "connectors.underspecified")
    assert "`build`" in message


def test_headers_are_rejected_on_a_build_connector_exactly_as_on_an_image_one() -> None:
    # SIBLING PATH (AGENTS.md's parity-seam rule). The guard was keyed to
    # `image`, and a `build:` connector is equally hosted, so without widening
    # it to hosted-ness a built connector could declare `headers` and be
    # accepted -- a remote-only field riding the build form, silently ignored by
    # the renderer. Armed here through the build form ONLY, so reverting the
    # guard to `spec.image and spec.headers` fails this test while the
    # image-form test above still passes. That gap IS the seam.
    build_form = _codes(
        {
            "connectors": {
                "k8s-write": {
                    "build": {"context": "connectors/k8s-write", "platforms": ["linux/amd64"]},
                    "headers": {"Authorization": "Bearer ${ACME_TOKEN}"},
                }
            }
        }
    )
    image_form = _codes(
        {"connectors": {"k8s-write": {"image": "x:1", "headers": {"Authorization": "Bearer x"}}}}
    )
    assert "connectors.hosted_has_headers" in build_form
    assert "connectors.hosted_has_headers" in image_form


def test_an_unknown_key_inside_build_is_rejected_not_ignored() -> None:
    # `target:` is a plausible thing to reach for and Curie models none of it.
    # Silently dropping it means a build that produces the wrong stage while
    # reporting success.
    assert (
        _codes(
            {
                "connectors": {
                    "tempo": {
                        "build": {
                            "context": "connectors/tempo",
                            "platforms": ["linux/amd64"],
                            "target": "runtime",
                        }
                    }
                }
            }
        )
        != []
    )


# --------------------------------------------------------------------------- #
# The Python half of the frozen Python/Rust declaration seam
# --------------------------------------------------------------------------- #
def test_every_build_declaration_vector_declares_only_modelled_keys() -> None:
    # A key added for the Rust lane alone would otherwise sit here unread, so
    # the corpus would grow a field this suite silently ignores.
    for vector in _BUILD_DECL["vectors"]:
        extra = set(vector) - _BUILD_DECL_KEYS
        assert not extra, f"{vector['name']}: unmodelled vector keys {sorted(extra)}"
        assert vector["expect"] in {"accept", "reject"}


@pytest.mark.parametrize(
    "vector",
    [v for v in _BUILD_DECL["vectors"] if "fixture" not in v],
    ids=lambda v: v["name"],
)
def test_build_declaration_vectors(vector: dict) -> None:
    # Driven off tests/vectors/connector-build-decl.json rather than inline
    # literals so the Rust reader in cli/src/connector_build.rs cannot diverge:
    # changing an expectation here fails both suites, and changing one language
    # without the vector fails that language. A vector carrying `fixture` is a
    # filesystem case only the CLI's path resolver can see (a symlinked
    # Dockerfile), so it is skipped here and materialized by the Rust suite.
    parsed, errors = validate_connectors(vector["document"])
    codes = [code for code, _ in errors]
    if vector["expect"] == "accept":
        assert errors == [], f"{vector['name']} must validate clean, got {codes}"
        assert parsed is not None
        for connector, dockerfile in vector.get("resolved_dockerfile", {}).items():
            build = parsed.connectors[connector].build
            assert build is not None
            assert build.dockerfile == dockerfile
    else:
        assert parsed is None
        # Subset, never equality: validate_connectors accumulates every problem
        # in one pass so an author sees the whole file at once.
        for expected in vector["codes"]:
            assert expected in codes, f"{vector['name']} expected {expected}, got {codes}"


def test_connector_declaration_field_names_match_the_frozen_vector() -> None:
    # The gap review finding r2-1 named: plugin-format.schema.json carries no
    # Connector* $defs, so `curie dev field-parity` compares nothing for the
    # Rust mirrors and a new Python field would land with every gate green.
    # Adding one without editing the vector fails here; editing the vector to
    # make this pass then fails the Rust half until the mirror gains the field.
    from plugin_format.connectors import ConnectorBuild, ConnectorSpec

    fields = _vector_file("connector-fields.json")["models"]
    assert set(ConnectorSpec.model_fields) == set(fields["ConnectorSpec"])
    assert set(ConnectorBuild.model_fields) == set(fields["ConnectorBuild"])


# --------------------------------------------------------------------------- #
# Names that forge the renderer's `-mcp-` join -- #1446
#
# The renderer joins the agent and the connector with the literal `-mcp-`
# (`{release}-{agent}-mcp-{connector}`). That literal is a bare substring inside
# one DNS label, not a structural separator, so a connector whose name starts
# with `mcp-` or contains `-mcp-` makes the rendered name ambiguous about where
# the agent ends and the connector begins: a DIFFERENT agent/connector pair then
# renders the same Service, the same Deployment, both the same NetworkPolicies
# and the same `app.kubernetes.io/name` pod selector. The connector is
# deliberately unauthenticated (ADR-0086), so the object name is the only thing
# binding a sandbox to a credential -- a collision quietly hands one agent
# another agent's production token.
#
# The renderer refuses too, as a backstop. This validator is the primary gate,
# because it is the one that reaches the author with a name and a fix instead of
# a 500 at deploy time.
# --------------------------------------------------------------------------- #
FORGING_CONNECTOR_NAMES = ["mcp-c", "b-mcp-c", "mcp-grafana"]
SAFE_CONNECTOR_NAMES = ["grafana-mcp", "c-mcp", "mcp", "grafana", "kubernetes", "netpol-probe"]


@pytest.mark.parametrize("name", FORGING_CONNECTOR_NAMES)
def test_a_connector_name_that_forges_the_render_join_is_rejected(name: str) -> None:
    # The spec here is otherwise perfectly valid -- a plain hosted image, a
    # well-formed RFC 1123 name, nothing reserved. So this can only pass if the
    # new check fires on its own; it cannot be masked by `connectors.bad_name`
    # or `connectors.underspecified` standing in for it.
    assert _codes({"connectors": {name: {"image": "x:1"}}}) == ["connectors.ambiguous_name"]


@pytest.mark.parametrize("name", SAFE_CONNECTOR_NAMES)
def test_a_connector_name_that_merely_ends_in_mcp_is_accepted(name: str) -> None:
    # The asymmetry that looks like a bug: a TRAILING `-mcp` is fine on the
    # connector, because nothing follows the connector to complete a second
    # join, while it is fatal on the agent, which the join follows directly.
    # Refusing these would be an over-rejection with a real cost -- `kubernetes`
    # and `netpol-probe` are the connector names shipped in examples/, and every
    # install declaring one would stop deploying for no security gain.
    assert "connectors.ambiguous_name" not in _codes({"connectors": {name: {"image": "x:1"}}})


def test_the_ambiguous_name_error_says_what_to_do() -> None:
    # An author reads this message and has to fix it without opening the
    # renderer's source: it must name the connector, say that the name forges a
    # second `-mcp-` in the object name Curie derives, and say to rename.
    _, errors = validate_connectors({"connectors": {"mcp-grafana": {"image": "x:1"}}})
    message = next(m for c, m in errors if c == "connectors.ambiguous_name")
    assert "mcp-grafana" in message
    assert "-mcp-" in message
    assert "rename" in message.lower()


def test_the_validator_and_the_renderer_share_one_join_rule() -> None:
    # Two copies of the rule would drift, and the drift is silent in the worst
    # direction: a name the validator accepts and the renderer refuses turns a
    # friendly bundle error into a 500 at deploy time, and a name the validator
    # refuses but the renderer accepts blocks a bundle that would have been
    # fine. Modelled on the placeholder-list pin above -- the renderer owns the
    # rule, the validator only asks.
    from plugin_format.connector_render import connector_forges_join

    for name in FORGING_CONNECTOR_NAMES + SAFE_CONNECTOR_NAMES:
        rejected = "connectors.ambiguous_name" in _codes({"connectors": {name: {"image": "x:1"}}})
        assert rejected is connector_forges_join(name), name


def test_a_forging_name_and_a_bad_shape_report_both() -> None:
    # Same accumulation convention as the reserved-name check above: the author
    # sees every problem in the file in one pass, rather than discovering the
    # next one only after renaming to fix the first. This fails if the new check
    # lands as an `elif` on the shape check. Paired against `image` + `url`
    # rather than the underspecified shape below -- that shape is hosted
    # (`image` is set), so the ambiguous-name guard is live for it too.
    codes = _codes({"connectors": {"mcp-c": {"image": "x:1", "url": "https://y/mcp"}}})
    assert "connectors.ambiguous_name" in codes
    assert "connectors.ambiguous" in codes


def test_a_forging_name_on_an_underspecified_connector_is_not_yet_judged() -> None:
    # Neither `image:` nor `url:` is set, so `is_hosted` is False and the
    # ambiguous-name guard does not fire -- not because the name is safe, but
    # because we do not yet know it will ever reach `object_name`. If the
    # author resolves `connectors.underspecified` by adding `url:`, this exact
    # name is perfectly legal. Reporting `connectors.ambiguous_name` here would
    # be a spurious error chasing a shape the author hasn't chosen yet.
    assert _codes({"connectors": {"mcp-c": {"secrets": ["T"]}}}) == ["connectors.underspecified"]


def test_a_forging_remote_connector_name_is_accepted() -> None:
    # `render()` emits zero objects for a `url` connector and `mcp_entry()`
    # returns the authored URL verbatim, so `object_name` is never called on
    # this name -- it cannot collide with anything. Refusing it anyway would
    # break a previously valid bundle for a collision it cannot cause.
    assert "connectors.ambiguous_name" not in _codes(
        {"connectors": {"mcp-internal": {"url": "https://mcp.internal/mcp"}}}
    )
    assert "connectors.ambiguous_name" not in _codes(
        {"connectors": {"x-mcp-y": {"url": "https://mcp.internal/mcp"}}}
    )


@pytest.mark.parametrize("name", ["mcp-internal", "x-mcp-y"])
def test_the_same_forging_name_is_refused_only_when_hosted(name: str) -> None:
    # The important half of the boundary: the exact same name that is safe as
    # `url` (above) is still refused once it is `image` -- a hosted connector
    # renders real Kubernetes objects through `object_name`, so the collision
    # this check exists to prevent is live again. Pinning both outcomes side
    # by side is what stops a future widening of the remote exemption from
    # quietly disabling the hosted guard too.
    remote_codes = _codes({"connectors": {name: {"url": "https://mcp.internal/mcp"}}})
    hosted_codes = _codes({"connectors": {name: {"image": "x:1"}}})
    assert "connectors.ambiguous_name" not in remote_codes
    assert hosted_codes == ["connectors.ambiguous_name"]


def test_a_forging_hosted_connector_with_an_unhosted_url_stays_refused() -> None:
    # `unhosted_url` is the tier-3 fallback address, not a substitute for
    # `image` -- the connector is still hosted (`is_hosted` reads `image`) and
    # still renders real objects on any tier that can host it. This is the
    # shape most likely to be mis-gated by a later edit that broadens the
    # `url`-only exemption to "anything with a URL on it."
    assert _codes(
        {
            "connectors": {
                "mcp-grafana": {"image": "x:1", "unhosted_url": "${GRAFANA_MCP_URL}"}
            }
        }
    ) == ["connectors.ambiguous_name"]


def test_bundle_surfaces_an_ambiguous_connector_name(tmp_path: Path) -> None:
    # Through the real entry point. `validate.py` forwards validator codes
    # verbatim with no allowlist, and this is what proves it -- a new code that
    # never reaches `validate_bundle` never reaches an author either.
    root = _bundle(tmp_path, "connectors:\n  mcp-grafana:\n    image: x:1\n")
    result = validate_bundle(str(root))
    assert not result.valid
    offending = [e for e in result.errors if e.code == "connectors.ambiguous_name"]
    assert offending, [e.code for e in result.errors]
    assert "mcp-grafana" in offending[0].message
