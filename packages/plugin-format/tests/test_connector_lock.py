"""``connectors.lock.yaml``: resolving a declared build to one pinned image (ADR 0113).

``apply_lock`` is the single enforcement point for the ADR's "the resolved
digest is the only identity rendered into a Deployment or used to start the
local connector". It takes the declaration and the lock and returns a derived
``ConnectorsFile`` in which every ``build:`` connector has become an ordinary
``image:`` connector, so ``render``, ``mcp_entry``, ``is_hosted``, and the
worker's reconcile plan all work unchanged downstream and there is exactly one
place a mutable tag can be refused.

Every case here asserts through that function or through ``validate_bundle``,
never against a model's fields: a lock whose entry parses and is then ignored
is the failure this file exists to catch.

The corpus lives in ``tests/vectors/connector-lock.json`` and
``tests/vectors/connector-source-digest.json`` because the Rust CLI reads the
same bytes -- it writes the lock and preflights it before the platform ever sees
the bundle, and the two lanes cannot share code.

The module under test is imported inside each test rather than at module scope
so the corpus still collects while it does not exist yet: the contract is
readable before the implementation is.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
from plugin_format.connectors import validate_connectors

_VECTORS = Path(__file__).parents[3] / "tests" / "vectors"


def _vector_file(name: str) -> dict:
    return json.loads((_VECTORS / name).read_text(encoding="utf-8"))


_LOCKS = _vector_file("connector-lock.json")
_DIGESTS = _vector_file("connector-source-digest.json")

_LOCK_KEYS = {
    "name",
    "why",
    "connectors",
    "lock",
    "portable",
    "expect",
    "resolved",
    "resolved_runner",
    "codes",
}
_DIGEST_KEYS = {"name", "why", "tree", "build", "source_digest"}

# A registry manifest digest is `<repo>@sha256:` plus 64 lowercase hex
# characters; a local image id is the bare `sha256:` form `docker image inspect
# --format {{.Id}}` reports. Both are fixed by the OCI distribution spec's
# content-addressable digest grammar
# (https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests),
# not by anything in this repository, which is why the literals below are
# spelled out rather than derived from the implementation.
_REGISTRY_IMAGE = (
    "ghcr.io/acme-corp/acme-bot-k8s-write-mcp@sha256:"
    "0000000000000000000000000000000000000000000000000000000000000000"
)
_LOCAL_IMAGE = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
_SOURCE_DIGEST = "sha256:2222222222222222222222222222222222222222222222222222222222222222"

BUILT = {
    "connectors": {
        "k8s-write": {
            "build": {
                "context": "connectors/k8s-write",
                "platforms": ["linux/amd64", "linux/arm64"],
            }
        }
    }
}


def _declared(document: Any) -> Any:
    parsed, errors = validate_connectors(document)
    assert errors == [], errors
    return parsed


def _lock(image: str, delivery: str = "registry") -> dict:
    return {
        "version": 1,
        "connectors": {
            "k8s-write": {
                "image": image,
                "delivery": delivery,
                "platforms": ["linux/amd64", "linux/arm64"],
                "source_digest": _SOURCE_DIGEST,
            }
        },
    }


def _parse_lock(document: Any) -> Any:
    from plugin_format import connector_lock

    parsed, errors = connector_lock.validate_connector_lock(document)
    assert errors == [], errors
    assert parsed is not None
    return parsed


# --------------------------------------------------------------------------- #
# The file name every lane names once
# --------------------------------------------------------------------------- #
def test_the_lock_file_name_is_defined_beside_its_parser() -> None:
    # Same reason CONNECTORS_FILE lives beside validate_connectors: the CLI
    # writes this file, the packer must not exclude it, the validator reads it,
    # and the API reads it again. Four hand-typed copies of a filename is how
    # one of them ends up plural.
    from plugin_format import connector_lock

    assert connector_lock.CONNECTOR_LOCK_FILE == "connectors.lock.yaml"


# --------------------------------------------------------------------------- #
# apply_lock: the single enforcement point
# --------------------------------------------------------------------------- #
def test_a_locked_build_becomes_an_ordinary_image_connector() -> None:
    # The property every downstream reader depends on. `build` is cleared as
    # well as `image` being set: leaving it in place means a second reader can
    # still see an unresolved build and take the wrong branch.
    from plugin_format import connector_lock

    resolved = connector_lock.apply_lock(
        _declared(BUILT), _parse_lock(_lock(_REGISTRY_IMAGE)), portable=True
    )
    spec = resolved.connectors["k8s-write"]
    assert spec.image == _REGISTRY_IMAGE
    assert spec.build is None
    assert spec.is_hosted


def test_apply_lock_does_not_mutate_the_declaration_it_was_given() -> None:
    # The API calls this on every render of every version. A reader that
    # rewrote the parsed declaration in place would leave a resolved image on a
    # cached ConnectorsFile and quietly serve it to the next caller.
    from plugin_format import connector_lock

    declared = _declared(BUILT)
    connector_lock.apply_lock(declared, _parse_lock(_lock(_REGISTRY_IMAGE)), portable=True)
    assert declared.connectors["k8s-write"].image is None
    assert declared.connectors["k8s-write"].build is not None


def test_a_declared_build_with_no_lock_at_all_is_refused() -> None:
    # `None` is the shape read_connector_lock returns for a bundle carrying no
    # lock file, and it must not degrade to "render it anyway with image None".
    from plugin_format import connector_lock

    with pytest.raises(ValueError) as exc:
        connector_lock.apply_lock(_declared(BUILT), None, portable=False)
    assert "k8s-write" in str(exc.value)


def test_a_tag_shaped_image_is_refused() -> None:
    # The outcome test for "the renderer and applier never deploy a mutable
    # connector tag" (ADR 0113). A tag can be repointed at a different artifact
    # after review and between tier runs, which is the failure mode this
    # platform has already hit. The reference below is otherwise perfectly well
    # formed, so only a real digest check refuses it.
    from plugin_format import connector_lock

    with pytest.raises(ValueError):
        connector_lock.apply_lock(
            _declared(BUILT),
            _parse_lock(_lock("ghcr.io/acme-corp/acme-bot-k8s-write-mcp:v1")),
            portable=True,
        )


def test_a_local_daemon_image_is_refused_where_portability_is_required() -> None:
    # A local image id names nothing a Kubernetes node can pull. Applying it
    # yields ImagePullBackOff on every node, long after the deploy reported
    # success.
    from plugin_format import connector_lock

    with pytest.raises(ValueError):
        connector_lock.apply_lock(
            _declared(BUILT), _parse_lock(_lock(_LOCAL_IMAGE, "local-daemon")), portable=True
        )


def test_the_same_local_daemon_image_applies_where_portability_is_not_required() -> None:
    # The regression pin for review round 2 finding r2-3. A local-tier version
    # is a legitimate stored artifact and rendering its manifests is harmless,
    # because nothing applies them at local tier. Refusing here broke every
    # `curie local deploy` in an earlier draft, so the two halves of this pair
    # must stay a pair: making the refusal unconditional turns this green test
    # red.
    from plugin_format import connector_lock

    resolved = connector_lock.apply_lock(
        _declared(BUILT), _parse_lock(_lock(_LOCAL_IMAGE, "local-daemon")), portable=False
    )
    assert resolved.connectors["k8s-write"].image == _LOCAL_IMAGE


def test_portable_is_keyword_only() -> None:
    # Two callers pass different values and the wrong one silently deploys an
    # unpullable image to a cluster. A positional call site cannot express that
    # mistake if there is no positional call site.
    from plugin_format import connector_lock

    with pytest.raises(TypeError):
        connector_lock.apply_lock(_declared(BUILT), _parse_lock(_lock(_REGISTRY_IMAGE)), True)


def test_a_resolved_build_renders_where_an_unresolved_one_raises() -> None:
    # The two halves of the guard, in one place: render refuses an unresolved
    # build (test_connector_render.py owns that assertion) and accepts the
    # resolved one, with the locked digest reaching the container spec. This is
    # the only path an image reaches a Deployment, so a lock that parses but is
    # never applied shows up here as `image: None`.
    from plugin_format import connector_lock, connector_render

    resolved = connector_lock.apply_lock(
        _declared(BUILT), _parse_lock(_lock(_REGISTRY_IMAGE)), portable=True
    )
    objects = connector_render.render(
        release="acme-rel",
        agent="acme-bot",
        namespace="acme-ns",
        app_name="curie",
        connector="k8s-write",
        spec=resolved.connectors["k8s-write"],
        secret_name="acme-rel-acme-bot-connector-secrets",
    )
    deployment = next(o for o in objects if o["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == _REGISTRY_IMAGE


# --------------------------------------------------------------------------- #
# The Python half of the frozen Python/Rust lock seam
# --------------------------------------------------------------------------- #
def test_every_lock_vector_declares_only_modelled_keys() -> None:
    for vector in _LOCKS["vectors"]:
        extra = set(vector) - _LOCK_KEYS
        assert not extra, f"{vector['name']}: unmodelled vector keys {sorted(extra)}"
        assert vector["expect"] in {"apply", "raise", "invalid"}


@pytest.mark.parametrize("vector", _LOCKS["vectors"], ids=lambda v: v["name"])
def test_lock_vectors(vector: dict) -> None:
    # Driven off tests/vectors/connector-lock.json so the Rust mirror in
    # cli/src/connector_build.rs cannot diverge. "invalid" means the document
    # does not survive validate_connector_lock, so a malformed lock is rejected
    # at bundle intake rather than surfacing at render time; "raise" means it
    # parses and apply_lock refuses it.
    from plugin_format import connector_lock

    declared = _declared(vector["connectors"])
    if vector["lock"] is None:
        parsed_lock = None
    else:
        parsed_lock, errors = connector_lock.validate_connector_lock(vector["lock"])
        codes = [code for code, _ in errors]
        if vector["expect"] == "invalid":
            assert parsed_lock is None
            assert codes, f"{vector['name']} must be rejected by validate_connector_lock"
            for expected in vector["codes"]:
                assert expected in codes, f"{vector['name']} expected {expected}, got {codes}"
            return
        assert errors == [], f"{vector['name']} must parse cleanly, got {codes}"

    if vector["expect"] == "raise":
        # The deploy path runs both resolvers; either may be the one refusing
        # (ADR 0173 puts the runner's refusals in resolve_runner_image).
        with pytest.raises(ValueError):
            connector_lock.apply_lock(declared, parsed_lock, portable=vector["portable"])
            connector_lock.resolve_runner_image(declared, parsed_lock, portable=vector["portable"])
        return

    resolved = connector_lock.apply_lock(declared, parsed_lock, portable=vector["portable"])
    for name, image in vector["resolved"].items():
        assert resolved.connectors[name].image == image
        assert resolved.connectors[name].build is None
    if "resolved_runner" in vector:
        assert (
            connector_lock.resolve_runner_image(declared, parsed_lock, portable=vector["portable"])
            == vector["resolved_runner"]
        )


def test_lock_model_field_names_match_the_frozen_vector() -> None:
    # See tests/vectors/connector-fields.json: the schema-driven field-parity
    # gate compares nothing for these structs because plugin-format.schema.json
    # carries no Connector* $defs, so this pair of assertions plus the Rust half
    # is the only thing that keeps the two languages in step.
    from plugin_format.connector_lock import ConnectorLockEntry, ConnectorLockFile

    fields = _vector_file("connector-fields.json")["models"]
    assert set(ConnectorLockFile.model_fields) == set(fields["ConnectorLockFile"])
    assert set(ConnectorLockEntry.model_fields) == set(fields["ConnectorLockEntry"])


def test_runner_lock_model_field_names_match_the_frozen_vector() -> None:
    from plugin_format.connector_lock import RunnerLockEntry

    fields = _vector_file("connector-fields.json")["models"]
    assert set(RunnerLockEntry.model_fields) == set(fields["RunnerLockEntry"])


# --------------------------------------------------------------------------- #
# resolve_runner_image: the single runner resolver (ADR 0173 decision 2)
# --------------------------------------------------------------------------- #
RUNNER = {
    "connectors": {},
    "runner": {"build": {"context": "runner", "platforms": ["linux/amd64", "linux/arm64"]}},
}
_RUNNER_IMAGE = "registry.example/acme/acme-bot-runner@sha256:" + "a" * 64
_RUNNER_BASE = "ghcr.io/curie-eng/curie-runner@sha256:" + "b" * 64


def _runner_lock(
    image: str = _RUNNER_IMAGE, base: str = _RUNNER_BASE, delivery: str = "registry"
) -> dict:
    return {
        "version": 1,
        "connectors": {},
        "runner": {
            "image": image,
            "base": base,
            "delivery": delivery,
            "platforms": ["linux/amd64", "linux/arm64"],
            "source_digest": _SOURCE_DIGEST,
        },
    }


def test_no_declared_runner_resolves_to_none() -> None:
    # Every existing bundle: the platform runner is used, lock or no lock.
    from plugin_format import connector_lock

    assert connector_lock.resolve_runner_image(_declared(BUILT), None, portable=True) is None
    assert (
        connector_lock.resolve_runner_image(
            _declared(BUILT), _parse_lock(_lock(_REGISTRY_IMAGE)), portable=True
        )
        is None
    )


def test_a_locked_runner_resolves_to_exactly_the_recorded_image() -> None:
    from plugin_format import connector_lock

    assert (
        connector_lock.resolve_runner_image(
            _declared(RUNNER), _parse_lock(_runner_lock()), portable=True
        )
        == _RUNNER_IMAGE
    )


@pytest.mark.parametrize(
    "lock",
    [
        None,
        {"version": 1, "connectors": {}},
        _runner_lock(image="registry.example/acme/acme-bot-runner:v1"),
        _runner_lock(base="ghcr.io/curie-eng/curie-runner:0.10.0"),
    ],
    ids=["no-lock", "no-runner-entry", "tag-image", "tag-base"],
)
def test_a_declared_runner_without_a_pinned_lock_is_refused(lock: dict | None) -> None:
    from plugin_format import connector_lock

    parsed = None if lock is None else _parse_lock(lock)
    with pytest.raises(ValueError) as exc:
        connector_lock.resolve_runner_image(_declared(RUNNER), parsed, portable=False)
    assert "runner" in str(exc.value)


def test_a_local_daemon_runner_is_refused_only_where_portability_is_required() -> None:
    from plugin_format import connector_lock

    lock = _parse_lock(
        _runner_lock(image=_LOCAL_IMAGE, base="sha256:" + "b" * 64, delivery="local-daemon")
    )
    assert (
        connector_lock.resolve_runner_image(_declared(RUNNER), lock, portable=False) == _LOCAL_IMAGE
    )
    with pytest.raises(ValueError):
        connector_lock.resolve_runner_image(_declared(RUNNER), lock, portable=True)


def test_resolve_runner_image_portable_is_keyword_only() -> None:
    from plugin_format import connector_lock

    with pytest.raises(TypeError):
        connector_lock.resolve_runner_image(  # type: ignore[misc]
            _declared(RUNNER), _parse_lock(_runner_lock()), True
        )


# --------------------------------------------------------------------------- #
# The runner Dockerfile takes its base as CURIE_RUNNER_IMAGE (shared corpus)
# --------------------------------------------------------------------------- #
_RUNNER_DOCKERFILES = _vector_file("runner-dockerfile-base.json")


def test_every_runner_dockerfile_vector_declares_only_modelled_keys() -> None:
    for vector in _RUNNER_DOCKERFILES["vectors"]:
        assert set(vector) == {"name", "why", "dockerfile", "expect"}, vector["name"]
        assert vector["expect"] in {"accept", "reject"}


@pytest.mark.parametrize("vector", _RUNNER_DOCKERFILES["vectors"], ids=lambda v: v["name"])
def test_runner_dockerfile_base_vectors(vector: dict) -> None:
    from plugin_format import connector_lock

    message = connector_lock.check_runner_dockerfile(vector["dockerfile"])
    if vector["expect"] == "accept":
        assert message is None, f"{vector['name']}: {message}"
    else:
        assert message, f"{vector['name']} must be refused"
        assert "CURIE_RUNNER_IMAGE" in message


# --------------------------------------------------------------------------- #
# source_digest: what makes "unchanged source, identical digest" a real proof
# --------------------------------------------------------------------------- #
def _materialize(root: Path, tree: dict[str, Any]) -> Path:
    # A tree value is either a content string or `{"content", "executable"}`.
    # The mode is part of the digest -- the build context tar carries it -- so a
    # materializer that dropped the object form would silently write the
    # executable vectors non-executable and fail against a corpus that is right.
    for rel, value in tree.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(value, str):
            content, executable = value, False
        else:
            content, executable = value["content"], bool(value.get("executable"))
        path.write_text(content, encoding="utf-8")
        mode = path.stat().st_mode & 0o7777
        path.chmod(mode | 0o100 if executable else mode & ~0o111)
    return root


def test_every_source_digest_vector_declares_only_modelled_keys() -> None:
    for vector in _DIGESTS["vectors"]:
        extra = set(vector) - _DIGEST_KEYS
        assert not extra, f"{vector['name']}: unmodelled vector keys {sorted(extra)}"


@pytest.mark.parametrize("vector", _DIGESTS["vectors"], ids=lambda v: v["name"])
def test_source_digest_vectors(vector: dict, tmp_path: Path) -> None:
    # The corpus is the algorithm's specification; the file's own `comment`
    # states it in full, because a corpus alone cannot say why two readers
    # disagree. The Rust port is frozen against the same bytes.
    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    context = _materialize(tmp_path / "ctx", vector["tree"])
    build = ConnectorBuild.model_validate(vector["build"])
    assert connector_lock.source_digest_of(context, build) == vector["source_digest"]


@pytest.mark.parametrize("relation", _DIGESTS["relations"], ids=lambda r: r.get("why", "")[:40])
def test_source_digest_relations(relation: dict, tmp_path: Path) -> None:
    # Where the exclusion and build-block rules are actually falsified. A reader
    # that ignores .dockerignore still passes every single-vector assertion by
    # recording whatever it computes; it cannot satisfy both halves of a
    # relation, because one pair must come out equal and another must not.
    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    by_name = {v["name"]: v for v in _DIGESTS["vectors"]}
    computed = []
    for i, name in enumerate(relation.get("same") or relation["distinct"]):
        vector = by_name[name]
        context = _materialize(tmp_path / f"ctx{i}", vector["tree"])
        computed.append(
            connector_lock.source_digest_of(context, ConnectorBuild.model_validate(vector["build"]))
        )
    if "same" in relation:
        assert computed[0] == computed[1], relation["why"]
    else:
        assert computed[0] != computed[1], relation["why"]


def test_the_digest_ignores_mtime_and_every_mode_bit_but_owner_execute(tmp_path: Path) -> None:
    # Deliberately unlike cli/src/bundle.rs's archive digest, which embeds
    # per-file mtime, uid and gid by design because it identifies an ARCHIVE and
    # not a source tree. Reusing that computation here would report every fresh
    # checkout as a changed source, so every build after a clone would look
    # stale and rebuild. The owner execute bit is the one exception (pinned by
    # the test below); a group or other bit differing by umask is not.
    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    tree = {"Dockerfile": "FROM scratch\n", "server.py": "print('acme')\n"}
    first = _materialize(tmp_path / "a", tree)
    second = _materialize(tmp_path / "b", tree)
    for path in second.rglob("*"):
        if path.is_file():
            os.utime(path, (1_000_000_000, 1_000_000_000))
            # Group and other bits, not owner execute: a checkout under a
            # different umask must not read as a changed source.
            path.chmod(0o644 if path.name == "Dockerfile" else 0o600)
    build = ConnectorBuild.model_validate(
        {"context": "connectors/tempo", "platforms": ["linux/amd64"]}
    )
    assert connector_lock.source_digest_of(first, build) == connector_lock.source_digest_of(
        second, build
    )


def test_making_a_file_executable_moves_the_digest(tmp_path: Path) -> None:
    # RED before the fix: `chmod +x entrypoint.sh` with no byte changed produced
    # the same digest, so the lock looked fresh while the image that would be
    # built from it differs -- Docker's context tar carries the mode and
    # BuildKit keys its cache on it. Asserted as an inequality against the same
    # tree rather than as a frozen literal, so it survives a stream change; the
    # corpus pair `an_entrypoint_without_the_executable_bit` /
    # `chmod_plus_x_on_the_entrypoint_moves_the_digest` pins the exact values on
    # both sides of the seam.
    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    tree = {"Dockerfile": "FROM scratch\n", "entrypoint.sh": "#!/bin/sh\nexec server\n"}
    plain = _materialize(tmp_path / "plain", tree)
    executable = _materialize(
        tmp_path / "exec",
        {
            "Dockerfile": tree["Dockerfile"],
            "entrypoint.sh": {"content": tree["entrypoint.sh"], "executable": True},
        },
    )
    build = ConnectorBuild.model_validate(
        {"context": "connectors/tempo", "platforms": ["linux/amd64"]}
    )
    assert connector_lock.source_digest_of(plain, build) != connector_lock.source_digest_of(
        executable, build
    ), "the owner execute bit is part of the build input, so it is part of the digest"


# --------------------------------------------------------------------------- #
# Symlinks: the one rule the JSON corpus cannot express
#
# A vector's `tree` is a path-to-content map, so it can state every ordering and
# exclusion rule but not a link. Both languages must agree here or the digest
# stops being a cross-language identity: `cli/src/connector_build.rs` refuses a
# symlinked context in `resolve_context` and skips every symlink in
# `collect_files`, so these are the Python twins of assertions the CLI suite
# already makes (`a_context_outside_the_bundle_is_refused`).
# --------------------------------------------------------------------------- #
def test_a_symlink_inside_the_context_is_neither_followed_nor_hashed(tmp_path: Path) -> None:
    # RED if the walk ever dereferences a link: the digest gains a record and
    # stops matching the CLI's, so every bundle with a symlinked file in its
    # context reports its lock stale on one side of the seam and current on the
    # other. Asserted as an equality against the same tree WITHOUT the links,
    # rather than as a frozen literal, so it survives any future stream change.
    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.env").write_text("TOKEN=abc\n", encoding="utf-8")
    (outside / "lib").mkdir()
    (outside / "lib" / "vendored.py").write_text("VENDORED = 1\n", encoding="utf-8")

    plain = _materialize(tmp_path / "plain", {"Dockerfile": "FROM scratch\n"})
    linked = _materialize(tmp_path / "linked", {"Dockerfile": "FROM scratch\n"})
    (linked / "secret.env").symlink_to(outside / "secret.env")
    (linked / "lib").symlink_to(outside / "lib", target_is_directory=True)

    build = ConnectorBuild.model_validate(
        {"context": "connectors/tempo", "platforms": ["linux/amd64"]}
    )
    assert connector_lock.source_digest_of(linked, build) == connector_lock.source_digest_of(
        plain, build
    ), "a symlink is never followed and never hashed, on both sides of the seam"


def test_a_symlinked_build_context_is_refused_rather_than_dereferenced(tmp_path: Path) -> None:
    # RED before the fix: `resolve_context` did not exist on the Python side at
    # all, so intake joined the path and hashed whatever it landed on. The digest
    # would then pin bytes the bundle does not carry, which can change under the
    # lock without the lock ever going stale.
    from plugin_format import connector_lock

    bundle = tmp_path / "bundle"
    (bundle / "connectors").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (bundle / "connectors" / "k8s-write").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        connector_lock.resolve_context(bundle, "connectors/k8s-write")


def test_a_context_resolving_outside_the_bundle_is_refused(tmp_path: Path) -> None:
    # The containment half, matching the CLI's `resolve_context` exactly: an
    # absolute path and a `..` climb both name a tree the bundle does not carry.
    from plugin_format import connector_lock

    bundle = tmp_path / "bundle"
    (bundle / "connectors" / "k8s-write").mkdir(parents=True)
    (tmp_path / "outside").mkdir()

    for context in ("../outside", "/etc"):
        with pytest.raises(ValueError, match="outside the bundle"):
            connector_lock.resolve_context(bundle, context)


def test_a_context_inside_the_bundle_resolves(tmp_path: Path) -> None:
    # The positive control. Without it every assertion above is satisfied by a
    # resolver that refuses everything, which would refuse every real bundle.
    from plugin_format import connector_lock

    bundle = tmp_path / "bundle"
    (bundle / "connectors" / "k8s-write").mkdir(parents=True)

    resolved = connector_lock.resolve_context(bundle, "connectors/k8s-write")
    assert resolved == (bundle / "connectors" / "k8s-write").resolve()
    assert connector_lock.resolve_context(bundle, ".") == bundle.resolve()
