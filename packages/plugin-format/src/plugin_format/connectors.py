"""Declared connectors: what a bundle needs running, not how to run it (ADR-0086).

``.mcp.json`` says an MCP server exists and where to reach it. It has never said
who *runs* it, so every bundle author writing a non-stdio connector had to
hand-author a Deployment, a Service, a Secret reference, container hardening,
and a NetworkPolicy -- roughly 180 lines of Kubernetes to stand up one server,
per bundle, each copy independently right or wrong.

``connectors.yaml`` closes that. A bundle declares intent; the platform derives
the objects. Three forms:

    connectors:
      grafana:                                  # hosted: Curie runs the image
        image: grafana/mcp-grafana:0.17.2
        args: [-t, streamable-http, -disable-write]
        env: {GRAFANA_URL: "https://grafana.example.com"}
        secrets: [GRAFANA_SERVICE_ACCOUNT_TOKEN]

      k8s-write:                                # hosted: Curie BUILDS the image
        build:
          context: connectors/k8s-write
          platforms: [linux/amd64, linux/arm64]
        secret_files: {K8S_WRITE_KUBECONFIG: /secrets/kubeconfig}

      internal:                                 # remote: already running
        url: https://mcp.internal/mcp
        secrets: [INTERNAL_TOKEN]

The ``build`` form (ADR 0113) replaces only where the image comes FROM. Every
other field keeps its exact meaning, and the resolved digest lives in
``connectors.lock.yaml``, never here -- see ``connector_lock``.

Secrets are NAMES only. Values arrive at deploy time through the existing
mechanism (ADR-0009), so this introduces no new way to carry a credential and
a bundle stays safe to commit.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .reserved_env import SECRET_NAME_RE, is_reserved_boot_env_name

# The file this module parses. Defined here, with the parser, so every reader
# (the deploy validator, the approval-policy gate-name derivation, the API's
# bundle reader) names one constant rather than its own copy of the string.
CONNECTORS_FILE = "connectors.yaml"

# A connector name becomes a Kubernetes object name and a DNS label, so it is
# held to the stricter of those: lowercase alphanumeric and dashes, starting and
# ending alphanumeric. Rejecting here beats a confusing apply-time failure.
_NAME_MAX = 40


class SecretRef(BaseModel):
    """A credential that already exists in the cluster (#1163).

    Curie renders a ``secretKeyRef`` at it and never reads the value, so the
    deploy identity does not need access to the credential at all. Rotation
    stops requiring a deploy: update the Secret, restart the connector.
    """

    model_config = ConfigDict(extra="forbid")

    # The env var the connector reads.
    name: str
    # The Kubernetes Secret holding it, provisioned out of band.
    from_secret: str
    # The key within that Secret. Defaults to the env var name, which is the
    # common case and saves repeating it.
    key: str | None = None

    def secret_key(self) -> str:
        return self.key or self.name


class ConnectorBuild(BaseModel):
    """Where a connector's image comes FROM, when the bundle carries its source.

    ADR 0113: an operator should not have to build an image, put it somewhere
    pullable, and paste a reference back into the bundle by hand. The bundle
    declares the build input -- which is what a reviewer can actually read in a
    source change -- and `curie build` records what it resolved to in
    ``connectors.lock.yaml``. The digest never appears here.
    """

    model_config = ConfigDict(extra="forbid")

    # Bundle-relative by construction. ADR 0113 requires the context be
    # constrained to the extracted bundle "without accepting arbitrary host
    # paths", so an absolute path or one that normalizes out of the bundle root
    # is refused by `validate_connectors` rather than resolved.
    context: str
    # Relative to the context, defaulted because the common case omits it. A
    # reader that left this empty would build whatever the daemon's own default
    # happens to be, which is a different file in a different place.
    dockerfile: str = "Dockerfile"
    # Required rather than defaulted. A silently single-arch build passes every
    # declaration check and then fails after apply as "no matching manifest for
    # linux/arm64" -- the exact failure
    # `examples/sre-bot/connectors/k8s-write/Dockerfile:1-5` documents -- and a
    # default would pick an architecture on the author's behalf without saying
    # so.
    platforms: list[str]


class ConnectorSpec(BaseModel):
    """One declared connector. Exactly one of ``image``, ``build`` or ``url``.

    Strict, unlike the rest of this package. The leniency mandate exists because
    real Claude Code plugin bundles carry keys this MVP does not model, and
    rejecting them would reject valid bundles. ``connectors.yaml`` is not a
    Claude Code artifact -- it is Curie's own file with no external producer, so
    an unrecognised key is a typo, and silently ignoring it would mean a
    connector that renders subtly wrong with no diagnostic.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # -- hosted form --
    image: str | None = None
    # The same hosted form, sourced rather than referenced (ADR 0113). Mutually
    # exclusive with `image`: two image sources on one connector means whichever
    # the reader picks silently ignores the other, and the ignored one is the
    # one the author edited last.
    build: ConnectorBuild | None = None
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    port: int = 8000

    # Where to reach this connector on a tier that CANNOT host it (#1160).
    #
    # The hosted form's escape hatch, not a second remote form. `cluster` runs
    # the image and derives the URL from the Service it created; `skill` hosts
    # nothing at all, and `local` does not host connectors yet (#1159). Without
    # this a bundle whose only connector is hosted simply has no connector on
    # those rungs, which is right when there is genuinely nothing to point at
    # and wrong when the developer has a copy running on port 8765.
    #
    # `${VAR}` is expanded from the sandbox environment by the MCP client, the
    # same expansion `.mcp.json` has always used -- so the value can be supplied
    # per machine with `--secret`, and the bundle stays safe to commit.
    unhosted_url: str | None = None

    # -- remote form --
    url: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)

    # -- both --
    #
    # Two forms, and the difference is WHO HOLDS THE CREDENTIAL.
    #
    #   secrets: [TOKEN]                      Curie resolves TOKEN at deploy
    #                                         time and owns the Secret.
    #   secrets:
    #     - name: TOKEN                       Curie points at a Secret someone
    #       from_secret: grafana-mcp          else provisioned and never sees
    #       key: TOKEN                        the value.
    #
    # The second exists because the first forces every deploy identity to be
    # able to read the credential -- CI, the operator, and anything driving a
    # deploy. For a shared team token that is backwards, and it is what blocks
    # an in-cluster reconciler from ever applying a connector (ADR-0090): a
    # reconciler cannot hold every agent's credentials, but it can reference a
    # Secret that already exists.
    secrets: list[str | SecretRef] = Field(default_factory=list)

    #   bearer_secret: PAT                    The env-var name the derived
    #                                         Authorization: Bearer ${NAME}
    #                                         header expands. Optional. A hosted
    #                                         connector with exactly one secrets
    #                                         entry still derives from that name
    #                                         (the github-mcp-server shape). A
    #                                         hosted connector with two or more
    #                                         must set this rather than silently
    #                                         using secrets[0] (#2559).
    bearer_secret: str | None = None

    #   sealed_secrets:                       The bundle CARRIES the credential,
    #     TOKEN: AgBv3n2K...                  encrypted to the cluster that will
    #                                         run it (ADR-0094).
    #
    # A third holder for the same question. The blob is useless without the
    # cluster's private key, so it can live in the repository -- public or
    # private -- beside the connector that reads it, and an agent repo becomes
    # self-contained: clone it, point a cluster at it, tools work.
    #
    # Keyed by the env var name so it reads like `env` and cannot express two
    # blobs for one variable. Additive and optional: a bundle that does not use
    # it is unaffected, which is what lets this land as a backward-compatible
    # change to a frozen contract.
    #
    # Validation refuses this field until a production decrypt path exists.
    # This prevents a connector from deploying without the credential it needs.
    sealed_secrets: dict[str, str] = Field(default_factory=dict)

    #   secret_files:                         Project a stored secret as a FILE
    #     K8S_KUBECONFIG: /secrets/kubeconfig instead of an environment variable.
    #
    # Same stored secret, same resolution, same per-agent Secret -- only the
    # delivery differs. It exists because `secrets:` renders a secretKeyRef and
    # nothing else, so a server that authenticates from a FILE has no way in:
    # kubeconfigs, TLS client certs, GCP service-account JSON, a CA bundle.
    # `connectors.yaml` forbids extra keys and has no `command`, so a bundle
    # cannot write $VAR out to a path itself either -- the credential was
    # simply unreachable (#1402).
    #
    # Keyed by SECRET NAME so the same name cannot be projected to two paths,
    # and so the key reads identically to `secrets:` -- a bundle moving from one
    # to the other changes the delivery, not the credential.
    #
    # Mounted 0400 and read-only into a container that already runs as non-root
    # with a read-only rootfs. Paths must be absolute and must not collide with
    # each other; `/tmp` is refused because a server's scratch space is a poor
    # place for a credential and an emptyDir there would shadow the mount.
    secret_files: dict[str, str] = Field(default_factory=dict)

    def bearer_secret_name(self) -> str | None:
        """The secret name the derived Bearer header expands, or None.

        Explicit ``bearer_secret`` wins. Otherwise a single declared secret is
        that name -- the github-mcp-server shape, so existing one-secret
        bundles keep working. Two or more without an explicit name is None:
        validation refuses that document rather than picking ``secrets[0]``.
        """

        if self.bearer_secret:
            return self.bearer_secret
        names = [s if isinstance(s, str) else s.name for s in self.secrets]
        if len(names) == 1:
            return names[0]
        return None

    def secret_names(self) -> list[str]:
        """Env var names this connector needs, any form."""

        named = [s if isinstance(s, str) else s.name for s in self.secrets]
        # secret_files are resolved from the same store and land in the same
        # per-agent Secret; only the projection differs, so they belong here.
        return named + list(self.sealed_secrets) + list(self.secret_files)

    def resolved_secrets(self) -> list[str]:
        """Only the names Curie must resolve a VALUE for.

        This is what the deploy path is told to write into the per-agent Secret,
        so anything the renderer points at that Secret MUST appear here. That is
        why `secret_files` belongs: it names a key in that same Secret, mounted
        as a file rather than injected as an env var, and the volume is rendered
        `optional: false`. Omitting them left the Secret with no such key -- and
        for a `secret_files`-only connector, with no keys at all, so the CLI
        skipped creating the Secret entirely and the pod never started, stuck on
        `FailedMount ... secret not found` (#1424).

        Two forms are excluded, and neither is an oversight:

        - a `SecretRef` (`from_secret`) points at a Secret provisioned out of
          band, and the whole point of ADR-0090 is that nothing in the deploy
          path ever handles its value -- resolving it would reimpose the
          credential access this form exists to remove;
        - a `sealed_secrets` blob IS the credential, carried by the bundle and
          sealed to the cluster that will run it, so the deploy path has no value
          to resolve; opening it belongs to the worker under ADR-0094, and asking
          an operator for a value here would ask for one the bundle already has.
        """

        named = [s for s in self.secrets if isinstance(s, str)]
        return named + list(self.secret_files)

    @property
    def is_hosted(self) -> bool:
        # Hosted-ness is about WHO RUNS THE PROCESS, not about whether the image
        # reference has been resolved yet. A `build:` connector with no lock
        # applied is hosted and unrenderable, which `connector_render.render`
        # refuses explicitly rather than emitting `image: null`.
        return self.image is not None or self.build is not None


class ConnectorsFile(BaseModel):
    """The parsed ``connectors.yaml``."""

    model_config = ConfigDict(extra="forbid")

    connectors: dict[str, ConnectorSpec] = Field(default_factory=dict)


_NAME_RE = re.compile(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?")

# The two keys Curie's own platform MCP servers occupy on
# ``ClaudeAgentOptions.mcp_servers``: the approval server and the durable
# state server. A connector declaring one of these names replaces a platform
# server in the agent's session -- silently, since both ride the same map a
# declared connector rides -- and the agent loses ``request_approval`` or the
# state tools with nothing logged until a skill calls one.
#
# The fence is these two exact names, not the ``curie-`` prefix, so a
# legitimate connector called ``curie-docs`` stays legal.
#
# The real owners are ``runner/src/curie_runner/approval.py``
# ``APPROVAL_SERVER_NAME`` and ``runner/src/curie_runner/state.py``
# ``STATE_SERVER_NAME``. ``runner`` depends on ``plugin-format``, never the
# reverse, so this validator cannot import those constants and re-enumerates
# them instead. ``runner/tests/test_connectors.py`` is the drift pin that
# keeps this copy honest.
#
# What each reserved name would displace, keyed by name, so the reserved-name
# error message and the reserved set share one source instead of two hand
# maintained copies.
_RESERVED_CONNECTOR_DISPLACES: dict[str, str] = {
    "curie": "`request_approval`",
    "curie-state": "the durable-state tools",
}
RESERVED_CONNECTOR_NAMES: frozenset[str] = frozenset(_RESERVED_CONNECTOR_DISPLACES)

# ``${CURIE_*}`` placeholders an author may write in args/env. The renderer
# substitutes these with values only Curie can derive (the Service name it
# invents, which embeds the release, agent, and namespace). Imported from the
# renderer so the accepted set and the substituted set are one list.
_PLACEHOLDER_RE = re.compile(r"\$\{(CURIE_[A-Z0-9_]*)\}")


def _known_placeholders() -> frozenset[str]:
    """The placeholder names the renderer substitutes.

    Imported lazily to keep this module free of a render-time dependency; the
    point is that there is exactly one list, so a placeholder the renderer
    handles is always one the validator accepts and vice versa.
    """

    from .connector_render import PLACEHOLDERS

    return frozenset(PLACEHOLDERS)


def _forges_the_render_join(name: str) -> bool:
    """Whether this connector name would forge a second ``-mcp-`` in the object name.

    Imported lazily for exactly the reason ``_known_placeholders`` above is:
    ``connector_render`` imports THIS module at top level, so importing it back
    at top level is a circular import.

    The rule is deliberately not restated here. The renderer owns it, so the
    name this validator refuses and the name the renderer refuses cannot drift
    -- and the drift would be silent in both directions: a name the validator
    accepts but the renderer refuses turns a friendly bundle error into a crash
    at deploy time, and a name the validator refuses but the renderer accepts
    blocks a bundle that was fine (#1446).
    """

    from .connector_render import connector_forges_join

    return connector_forges_join(name)


def _is_valid_name(name: str) -> bool:
    """RFC 1123 label, capped: the name becomes a k8s object and a DNS label."""

    return len(name) <= _NAME_MAX and bool(_NAME_RE.fullmatch(name))


# An OCI platform string: `os/arch`, optionally `os/arch/variant` (`linux/arm/v7`
# is a real target, so rejecting the third segment would be a false positive that
# blocks a legitimate build). A typo like `linux/amd_64` otherwise reaches buildx
# verbatim and fails there with a message about the daemon rather than the bundle.
_PLATFORM_RE = re.compile(r"[a-z0-9]+/[a-z0-9]+(/v[0-9]+)?")


def _escapes(path: str) -> bool:
    """Is this bundle-relative path absolute, empty, or outside the bundle root?

    Textual, because this validator sees a declaration and not always a tree:
    the CLI's ``resolve_context`` / ``resolve_dockerfile`` canonicalize on top of
    this and additionally refuse a symlink, which no amount of string inspection
    can see.
    """

    if not path.strip():
        return True
    parts = PurePosixPath(path).parts
    if PurePosixPath(path).is_absolute():
        return True
    depth = 0
    for part in parts:
        if part == "..":
            depth -= 1
            if depth < 0:
                return True
        elif part != ".":
            depth += 1
    return False


def _validate_build(
    where: str, build: ConnectorBuild, errors: list[tuple[str, str]]
) -> None:
    """The `build:` form's own rules (ADR 0113).

    Each condition carries its own code so a caller can assert the exact outcome
    rather than "something was wrong", and all of them accumulate like every
    other check here, so an author sees the whole file in one pass.
    """

    if _escapes(build.context):
        errors.append(
            (
                "connectors.build_context_escapes",
                f"{where}: `build.context` is {build.context!r}; it must be a path inside "
                "the bundle. An absolute path makes the operator's own machine the build "
                "context, and an empty one normalizes to the bundle root on one reader "
                "and to the process working directory on another",
            )
        )
    if _escapes(build.dockerfile):
        errors.append(
            (
                "connectors.build_dockerfile_escapes",
                f"{where}: `build.dockerfile` is {build.dockerfile!r}; it must be a path "
                "inside the build context. The CLI reads this file before the bundle is "
                "packed, which is before every other guard in the pipeline",
            )
        )
    if not build.platforms:
        errors.append(
            (
                "connectors.build_no_platforms",
                f"{where}: `build.platforms` is empty. A silently single-arch build passes "
                "every declaration check and then fails after apply as `no matching "
                "manifest for linux/arm64`, so the target set is stated, never guessed",
            )
        )
    for platform in build.platforms:
        if not _PLATFORM_RE.fullmatch(platform):
            errors.append(
                (
                    "connectors.build_bad_platform",
                    f"{where}: `{platform}` is not an OCI platform. Use `os/arch` or "
                    "`os/arch/variant`, such as `linux/amd64` or `linux/arm/v7`",
                )
            )


def validate_connectors(data: Any) -> tuple[ConnectorsFile | None, list[tuple[str, str]]]:
    """Validate parsed ``connectors.yaml`` content.

    Returns ``(parsed, errors)`` where each error is ``(code, message)``. A
    non-empty error list means the file must be rejected -- these are all
    conditions that would otherwise surface as an opaque Kubernetes apply
    failure long after the author has stopped looking.
    """

    errors: list[tuple[str, str]] = []
    if data is None:
        return ConnectorsFile(), errors
    if not isinstance(data, dict):
        return None, [("connectors.not_object", "connectors.yaml must be a mapping")]

    try:
        parsed = ConnectorsFile.model_validate(data)
    except Exception as exc:  # pydantic ValidationError -- surface it verbatim
        return None, [("connectors.invalid", str(exc)[:400])]

    for name, spec in parsed.connectors.items():
        where = f"connectors.{name}"
        if not _is_valid_name(name):
            errors.append(
                (
                    "connectors.bad_name",
                    f"{where}: a connector name becomes a Kubernetes object and DNS "
                    "label, so it must be lowercase alphanumeric or dashes, start and "
                    f"end alphanumeric, and be at most {_NAME_MAX} characters",
                )
            )
        if name in RESERVED_CONNECTOR_NAMES:
            lost = _RESERVED_CONNECTOR_DISPLACES[name]
            errors.append(
                (
                    "connectors.reserved_name",
                    f"{where}: `{name}` is reserved for Curie's own platform MCP server "
                    "(the approval server / the durable state server), so a connector "
                    "declaring it would replace that server in the agent's session and "
                    f"the agent would lose {lost} -- rename the connector",
                )
            )
        # A separate `if`, never an `elif` on the shape check above: a name can
        # be both badly shaped and forging, and this validator's convention is
        # to report every applicable code so the author fixes the whole file in
        # one pass instead of discovering the next problem after each rename.
        #
        # Gated on `spec.is_hosted`: this guard exists because the name reaches
        # `object_name` and could collide with another agent/connector pair
        # there. A remote connector's name never reaches `object_name` at all
        # -- `render()` emits zero objects for it, and `mcp_entry()` returns the
        # authored `url` verbatim -- so there is no object name for it to make
        # ambiguous. Refusing it would reject a previously valid bundle for a
        # collision it cannot cause (#1446). A hosted connector that also sets
        # `unhosted_url` still has `image` set (`is_hosted` True) and stays
        # guarded, correctly: it renders real objects on the cluster tier.
        #
        # This is a deliberate ASYMMETRY with `deploy.ambiguous_agent_name`,
        # which stays unconditional. An agent name is durable platform identity
        # that outlives any one bundle version (ADR-0089 -- a typo there mints a
        # new agent), it feeds `object_name` for every hosted connector the
        # agent will EVER declare, and `deploy.yaml` validation does not read
        # `connectors.yaml`, so it cannot know whether a hosted connector even
        # exists yet. Refusing the agent name up front is the conservative
        # choice; refusing a remote connector name here is not.
        if spec.is_hosted and _forges_the_render_join(name):
            errors.append(
                (
                    "connectors.ambiguous_name",
                    f"{where}: `{name}` would forge a second `-mcp-` in the object name "
                    "Curie derives for this connector (`<release>-<agent>-mcp-"
                    "<connector>`), so it is no longer recoverable where the agent ends "
                    "and the connector begins -- a DIFFERENT agent/connector pair would "
                    "render the same Service, Deployment, both NetworkPolicies and the "
                    "same `app.kubernetes.io/name` pod selector, handing one agent's "
                    "sandbox the other's connector and the credential bound to it "
                    "(the connector is deliberately unauthenticated, ADR-0086) -- rename "
                    "the connector so it does not start with `mcp-` or contain `-mcp-`",
                )
            )
        forms = [bool(spec.image), bool(spec.build), bool(spec.url)]
        if sum(forms) > 1:
            errors.append(
                (
                    "connectors.ambiguous",
                    f"{where}: set exactly one of `image` (Curie runs it), `build` (Curie "
                    "builds it, then runs it) or `url` (already running) -- otherwise it "
                    "is unclear who owns the process, and which image source wins is "
                    "decided by whichever reader looks first",
                )
            )
        if not any(forms):
            errors.append(
                (
                    "connectors.underspecified",
                    f"{where}: set `image` for Curie to run it, `build` for Curie to build "
                    "it from source in this bundle, or `url` to point at something already "
                    "running",
                )
            )
        if spec.build is not None:
            _validate_build(where, spec.build, errors)
        if spec.url and (spec.args or spec.env):
            errors.append(
                (
                    "connectors.remote_has_runtime",
                    f"{where}: `args`/`env` configure a process Curie starts; a `url` "
                    "connector is already running, so they would be silently ignored",
                )
            )
        if spec.url and spec.unhosted_url:
            errors.append(
                (
                    "connectors.remote_has_unhosted_url",
                    f"{where}: `unhosted_url` is the hosted form's fallback for tiers that "
                    "cannot run the image. A `url` connector is already reachable everywhere, "
                    "so this would never apply",
                )
            )
        if spec.url and spec.secret_files:
            errors.append(
                (
                    "connectors.remote_has_secret_files",
                    f"{where}: `secret_files` project a file into a container Curie "
                    "starts; a `url` connector is already running elsewhere, so the "
                    "file would go nowhere",
                )
            )
        seen_paths: dict[str, str] = {}
        for sname, path in spec.secret_files.items():
            if not path.startswith("/"):
                errors.append(
                    (
                        "connectors.secret_file_relative_path",
                        f"{where}: secret_files[{sname}] is {path!r}; the mount path must "
                        "be absolute, because it is a container path and not relative to "
                        "anything the bundle controls",
                    )
                )
            # /tmp is a server's scratch space. A credential does not belong
            # there, and an emptyDir mounted at /tmp would shadow the file.
            if path == "/tmp" or path.startswith("/tmp/"):
                errors.append(
                    (
                        "connectors.secret_file_in_tmp",
                        f"{where}: secret_files[{sname}] mounts under /tmp, which is "
                        "scratch space -- pick a dedicated path such as /secrets/...",
                    )
                )
            if path in seen_paths:
                errors.append(
                    (
                        "connectors.secret_file_path_collision",
                        f"{where}: secret_files[{sname}] and secret_files"
                        f"[{seen_paths[path]}] both mount at {path}; one would silently "
                        "win and the other credential would never appear",
                    )
                )
            seen_paths[path] = sname
        overlap = set(spec.secret_files) & {
            s if isinstance(s, str) else s.name for s in spec.secrets
        }
        for name in sorted(overlap):
            errors.append(
                (
                    "connectors.secret_both_env_and_file",
                    f"{where}: {name} is declared in both `secrets` and `secret_files`. "
                    "Pick one delivery -- a credential in an env var AND on disk doubles "
                    "the places it can leak from for no benefit",
                )
            )
        if spec.is_hosted and spec.headers:
            errors.append(
                (
                    "connectors.hosted_has_headers",
                    f"{where}: `headers` apply to a remote endpoint; configure a hosted "
                    "connector with `env` and `args` instead",
                )
            )
        for declared in spec.secrets:
            if isinstance(declared, str):
                continue
            if not declared.from_secret.strip():
                errors.append(
                    (
                        "connectors.empty_secret_ref",
                        f"{where}: `{declared.name}` sets an empty `from_secret`. An empty "
                        "reference renders a secretKeyRef at a Secret named '', which the "
                        "API server rejects at apply -- long after the deploy looked fine.",
                    )
                )
        for env_name, blob in spec.sealed_secrets.items():
            if not env_name.strip():
                errors.append(
                    (
                        "connectors.empty_sealed_name",
                        f"{where}: a `sealed_secrets` entry has an empty env var name.",
                    )
                )
            if not blob.strip():
                errors.append(
                    (
                        "connectors.empty_sealed_value",
                        f"{where}: `{env_name}` has an empty sealed value. An empty blob "
                        "cannot decrypt to anything, so the connector would start without "
                        "the credential it needs.",
                    )
                )
        if spec.sealed_secrets:
            errors.append(
                (
                    "connectors.sealed_secrets_unsupported",
                    f"{where}: `sealed_secrets` is declared but nothing decrypts it yet. "
                    "Refusing rather than deploying without its credential. "
                    "Use `secrets:` until then.",
                )
            )
        # A connector-declared secret NAME is held to exactly the policy a
        # manifest `secrets` name gets (`validate._validate_secrets`): the same
        # env-var shape and the same reserved fence, from the same module, so
        # the two declaration sites cannot drift apart. Without this the fence
        # was bypassable by moving the name from `plugin.json` to here: a bundle
        # declaring `secrets: [ANTHROPIC_API_KEY]` has the operator's own model
        # credential resolved from their environment or vault and handed to the
        # bundle's own container, and at the skill tier that happens before this
        # validator ever runs.
        declared_names = [s if isinstance(s, str) else s.name for s in spec.secrets]
        declared_names += list(spec.secret_files)
        for declared_name in declared_names:
            if not SECRET_NAME_RE.match(declared_name):
                errors.append(
                    (
                        "connectors.secret_name_invalid",
                        f"{where}: secret name {declared_name!r} must be an env-var-style "
                        "name (uppercase letters, digits, underscore; not starting with a "
                        "digit) -- it is delivered as an environment variable, so a name "
                        "outside that shape can never bind",
                    )
                )
            elif is_reserved_boot_env_name(declared_name):
                errors.append(
                    (
                        "connectors.secret_name_reserved",
                        f"{where}: secret name {declared_name!r} is reserved: it is a "
                        "platform boot-env, model-credential, or redirect/capture-capable "
                        "key and cannot be used for a connector secret",
                    )
                )
        if spec.is_hosted:
            secret_env_names = [s if isinstance(s, str) else s.name for s in spec.secrets]
            if spec.bearer_secret:
                if spec.bearer_secret not in secret_env_names:
                    errors.append(
                        (
                            "connectors.bearer_secret_unknown",
                            f"{where}: bearer_secret {spec.bearer_secret!r} is not in "
                            "`secrets`. The derived Authorization header names an env "
                            "var the connector never declared, so the placeholder "
                            "would expand empty.",
                        )
                    )
            elif len(secret_env_names) > 1:
                errors.append(
                    (
                        "connectors.bearer_secret_required",
                        f"{where}: a hosted connector with more than one secret must "
                        "set `bearer_secret` to the name the Authorization header "
                        "expands. Picking secrets[0] is positional and binds every "
                        "declared name into the sandbox.",
                    )
                )
        seen_secret_names: set[str] = set()
        for secret_name in spec.secret_names():
            if secret_name in seen_secret_names:
                errors.append(
                    (
                        "connectors.duplicate_secret",
                        f"{where}: `{secret_name}` is declared twice. Two env entries with "
                        "one name means the container silently gets whichever the renderer "
                        "emitted last -- possibly the wrong Secret entirely.",
                    )
                )
            seen_secret_names.add(secret_name)
        if not (1 <= spec.port <= 65535):
            errors.append(("connectors.bad_port", f"{where}: port {spec.port} is out of range"))
        for text in [*spec.args, *spec.env.values()]:
            for found in _PLACEHOLDER_RE.findall(text):
                if found in _known_placeholders():
                    continue
                errors.append(
                    (
                        "connectors.unknown_placeholder",
                        f"{where}: `${{{found}}}` is not a Curie placeholder. A typo here is "
                        "not caught at runtime -- the literal text reaches the container, so "
                        "the connector starts and then rejects every call. Known: "
                        + ", ".join(f"${{{p}}}" for p in sorted(_known_placeholders())),
                    )
                )
        for key in spec.env:
            if not key.replace("_", "").isalnum() or key[:1].isdigit():
                errors.append(
                    ("connectors.bad_env_name", f"{where}: `{key}` is not a valid env var name")
                )

    return (parsed if not errors else None), errors
