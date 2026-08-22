"""Declared connectors: what a bundle needs running, not how to run it (ADR-0086).

``.mcp.json`` says an MCP server exists and where to reach it. It has never said
who *runs* it, so every bundle author writing a non-stdio connector had to
hand-author a Deployment, a Service, a Secret reference, container hardening,
and a NetworkPolicy -- roughly 180 lines of Kubernetes to stand up one server,
per bundle, each copy independently right or wrong.

``connectors.yaml`` closes that. A bundle declares intent; the platform derives
the objects. Two forms:

    connectors:
      grafana:                                  # hosted: Curie runs the image
        image: grafana/mcp-grafana:0.17.2
        args: [-t, streamable-http, -disable-write]
        env: {GRAFANA_URL: "https://grafana.example.com"}
        secrets: [GRAFANA_SERVICE_ACCOUNT_TOKEN]

      internal:                                 # remote: already running
        url: https://mcp.internal/mcp
        secrets: [INTERNAL_TOKEN]

Secrets are NAMES only. Values arrive at deploy time through the existing
mechanism (ADR-0009), so this introduces no new way to carry a credential and
a bundle stays safe to commit.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

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


class ConnectorSpec(BaseModel):
    """One declared connector. Exactly one of ``image`` or ``url``.

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
        return self.image is not None


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
    # PROTOTYPE (Draft ADR-0115, not accepted -- docs/demo/ADR-0115-PROTOTYPE-NOTES.md).
    "curie-delegate": "the agent-delegation tools",
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


def _is_valid_name(name: str) -> bool:
    """RFC 1123 label, capped: the name becomes a k8s object and a DNS label."""

    return len(name) <= _NAME_MAX and bool(_NAME_RE.fullmatch(name))


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
        if spec.image and spec.url:
            errors.append(
                (
                    "connectors.ambiguous",
                    f"{where}: set either `image` (Curie runs it) or `url` (already "
                    "running), not both -- otherwise it is unclear who owns the process",
                )
            )
        if not spec.image and not spec.url:
            errors.append(
                (
                    "connectors.underspecified",
                    f"{where}: set `image` for Curie to run it, or `url` to point at "
                    "something already running",
                )
            )
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
        if spec.image and spec.headers:
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
