"""Shared bundle persistence used by the upload endpoint (B2) and git flow (J1).

Validation and storage are split so a caller can reject an invalid bundle before
creating any database rows, then store the validated bytes under the immutable
per-version key.
"""

import hashlib
import tempfile
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import plugin_format
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from curie_api.crud import versions as crud_versions
from curie_api.schemas.versions import BundleOut

from . import bundles
from .config import Settings, get_settings
from .models import AgentVersion
from .storage import ObjectStore


class BundleInvalid(Exception):
    """A bundle failed plugin-format validation; carries the actionable errors."""

    def __init__(self, errors: list[dict[str, str]]) -> None:
        super().__init__("bundle failed validation")
        self.errors = errors


class BundleTooLarge(Exception):
    """An already-stored bundle fails the CURRENT size/ratio caps.

    Raised by ``revalidate_stored_bundle``, and by the route checker below when
    it extracts those same stored bytes (#2436) -- the backward-compatibility
    case ADR-0059 decision 3 commits to: a bundle stored before these caps existed
    (or under looser ones) must be rejected here, at deploy time, with an
    actionable message, rather than surfacing later as an opaque
    init-container failure or a mid-extract eviction on the node.

    ``code`` is the git-flow half of the same commitment the message below is:
    it lets ``gitflow._rejected`` build the rejection envelope out of the
    exception alone, so the code an operator greps for and the sentence they
    read can never come from two different places.
    """

    code = "bundle.too_large"

    @classmethod
    def for_stored_bundle(cls, version_id: uuid.UUID, exc: Exception) -> "BundleTooLarge":
        """The one wording for "these stored bytes no longer fit the caps".

        Several bounds checks can be the first to reach that condition on a
        given delivery -- ``revalidate_stored_bundle``, git flow's reuse-path
        ``deploy.yaml`` read, and (since #2436) the route checker's own extract
        -- and ADR-0059 decision 3's commitment is an operator-facing MESSAGE,
        not just a code. Building it here keeps the sentence identical whichever
        one fires, the same way ``ApprovalRoutesUnbound`` builds its two
        envelopes from one place.
        """

        return cls(
            f"stored bundle for version {version_id} fails the current bundle "
            f"size/ratio limits and must be rebuilt and re-uploaded: {exc}"
        )


class SealingKeyCustody(Exception):
    """A stored bundle declares a reserved sealing key in a form other than a SecretRef.

    @spec ACTION-EXECUTOR-16. Intake refuses these declarations, but a version
    stored before it did can still be deployed, promoted or rolled back to,
    and at the cluster tier the deploy path would then resolve a plain name
    into the per-agent connector Secret. Raised by
    ``check_stored_sealing_key_custody``; the message is the intake refusal's
    own custody reason, and ``code`` is the intake issue's code, so a git push
    rejection and a 422 detail say the same thing.
    """

    code = bundles.SEALING_KEY_CODE


class ApprovalRoutesUnbound(Exception):
    """A version's bundle declares an approval route the agent never bound (#2436).

    The configuration-time half of a join nothing cross-checked before: routes
    DECLARED in the bundle manifest (``approvalPolicy.gates[].route``, versioned
    with the agent) against routes BOUND by the operator
    (``agents.approval_routes``, per agent, mutable). ADR-0050's rationale
    already asserts the residual widening it leaves open is bounded because "the
    bundle can only name a route the operator has itself bound to a channel";
    ADR-0046 refuses an unbound one at REQUEST time, by escalating, which is too
    late to stop a bad deploy. This exception is where that claim becomes true.

    PRESENCE of a key in ``approval_routes`` is the join. The binding's CONTENTS
    are deliberately not checked: ``ApprovalRouteBinding`` already requires a
    resolution target for anything written through the API, so a malformed entry
    can only arrive by a direct JSONB write, and validity-checking here would put
    a third copy of the worker's authority envelope in the codebase for the
    copies to drift. "Bound to junk" stays the worker's request-time fail-closed
    backstop (``curie_worker.kernel``'s ``_parse_approval_targets``, ADR-0046),
    which this change must not weaken; this gate closes only "declared but never
    bound".

    Carries both route lists, and builds both messages, so the two envelopes --
    an API 422 detail and the git push ``approval_routes.unbound``
    ``WebhookResult`` error -- render one wording from one place, the same way
    ``BundleTooLarge`` does for its two. The ``code`` that git-push envelope
    carries lives here too, so ``gitflow._rejected`` needs nothing but the
    exception.
    """

    code = "approval_routes.unbound"

    def __init__(self, message: str, *, unbound: Iterable[str], bound: Iterable[str]) -> None:
        super().__init__(message)
        self.unbound = sorted(unbound)
        self.bound = sorted(bound)

    @classmethod
    def for_routes(
        cls, version_id: uuid.UUID, *, unbound: Iterable[str], bound: Iterable[str]
    ) -> "ApprovalRoutesUnbound":
        """The ordinary refusal: these declared routes have no binding."""

        return cls(
            f"the bundle for version {version_id} declares approval route(s)"
            f" {_quoted(unbound)} with no entry in this agent's approval_routes;"
            f" {_bound_clause(bound)}. Bind every declared route on this agent"
            " before deploying this version.",
            unbound=unbound,
            bound=bound,
        )

    @classmethod
    def for_unreadable_policy(
        cls, version_id: uuid.UUID, *, bound: Iterable[str]
    ) -> "ApprovalRoutesUnbound":
        """The fail-closed refusal: the declared set itself cannot be read.

        Distinct wording because the operator's fix is a different one: the
        bundle is at fault, not the bindings. Accepting instead would be the
        fail-open shape ADR-0050 exists to prevent, and it is why the reader
        returns a poison value rather than an empty set.
        """

        return cls(
            f"the bundle for version {version_id} declares an approvalPolicy whose routes"
            " cannot be read, so they cannot be checked against this agent's"
            f" approval_routes; {_bound_clause(bound)}. Rebuild the bundle so every"
            " declared gate names a tool and a route, then deploy it again.",
            unbound=(),
            bound=bound,
        )


def _quoted(names: Iterable[str]) -> str:
    """Route names, sorted and quoted with ``!r``.

    ``repr`` rather than bare text so a padded or oddly cased key is VISIBLE in
    the error: both runtime consumers of this map (``kernel.py``'s
    ``(approval_routes or {}).get(route_name)`` and
    ``crud.approvals.get_approval_route_binding``) do an exact dict lookup, so `" ops "`
    binds nothing and the operator has to be able to see why.
    """

    return ", ".join(repr(name) for name in sorted(names))


def _bound_clause(bound: Iterable[str]) -> str:
    """Name the BOUND routes alongside the unbound ones.

    The same courtesy the runner's in-turn tool error already gives the model
    (``resolve_policy_route``: "pass route as one of: ..."), so an operator can
    see the typo without a second request.
    """

    names = sorted(bound)
    return f"bound routes are {_quoted(names)}" if names else "this agent binds no approval routes"


def validate_archive(
    data: bytes, settings: Settings | None = None
) -> tuple[str, str]:
    """Validate an archive's bytes. Returns (extension, content_type).

    Raises ``bundles.UnsupportedArchive`` if the bytes are not a zip/tar(.gz),
    ``BundleInvalid`` if the plugin bundle fails validation, and (via
    ``safe_extract``) ``bundles.UnsupportedArchive`` again if the archive
    exceeds the configured uncompressed-size or compression-ratio cap.
    """

    settings = settings or get_settings()
    with tempfile.TemporaryDirectory() as tmp:
        extension, content_type, result = bundles.extract_and_validate(
            data,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
    if not result.valid:
        raise BundleInvalid([e.model_dump() for e in result.errors])
    return extension, content_type


async def revalidate_stored_bundle(
    store: ObjectStore, version: AgentVersion, settings: Settings | None = None
) -> None:
    """Re-check an already-stored bundle against the CURRENT size/ratio caps.

    A no-op when the version carries no bundle yet. Otherwise fetches the
    immutable bytes and reruns the same pre-scan ``safe_extract`` applies
    (unsafe entries, uncompressed-size and compression-ratio caps) via
    ``plugin_format.check_archive_bounds``, which extracts nothing -- cheap
    enough to run on every deploy/promote. Called before a version becomes
    deployable (``crud.deployments.create_deployment_row``'s callers), so a legacy bundle
    that predates these caps, or was stored under looser ones, fails here with
    a clear ``BundleTooLarge`` instead of only surfacing once some sandbox
    substrate tries to fetch and extract it.
    """

    if version.bundle_ref is None:
        return
    settings = settings or get_settings()
    data = await store.get(version.bundle_ref)
    try:
        await run_in_threadpool(
            plugin_format.check_archive_bounds,
            data,
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
    except plugin_format.UnsupportedArchive as exc:
        raise BundleTooLarge.for_stored_bundle(version.id, exc) from exc


def _stored_custody_reasons(data: bytes, settings: Settings) -> list[str]:
    """Blocking half of ``check_stored_sealing_key_custody``; run in a threadpool."""

    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp)
        bundles.extract_stored_bundle(
            data,
            dest,
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        issues = bundles.sealing_key_custody_issues(plugin_format.bundle_root(dest))
        return [issue.message for issue in issues]


async def check_stored_sealing_key_custody(
    store: ObjectStore, version: AgentVersion, settings: Settings | None = None
) -> None:
    """Refuse a stored bundle that intake would now refuse for sealing key custody.

    @spec ACTION-EXECUTOR-16: custody holds on every path that makes a version
    the one that boots, not only on first upload. A no-op when the version
    carries no bundle yet, like ``revalidate_stored_bundle``. Stored bytes the
    current caps refuse report ``BundleTooLarge``, the settled answer for them
    (ADR-0059 decision 3).
    """

    if version.bundle_ref is None:
        return
    settings = settings or get_settings()
    data = await store.get(version.bundle_ref)
    try:
        reasons = await run_in_threadpool(_stored_custody_reasons, data, settings)
    except plugin_format.UnsupportedArchive as exc:
        raise BundleTooLarge.for_stored_bundle(version.id, exc) from exc
    if reasons:
        raise SealingKeyCustody("; ".join(reasons))


def _declared_routes(data: bytes, settings: Settings) -> set[str] | None:
    """Blocking half of ``check_routes_from_bytes``; run in a threadpool."""

    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp)
        bundles.extract_stored_bundle(
            data,
            dest,
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        return bundles.declared_approval_routes(dest)


async def check_routes_from_bytes(
    data: bytes, routes: Any, version_id: uuid.UUID, settings: Settings | None = None
) -> None:
    """Refuse an archive's own bytes when they declare a route ``routes`` lacks.

    For the callers that already hold the bytes -- the bundle upload, where the
    object is not stored yet, and git flow's fresh-clone attach -- so neither
    pays a store round trip. Extraction is bounded by the operator-configured
    ``Settings`` caps, never ``plugin_format``'s library defaults (#815), and
    runs off the event loop, following ``gitflow._read_stored_targets``.

    Reading and deciding are ONE call so the reader's ``None`` poison value
    never leaves this module. Handed out, it would ask every caller to remember
    that an empty-looking answer means "refuse", and a caller that forgets is
    exactly the fail-open shape ADR-0050 exists to prevent.

    Raises ``bundles.UnsupportedArchive`` when a cap refuses the bytes. That is
    left untranslated here because both byte-holding callers cleared
    ``validate_archive`` under these same caps a moment earlier, so only the
    stored-bytes wrapper below -- where the caps CAN have moved under the object
    -- maps it onto ``BundleTooLarge``.
    """

    settings = settings or get_settings()
    declared = await run_in_threadpool(_declared_routes, data, settings)
    _raise_if_routes_unbound(declared, routes, version_id)


def _raise_if_routes_unbound(
    declared: set[str] | None, routes: Any, version_id: uuid.UUID
) -> None:
    """The pure decision: refuse unless every declared route has a binding.

    Comparison is VERBATIM and case-sensitive, because the two lookups this
    protects are exact dict lookups (see ``_quoted``). A ``declared`` of ``None``
    is the reader's poison value and refuses. A ``routes`` that is not a dict
    (a direct JSONB write) counts as no bindings, mirroring
    ``crud.approvals.get_approval_route_binding``'s ``isinstance(..., dict)`` guard rather
    than raising. A bound route no bundle declares is never an error: the join is
    one directional, and pre-binding ahead of a bundle bump is supported (AC2).
    """

    bound: set[str] = set(routes) if isinstance(routes, dict) else set()
    if declared is None:
        raise ApprovalRoutesUnbound.for_unreadable_policy(version_id, bound=bound)
    unbound = declared - bound
    if unbound:
        raise ApprovalRoutesUnbound.for_routes(version_id, unbound=unbound, bound=bound)


async def check_approval_route_bindings(
    store: ObjectStore,
    version: AgentVersion,
    routes: Any,
    settings: Settings | None = None,
) -> None:
    """Refuse a version whose stored bundle declares a route ``routes`` lacks.

    The store-fetching wrapper, for the callers that hold a version rather than
    bytes. A no-op when the version carries no bundle yet, exactly like
    ``revalidate_stored_bundle`` -- and that no-op is precisely why the bundle
    upload and git flow's attach need their own conditional gate: "deploy
    bundleless, then attach" would otherwise walk straight past this into the
    worker's active-deployment join (``curie_worker.binding``).

    ``routes`` is the map to judge: the agent's own ``approval_routes`` for the
    callers asking about the stored state, or the PROPOSED map the
    ``PATCH /agents/{id}`` preflight is about to write -- where an explicit
    ``{}`` is a real proposal and is judged, not skipped. Two exceptions come out
    of here -- ``ApprovalRoutesUnbound``, and ``BundleTooLarge`` for stored bytes
    the current caps refuse; a store failure propagates exactly as it already
    does from ``revalidate_stored_bundle``.
    """

    if version.bundle_ref is None:
        return
    settings = settings or get_settings()
    data = await store.get(version.bundle_ref)
    try:
        await check_routes_from_bytes(data, routes, version.id, settings)
    except plugin_format.UnsupportedArchive as exc:
        # Reading what the bundle DECLARES means extracting it, and this
        # wrapper's bytes are STORED bytes -- so an operator who tightened the
        # caps under an already-live bundle would otherwise get an uncaught
        # extraction error out of a gate that has nothing to say about routes
        # (#2436). Stored bytes that no longer fit have one settled answer
        # (ADR-0059 decision 3): `bundle.too_large`, naming the version to
        # rebuild. Translating here rather than at each call site keeps
        # that answer in one place, and keeps it off the byte-holding callers,
        # whose archive cleared `validate_archive` under these same caps moments
        # earlier. Exact, not a catch-all: `safe_extract` also raises this for
        # unsafe entries, but those were refused before the object could be
        # stored and the key is immutable, so a cap violation is all that is
        # left -- the reasoning git flow's reuse path already records.
        raise BundleTooLarge.for_stored_bundle(version.id, exc) from exc


async def store_bundle(
    store: ObjectStore,
    session: AsyncSession,
    agent_id: uuid.UUID,
    version: AgentVersion,
    data: bytes,
    extension: str,
    content_type: str,
) -> BundleOut:
    """Store validated bytes under the immutable key and record them."""

    key = f"bundles/{agent_id}/{version.id}{extension}"
    digest = hashlib.sha256(data).hexdigest()
    await store.put(key, data, content_type)
    await crud_versions.attach_bundle(session, version, key, digest)
    return BundleOut(
        version_id=version.id,
        bundle_ref=key,
        bundle_sha256=digest,
        size_bytes=len(data),
    )
