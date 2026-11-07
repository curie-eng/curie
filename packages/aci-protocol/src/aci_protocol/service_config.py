"""Shared worker/dispatcher settings plumbing (#496).

The worker and dispatcher are separate services that nonetheless read the SAME
handful of platform env vars (the API base URL + key, the runs stream, the
heartbeat file + interval). Those names used to be hand-mirrored as string
literals in each service's pydantic config, so a rename could drift one out of
sync with the other. They are declared ONCE here and imported by both.

``SHIMMER_ENV`` is the exception and stays declared here on purpose: #1312 moved
the whole shimmer to the worker, so only one service reads it today. The
constant is kept because the name is an operator-facing contract that outlived
the split, and because a second reader is exactly what this module exists to keep
honest if the caption ever grows one.

This module also owns the one-release deprecation of the platform-API-base-URL
env name: the canonical name is ``CURIE_API_URL`` (the name the CLI and the
platform API already use), and the services' historical ``CURIE_API_BASE_URL``
is accepted as a deprecated alias that logs a warning naming the replacement.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import Any

from pydantic import AliasChoices
from pydantic.fields import FieldInfo
from pydantic_settings.sources import EnvSettingsSource

logger = logging.getLogger(__name__)

# The env names the worker and dispatcher share, declared once (#496).
API_URL_ENV = "CURIE_API_URL"
# Deprecated twin of API_URL_ENV, accepted for one release with a warning.
API_URL_ENV_DEPRECATED = "CURIE_API_BASE_URL"
API_KEY_ENV = "CURIE_API_KEY"
STREAM_ENV = "CURIE_STREAM"
DEAD_LETTER_STREAM_ENV = "CURIE_DEAD_LETTER_STREAM"
HEARTBEAT_FILE_ENV = "CURIE_HEARTBEAT_FILE"
HEARTBEAT_INTERVAL_ENV = "CURIE_HEARTBEAT_INTERVAL_SECONDS"
SHIMMER_ENV = "CURIE_SHIMMER"

# The transport literals the services hand-mirrored, declared once (#492). Same
# rationale as the env names above: a rename used to drift one lane out of sync
# with another. These are plain string constants, NOT Pydantic models -- they are
# not in the exported JSON Schema and not in the wire fingerprint, so they cannot
# force a protocol bump. The frozen wire contract stays transport-agnostic.
#
# RUNS_STREAM_DEFAULT and WORKER_GROUP_DEFAULT are DEFAULTS: each service binds
# them through its pydantic config field, so the env-override path (#496's
# STREAM_ENV / CURIE_CONSUMER_GROUP) is preserved. STREAM_PAYLOAD_FIELD is the
# stream field holding a payload model's JSON.
RUNS_STREAM_DEFAULT = "curie:runs"
WORKER_GROUP_DEFAULT = "curie-workers"
STREAM_PAYLOAD_FIELD = "payload"

# The eval lane's twins of the two above (#626, the deliberate deferral from
# #492). The eval stream and its own consumer group were still hand-mirrored per
# lane (the API producer, the worker consumer), the same drift risk #492 closed
# for the runs stream. Same treatment: one declaration here, env-overridable
# through each lane's pydantic field (CURIE_EVAL_STREAM /
# CURIE_EVAL_CONSUMER_GROUP). Not wire models either, so no protocol bump.
EVAL_STREAM_DEFAULT = "curie:evals"
EVAL_CONSUMER_GROUP_DEFAULT = "curie-eval-workers"


def derive_dead_letter_stream_name(stream: str, override: str) -> str:
    """The ONE dead-letter graveyard derivation both lanes must share (#668).

    The worker's delivery cap (ADR-0039, #505) moves a permanently-failing entry
    to this graveyard, and the API's dead-letter watcher (#531) plus the
    ResumeQueue backstop (#532) READ it. Both sides derive the name from the same
    two operator inputs -- the base runs stream (``CURIE_STREAM``) and the
    explicit override (``CURIE_DEAD_LETTER_STREAM``) -- so the writer and the
    readers can never point at different streams. That drift was issue #668: the
    API hardcoded ``<runs_stream>:dead`` and ignored the override, so any operator
    override silently blinded the watcher while the worker dead-lettered elsewhere.

    An empty ``override`` derives ``<stream>:dead`` (the default graveyard); a
    non-empty ``override`` is the graveyard name verbatim. This is a plain
    function, NOT a wire/Pydantic model: like the transport-literal constants
    above it is absent from the JSON Schema and the wire fingerprint, so it forces
    no PROTOCOL_VERSION bump. Each lane binds ``stream``/``override`` through its
    own pydantic config field (env-overridable), then calls this to resolve.
    """
    return override or f"{stream}:dead"


def api_url_validation_alias() -> AliasChoices:
    """The ``validation_alias`` for the platform API base URL field: the canonical
    ``CURIE_API_URL`` first, then the deprecated ``CURIE_API_BASE_URL``.

    Listing the canonical name first means it wins when both are set. The
    deprecated name still resolves (so a chart/compose/env that predates the
    rename keeps working for one release); pair this with
    :func:`warn_if_deprecated_api_url_env` to surface the deprecation.
    """
    return AliasChoices(API_URL_ENV, API_URL_ENV_DEPRECATED)


def warn_if_deprecated_api_url_env(env: Mapping[str, str] | None = None) -> None:
    """Log a deprecation warning when only the OLD API-base-URL env name is set.

    Fires exactly when ``CURIE_API_BASE_URL`` is present and the canonical
    ``CURIE_API_URL`` is not, so a config that already moved to the new name is
    silent and one still on the old name is told what to switch to. Uses the
    module logger (not ``warnings.warn``, which is filtered by default) so the
    message actually reaches an operator's service logs (#496).
    """
    environ = os.environ if env is None else env
    if API_URL_ENV_DEPRECATED in environ and API_URL_ENV not in environ:
        logger.warning(
            "%s is deprecated and will be removed in a future release; set %s instead.",
            API_URL_ENV_DEPRECATED,
            API_URL_ENV,
        )


class AliasOnlyEnvSource(EnvSettingsSource):
    """Env source that reads an aliased field ONLY from its ``validation_alias``.

    ``populate_by_name=True`` is set so tests can construct a config with
    field-name kwargs (``WorkerConfig(fake_model=True)``). But in
    pydantic-settings that same flag makes the default env source append the
    bare uppercased field name as a fallback env key for every aliased field --
    so ``api_key`` (alias ``CURIE_API_KEY``) would also silently read a stray
    ``API_KEY``. That breaks the behavior-preserving contract of the refactor.
    We drop the field-name fallback for aliased fields; non-aliased fields keep
    reading their plain uppercased name, and kwarg population is untouched
    (it runs through the init source, not here).

    A field whose ``validation_alias`` is an ``AliasChoices`` (e.g. the canonical
    + deprecated API-URL pair) still has both alias names extracted by the parent;
    only the field-name fallback is filtered, so both names keep resolving.
    """

    def _extract_field_info(
        self,
        field: FieldInfo,
        field_name: str,
        model_config: Mapping[str, Any] | None = None,
    ) -> list[tuple[str, str, bool]]:
        infos = super()._extract_field_info(field, field_name, model_config)
        if field.validation_alias is not None:
            infos = [info for info in infos if info[0] != field_name]
        return infos
