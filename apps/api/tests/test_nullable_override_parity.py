"""The durable gate for nullable operator overrides on REQUEST bodies (#1389).

Issue #1334 repaired `model`/`thinking` on AgentCreate and AgentUpdate and its
commit claimed "the next nullable override cannot drift". That claim was false
the day it landed: #1349 had added `EvalTriggerRequest.model` about 21 hours
earlier and it went unguarded, so a blank model booted an eval that the matrix
then labelled `''`. The old parity test could not see it, because it hardcoded
the two field names against one endpoint.

So this file does not name any model. It walks the live FastAPI app, collects
every REQUEST-BODY pydantic model declared in `KNOWN_BODY_MODULES` (response
DTOs are out of scope by construction: they are never a request body), and
asserts each nullable `model`/`thinking` override REFUSES "" and "   " while
still accepting null. A new request body carrying such a field is covered the
moment it is routed, with no edit here.

The honest blind spots, so this file does not overclaim. First, the sweep can
only see a body FastAPI models, i.e. a route with a `body_field`. Five mutating
routes declare none and are invisible here -- `/github/webhook` (reads the raw
body for signature verification), `/agents/{agent_id}/kill`,
`/agents/{agent_id}/resume`, `/agents/{agent_id}/threads/{thread_key}/reset` and
`/langfuse/traces/{trace_id}/eval-case`. None carries an override today, and any
override added to one would have to arrive as a real body model to be parsed.
Three narrower ones remain, each a deliberate limit rather than an oversight:

1. A body modelled as a dataclass or TypedDict rather than a pydantic
   `BaseModel`. FastAPI accepts both, but `_referenced_models` returns an empty
   list for them, so such a body is skipped AND contributes nothing to the
   unknown-module alarm. Nothing in this repo declares one today.
2. A THIRD override name. `OVERRIDE_FIELDS` is a hardcoded tuple, and it is the
   one hardcoded thing left here -- but it is the field VOCABULARY, not the
   endpoint or the model, so a new override name (`reasoning_effort`, `harness`)
   must be added to it or it drifts unguarded.
3. A NON-nullable `model: str`. `_is_nullable_string_override` matches
   `str | None` by construction, so a required override is outside this gate
   even though "" would sail through it just the same.

DB-free on purpose: pure route introspection plus pydantic validation, so it
takes neither the `client` nor the `clean_db` fixture and needs no Postgres.
"""

import types
import typing
from functools import cache
from typing import Any, get_args, get_origin

import pytest
from curie_api.main import create_app
from fastapi.routing import APIRoute
from pydantic import BaseModel, ValidationError

# The two nullable operator overrides this repo forwards into the sandbox as
# CURIE_MODEL / CURIE_THINKING. Blank means "skip the platform default and take
# the harness's own behavior", which is never what a caller means.
OVERRIDE_FIELDS = ("model", "thinking")
# Both halves of the refusal `_nullable_override_validator` raises: it names the
# field as empty AND points the caller at null, and both are part of the shared
# contract an operator relies on. Asserting the message TEXT proves that shared
# CONTRACT fired, not the identity of the shared predicate object -- a bespoke
# per-field validator copying both fragments would pass. That is deliberate:
# pinning the predicate object structurally would couple out-of-module wire
# bodies to `curie_api` internals, and they are not allowed to depend on those.
REFUSAL_FRAGMENT = "must not be empty"
REMEDIATION_FRAGMENT = "null"
BLANKS = ("", "   ")
# Every module a routed request body may be declared in. `wirebody.py` binds
# `aci_protocol.wire` models straight onto routes (approvals, eval reports), so
# filtering on `curie_api.schemas` alone would drop real request bodies in
# silence -- and `wire.EvalJob` already carries a `model: str | None`. Discovery
# asserts nothing routed sits outside this set, so the next body declared in a
# new module forces a decision here instead of vanishing from the gate.
# `curie_api.routers.channels` is the third: ADR-0096 phase 2 keeps the channel
# ingress API's request models in its own router file (a deliberate file seam),
# so registering the module here is the decision this alarm asks for -- it WIDENS
# the sweep onto those bodies rather than exempting them.
KNOWN_BODY_MODULES = frozenset(
    {
        "curie_api.schemas",
        "aci_protocol.wire",
        "aci_protocol.turn",
        "curie_api.routers.channels",
        "curie_api.routers.github_reviews",
    }
)
# FastAPI synthesises one wrapper model per multi-param body (`Body_<operation>`)
# in its own module. It is only a container the walk descends THROUGH when every
# member is itself a model; the moment a route declares a SCALAR body param
# (`model: str | None = Body(None)`, the ordinary FastAPI idiom) the wrapper IS
# where the override field lives. So it is swept like any other body rather than
# exempted -- exempting it made the wrapper invisible to both the sweep and the
# unknown-module alarm, which is the widest silent-skip a reflective gate can have.
SYNTHETIC_BODY_MODULE_PREFIX = "fastapi."
# A vacuous reflective sweep is the failure mode that makes this whole file
# worthless, so the discovery has to prove it reached these before it is trusted.
REQUIRED_PAIRS = {
    ("EvalTriggerRequest", "model"),
    ("AgentCreate", "model"),
    ("AgentCreate", "thinking"),
    ("AgentUpdate", "model"),
    ("AgentUpdate", "thinking"),
}


def _api_routes(routes: Any, seen: set[int]) -> list[APIRoute]:
    """Flatten every APIRoute reachable from the app.

    FastAPI does not hand back a flat list: an `include_router` call installs a
    wrapper route that keeps the real router on `original_router`, so a naive
    `for r in app.routes` sees `/health` and nothing else. Follow both `routes`
    and `original_router`; the non-empty assertions below fail loudly if a
    future FastAPI reshapes this again.
    """

    found: list[APIRoute] = []
    for route in routes or []:
        if id(route) in seen:
            continue
        seen.add(id(route))
        if isinstance(route, APIRoute):
            found.append(route)
        found.extend(_api_routes(getattr(route, "routes", None), seen))
        original = getattr(route, "original_router", None)
        if original is not None:
            found.extend(_api_routes(getattr(original, "routes", None), seen))
    return found


def _referenced_models(annotation: Any) -> list[type[BaseModel]]:
    """Every pydantic model mentioned by an annotation, unwrapping Annotated,
    unions and generic containers so `list[Foo] | None` still yields Foo."""

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    origin = get_origin(annotation)
    if origin is typing.Annotated:
        return _referenced_models(get_args(annotation)[0])
    if origin is not None:
        return [m for arg in get_args(annotation) for m in _referenced_models(arg)]
    return []


@cache
def _walk_request_bodies() -> tuple[tuple[type[BaseModel], ...], frozenset[str]]:
    """Walk the app once and return (collected models, unknown body modules).

    Recurses through nested pydantic fields so a body model embedded in another
    body model is covered too, and collects FastAPI's synthetic multi-param
    `Body_*` wrapper as a body in its own right, because a scalar `Body()` param
    puts the override field directly on that wrapper. The second element is every
    module a reachable body model came from that is neither in
    `KNOWN_BODY_MODULES` nor a synthetic wrapper, so a body declared somewhere
    new fails the discovery test rather than being dropped in silence. Cached:
    `create_app()` is not cheap and every test here reaches for the same answer.
    """

    collected: dict[str, type[BaseModel]] = {}
    unknown: set[str] = set()
    walked: set[type[BaseModel]] = set()

    def walk(model: type[BaseModel]) -> None:
        if model in walked:
            return
        walked.add(model)
        if model.__module__ in KNOWN_BODY_MODULES or model.__module__.startswith(
            SYNTHETIC_BODY_MODULE_PREFIX
        ):
            collected[model.__name__] = model
        else:
            unknown.add(model.__module__)
        for field in model.model_fields.values():
            for nested in _referenced_models(field.annotation):
                walk(nested)

    for route in _api_routes(create_app().routes, set()):
        body_field = route.body_field
        if body_field is None:
            continue
        for model in _referenced_models(body_field.field_info.annotation):
            walk(model)
    ordered = sorted(collected.values(), key=lambda m: m.__name__)
    return tuple(ordered), frozenset(unknown)


def _collect_request_models() -> tuple[type[BaseModel], ...]:
    """Every request-body model declared in a `KNOWN_BODY_MODULES` module, plus
    FastAPI's synthetic multi-param wrappers, which carry scalar body fields."""

    return _walk_request_bodies()[0]


def _unwrap_annotated(annotation: Any) -> Any:
    return get_args(annotation)[0] if get_origin(annotation) is typing.Annotated else annotation


def _is_nullable_string_override(annotation: Any) -> bool:
    """True for `str | None`, the shape of every operator override here.

    Every union MEMBER is unwrapped too, not just a top-level `Annotated`:
    `Annotated[str, Field(max_length=64)] | None` is the same nullable override
    wearing a constraint, and that idiom already lives in `schemas.py` on
    `BudgetConfig`. Matching only the bare `str | None` would skip the
    constrained spelling in silence, which is the exact drift this file exists
    to catch.
    """

    annotation = _unwrap_annotated(annotation)
    if get_origin(annotation) not in (typing.Union, types.UnionType):
        return False
    args = {_unwrap_annotated(arg) for arg in get_args(annotation)}
    return str in args and type(None) in args


def _override_pairs() -> list[tuple[type[BaseModel], str]]:
    return [
        (model, name)
        for model in _collect_request_models()
        for name, field in model.model_fields.items()
        if name in OVERRIDE_FIELDS and _is_nullable_string_override(field.annotation)
    ]


def _errors_for(model: type[BaseModel], field: str, value: Any) -> list[Any]:
    """Validate a single field in isolation and return only ITS errors.

    Other required fields raise "missing" in the same exception; pydantic
    collects them all, so filter on `loc` rather than counting.
    """

    try:
        model.model_validate({field: value})
    except ValidationError as exc:
        return [e for e in exc.errors() if e["loc"] == (field,)]
    return []


@pytest.fixture(scope="module")
def override_pairs() -> list[tuple[type[BaseModel], str]]:
    """The discovered (model, field) pairs, proven non-vacuous once.

    A reflective sweep that silently walks an empty set passes forever while the
    defect sits intact, so the pairs every test below consumes are gated here on
    the exact pairs we already know are overrides. Shared so a discovery
    regression reddens in one place rather than surfacing as a different
    assertion in each test.
    """

    pairs = _override_pairs()
    found = {(model.__name__, field) for model, field in pairs}
    assert REQUIRED_PAIRS <= found, f"discovery missed {REQUIRED_PAIRS - found}"
    return pairs


def test_discovery_reaches_the_known_request_bodies(
    override_pairs: list[tuple[type[BaseModel], str]],
) -> None:
    """Prove the walk found real models, that the known pairs are among them
    (the fixture), and that no routed body slipped out of the collected
    modules -- an unknown module means a real request body the gate below never
    sees, so it fails here rather than shrinking the sweep in silence."""

    assert _collect_request_models(), "no request-body models discovered"
    assert override_pairs
    _, unknown_modules = _walk_request_bodies()
    assert not unknown_modules, (
        f"routed request bodies declared outside KNOWN_BODY_MODULES: "
        f"{sorted(unknown_modules)}. Add the module to KNOWN_BODY_MODULES so "
        f"its bodies are swept, or the gate cannot see their overrides."
    )


@pytest.mark.parametrize("blank", BLANKS)
def test_every_request_body_override_refuses_blank(
    blank: str, override_pairs: list[tuple[type[BaseModel], str]]
) -> None:
    """#1389: the gate itself. Empty and whitespace-only are refused on EVERY
    routed request body, by the shared refusal contract, not per-endpoint by
    hand -- including the half that tells the caller null is the reset."""

    assert override_pairs
    for model, field in override_pairs:
        errors = _errors_for(model, field, blank)
        assert errors, f"{model.__name__}.{field} accepted {blank!r}"
        assert any(REFUSAL_FRAGMENT in e["msg"] for e in errors), (
            f"{model.__name__}.{field} refused {blank!r} but not via the shared "
            f"override refusal: {errors}"
        )
        assert any(REMEDIATION_FRAGMENT in e["msg"] for e in errors), (
            f"{model.__name__}.{field} refused {blank!r} without pointing the "
            f"caller at null, which is the reset an operator has to learn: "
            f"{errors}"
        )


def test_every_request_body_override_still_accepts_null(
    override_pairs: list[tuple[type[BaseModel], str]],
) -> None:
    """Clearing an override back to the platform default stays legal: the gate
    must refuse blank WITHOUT taking null down with it."""

    assert override_pairs
    for model, field in override_pairs:
        assert not _errors_for(model, field, None), (
            f"{model.__name__}.{field} rejected null, which is the clear"
        )
