from collections.abc import Callable
from typing import Any, Literal

from ..runner_resources import RunnerResourcesError, validate_runner_resources

# Shared by provider_installations and channel_identities (#2909): a
# provider installation's and a channel identity's `provider` column both
# draw from this vocabulary, and the admin routes' list filters on it too.
ProviderName = Literal[
    "slack", "m365", "github", "jira", "linear", "confluence", "quickbooks", "other"
]


def nullable_override_validator(field: str, examples: str) -> Callable[[str | None], str | None]:
    """Build the gate shared by every nullable operator override (#1310, #1355, #1392).

    Refuses a blank and normalizes what it accepts: the returned value is
    stripped, so one paste stores one value no matter which surface it came
    through.

    `model` and `thinking` are the same kind of field consumed the same way, and
    they must answer an empty string the same way. `apply_model_env` reads each as
    `override if override is not None else config.<field>`, so `""` is not None and
    wins the ternary, and then `if value:` is falsy and NO boot key is emitted.
    An empty override therefore skips the platform default an operator configured
    and hands the decision to the model's own built-in -- the opposite of what
    someone typing `""` to clear a value expects. Explicit JSON null is the reset,
    and the error says so.

    Whitespace is worse than empty and is refused by the same predicate: `"  "`
    passes the falsy check downstream and is stored AND forwarded as a garbage
    value.

    #1334 attached this to `thinking` only, and its twin on the adjacent line kept
    accepting what its sibling refused. One factory, two call sites, so the next
    nullable override cannot drift either.

    None passes through: on PATCH that is the clear, on create it is "no override".
    The VOCABULARY of each field is deliberately not checked here -- a model id
    belongs to whatever harness is configured, and thinking's
    `disabled`/`adaptive`/`enabled:<n>` belongs to the runner
    (`curie_runner.thinking`) -- so swapping the harness is not a schema change.

    Args:
        field: the field name, used to open the error message.
        examples: a trailing clause naming valid values for this field.

    Returns:
        A pydantic field validator that refuses empty and whitespace-only values
        and returns every other value stripped.
    """

    def _validate(value: str | None) -> str | None:
        if value is None:
            return value
        if not value.strip():
            raise ValueError(
                f"{field} must not be empty: an empty value skips the platform "
                f"default and selects the model's own behavior, which is not what "
                f"clearing means. Send null to clear the override back to the "
                f"platform default, or {examples}."
            )
        # Normalize here, not in each client (#1392). Leading or trailing
        # whitespace is never meaningful in either override, and storing it
        # verbatim is a bug that surfaces far from its cause: a padded model id
        # is forwarded as CURIE_MODEL and rejected by the provider at the
        # agent's NEXT turn, long after the command that stored it exited 0.
        #
        # This is the API's job because the API is the gate every client passes
        # through -- the same reasoning `validate_channel_binding` states for
        # itself ("the authoritative gate for every caller (UI, API, CLI)"). The
        # console already trimmed and the CLI did not, so the same paste stored
        # two different values depending on which surface an operator used;
        # fixing only the CLI would have left curl and every future client on
        # the old behavior.
        return value.strip()

    return _validate


def validate_optional_commit_sha(value: str | None) -> str | None:
    if value is None:
        return None
    if not value.strip():
        raise ValueError("commit_sha must not be empty; omit it when unavailable")
    return value.strip()


validate_thinking_override = nullable_override_validator(
    "thinking", "a value like 'disabled', 'adaptive' or 'enabled:2000'"
)
validate_model_override = nullable_override_validator(
    "model", "a model id like 'claude-sonnet-5' or 'kimi-k2'"
)


def validate_runner_resource_value(value: Any) -> Any:
    try:
        return validate_runner_resources(value)
    except RunnerResourcesError as exc:
        raise ValueError(str(exc)) from exc
