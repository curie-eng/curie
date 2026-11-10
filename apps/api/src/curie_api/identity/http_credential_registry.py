"""Pure credential binding. @spec HTTP-CREDENTIAL-REGISTRY-V1-1 to HTTP-CREDENTIAL-REGISTRY-V1-6."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal, Never, Self, SupportsIndex

from curie_api.identity.http_transport import LookupTarget, ProviderBearer
from curie_api.schemas.http_authority import AuthorityConfigV1, AuthorityModel, HttpSelectionV1
from curie_api.schemas.http_identity import FreshnessLimits

_Key = tuple[str, int]
_Entry = tuple[str, str, str, ProviderBearer]


class CredentialRegistryRefusal(AuthorityModel):
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""

    code: Literal["invalid_provider_credential_registry", "provider_credential_unavailable"]


_INVALID = CredentialRegistryRefusal(code="invalid_provider_credential_registry")
_UNAVAILABLE = CredentialRegistryRefusal(code="provider_credential_unavailable")


class _Opaque:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""

    __slots__ = ()

    def __new__(cls, *args: object, **kwargs: object) -> Self:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise TypeError("provider_credential_handle_not_public") from None

    def __setattr__(self, name: str, value: object) -> None:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise AttributeError("immutable_provider_credential_handle") from None

    def __delattr__(self, name: str) -> None:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise AttributeError("immutable_provider_credential_handle") from None

    def __getstate__(self) -> Never:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise TypeError("provider_credential_handle_not_serializable") from None

    def __reduce__(self) -> Never:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise TypeError("provider_credential_handle_not_serializable") from None

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise TypeError("provider_credential_handle_not_serializable") from None


class ResolvedProviderCredential(_Opaque):
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-3, HTTP-CREDENTIAL-REGISTRY-V1-4."""

    __slots__ = ("_target", "_limits", "_bearer")
    _target: LookupTarget
    _limits: FreshnessLimits
    _bearer: ProviderBearer

    @property
    def target(self) -> LookupTarget:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-3."""
        return self._target

    @property
    def limits(self) -> FreshnessLimits:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-3."""
        return self._limits

    @property
    def bearer(self) -> ProviderBearer:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-3."""
        return self._bearer

    def __repr__(self) -> str:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        return "ResolvedProviderCredential()"

    def __bool__(self) -> bool:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        raise TypeError("provider_credential_is_not_permission")


def _http_configuration(value: object) -> tuple[AuthorityConfigV1, HttpSelectionV1] | None:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1, HTTP-CREDENTIAL-REGISTRY-V1-3."""
    if not isinstance(value, AuthorityConfigV1):
        return None
    config = AuthorityConfigV1.model_validate(value)
    if not isinstance(config.selection, HttpSelectionV1):
        return None
    return config, config.selection


class ProviderCredentialRegistry(_Opaque):
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1 to HTTP-CREDENTIAL-REGISTRY-V1-5."""

    __slots__ = ("_entries",)
    _entries: Mapping[_Key, _Entry]

    def __repr__(self) -> str:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-4."""
        return "ProviderCredentialRegistry()"

    def resolve(
        self, configuration: AuthorityConfigV1,
    ) -> ResolvedProviderCredential | CredentialRegistryRefusal:
        """@spec HTTP-CREDENTIAL-REGISTRY-V1-2 to HTTP-CREDENTIAL-REGISTRY-V1-5."""
        try:
            validated = _http_configuration(configuration)
            if validated is None:
                return _UNAVAILABLE
            config, selection = validated
            reference = selection.credential_ref
            entry = self._entries.get((reference.id, reference.generation))
            if entry is None or entry[:3] != (
                config.tenant_id, selection.issuer, selection.lookup_url,
            ):
                return _UNAVAILABLE
            result = object.__new__(ResolvedProviderCredential)
            object.__setattr__(result, "_target", LookupTarget(
                endpoint=selection.lookup_url, tenant_id=config.tenant_id,
                expected_issuer=selection.issuer, authority_version=config.version,
            ))
            object.__setattr__(result, "_limits", FreshnessLimits(
                max_identity_age_ms=selection.max_identity_age_ms,
                max_membership_age_ms=selection.max_membership_age_ms,
                max_clock_skew_ms=selection.max_clock_skew_ms,
            ))
            object.__setattr__(result, "_bearer", entry[3])
            return result
        except Exception:  # noqa: BLE001 - the boundary exposes only its stable refusal
            return _UNAVAILABLE


def build_provider_credential_registry(
    registrations: tuple[tuple[AuthorityConfigV1, ProviderBearer], ...],
) -> ProviderCredentialRegistry | CredentialRegistryRefusal:
    """@spec HTTP-CREDENTIAL-REGISTRY-V1-1, HTTP-CREDENTIAL-REGISTRY-V1-2.

    @spec HTTP-CREDENTIAL-REGISTRY-V1-5.
    """
    try:
        if type(registrations) is not tuple or len(registrations) > 256:
            return _INVALID
        entries: dict[_Key, _Entry] = {}
        for registration in registrations:
            if type(registration) is not tuple or len(registration) != 2:
                return _INVALID
            config_input, bearer_input = registration
            validated = _http_configuration(config_input)
            if validated is None or not isinstance(bearer_input, ProviderBearer):
                return _INVALID
            config, selection = validated
            reference = selection.credential_ref
            key = (reference.id, reference.generation)
            if key in entries:
                return _INVALID
            entries[key] = (
                config.tenant_id, selection.issuer, selection.lookup_url,
                ProviderBearer(bearer_input.value),
            )
        result = object.__new__(ProviderCredentialRegistry)
        object.__setattr__(result, "_entries", MappingProxyType(entries))
        return result
    except Exception:  # noqa: BLE001 - validation failures never expose initializer material
        return _INVALID
