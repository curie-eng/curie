"""Static endpoints have one unambiguous, authenticated transport address."""

import pytest
from curie_internal.connector_endpoints import (
    ConnectorEndpoint,
    ConnectorEndpointBinding,
    StaticConnectorEndpoints,
)


@pytest.mark.parametrize("field", ["base_url", "upload_url"])
@pytest.mark.parametrize(
    "url",
    [
        "https://example.com?",
        "https://example.com#",
        "https://example.com/x?",
        "https://example.com/x#",
        r"https://evil.example.com\.good.example.com",
        r"https://example.com/x\y",
        "https://%65xample.com",
        "https://éxample.com",
        "https://example.com:0",
        "https://example.com:",
        "https://[::1]:",
        "https://[::1]:0",
    ],
)
def test_ambiguous_urls_are_rejected(field: str, url: str) -> None:
    with pytest.raises(ValueError):
        ConnectorEndpoint.model_validate({"base_url": "https://example.com", field: url})


@pytest.mark.parametrize("field", ["base_url", "upload_url"])
def test_cleartext_requires_an_explicit_operator_opt_in(field: str) -> None:
    data = {"base_url": "https://example.com", field: "http://connectors.example.com"}
    with pytest.raises(ValueError, match="insecure_http"):
        ConnectorEndpoint.model_validate(data)
    assert ConnectorEndpoint.model_validate({**data, "insecure_http": True}).insecure_http


@pytest.mark.parametrize("url", ["http://localhost", "http://127.0.0.1", "http://[::1]:8000"])
def test_loopback_can_use_http(url: str) -> None:
    assert ConnectorEndpoint(base_url=url).base_url == url


def test_urls_are_normalized_before_storage() -> None:
    endpoint = ConnectorEndpoint(
        base_url="HTTPS://EXAMPLE.COM:443/root/", upload_url="HTTPS://EXAMPLE.COM/upload/"
    )
    assert endpoint.base_url == "https://example.com:443/root"
    assert endpoint.upload_url == "https://example.com/upload"


@pytest.mark.parametrize("version", [True, 1.0, "1"])
@pytest.mark.parametrize("model", [StaticConnectorEndpoints, ConnectorEndpointBinding])
def test_version_is_an_integer_literal(model: type, version: object) -> None:
    with pytest.raises(ValueError, match="version"):
        model.model_validate(
            {
                "version": version,
                **(
                    {"agents": {}}
                    if model is StaticConnectorEndpoints
                    else {"agent": "acme-dev", "connectors": {}}
                ),
            }
        )


@pytest.mark.parametrize(
    "model,data",
    [
        (ConnectorEndpoint, {"base_url": "https://user:private-test-value@example.com"}),
        (
            ConnectorEndpointBinding,
            {
                "version": 1,
                "agent": "acme-dev",
                "connectors": {
                    "reviewer": {"base_url": "https://user:private-test-value@example.com"}
                },
            },
        ),
        (
            StaticConnectorEndpoints,
            {
                "version": 1,
                "agents": {
                    "acme-dev": {
                        "reviewer": {"base_url": "https://user:private-test-value@example.com"}
                    }
                },
            },
        ),
    ],
)
def test_validation_errors_do_not_echo_url_credentials(model: type, data: dict) -> None:
    with pytest.raises(ValueError) as error:
        model.model_validate(data)
    assert "private-test-value" not in str(error.value)
    assert "https://user" not in str(error.value)
