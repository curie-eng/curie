"""Falsify the real Helm assertion scripts with disposable chart mutations."""

import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_independent

CHART = Path(__file__).resolve().parents[1]
HOST_RULE = (
    '- host: {{ required "api.ingress.host is required when the ingress is enabled" '
    ".Values.api.ingress.host | quote }}"
)


def run_mutation(template: str, before: str, after: str, script: str) -> None:
    with tempfile.TemporaryDirectory(prefix="curie-chart-identity-") as directory:
        chart = Path(directory) / "curie"
        shutil.copytree(CHART, chart)
        target = chart / "templates" / template
        source = target.read_text()
        assert before in source, "mutation no longer reaches its template"
        target.write_text(source.replace(before, after, 1))
        result = subprocess.run(
            ["bash", str(chart / "ci" / script)], capture_output=True, text=True, check=False
        )
        assert result.returncode != 0, result.stdout + result.stderr
        assert "FAIL" in result.stdout + result.stderr


@pytest.mark.parametrize(
    "template,before,after",
    [
        (
            "api.yaml",
            'name: {{ include "curie.checkedObjectName" '
            '(dict "kind" "Service" "name" (printf "%s-api" '
            '(include "curie.fullname" .))) }}',
            "name: wrong-api",
        ),
        (
            "api-ingress.yaml",
            '                name: {{ include "curie.fullname" . }}-api',
            "                name: curie-api",
        ),
        ("api-ingress.yaml", HOST_RULE, "- host: wrong.example.com"),
        ("api-ingress.yaml", "pathType: {{ .Values.api.ingress.pathType }}", "pathType: Exact"),
        (
            "api-ingress.yaml",
            "{{- toYaml . | nindent 4 }}",
            "cert-manager.io/cluster-issuer: wrong-issuer",
        ),
        (
            "api-ingress.yaml",
            "ingressClassName: {{ . | quote }}",
            'ingressClassName: "wrong-class"',
        ),
        ("api-ingress.yaml", "path: {{ .Values.api.ingress.path }}", "path: /wrong"),
        ("api-ingress.yaml", HOST_RULE, "- host: {{ .Values.api.ingress.host | quote }}"),
    ],
)
def test_ingress_identity_and_routing_mutations_are_refused(
    template: str, before: str, after: str
) -> None:
    run_mutation(template, before, after, "api-ingress-assertions.sh")


@pytest.mark.parametrize(
    "before,after",
    [
        ("name: GITHUB_APP_PRIVATE_KEY", "name: WRONG_APP_PRIVATE_KEY"),
        (
            'name: {{ include "curie.secretName" . }}\n                  key: githubAppPrivateKey',
            "name: wrong-secret\n                  key: githubAppPrivateKey",
        ),
        ("key: githubAppPrivateKey", "key: githubAppPrivateKeyTypo"),
        ("key: {{ .Values.api.githubAppExistingSecretKey | quote }}", "key: privateKey"),
        ("name: {{ .Values.api.githubAppExistingSecret | quote }}", "name: wrong-secret"),
    ],
)
def test_credential_identity_and_key_mutations_are_refused(before: str, after: str) -> None:
    run_mutation("api.yaml", before, after, "github-app-credential-assertions.sh")


@pytest.mark.parametrize("value", ["{}", "null"])
def test_empty_tls_values_render_without_a_nil_pointer(value: str) -> None:
    with tempfile.TemporaryDirectory() as directory:
        values = Path(directory) / "values.yaml"
        values.write_text(
            f"api:\n  ingress:\n    enabled: true\n    host: api.example.com\n    tls: {value}\n"
        )
        result = subprocess.run(
            ["helm", "template", "identity-test", str(CHART), "-f", str(values)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "nil pointer" not in result.stderr
