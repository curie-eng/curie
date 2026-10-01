"""Falsify the real Helm assertion scripts with disposable chart mutations."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

CHART = Path(__file__).resolve().parents[1]
HOST_RULE = (
    '- host: {{ required "api.ingress.host is required when the ingress is enabled" '
    ".Values.api.ingress.host | quote }}"
)


class ApiIdentityAssertions(unittest.TestCase):
    def run_mutation(self, template: str, before: str, after: str, script: str) -> None:
        with tempfile.TemporaryDirectory(prefix="curie-chart-identity-") as directory:
            chart = Path(directory) / "curie"
            shutil.copytree(CHART, chart)
            target = chart / "templates" / template
            source = target.read_text()
            self.assertIn(before, source, "mutation no longer reaches its template")
            target.write_text(source.replace(before, after, 1))
            result = subprocess.run(
                ["bash", str(chart / "ci" / script)], capture_output=True, text=True, check=False
            )
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("FAIL", result.stdout + result.stderr)

    def test_ingress_identity_and_routing_mutations_are_refused(self) -> None:
        mutations = [
            ("api.yaml", 'name: {{ include "curie.fullname" . }}-api', "name: wrong-api"),
            (
                "api-ingress.yaml",
                '                name: {{ include "curie.fullname" . }}-api',
                "                name: curie-api",
            ),
            (
                "api-ingress.yaml",
                HOST_RULE,
                "- host: wrong.example.com",
            ),
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
            (
                "api-ingress.yaml",
                HOST_RULE,
                "- host: {{ .Values.api.ingress.host | quote }}",
            ),
        ]
        for template, before, after in mutations:
            with self.subTest(template=template, mutation=after):
                self.run_mutation(template, before, after, "api-ingress-assertions.sh")

    def test_credential_identity_and_key_mutations_are_refused(self) -> None:
        mutations = [
            ("name: GITHUB_APP_PRIVATE_KEY", "name: WRONG_APP_PRIVATE_KEY"),
            (
                'name: {{ include "curie.secretName" . }}\n'
                "                  key: githubAppPrivateKey",
                "name: wrong-secret\n                  key: githubAppPrivateKey",
            ),
            ("key: githubAppPrivateKey", "key: githubAppPrivateKeyTypo"),
            ("key: {{ .Values.api.githubAppExistingSecretKey | quote }}", "key: privateKey"),
            ("name: {{ .Values.api.githubAppExistingSecret | quote }}", "name: wrong-secret"),
        ]
        for before, after in mutations:
            with self.subTest(mutation=after):
                self.run_mutation("api.yaml", before, after, "github-app-credential-assertions.sh")

    def test_empty_tls_values_render_without_a_nil_pointer(self) -> None:
        for value in ["{}", "null"]:
            with self.subTest(tls=value), tempfile.TemporaryDirectory() as directory:
                values = Path(directory) / "values.yaml"
                values.write_text(
                    "api:\n  ingress:\n    enabled: true\n    host: api.example.com\n"
                    f"    tls: {value}\n"
                )
                result = subprocess.run(
                    ["helm", "template", "identity-test", str(CHART), "-f", str(values)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("nil pointer", result.stderr)


if __name__ == "__main__":
    unittest.main()
