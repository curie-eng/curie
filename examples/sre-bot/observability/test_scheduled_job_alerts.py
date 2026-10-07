"""Contract tests for the opt-in scheduled job Prometheus alerts.

Run with ``python3 examples/sre-bot/observability/test_scheduled_job_alerts.py``.
The evaluator cases use the generated rule groups and Prometheus 3.5.0's
promtool in a disposable container; they do not contact a Kubernetes cluster.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

# @spec SRE-SCHEDULED-JOBS c1 c2 c3 c4 c5
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CLI = REPOSITORY_ROOT / "examples/sre-bot/observability/scheduled_job_alerts.py"
NAMESPACE = "acme-system"
DESCRIPTION = "Maintenance credentials and inventory depend on these jobs."
PROMETHEUS_IMAGE = "prom/prometheus:v3.5.0"


class ScheduledJobAlertsCLITests(unittest.TestCase):
    # @spec SRE-SCHEDULED-JOBS c3 c4 c5
    def test_cli_emits_opt_in_values_with_scoped_labeled_alert_rules(self) -> None:
        result = self._run_cli("--namespace", NAMESPACE, "--description", DESCRIPTION)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")

        values = json.loads(result.stdout)
        self.assertEqual(set(values), {"serverFiles"})
        server_files = values["serverFiles"]
        self.assertIn("alerts", server_files)
        self.assertNotIn("alerting_rules.yml", server_files)
        groups = server_files["alerts"]["groups"]
        rules = [rule for group in groups for rule in group["rules"]]
        self.assertEqual(len(rules), 2)

        for rule in rules:
            labels = rule["labels"]
            self.assertEqual(labels["severity"], "page")
            self.assertEqual(labels["component"], "scheduled-job")
            self.assertIn(NAMESPACE, rule["expr"])
            self.assertEqual(rule["annotations"]["description"], DESCRIPTION)
            self.assertIn("{{ $labels.cronjob }}", rule["annotations"]["summary"])

        rule_text = json.dumps(groups)
        for expected_label in ("namespace", "cronjob"):
            self.assertIn(expected_label, rule_text)

    # @spec SRE-SCHEDULED-JOBS c3 c4
    def test_cli_rejects_invalid_namespace_before_writing_values(self) -> None:
        result = self._run_cli("--namespace", "Bad_Namespace", "--description", DESCRIPTION)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("error:", result.stderr.lower())

    # @spec SRE-SCHEDULED-JOBS c3 c4
    def test_cli_requires_nonempty_impact_description_before_writing_values(self) -> None:
        result = self._run_cli("--namespace", NAMESPACE, "--description", " ")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("error:", result.stderr.lower())

    # @spec SRE-SCHEDULED-JOBS c4
    def _run_cli(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(CLI), *arguments],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )


class ScheduledJobAlertsPromtoolTests(unittest.TestCase):
    # @spec SRE-SCHEDULED-JOBS c1 c2 c3 c4 c5
    def test_generated_alert_rules_match_schedule_and_suspension_cases(self) -> None:
        cli = subprocess.run(
            ["python3", str(CLI), "--namespace", NAMESPACE, "--description", DESCRIPTION],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(cli.returncode, 0, cli.stderr)
        generated_values = json.loads(cli.stdout)
        groups = generated_values["serverFiles"]["alerts"]["groups"]

        rules = [rule for group in groups for rule in group["rules"]]
        suspension_alert = next(
            rule["alert"] for rule in rules if "kube_cronjob_spec_suspend" in rule["expr"]
        )
        unsuccessful_alert = next(
            rule["alert"]
            for rule in rules
            if "kube_cronjob_status_last_schedule_time" in rule["expr"]
        )
        cases = self._promtool_cases(suspension_alert, unsuccessful_alert)
        with tempfile.TemporaryDirectory(prefix="scheduled-job-alerts-") as temp_dir:
            fixture_dir = Path(temp_dir)
            # The evaluator image runs as nobody on native Linux bind mounts.
            fixture_dir.chmod(0o755)
            rules_path = fixture_dir / "scheduled_job_alerts.rules.json"
            cases_path = fixture_dir / "scheduled_job_alerts.test.json"
            rules_path.write_text(json.dumps({"groups": groups}), encoding="utf-8")
            cases_path.write_text(
                json.dumps(
                    {
                        "rule_files": ["/work/scheduled_job_alerts.rules.json"],
                        "evaluation_interval": "1m",
                        "tests": cases,
                    }
                ),
                encoding="utf-8",
            )

            # The image is pinned by the specification. --rm keeps this local
            # evaluator run disposable, and the only mounted data is generated
            # in this test's temporary directory.
            result = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--entrypoint",
                    "/bin/promtool",
                    "--volume",
                    f"{fixture_dir}:/work:ro",
                    PROMETHEUS_IMAGE,
                    "test",
                    "rules",
                    "/work/scheduled_job_alerts.test.json",
                ],
                cwd=REPOSITORY_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    # @spec SRE-SCHEDULED-JOBS c1 c2 c3
    def _promtool_cases(
        self, suspension_alert: str, unsuccessful_alert: str
    ) -> list[dict[str, object]]:
        suspended = self._series("kube_cronjob_spec_suspend", value="1+0x20")
        nonsuspended = self._series("kube_cronjob_spec_suspend", value="0+0x20")
        failed_with_previous_success = [
            nonsuspended,
            self._series("kube_cronjob_status_last_schedule_time", value="200+0x20"),
            self._series("kube_cronjob_status_last_successful_time", value="100+0x20"),
        ]
        failed_without_previous_success = [
            nonsuspended,
            self._series("kube_cronjob_status_last_schedule_time", value="200+0x20"),
        ]
        successful_schedule = [
            nonsuspended,
            self._series("kube_cronjob_status_last_schedule_time", value="200+0x20"),
            self._series("kube_cronjob_status_last_successful_time", value="200+0x20"),
        ]

        return [
            {
                "name": "suspension is pending before its one minute hold",
                "interval": "1m",
                "input_series": [suspended],
                "alert_rule_test": [
                    {
                        "eval_time": "0m",
                        "alertname": suspension_alert,
                        "exp_alerts": [],
                    }
                ],
            },
            {
                "name": "suspension pages after its one minute hold",
                "interval": "1m",
                "input_series": [suspended],
                "alert_rule_test": [
                    {
                        "eval_time": "1m",
                        "alertname": suspension_alert,
                        "exp_alerts": [
                            self._expected_alert("Scheduled job credential-rotation is suspended")
                        ],
                    }
                ],
            },
            {
                "name": "nonsuspended job stays quiet",
                "interval": "1m",
                "input_series": [nonsuspended],
                "alert_rule_test": [
                    {
                        "eval_time": "5m",
                        "alertname": suspension_alert,
                        "exp_alerts": [],
                    }
                ],
            },
            {
                "name": "failed latest schedule pages after fifteen minutes",
                "interval": "1m",
                "input_series": failed_with_previous_success,
                "alert_rule_test": [
                    {
                        "eval_time": "15m",
                        "alertname": unsuccessful_alert,
                        "exp_alerts": [
                            self._expected_alert(
                                "Scheduled job credential-rotation's last run did not succeed"
                            )
                        ],
                    }
                ],
            },
            {
                "name": "unsuccessful schedule stays pending before fifteen minutes",
                "interval": "1m",
                "input_series": failed_with_previous_success,
                "alert_rule_test": [
                    {
                        "eval_time": "14m",
                        "alertname": unsuccessful_alert,
                        "exp_alerts": [],
                    }
                ],
            },
            {
                "name": "schedule without a successful timestamp pages",
                "interval": "1m",
                "input_series": failed_without_previous_success,
                "alert_rule_test": [
                    {
                        "eval_time": "15m",
                        "alertname": unsuccessful_alert,
                        "exp_alerts": [
                            self._expected_alert(
                                "Scheduled job credential-rotation's last run did not succeed"
                            )
                        ],
                    }
                ],
            },
            {
                "name": "latest successful schedule stays quiet",
                "interval": "1m",
                "input_series": successful_schedule,
                "alert_rule_test": [
                    {
                        "eval_time": "20m",
                        "alertname": unsuccessful_alert,
                        "exp_alerts": [],
                    }
                ],
            },
            {
                "name": "unscheduled job stays quiet",
                "interval": "1m",
                "input_series": [nonsuspended],
                "alert_rule_test": [
                    {
                        "eval_time": "20m",
                        "alertname": unsuccessful_alert,
                        "exp_alerts": [],
                    }
                ],
            },
            {
                "name": "matching job in another namespace stays quiet",
                "interval": "1m",
                "input_series": [
                    self._series(
                        "kube_cronjob_spec_suspend",
                        namespace="other-system",
                        value="1+0x20",
                    ),
                    self._series(
                        "kube_cronjob_status_last_schedule_time",
                        namespace="other-system",
                        value="200+0x20",
                    ),
                    self._series(
                        "kube_cronjob_status_last_successful_time",
                        namespace="other-system",
                        value="0+0x20",
                    ),
                ],
                "alert_rule_test": [
                    {
                        "eval_time": "20m",
                        "alertname": suspension_alert,
                        "exp_alerts": [],
                    },
                    {
                        "eval_time": "20m",
                        "alertname": unsuccessful_alert,
                        "exp_alerts": [],
                    },
                ],
            },
        ]

    # @spec SRE-SCHEDULED-JOBS c1 c2 c3
    def _series(
        self,
        metric: str,
        *,
        namespace: str = NAMESPACE,
        value: str,
    ) -> dict[str, str]:
        return {
            "series": (f'{metric}{{namespace="{namespace}",cronjob="credential-rotation"}}'),
            "values": value,
        }

    # @spec SRE-SCHEDULED-JOBS c3
    def _expected_alert(self, summary: str) -> dict[str, object]:
        return {
            "exp_labels": {
                "namespace": NAMESPACE,
                "cronjob": "credential-rotation",
                "severity": "page",
                "component": "scheduled-job",
            },
            "exp_annotations": {"summary": summary, "description": DESCRIPTION},
        }


if __name__ == "__main__":
    unittest.main(verbosity=2)
