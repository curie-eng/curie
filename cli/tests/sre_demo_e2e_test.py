"""Executing regressions for the nightly driver's outcome checks, with external fixtures."""

import asyncio
import importlib.util
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "sre-demo-e2e.sh"


class DriverOutcomes(unittest.TestCase):
    def run_function(self, body, payload=None):
        with tempfile.TemporaryDirectory() as directory:
            fixture = pathlib.Path(directory) / "external.json"
            fixture.write_text(json.dumps(payload or {}))
            env = {
                "PATH": os.environ["PATH"],
                "HOME": directory,
                "CURIE_CREDENTIALS": "test-placeholder",
                "CI_SLACK_APP_TOKEN": "test-placeholder",
                "CI_SLACK_BOT_TOKEN": "test-placeholder",
                "CI_SLACK_USER_TOKEN": "test-placeholder",
                "CI_SLACK_CHANNEL_ID": "C0EXAMPLE1",
                "CI_THROWAY_REPO": "acme-corp/acme-bot",
                "FIXTURE": str(fixture),
                "SCRIPT": str(SCRIPT),
            }
            return subprocess.run(
                ["bash", "-c", 'source "$SCRIPT" prereqs >/dev/null\n' + body],
                env=env,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

    def reply(self, messages):
        return self.run_function(
            'BOT_ID=U0EXAMPLE2\nslack_api() { cat "$FIXTURE"; }\n'
            "sleep() { :; }\nwait_thread_reply 100.000001 1",
            {"ok": True, "messages": messages},
        )

    def test_placeholder_is_not_a_reply(self):
        result = self.reply(
            [
                {"ts": "100.000001", "user": "U0EXAMPLE1", "text": "request"},
                {
                    "ts": "100.000002",
                    "thread_ts": "100.000001",
                    "user": "U0EXAMPLE2",
                    "text": "On it. Working on your request.",
                },
            ]
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_another_author_is_not_the_target_reply(self):
        result = self.reply(
            [
                {"ts": "100.000001", "user": "U0EXAMPLE1", "text": "request"},
                {
                    "ts": "100.000002",
                    "thread_ts": "100.000001",
                    "user": "U0EXAMPLE3",
                    "text": "A complete looking response",
                },
            ]
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_substantive_target_reply_is_returned_without_root_echo(self):
        result = self.reply(
            [
                {"ts": "100.000001", "user": "U0EXAMPLE1", "text": "private-root-sentinel"},
                {
                    "ts": "100.000002",
                    "thread_ts": "100.000001",
                    "user": "U0EXAMPLE2",
                    "text": "Verified namespace list",
                },
            ]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Verified namespace list", result.stdout)
        self.assertNotIn("private-root-sentinel", result.stdout)

    def test_desired_replicas_without_ready_pods_fail(self):
        result = self.run_function(
            'spec_replicas_of() { echo 2; }\nkubectl() { cat "$FIXTURE"; }\n'
            "sleep() { :; }\nwait_replicas demo app 2 1",
            {
                "metadata": {"generation": 3},
                "spec": {"replicas": 2},
                "status": {
                    "observedGeneration": 2,
                    "readyReplicas": 0,
                    "availableReplicas": 0,
                    "updatedReplicas": 0,
                    "replicas": 1,
                },
            },
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_ready_observed_generation_passes(self):
        result = self.run_function(
            'spec_replicas_of() { echo 2; }\nkubectl() { cat "$FIXTURE"; }\n'
            "wait_replicas demo app 2 1",
            {
                "metadata": {"generation": 3},
                "spec": {"replicas": 2},
                "status": {
                    "observedGeneration": 3,
                    "readyReplicas": 2,
                    "availableReplicas": 2,
                    "updatedReplicas": 2,
                    "replicas": 2,
                },
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_connector_requires_the_exact_pin_and_unique_deployment(self):
        image = (
            "ghcr.io/containers/kubernetes-mcp-server@sha256:"
            "6d650f4bd6ac303ad82713c997e73a2d001602f9bf17392c9b9a0e30e29c6423"
        )

        def deployment(name, tag):
            return {
                "metadata": {"name": name},
                "spec": {"template": {"spec": {"containers": [{"image": tag}]}}},
            }

        for images, expected in [
            ([deployment("ours", image)], 0),
            ([deployment("wrong", image.split("@")[0] + ":latest")], 1),
            ([deployment("ours", image), deployment("other", image)], 1),
        ]:
            with self.subTest(images=images):
                result = self.run_function(
                    'kubectl() { cat "$FIXTURE"; }\nconnector_deployment', {"items": images}
                )
                self.assertEqual(result.returncode, expected, result.stderr)

    def test_retained_row_evidence_contains_only_sanitized_status(self):
        result = self.run_function(
            'evidence_dir="$HOME"\nOBSERVATION_FAILURES=0\n'
            'CURIE_SRE_DEMO_RESULTS_FILE="$HOME/outcomes.jsonl"\n'
            "bad() { echo private-diagnostic-sentinel; return 1; }\n"
            'run_assertion read bad\ncat "$CURIE_SRE_DEMO_RESULTS_FILE"'
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {"row": "read", "status": "FAILED"})
        self.assertNotIn("private-diagnostic-sentinel", result.stdout + result.stderr)

    def progressive_reply(self, later, body):
        messages = [
            {
                "ok": True,
                "messages": [
                    {
                        "ts": "100.000004",
                        "thread_ts": "100.000001",
                        "user": "U0EXAMPLE2",
                        "text": "Working on that now",
                    }
                ],
            },
            {
                "ok": True,
                "messages": [
                    {
                        "ts": "100.000004",
                        "thread_ts": "100.000001",
                        "user": "U0EXAMPLE2",
                        "text": later,
                    }
                ],
            },
        ]
        return self.run_function(
            "BOT_ID=U0EXAMPLE2\nslack_api() {\n"
            ' python3 - "$FIXTURE" "$HOME/cursor" <<\'PYREPLY\'\n'
            "import json,pathlib,sys\np=pathlib.Path(sys.argv[2])\n"
            "i=int(p.read_text()) if p.exists() else 0\np.write_text(str(i+1))\n"
            "print(json.dumps(json.load(open(sys.argv[1]))[min(i,1)]))\nPYREPLY\n}\n"
            "sleep() { :; }\n" + body,
            messages,
        )

    def test_namespace_observation_waits_past_preamble(self):
        result = self.progressive_reply(
            "kube-system sre-e2e-example",
            'EXPECTED_NAMESPACES=\'{"items":[{"metadata":{"name":"sre-e2e-example"}}]}\' '
            "wait_thread_reply 100.000001 1 100.000003 namespaces",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("sre-e2e-example", result.stdout)

    def test_coding_inspects_off_repo_link_after_preamble(self):
        result = self.progressive_reply(
            "https://github.com/acme-corp/another-bot/pull/17",
            'mention_bot() { echo \'{"ts":"100.000003"}\'; }\n'
            "READ_THREAD_TS=100.000001\nassert_coding_handoff",
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("outside the authorized repository", result.stderr)

    def test_operator_resolution_is_not_a_slack_click(self):
        result = self.run_function("curie_bin() { echo /bin/true; }\napprove example-id")
        self.assertNotEqual(result.returncode, 0)

    def test_repo_name_echo_cannot_prove_a_coding_pr(self):
        result = self.run_function(
            'mention_bot() { echo \'{"ts":"100.000001"}\'; }\n'
            'wait_thread_reply() { cat "$FIXTURE"; }\n'
            'gh() { echo "[]"; }\nREAD_THREAD_TS=100.000001\nassert_coding_handoff',
            {
                "messages": [
                    {
                        "ts": "100.000001",
                        "user": "U0EXAMPLE1",
                        "text": "Open a PR in acme-corp/acme-bot",
                    }
                ]
            },
        )
        self.assertNotEqual(result.returncode, 0, result.stderr)

    def test_previous_turn_reply_cannot_satisfy_later_instruction(self):
        result = self.run_function(
            'BOT_ID=U0EXAMPLE2\nslack_api() { cat "$FIXTURE"; }\n'
            "sleep() { :; }\nwait_thread_reply 100.000001 1 100.000003",
            {
                "ok": True,
                "messages": [
                    {
                        "ts": "100.000002",
                        "thread_ts": "100.000001",
                        "user": "U0EXAMPLE2",
                        "text": "A substantive answer to the earlier instruction",
                    }
                ],
            },
        )
        self.assertNotEqual(result.returncode, 0)

    def test_prior_message_edit_does_not_prove_later_instruction_delivery(self):
        result = self.run_function(
            'BOT_ID=U0EXAMPLE2\nslack_api() { cat "$FIXTURE"; }\n'
            "sleep() { :; }\nwait_thread_reply 100.000001 1 100.000003",
            {
                "ok": True,
                "messages": [
                    {
                        "ts": "100.000002",
                        "thread_ts": "100.000001",
                        "user": "U0EXAMPLE2",
                        "edited": {"ts": "100.000004", "user": "U0EXAMPLE2"},
                        "text": "Revised answer after the later instruction",
                    }
                ],
            },
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_pending_is_scoped_to_exact_conversation(self):
        result = self.run_function(
            'list_pending() { cat "$FIXTURE"; }\nthread_pending 100.000001',
            {
                "truncated": False,
                "pending": [
                    {"id": "ours", "conversation_id": "100.000001", "status": "pending"},
                    {"id": "other", "conversation_id": "100.000009", "status": "pending"},
                ],
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([r["id"] for r in json.loads(result.stdout)], ["ours"])

    def test_pending_scale_uses_the_actual_sdk_tool_identity(self):
        result = self.run_function(
            'thread_pending() { cat "$FIXTURE"; }\nwait_scale_pending 100.000001',
            [{"id": "ours", "granted_tool": "mcp__kubernetes__resources_scale"}],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "ours")

    def test_other_pending_tools_cannot_satisfy_scale(self):
        result = self.run_function(
            'thread_pending() { cat "$FIXTURE"; }\nwait_scale_pending 100.000001',
            [{"id": "ours", "granted_tool": "other/resources_scale"}],
        )
        self.assertNotEqual(result.returncode, 0)

    def test_incomplete_pending_list_cannot_prove_absence(self):
        result = self.run_function(
            'list_pending() { cat "$FIXTURE"; }\nthread_pending 100.000001',
            {"truncated": True, "pending": []},
        )
        self.assertNotEqual(result.returncode, 0)

    def test_failed_and_blocked_rows_continue_without_fallthrough_success(self):
        result = self.run_function(
            'evidence_dir="$HOME"\nOBSERVATION_FAILURES=0\n'
            'bad() { false; echo forbidden-fallthrough >"$HOME/fallthrough"; }\n'
            'blocked() { return 3; }\ngood() { echo observed >"$HOME/continued"; }\n'
            "run_assertion read bad\nrun_assertion scale blocked\n"
            "run_assertion configuration-denial good\n"
            '[[ "$OBSERVATION_FAILURES" == 2 && -f "$HOME/continued" '
            '&& ! -e "$HOME/fallthrough" ]]',
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("read: FAILED", result.stderr)
        self.assertIn("scale: BLOCKED", result.stderr)
        self.assertIn("configuration-denial: PASS", result.stderr)

    def test_slack_token_is_not_a_process_argument(self):
        result = self.run_function(
            'python3() { [[ "$*" != *test-private-token* '
            '&& "$CURIE_SRE_SLACK_TOKEN" == test-private-token ]]; }\n'
            "slack_api test-private-token auth.test",
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_platform_repository_is_refused_before_live_work(self):
        for repo in ["curie-eng/curie", "Curie-Eng/AgentOS", "invalid-repo-shape"]:
            with self.subTest(repo=repo):
                result = self.run_function(f"CI_THROWAY_REPO={repo} phase_prereqs")
                self.assertNotEqual(result.returncode, 0, result.stderr)

    def pr(self, **overrides):
        payload = {
            "url": "https://github.com/acme-corp/acme-bot/pull/17",
            "state": "OPEN",
            "isCrossRepository": False,
            "createdAt": "2026-09-05T01:00:01Z",
            "files": [{"path": "README.md", "additions": 1, "deletions": 0}],
            "commits": [{"oid": "a" * 40}],
            "headRefOid": "a" * 40,
            "baseRefName": "main",
            "headRefName": "task/example",
            "author": {"login": "acme-bot"},
            "statusCheckRollup": [
                {
                    "__typename": "CheckRun",
                    "name": "tests",
                    "status": "COMPLETED",
                    "conclusion": "SUCCESS",
                }
            ],
        }
        payload.update(overrides)
        return self.run_function(
            'cat "$FIXTURE" | verify_pr_metadata 17 2026-09-05T01:00:00Z',
            payload,
        )

    def test_real_fresh_pr_metadata_with_changes_and_checks_passes(self):
        result = self.pr()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_fresh_fractional_or_offset_timestamp_is_compared_as_an_instant(self):
        for stamp in ["2026-09-05T01:00:00.123Z", "2026-09-05T01:00:00.123+00:00"]:
            with self.subTest(stamp=stamp):
                result = self.pr(createdAt=stamp)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_pending_pr_checks_are_unproved_not_failed(self):
        result = self.pr(
            statusCheckRollup=[
                {
                    "__typename": "CheckRun",
                    "status": "IN_PROGRESS",
                    "conclusion": None,
                }
            ]
        )
        self.assertEqual(result.returncode, 3, result.stderr)

    def test_unavailable_gh_verifier_blocks_only_coding_row(self):
        result = self.run_function(
            'mention_bot() { echo \'{"ts":"100.000003"}\'; }\n'
            'wait_thread_reply() { echo "https://github.com/acme-corp/acme-bot/pull/17"; }\n'
            "gh() { return 77; }\nREAD_THREAD_TS=100.000001\nassert_coding_handoff",
        )
        self.assertEqual(result.returncode, 3, result.stderr)

    def test_stale_empty_wrong_repo_and_unchecked_prs_fail(self):
        for override in [
            {"createdAt": "2026-09-04T01:00:00Z"},
            {"files": []},
            {"commits": []},
            {"url": "https://github.com/acme-corp/another-bot/pull/17"},
            {"statusCheckRollup": []},
            {
                "statusCheckRollup": [
                    {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "FAILURE"}
                ]
            },
        ]:
            with self.subTest(override=override):
                self.assertNotEqual(self.pr(**override).returncode, 0)

    def test_only_one_authorized_pr_url_is_accepted(self):
        result = self.run_function(
            'echo "https://github.com/acme-corp/acme-bot/pull/17" | pr_number_from_reply',
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "17")
        result = self.run_function(
            'echo "https://github.com/acme-corp/acme-bot/pull/17 '
            'https://github.com/acme-corp/another-bot/pull/18" | pr_number_from_reply',
        )
        self.assertNotEqual(result.returncode, 0)


class MCPOutcomes(unittest.TestCase):
    def probe(self, *, names=None, read_error=False, forbidden=None, cursor=None):
        # Real installed MCP result/error types; only the remote connector is
        # mocked. Error semantics: MCP specification 2025-11-25/server/tools.
        from mcp.types import CallToolResult, ListToolsResult, TextContent, Tool

        spec = importlib.util.spec_from_file_location(
            "sre_demo_mcp_probe",
            SCRIPT.with_name("sre-demo-mcp-probe.py"),
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if forbidden is None:
            forbidden = CallToolResult(
                isError=True,
                content=[TextContent(type="text", text="unknown tool configuration_view")],
            )
        session = SimpleNamespace(
            list_tools=AsyncMock(
                return_value=ListToolsResult(
                    tools=[
                        Tool(name=name, inputSchema={"type": "object"})
                        for name in (names if names is not None else ["namespaces_list"])
                    ],
                    nextCursor=cursor,
                )
            ),
            call_tool=AsyncMock(
                side_effect=[
                    CallToolResult(
                        isError=read_error,
                        content=[TextContent(type="text", text="default sre-e2e-example")],
                    ),
                    forbidden,
                ]
            ),
        )
        result = asyncio.run(module.probe_session(session, "sre-e2e-example"))
        self.assertEqual(
            [c.args[0] for c in session.call_tool.call_args_list],
            ["namespaces_list", "configuration_view"],
        )
        return result

    def test_catalog_read_and_explicit_forbidden_invocation_pass(self):
        self.assertEqual(self.probe()["forbidden_invocation"], "pass")

    def test_unknown_tool_protocol_error_is_a_refusal(self):
        from mcp.shared.exceptions import MCPError

        self.assertEqual(
            self.probe(forbidden=MCPError(-32602, "unknown tool: configuration_view"))["catalog"],
            "pass",
        )

    def test_missing_kubeconfig_is_not_an_unknown_tool_refusal(self):
        from mcp.types import CallToolResult, TextContent

        with self.assertRaises(AssertionError):
            self.probe(
                forbidden=CallToolResult(
                    isError=True,
                    content=[
                        TextContent(
                            type="text", text="configuration_view failed: kubeconfig not found"
                        ),
                    ],
                )
            )

    def test_catalog_read_failures_and_transport_errors_do_not_pass(self):
        from mcp.shared.exceptions import MCPError

        for kwargs in [
            {"names": []},
            {"names": ["namespaces_list", "configuration_view"]},
            {"cursor": "another-page"},
            {"read_error": True},
            {"forbidden": MCPError(-32603, "internal server error")},
            {"forbidden": ConnectionError("unavailable")},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises((AssertionError, ConnectionError)):
                self.probe(**kwargs)

    def test_successful_secret_response_is_never_an_accepted_denial(self):
        from mcp.types import CallToolResult, TextContent

        with self.assertRaises(AssertionError) as caught:
            self.probe(
                forbidden=CallToolResult(
                    isError=False,
                    content=[TextContent(type="text", text="private-kubeconfig-sentinel")],
                )
            )
        self.assertNotIn("private-kubeconfig-sentinel", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
