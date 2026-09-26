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

    def parse_turn(self, payload):
        return self.run_function(
            'cat "$FIXTURE" | turn_is_reply',
            payload,
        )

    def test_demo_upgrade_credential_has_separate_denied_identity(self):
        reader = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{"name": "in-cluster", "cluster": {"server": "https://kubernetes.default.svc"}}],
            "users": [{"name": "reader", "user": {"token": "reader-token"}}],
            "contexts": [
                {"name": "reader", "context": {"cluster": "in-cluster", "user": "reader"}}
            ],
            "current-context": "reader",
        }
        result = self.run_function(
            '''
NAMESPACE=curie
kubectl() {
  printf '%s\\n' "$*" >> "$HOME/kubectl-calls"
  case "$*" in
    *'auth can-i'*) echo no ;;
    *'create token'*) echo denied-token ;;
  esac
}
build_denied_upgrade_kubeconfig < "$FIXTURE"
''',
            reader,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads(result.stdout)
        self.assertEqual(
            config["users"],
            [{"name": "sre-demo-upgrade-denied", "user": {"token": "denied-token"}}],
        )
        self.assertEqual(config["current-context"], "sre-demo-upgrade-denied")
        self.assertNotIn("reader-token", result.stdout)

    def test_demo_upgrade_credential_refuses_an_upgrade_grant(self):
        result = self.run_function(
            '''
NAMESPACE=curie
kubectl() {
  case "$*" in
    *'auth can-i'*) echo yes ;;
    *'create token'*) echo denied-token ;;
  esac
}
build_denied_upgrade_kubeconfig < "$FIXTURE"
''',
            {"users": [{"name": "reader", "user": {"token": "reader-token"}}]},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected upgrade grant", result.stderr)

    def test_timeout_json_is_not_a_reply(self):
        result = self.parse_turn({"reply": None, "finalized": False, "timed_out": True})
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_awaiting_approval_json_is_not_a_finished_reply(self):
        result = self.parse_turn(
            {
                "reply": "waiting on approval",
                "thread": "100.000001",
                "finalized": False,
                "awaiting_approval": True,
            }
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_finalized_reply_is_returned(self):
        result = self.parse_turn(
            {
                "reply": "Verified namespace list",
                "thread": "100.000002",
                "finalized": True,
            }
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Verified namespace list", result.stdout)

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

    def test_operator_resolution_uses_cluster_approvals(self):
        result = self.run_function(
            """
curie_bin() { printf '%s' "$HOME/fake-curie"; }
cat > "$HOME/fake-curie" <<'EOF'
#!/bin/sh
printf '%s\\n' "$*" > "$HOME/args"
echo '{"resolved":{"id":"example-id","status":"approved"}}'
EOF
chmod +x "$HOME/fake-curie"
assert_operator_audit() { printf '%s\\n' "$1" > "$HOME/audit"; }
approve example-id
grep -q -- 'cluster' "$HOME/args"
grep -q -- 'approvals' "$HOME/args"
grep -q -- '--resolve example-id' "$HOME/args"
test "$(cat "$HOME/audit")" = example-id
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rearm_receives_the_scale_id_across_row_subshells(self):
        # run_assertion runs each row in a subshell, so a plain variable set by
        # the scale row never reached the re-arm row (#3207).
        result = self.run_function(
            """
evidence_dir="$HOME"
OBSERVATION_FAILURES=0
wait_replicas() { :; }
drive_gated_turn() { case "$3" in approve) printf first-id ;; reject) printf second-id ;; esac; }
run_assertion scale assert_scale
run_assertion rearm assert_rearm
test "$OBSERVATION_FAILURES" = 0
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("rearm: PASS", result.stderr)

    def test_rearm_still_rejects_a_reused_scale_id(self):
        result = self.run_function(
            """
evidence_dir="$HOME"
OBSERVATION_FAILURES=0
wait_replicas() { :; }
drive_gated_turn() { printf same-id; }
run_assertion scale assert_scale
run_assertion rearm assert_rearm
"""
        )
        self.assertIn("rearm: FAILED", result.stderr)

    def test_operator_audit_forwards_the_api_service_http_port(self):
        # The chart serves curie-api on a named http port (8000); a hardcoded
        # 80 made kubectl port-forward exit before every audit check (#3207).
        result = self.run_function(
            """
mkdir -p "$HOME/bin"
cat > "$HOME/bin/kubectl" <<'SHIM'
#!/bin/sh
case "$*" in
  *"get svc"*) printf 8000 ;;
  *port-forward*) printf '%s\\n' "$*" > "$HOME/forward"; exit 1 ;;
esac
SHIM
chmod +x "$HOME/bin/kubectl"
PATH="$HOME/bin:$PATH"
discover_release_secret() { echo curie-secrets; }
assert_operator_audit example-id || true
grep -q -- 'svc/curie-api [0-9]*:8000$' "$HOME/forward"
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_turn_that_exits_without_an_approval_stops_the_wait(self):
        result = self.run_function(
            """
pending_for_tool() { return 1; }
sleep 0 & TURN_PID=$!
wait "$TURN_PID"
SECONDS=0
status=0
wait_pending_tool mcp__kubernetes__resources_scale 60 || status=$?
test "$status" = 4 && test "$SECONDS" -lt 10
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_gated_turn_retries_once_only_when_no_approval_appeared(self):
        result = self.run_function(
            """
evidence_dir="$HOME"
curie_bin() { printf '%s' "$HOME/fake-curie"; }
printf '#!/bin/sh\\necho x >> "$HOME/turns"\\n' > "$HOME/fake-curie"
chmod +x "$HOME/fake-curie"
wait_pending_tool() { sleep 0.2; return 4; }
drive_gated_turn "scale it" mcp__kubernetes__resources_scale approve && exit 9
test "$(wc -l < "$HOME/turns")" -eq 2
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_gated_turn_is_not_retried_after_a_pending_wait_timeout(self):
        result = self.run_function(
            """
evidence_dir="$HOME"
curie_bin() { printf '%s' "$HOME/fake-curie"; }
printf '#!/bin/sh\\necho x >> "$HOME/turns"\\n' > "$HOME/fake-curie"
chmod +x "$HOME/fake-curie"
wait_pending_tool() { sleep 0.2; return 1; }
drive_gated_turn "scale it" mcp__kubernetes__resources_scale approve && exit 9
test "$(wc -l < "$HOME/turns")" -eq 1
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_cluster_turn_invokes_cluster_message(self):
        result = self.run_function(
            """
curie_bin() { printf '%s' "$HOME/fake-curie"; }
cat > "$HOME/fake-curie" <<'EOF'
#!/bin/sh
printf '%s\\n' "$*" > "$HOME/args"
echo '{"reply":"ok","thread":"100.000001","finalized":true}'
EOF
chmod +x "$HOME/fake-curie"
cluster_turn "List namespaces"
grep -q -- 'cluster' "$HOME/args"
grep -q -- 'message' "$HOME/args"
grep -q -- 'List namespaces' "$HOME/args"
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_bind_operator_route_sets_users(self):
        result = self.run_function(
            """
curie_bin() { printf '%s' "$HOME/fake-curie"; }
cat > "$HOME/fake-curie" <<'EOF'
#!/bin/sh
printf '%s\\n' "$*" > "$HOME/args"
echo '{}'
EOF
chmod +x "$HOME/fake-curie"
bind_operator_route
grep -q -- '--route-approvers' "$HOME/args"
grep -q -- 'users:U0EXAMPLE1' "$HOME/args"
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_operator_audit_rejects_a_chat_principal(self):
        result = self.run_function(
            """
export CURIE_SRE_AUDIT_OPERATOR=U0EXAMPLE1
cat "$FIXTURE" | audit_is_operator
""",
            [
                {
                    "action": "approved",
                    "authorized": True,
                    "actor": "U0EXAMPLE1",
                    "principal_kind": "chat",
                }
            ],
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_operator_audit_accepts_an_operator_principal(self):
        result = self.run_function(
            """
export CURIE_SRE_AUDIT_OPERATOR=U0EXAMPLE1
cat "$FIXTURE" | audit_is_operator
""",
            [
                {
                    "action": "approved",
                    "authorized": True,
                    "actor": "U0EXAMPLE1",
                    "principal_kind": "operator",
                }
            ],
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_enqueued_json_is_not_a_finished_reply(self):
        result = self.parse_turn(
            {"status": "enqueued", "channel": "C0LOCALDEV", "thread": "100.000001"}
        )
        self.assertNotEqual(result.returncode, 0)

    def test_missing_thread_cannot_prove_delivery(self):
        result = self.run_function(
            'echo \'{"reply":"ok","finalized":true}\' | turn_thread'
        )
        self.assertNotEqual(result.returncode, 0, result.stderr)

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
            'list_pending() { cat "$FIXTURE"; }\nwait_scale_pending 1',
            {
                "truncated": False,
                "pending": [
                    {
                        "id": "ours",
                        "status": "pending",
                        "granted_tool": "mcp__kubernetes__resources_scale",
                    }
                ],
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "ours")

    def test_other_pending_tools_cannot_satisfy_scale(self):
        result = self.run_function(
            'list_pending() { cat "$FIXTURE"; }\nsleep() { :; }\nwait_scale_pending 1',
            {
                "truncated": False,
                "pending": [
                    {
                        "id": "ours",
                        "status": "pending",
                        "granted_tool": "other/resources_scale",
                    }
                ],
            },
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

    def test_script_does_not_name_ci_slack_secrets(self):
        text = SCRIPT.read_text()
        for needle in (
            "CI_SLACK_APP_TOKEN",
            "CI_SLACK_BOT_TOKEN",
            "CI_SLACK_USER_TOKEN",
            "CI_SLACK_CHANNEL_ID",
        ):
            self.assertNotIn(needle, text)

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

    def test_pinned_server_quoted_unknown_tool_error_is_a_refusal(self):
        from mcp.shared.exceptions import MCPError

        # Observed from the pinned kubernetes-mcp-server digest on kind (#3207):
        # tools/call configuration_view -> -32602 'unknown tool "configuration_view"'.
        self.assertEqual(
            self.probe(forbidden=MCPError(-32602, 'unknown tool "configuration_view"'))[
                "forbidden_invocation"
            ],
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
