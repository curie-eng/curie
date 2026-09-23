"""Focused contracts for the provider harness safety boundary."""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import signal
import sqlite3
import stat
import sys
import tempfile
import unittest

import yaml

HARNESS_PATH = pathlib.Path(__file__).parents[1] / "scripts" / "provider_harness.py"
SPEC = importlib.util.spec_from_file_location("provider_harness", HARNESS_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load provider harness from {HARNESS_PATH}")
provider_harness = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = provider_harness
SPEC.loader.exec_module(provider_harness)


class ProviderHarnessContracts(unittest.TestCase):
    def test_blocked_account_public_access_is_refused_without_mutation(self):
        class KindInventoryBoundary:
            def __init__(self, delegate, private_dir):
                self.delegate = delegate
                self.private_dir = private_dir

            def run(self, argv, action, **kwargs):
                if list(argv) == ["kind", "get", "clusters"]:
                    stderr_path = provider_harness.write_private_file(
                        self.private_dir / "kind-inventory.stderr", b""
                    )
                    return provider_harness.ToolResult(0, b"", stderr_path)
                return self.delegate.run(argv, action, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            executable_dir = root / "bin"
            executable_dir.mkdir()
            command_log = root / "aws-commands.jsonl"
            aws_stub = executable_dir / "aws"
            aws_stub.write_text(
                """#!/usr/bin/env python3
import json
import os
import sys

arguments = sys.argv[1:]
with open(os.environ["AWS_STUB_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(arguments) + "\\n")
if arguments[:2] == ["sts", "get-caller-identity"]:
    print(json.dumps({"Account": "000000000000"}))
elif arguments[:2] == ["resourcegroupstaggingapi", "get-resources"]:
    print(json.dumps({"ResourceTagMappingList": []}))
elif arguments[:2] == ["s3control", "get-public-access-block"]:
    print(json.dumps({"PublicAccessBlockConfiguration": {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }}))
else:
    raise SystemExit(2)
""",
                encoding="utf-8",
            )
            aws_stub.chmod(0o700)
            case = provider_harness.HarnessCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="preinstalled",
                real_aws=True,
                curie_bin=root / "curie",
            )
            work = case.work
            try:
                case.aws_env["AWS_STUB_LOG"] = str(command_log)
                case.aws_env["PATH"] = f"{executable_dir}{os.pathsep}{case.aws_env['PATH']}"
                case.runner = KindInventoryBoundary(case.runner, work)

                # Read contracts are from the provider APIs. No account setting is written.
                # https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html
                # https://docs.aws.amazon.com/resourcegroupstagging/latest/APIReference/API_GetResources.html
                # https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_GetPublicAccessBlock.html
                with self.assertRaisesRegex(
                    provider_harness.HarnessError,
                    "account S3 public access settings block",
                ):
                    case.preflight()
                commands = [
                    json.loads(line)
                    for line in command_log.read_text(encoding="utf-8").splitlines()
                ]
                self.assertEqual(
                    [command[:2] for command in commands],
                    [
                        ["sts", "get-caller-identity"],
                        ["resourcegroupstaggingapi", "get-resources"],
                        ["s3control", "get-public-access-block"],
                    ],
                )
                for command in commands:
                    self.assertIn("--profile", command)
                    self.assertEqual(command[command.index("--profile") + 1], "theconnman")
                    self.assertIn("--region", command)
                    self.assertEqual(command[command.index("--region") + 1], "us-east-1")
                    self.assertFalse(
                        any(token.startswith(("put-", "delete-", "create-")) for token in command)
                    )
            finally:
                shutil.rmtree(work)

    def test_emulator_aws_calls_require_an_isolated_endpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = provider_harness.HarnessCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="preinstalled",
                real_aws=False,
                curie_bin=root / "curie",
            )
            try:
                with self.assertRaisesRegex(
                    provider_harness.HarnessError, "emulator AWS endpoint is unavailable"
                ):
                    case.aws(
                        "secretsmanager",
                        "list-secrets",
                        action="prove emulator endpoint guard",
                    )
                self.assertEqual(case.commands, [])
            finally:
                shutil.rmtree(case.work)

    def test_configured_aws_endpoints_are_ignored_but_explicit_emulator_endpoint_is_used(self):
        ambient = {
            "AWS_ENDPOINT_URL": "https://ambient.example.com",
            "AWS_ENDPOINT_URL_SECRETS_MANAGER": "https://service.example.com",
            "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
        }
        previous = {key: os.environ.get(key) for key in ambient}
        os.environ.update(ambient)
        try:
            environment = provider_harness.aws_environment()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertNotIn("AWS_ENDPOINT_URL", environment)
        self.assertNotIn("AWS_ENDPOINT_URL_SECRETS_MANAGER", environment)
        self.assertEqual(environment["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"], "true")

        class CaptureRunner:
            def __init__(self, private_dir):
                self.private_dir = private_dir
                self.argv = None
                self.environment = None

            def run(self, argv, _action, *, env=None, **_kwargs):
                self.argv = list(argv)
                self.environment = env
                stderr_path = provider_harness.write_private_file(
                    self.private_dir / "aws.stderr", b""
                )
                return provider_harness.ToolResult(0, b"{}", stderr_path)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = provider_harness.HarnessCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="preinstalled",
                real_aws=False,
                curie_bin=root / "curie",
            )
            capture = CaptureRunner(case.work)
            try:
                case.aws_env = environment
                case.aws_endpoint = "http://127.0.0.1:5000"
                case.runner = capture
                case.aws(
                    "secretsmanager",
                    "list-secrets",
                    action="capture explicit emulator endpoint",
                )
                self.assertIsNotNone(capture.argv)
                endpoint_index = capture.argv.index("--endpoint-url")
                self.assertEqual(capture.argv[endpoint_index + 1], case.aws_endpoint)
                self.assertEqual(capture.environment["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"], "true")
            finally:
                shutil.rmtree(case.work)

    def test_cleanup_attempts_every_target_and_verification_after_a_delete_failure(self):
        class FaultInjectedCleanupCase(provider_harness.HarnessCase):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.attempted = []
                self.verified = False

            def delete_cleanup_target(self, target, *, real_cluster_already_attempted):
                self.attempted.append(target.identity)
                if target.identity.endswith("-fails"):
                    raise provider_harness.HarnessError("injected delete failure")

            def verify_cleanup(self, targets):
                self.verified = True
                self.assertion_target_count = len(targets)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = FaultInjectedCleanupCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="none",
                real_aws=False,
                curie_bin=root / "curie",
            )
            try:
                with provider_harness.ResourceLedger(case.ledger_path) as ledger:
                    case.ledger = ledger
                    identities = [
                        "curie-aws-secrets-e2e-first",
                        "curie-aws-secrets-e2e-fails",
                        "curie-aws-secrets-e2e-last",
                    ]
                    for identity in identities:
                        ledger.record_intent("docker_image", identity)
                    with self.assertRaisesRegex(
                        provider_harness.HarnessError, "injected delete failure"
                    ):
                        case.cleanup()
                case.ledger = None
                self.assertEqual(case.attempted, list(reversed(identities)))
                self.assertTrue(case.verified)
                self.assertEqual(case.assertion_target_count, len(identities))
            finally:
                shutil.rmtree(case.work)

    def test_cleanup_verification_attempts_every_check_after_an_earlier_failure(self):
        class RecordingVerificationCase(provider_harness.HarnessCase):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.verifications = []

            def current_clusters(self):
                self.verifications.append("kind set")
                return {"unexpected-cluster"}

            def aws_reports_absent(self, service, operation, *_args):
                self.verifications.append(f"{service}:{operation}")
                return False

            def aws(self, service, operation, *_args, **_kwargs):
                self.verifications.append(f"{service}:{operation}")
                stderr_path = provider_harness.write_private_file(
                    self.work / "verification.stderr", b""
                )
                return provider_harness.ToolResult(
                    0, b'{"ResourceTagMappingList": []}', stderr_path
                )

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = RecordingVerificationCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="preinstalled",
                real_aws=True,
                curie_bin=root / "curie",
            )
            case.account_id = "000000000000"
            case.prior_clusters = {"prior-cluster"}
            targets = [
                provider_harness.CleanupTarget(1, "secretsmanager", case.primary_name, "created"),
                provider_harness.CleanupTarget(2, "iam_role", case.role_name, "created"),
                provider_harness.CleanupTarget(
                    3,
                    "iam_oidc_provider",
                    f"{provider_harness.OWNED_PREFIX}issuer.example.com",
                    "created",
                ),
                provider_harness.CleanupTarget(4, "s3_bucket", case.bucket_name, "created"),
            ]
            original_wait = provider_harness.wait_until

            def immediate_wait(action, predicate, timeout=180, interval=2.0):
                del timeout, interval
                if not predicate():
                    raise provider_harness.HarnessError(f"timed out while waiting for {action}")

            provider_harness.wait_until = immediate_wait
            try:
                with self.assertRaisesRegex(
                    provider_harness.HarnessError, "kind set mismatch"
                ) as raised:
                    case.verify_cleanup(targets)
                self.assertIn("Secrets Manager absence", str(raised.exception))
                self.assertIn("IAM role absence", str(raised.exception))
                self.assertIn("IAM OIDC provider absence", str(raised.exception))
                self.assertIn("S3 bucket absence", str(raised.exception))
                self.assertEqual(
                    case.verifications,
                    [
                        "kind set",
                        "secretsmanager:describe-secret",
                        "iam:get-role",
                        "iam:get-open-id-connect-provider",
                        "s3api:head-bucket",
                        "resourcegroupstaggingapi:get-resources",
                    ],
                )
            finally:
                provider_harness.wait_until = original_wait
                shutil.rmtree(case.work)

    def test_rendered_sync_manifests_match_external_secrets_contract(self):
        class RecordingCase(provider_harness.HarnessCase):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.applied = []

            def apply(self, manifest, action, namespace=provider_harness.NAMESPACE):
                self.applied.append((manifest, action, namespace))

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = RecordingCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="preinstalled",
                real_aws=True,
                curie_bin=root / "curie",
            )
            try:
                # https://external-secrets.io/latest/provider/aws-secrets-manager/
                # https://external-secrets.io/latest/api/pushsecret/
                case.apply_sync_objects()
                self.assertEqual(len(case.applied), 1)
                documents = [
                    json.loads(document) for document in case.applied[0][0].split(b"\n---\n")
                ]
                store, external, push = documents
                self.assertEqual(
                    store["spec"]["provider"]["aws"],
                    {
                        "service": "SecretsManager",
                        "region": "us-east-1",
                        "auth": {"jwt": {"serviceAccountRef": {"name": "acme-harness-eso"}}},
                    },
                )
                self.assertEqual(
                    external["spec"]["data"][0],
                    {
                        "secretKey": provider_harness.STATIC_KEY,
                        "remoteRef": {
                            "key": case.primary_name,
                            "property": provider_harness.STATIC_KEY,
                        },
                    },
                )
                self.assertEqual(push["spec"]["deletionPolicy"], "None")
                match = push["spec"]["data"][0]["match"]
                self.assertEqual(match["remoteRef"]["remoteKey"], case.backup_name)
                self.assertEqual(match["remoteRef"]["property"], provider_harness.ROTATED_KEY)
                metadata = push["spec"]["data"][0]["metadata"]["spec"]
                self.assertEqual(metadata["tags"], {"purpose": provider_harness.PURPOSE})
            finally:
                shutil.rmtree(case.work)

    def test_rotation_rbac_is_scoped_to_one_secret(self):
        class RecordingCase(provider_harness.HarnessCase):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.applied = []

            def apply(self, manifest, action, namespace=provider_harness.NAMESPACE):
                self.applied.append((manifest, action, namespace))

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = RecordingCase(
                repo_root=root,
                snapshot=HARNESS_PATH.parents[2],
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="preinstalled",
                real_aws=False,
                curie_bin=root / "curie",
            )
            try:
                with provider_harness.ResourceLedger(case.ledger_path) as ledger:
                    case.ledger = ledger
                    case.create_namespace_and_rbac()
                case.ledger = None

                self.assertEqual(len(case.applied), 2)
                documents = list(yaml.safe_load_all(case.applied[1][0]))
                role = next(document for document in documents if document["kind"] == "Role")
                binding = next(
                    document for document in documents if document["kind"] == "RoleBinding"
                )
                self.assertEqual(
                    role["rules"],
                    [
                        {
                            "apiGroups": [""],
                            "resources": ["secrets"],
                            "resourceNames": [provider_harness.TARGET_SECRET],
                            "verbs": ["get", "patch"],
                        }
                    ],
                )
                self.assertEqual(
                    binding["subjects"],
                    [{"kind": "ServiceAccount", "name": "acme-harness-rotation"}],
                )
                self.assertEqual(binding["roleRef"]["name"], "acme-harness-rotation")
            finally:
                shutil.rmtree(case.work)

    def test_generated_cluster_node_and_aws_names_fit_provider_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            snapshot = root / "snapshot"
            snapshot.mkdir()
            case = provider_harness.HarnessCase(
                repo_root=root,
                snapshot=snapshot,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="none",
                real_aws=True,
                curie_bin=root / "curie",
            )
            work = case.work
            try:
                # The kind node ceiling reproduces the observed Docker sethostname failure.
                # AWS limits:
                # https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html
                # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
                # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_CreateSecret.html
                limits = {
                    "kind cluster": (case.cluster, 1, 63),
                    "kind control plane node": (f"{case.cluster}-control-plane", 1, 63),
                    "Secrets Manager primary": (case.primary_name, 1, 512),
                    "Secrets Manager backup": (case.backup_name, 1, 512),
                    "IAM role": (case.role_name, 1, 64),
                    "IAM inline policy": (case.policy_name, 1, 128),
                    "S3 bucket": (case.bucket_name, 3, 63),
                }
                for resource, (name, minimum, maximum) in limits.items():
                    with self.subTest(resource=resource, name=name):
                        self.assertGreaterEqual(len(name), minimum)
                        self.assertLessEqual(len(name), maximum)
            finally:
                shutil.rmtree(work)
            self.assertFalse(work.exists())

    def test_run_retains_private_diagnostics_on_failure_and_removes_them_on_success(self):
        class SuccessfulCase(provider_harness.HarnessCase):
            def _run(self):
                provider_harness.write_private_file(self.work / "diagnostic.log", "safe")

        class FailingCase(provider_harness.HarnessCase):
            def _run(self):
                provider_harness.write_private_file(self.work / "diagnostic.log", "safe")
                raise provider_harness.HarnessError("controlled failure")

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            snapshot = root / "snapshot"
            snapshot.mkdir()
            arguments = {
                "repo_root": root,
                "snapshot": snapshot,
                "commit": "a" * 40,
                "seed": {
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                "mode": "none",
                "real_aws": False,
                "curie_bin": root / "curie",
            }

            failing = FailingCase(**arguments)
            failed_work = failing.work
            try:
                diagnostics = io.StringIO()
                with contextlib.redirect_stderr(diagnostics):
                    with self.assertRaisesRegex(
                        provider_harness.HarnessError, "controlled failure"
                    ):
                        failing.run()
                diagnostic_file = failed_work / "diagnostic.log"
                self.assertTrue(failed_work.is_dir())
                self.assertEqual(stat.S_IMODE(failed_work.stat().st_mode), 0o700)
                self.assertEqual(stat.S_IMODE(diagnostic_file.stat().st_mode), 0o600)
                self.assertIn(str(failed_work), diagnostics.getvalue())
            finally:
                shutil.rmtree(failed_work)

            successful = SuccessfulCase(**arguments)
            successful_work = successful.work
            successful.run()
            self.assertFalse(successful_work.exists())

    def test_cleanup_phase_signal_interrupts_completion_and_next_guard_is_fresh(self):
        class CleanupSignalCase(provider_harness.HarnessCase):
            def preflight(self):
                pass

            def _run_body(self):
                pass

            def cleanup(self):
                handler = signal.getsignal(signal.SIGTERM)
                if not callable(handler):
                    raise AssertionError("cleanup signal handler is not callable")
                handler(signal.SIGTERM, None)

            def write_evidence(self, orchestrator_status, cleanup_status):
                del orchestrator_status, cleanup_status

        handled = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
        original = {sig: signal.getsignal(sig) for sig in handled}
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = CleanupSignalCase(
                repo_root=root,
                snapshot=root,
                commit="a" * 40,
                seed={
                    provider_harness.STATIC_KEY: "synthetic-static",
                    provider_harness.ROTATED_KEY: "synthetic-initial",
                },
                mode="none",
                real_aws=False,
                curie_bin=root / "curie",
            )
            case_guard = provider_harness.install_signal_handlers()
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(provider_harness.HarnessInterrupted):
                        case.run()
                with sqlite3.connect(case.ledger_path) as database:
                    completion = database.execute(
                        "SELECT orchestrator_status, cleanup_status FROM completion"
                    ).fetchone()
                self.assertEqual(completion, ("interrupted", "complete"))
                self.assertEqual(case.cleanup_signals, [signal.SIGTERM])
            finally:
                case_guard.restore()
                shutil.rmtree(case.work)

            fresh_guard = provider_harness.install_signal_handlers()
            try:
                handler = signal.getsignal(signal.SIGTERM)
                if not callable(handler):
                    self.fail("fresh signal handler is not callable")
                with self.assertRaises(provider_harness.HarnessInterrupted):
                    handler(signal.SIGTERM, None)
                self.assertEqual(fresh_guard.received_signals, [signal.SIGTERM])
            finally:
                fresh_guard.restore()
                for sig, handler in original.items():
                    self.assertEqual(signal.getsignal(sig), handler)

    def test_ci_interruption_stops_before_constructing_the_next_mode(self):
        # This covers mode loop control flow. The live selector proves orchestration.
        class InterruptingCase:
            def __init__(self, *args):
                constructed_modes.append(args[4])

            def run(self):
                raise provider_harness.HarnessInterrupted(signal.SIGTERM)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            executable = root / "curie"
            executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            executable.chmod(0o700)
            constructed_modes = []
            original_argv = sys.argv
            original_require_tools = provider_harness.require_tools
            original_candidate_snapshot = provider_harness.candidate_snapshot
            original_case = provider_harness.HarnessCase

            def controlled_snapshot(_repo_root, _runner):
                context = tempfile.TemporaryDirectory(
                    prefix="provider-harness-control-flow-", dir=root
                )
                return "a" * 40, pathlib.Path(context.name), context

            sys.argv = [
                str(HARNESS_PATH),
                "--curie-bin",
                str(executable),
                "--ci",
            ]
            provider_harness.require_tools = lambda _tools: None
            provider_harness.candidate_snapshot = controlled_snapshot
            provider_harness.HarnessCase = InterruptingCase
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    status = provider_harness.main()
                self.assertEqual(status, provider_harness.INTERRUPTED_EXIT)
                self.assertEqual(constructed_modes, ["preinstalled"])
            finally:
                provider_harness.HarnessCase = original_case
                provider_harness.candidate_snapshot = original_candidate_snapshot
                provider_harness.require_tools = original_require_tools
                sys.argv = original_argv

    def test_seed_shape_and_owned_resource_names_are_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            valid_path = root / "seed.json"
            valid = {
                "STATIC_KEY": "synthetic-static",
                "ROTATED_KEY": "synthetic-initial",
            }
            valid_path.write_text(json.dumps(valid), encoding="utf-8")
            self.assertEqual(provider_harness.load_seed(valid_path), valid)

            nested_path = root / "nested.json"
            nested_path.write_text(
                json.dumps({**valid, "ROTATED_KEY": {"nested": True}}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                provider_harness.load_seed(nested_path)

            unsafe_path = root / "unsafe.json"
            unsafe_path.write_text(
                json.dumps({**valid, "../STATIC_KEY": "unsafe"}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                provider_harness.load_seed(unsafe_path)

        owned = "curie-aws-secrets-e2e-r1-acme-connector-secrets"
        self.assertEqual(provider_harness.require_owned_name(owned), owned)
        with self.assertRaises(ValueError):
            provider_harness.require_owned_name("shared-connector-secrets")

    def test_owned_secret_path_requires_owned_prefix_and_one_safe_leaf(self):
        owned = "curie-aws-secrets-e2e-20260922000000-abc123/acme-harness-fixture"
        self.assertEqual(provider_harness.require_owned_secret_path(owned), owned)
        self.assertEqual(
            provider_harness.require_owned_secret_path(owned + "-rotated"), owned + "-rotated"
        )
        for rejected in (
            "shared-prefix/acme-harness-fixture",
            "curie-aws-secrets-e2e-r1/../acme-harness-fixture",
            "curie-aws-secrets-e2e-r1/acme..fixture",
            "curie-aws-secrets-e2e-r1/acme/fixture",
            "curie-aws-secrets-e2e-r1/Acme-Fixture",
            "Curie-aws-secrets-e2e-r1/acme-fixture",
            "curie-aws-secrets-e2e-r1",
            "curie-aws-secrets-e2e-r1/",
        ):
            with self.assertRaises(ValueError, msg=rejected):
                provider_harness.require_owned_secret_path(rejected)

    def test_sampling_blind_stretch_bounds_a_steady_reader(self):
        samples = [(t * 0.25, t * 0.25 + 0.1, "0" * 64) for t in range(41)]
        blind = provider_harness.max_sampling_blind_seconds(samples, 0.0, 10.1)
        self.assertLessEqual(blind, provider_harness.ROTATION_MAX_BLIND_SECONDS)

    def test_sampling_blind_stretch_fails_a_stalled_reader(self):
        stalled = [(0.0, 45.0, "0" * 64)]
        self.assertGreater(
            provider_harness.max_sampling_blind_seconds(stalled, 0.0, 45.0),
            provider_harness.ROTATION_MAX_BLIND_SECONDS,
        )
        gap = [(0.0, 0.1, "0" * 64), (5.0, 5.1, "0" * 64)]
        self.assertGreater(
            provider_harness.max_sampling_blind_seconds(gap, 0.0, 5.1),
            provider_harness.ROTATION_MAX_BLIND_SECONDS,
        )
        self.assertEqual(provider_harness.max_sampling_blind_seconds([], 0.0, 3.0), 3.0)

    def test_rotation_sample_evaluation_passes_a_clean_phase(self):
        r0, r1, r2 = "0" * 64, "1" * 64, "2" * 64
        rotations = [(3.0, 3.4, r1), (6.0, 6.3, r2)]
        samples = [(0.0, 0.1, r0), (2.9, 3.05, r0), (3.5, 3.6, r1), (6.4, 6.5, r2)]
        self.assertIsNone(provider_harness.first_rotation_sample_violation(samples, r0, rotations))

    def test_rotation_sample_evaluation_flags_a_revert_between_rotations(self):
        r0, r1, r2 = "0" * 64, "1" * 64, "2" * 64
        rotations = [(3.0, 3.4, r1), (6.0, 6.3, r2)]
        samples = [(3.5, 3.6, r1), (4.0, 4.1, r0), (6.4, 6.5, r2)]
        violation = provider_harness.first_rotation_sample_violation(samples, r0, rotations)
        self.assertEqual(violation["sample_index"], 1)
        self.assertEqual(violation["allowed"], [r1[:12]])
        missing = provider_harness.first_rotation_sample_violation(
            [(4.0, 4.1, None)], r0, rotations
        )
        self.assertEqual(missing["sample_index"], 0)

    def test_rotation_sample_evaluation_allows_either_side_of_an_in_flight_write(self):
        r0, r1, r2 = "0" * 64, "1" * 64, "2" * 64
        rotations = [(3.0, 3.4, r1), (6.0, 6.3, r2)]
        for seen in (r1, r2):
            self.assertIsNone(
                provider_harness.first_rotation_sample_violation([(6.1, 6.2, seen)], r0, rotations)
            )
        self.assertIsNotNone(
            provider_harness.first_rotation_sample_violation([(6.1, 6.2, r0)], r0, rotations)
        )

    def test_sensitive_tool_run_leaves_no_stdout_on_disk(self):
        with tempfile.TemporaryDirectory() as raw:
            private = pathlib.Path(raw)
            commands: list[str] = []
            runner = provider_harness.ToolRunner(private, commands)
            marker = "synthetic-secret-marker-7f3a"
            result = runner.run(
                [sys.executable, "-c", f"print({marker!r})"], "print marker", sensitive=True
            )
            self.assertEqual(result.stdout.decode().strip(), marker)
            self.assertEqual(len(commands), 1)
            self.assertFalse(list(private.glob("*.stdout")))
            for path in private.iterdir():
                self.assertNotIn(marker.encode(), path.read_bytes())
            plain = runner.run([sys.executable, "-c", "print('x')"], "print plain")
            self.assertEqual(plain.stdout.strip(), b"x")
            self.assertEqual(len(list(private.glob("*.stdout"))), 1)

    def test_rotation_report_parser_accepts_known_outcomes_only(self):
        good = json.dumps(
            {
                "entry": "acme-harness-fixture",
                "seeds": [{"key": "ROTATED_KEY", "outcome": "already_present"}],
            }
        ).encode()
        self.assertEqual(
            provider_harness.parse_rotation_report(good + b"\n", ["ROTATED_KEY"]),
            {"ROTATED_KEY": "already_present"},
        )
        for outcome in ("no_backup", "not_in_backup", "created", "added"):
            payload = json.dumps(
                {"entry": "e", "seeds": [{"key": "ROTATED_KEY", "outcome": outcome}]}
            ).encode()
            self.assertEqual(
                provider_harness.parse_rotation_report(payload, ["ROTATED_KEY"]),
                {"ROTATED_KEY": outcome},
            )
        rejected = [
            {"entry": "e", "seeds": [{"key": "ROTATED_KEY", "outcome": "overwritten"}]},
            {"entry": "e", "seeds": []},
            {"entry": "e"},
            {"seeds": [{"key": "ROTATED_KEY", "outcome": "added"}]},
            {"entry": "e", "seeds": [{"key": "OTHER_KEY", "outcome": "added"}]},
            {
                "entry": "e",
                "seeds": [
                    {"key": "ROTATED_KEY", "outcome": "added"},
                    {"key": "ROTATED_KEY", "outcome": "added"},
                ],
            },
        ]
        for payload in rejected:
            with self.assertRaises(provider_harness.HarnessError, msg=payload):
                provider_harness.parse_rotation_report(
                    json.dumps(payload).encode(), ["ROTATED_KEY"]
                )
        with self.assertRaises(provider_harness.HarnessError):
            provider_harness.parse_rotation_report(good + b"\n" + good, ["ROTATED_KEY"])
        with self.assertRaises(provider_harness.HarnessError):
            provider_harness.parse_rotation_report(b"", ["ROTATED_KEY"])

    def test_private_files_are_created_with_owner_only_access_and_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for name in ["static.value", "profile", "config", "token"]:
                path = root / name
                returned = provider_harness.write_private_file(path, "private-value")
                self.assertEqual(pathlib.Path(returned), path)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                provider_harness.require_private_file(path)

            group_readable = root / "group-readable"
            group_readable.write_text("private-value", encoding="utf-8")
            group_readable.chmod(0o640)
            with self.assertRaises(PermissionError):
                provider_harness.require_private_file(group_readable)

    def test_ledger_commits_intent_selects_exact_cleanup_and_records_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger_path = pathlib.Path(directory) / "run.sqlite"
            first_identity = "curie-aws-secrets-e2e-r1-cluster"
            second_identity = "curie-aws-secrets-e2e-r1-primary"

            with provider_harness.ResourceLedger(ledger_path) as ledger:
                first = ledger.record_intent("kind_cluster", first_identity)
                with sqlite3.connect(ledger_path) as observer:
                    observed = observer.execute(
                        "SELECT state, identity FROM resources WHERE id = ?", (first,)
                    ).fetchone()
                self.assertEqual(observed, ("intent", first_identity))

                ledger.record_intent("secretsmanager", second_identity)
                ledger.mark_created(first, first_identity)
                targets = {(row.kind, row.identity, row.state) for row in ledger.cleanup_targets()}
                self.assertEqual(
                    targets,
                    {
                        ("kind_cluster", first_identity, "created"),
                        ("secretsmanager", second_identity, "intent"),
                    },
                )
                ledger.record_completion("interrupted", "complete")

            self.assertEqual(stat.S_IMODE(ledger_path.stat().st_mode), 0o600)
            with sqlite3.connect(ledger_path) as observer:
                completion = observer.execute(
                    "SELECT orchestrator_status, cleanup_status FROM completion"
                ).fetchone()
            self.assertEqual(completion, ("interrupted", "complete"))

    def test_signal_redelivery_and_rendered_output_keep_private_values_safe(self):
        handled = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
        original = {sig: signal.getsignal(sig) for sig in handled}
        guard = provider_harness.install_signal_handlers()
        try:
            first_handler = signal.getsignal(signal.SIGTERM)
            with self.assertRaises(provider_harness.HarnessInterrupted):
                first_handler(signal.SIGTERM, None)

            for sig in handled:
                signal.getsignal(sig)(sig, None)
            self.assertEqual(guard.received_signals[0], signal.SIGTERM)
            self.assertGreaterEqual(len(guard.received_signals), 4)
        finally:
            guard.restore()
            for sig, handler in original.items():
                self.assertEqual(signal.getsignal(sig), handler)

        raw = "private-provider-value"
        digest = provider_harness.digest_prefix(raw)
        self.assertEqual(digest, hashlib.sha256(raw.encode()).hexdigest()[:12])
        self.assertNotIn(raw, digest)

        rendered = provider_harness.format_tool_error("create provider entry", 17, raw)
        self.assertIn("create provider entry", rendered)
        self.assertIn("17", rendered)
        self.assertNotIn(raw, rendered)


class RoutingSuiteHelpers(unittest.TestCase):
    def test_helm_release_decodes_through_both_base64_layers_and_gzip(self):
        import base64
        import gzip

        release = {"name": "rt", "config": {"api": {"existingSecret": "rt-x"}}}
        helm_layer = base64.b64encode(gzip.compress(json.dumps(release).encode()))
        k8s_layer = base64.b64encode(helm_layer).decode()
        self.assertEqual(provider_harness.decode_helm_release(k8s_layer), release)

    def test_forbidden_scan_finds_raw_and_base64_at_every_alignment(self):
        import base64

        value = b"synthetic-sentinel-0123456789abcdef"
        forbidden = {"sentinel": value, "other": b"synthetic-absent-value"}
        self.assertEqual(
            provider_harness.forbidden_hits(b"prefix " + value + b" suffix", forbidden),
            ["sentinel"],
        )
        for pad in (b"", b"a", b"ab", b"abc"):
            encoded = base64.b64encode(pad + value + b"tail")
            self.assertNotIn(value, encoded)
            self.assertEqual(provider_harness.forbidden_hits(encoded, forbidden), ["sentinel"], pad)
        self.assertEqual(provider_harness.forbidden_hits(b"clean text", forbidden), [])
        self.assertEqual(provider_harness.forbidden_hits(b"anything", {"empty": b""}), [])

    def test_secret_key_ref_discovery_lists_only_running_consumers(self):
        def pod(name, phase="Running", running=True, deleting=False):
            env = [
                {"name": "PLAIN", "value": "x"},
                {
                    "name": "AGENT_CREDENTIALS",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": "rt-curie-runner-credentials",
                            "key": "agentCredentials",
                        }
                    },
                },
                {
                    "name": "OTHER",
                    "valueFrom": {
                        "secretKeyRef": {"name": "rt-curie-runner-credentials", "key": "k"}
                    },
                },
            ]
            metadata = {"name": name}
            if deleting:
                metadata["deletionTimestamp"] = "2026-09-23T00:00:00Z"
            return {
                "metadata": metadata,
                "spec": {"containers": [{"name": "worker", "env": env}]},
                "status": {
                    "phase": phase,
                    "containerStatuses": [
                        {"name": "worker", "state": {"running": {}} if running else {"waiting": {}}}
                    ],
                },
            }

        pods = {
            "items": [
                pod("rt-curie-worker-a"),
                pod("rt-curie-worker-b", phase="Pending"),
                pod("rt-curie-worker-c", running=False),
                pod("rt-curie-worker-d", deleting=True),
            ]
        }
        self.assertEqual(
            provider_harness.secret_key_env_refs(
                pods, "rt-curie-runner-credentials", "agentCredentials"
            ),
            [("rt-curie-worker-a", "worker", "AGENT_CREDENTIALS")],
        )
        self.assertEqual(
            provider_harness.secret_key_env_refs(pods, "absent", "agentCredentials"), []
        )

    def test_external_secret_mappings_and_secret_string_values(self):
        listed = {
            "items": [
                {
                    "metadata": {"name": "rt-installation-id"},
                    "spec": {
                        "target": {"name": "rt-curie-installation-id"},
                        "data": [
                            {
                                "secretKey": "installationId",
                                "remoteRef": {
                                    "key": "p/rt/installation-id",
                                    "property": "installationId",
                                },
                            }
                        ],
                    },
                }
            ]
        }
        self.assertEqual(
            provider_harness.external_secret_mappings(listed),
            [
                (
                    "rt-curie-installation-id",
                    "installationId",
                    "p/rt/installation-id",
                    "installationId",
                )
            ],
        )
        self.assertEqual(
            provider_harness.secret_string_values('{"a": "one", "b": 2}'), ["one", "2"]
        )
        self.assertEqual(provider_harness.secret_string_values("plain"), ["plain"])

    def test_tag_form_images_skip_digests_and_own_images(self):
        rendered = (
            "      image: busybox:1.36.1\n"
            '        image: "valkey/valkey:8.1.10-alpine"\n'
            "      - image: postgres:16@sha256:" + "a" * 64 + "\n"
            "      image: curie-aws-secrets-e2e-api:abc\n"
        )
        self.assertEqual(
            provider_harness.tag_form_images(rendered, {"curie-aws-secrets-e2e-api:abc"}),
            ["busybox:1.36.1", "valkey/valkey:8.1.10-alpine"],
        )

    def test_routing_installation_names_only_and_drops_provider_for_control(self):
        images = {
            name: f"curie-aws-secrets-e2e-{name}:abc-123"
            for name in ("api", "worker", "dispatcher", "runner")
        }
        document = provider_harness.routing_installation(
            "curie-aws-secrets-e2e-routing", "rt", "kind-x", "curie-aws-secrets-e2e-s", images
        )
        self.assertEqual(document["secrets"]["provider"], "aws")
        self.assertNotIn("comms", document)
        self.assertEqual(document["set"]["api.image.repository"], "curie-aws-secrets-e2e-api")
        self.assertEqual(document["set"]["agentSandbox.runner.tag"], "abc-123")
        self.assertTrue(all(isinstance(value, str) for value in document["set"].values()))
        control = provider_harness.routing_installation(
            "n", "rt", "kind-x", "p", images, provider=False
        )
        self.assertNotIn("secrets", control)


class MotoContainerAddress(unittest.TestCase):
    def test_kind_network_address_is_read_and_absence_is_refused(self):
        inspected = [
            {
                "NetworkSettings": {
                    "Networks": {
                        "bridge": {"IPAddress": "172.17.0.2"},
                        "kind": {"IPAddress": "172.18.0.5"},
                    }
                }
            }
        ]
        self.assertEqual(provider_harness.moto_kind_ip(inspected), "172.18.0.5")
        with self.assertRaises(provider_harness.HarnessError):
            provider_harness.moto_kind_ip([{"NetworkSettings": {"Networks": {"bridge": {}}}}])


if __name__ == "__main__":
    unittest.main()
