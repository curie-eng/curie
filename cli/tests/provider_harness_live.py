"""Explicit live selectors for the provider harness safety boundary.

This filename intentionally does not match unittest's default ``test*.py``
pattern. Every selector mutates kind or the AWS account and must be named
explicitly.
"""

import importlib.util
import json
import os
import pathlib
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Callable, Sequence

REPO_ROOT = pathlib.Path(__file__).parents[2]
HARNESS_PATH = REPO_ROOT / "cli/scripts/provider_harness.py"
SPEC = importlib.util.spec_from_file_location("provider_harness_live_target", HARNESS_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load provider harness from {HARNESS_PATH}")
provider_harness = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = provider_harness
SPEC.loader.exec_module(provider_harness)

AWS_KINDS = {
    "secretsmanager",
    "s3_bucket",
    "iam_oidc_provider",
    "iam_role",
    "iam_role_policy",
}


def wait_for(description: str, predicate: Callable[[], bool], timeout: int = 300) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {description}")


def run(command: Sequence[str], *, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def aws(*arguments: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "aws",
            *arguments,
            "--profile",
            provider_harness.PROFILE,
            "--region",
            provider_harness.REGION,
        ],
        cwd=REPO_ROOT,
        env=provider_harness.aws_environment(),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def aws_json(*arguments: str) -> dict:
    result = aws(*arguments, "--output", "json")
    if result.returncode != 0:
        raise AssertionError(f"AWS read failed with exit status {result.returncode}")
    parsed = json.loads(result.stdout)
    if not isinstance(parsed, dict):
        raise AssertionError("AWS read returned a nonobject response")
    return parsed


def kind_clusters() -> set[str]:
    result = run(["kind", "get", "clusters"])
    if result.returncode != 0:
        raise AssertionError(f"kind inventory failed with exit status {result.returncode}")
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def new_case(root: pathlib.Path, *, real_aws: bool) -> provider_harness.HarnessCase:
    return provider_harness.HarnessCase(
        repo_root=root,
        snapshot=REPO_ROOT,
        commit="a" * 40,
        seed={
            provider_harness.STATIC_KEY: "synthetic-static",
            provider_harness.ROTATED_KEY: "synthetic-initial",
        },
        mode="preinstalled",
        real_aws=real_aws,
        curie_bin=root / "curie",
    )


def ledger_rows(path: pathlib.Path) -> list[tuple[str, str, str]]:
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1) as database:
            rows = database.execute(
                "SELECT kind, identity, state FROM resources ORDER BY id"
            ).fetchall()
    except (sqlite3.Error, OSError):
        return []
    return [(str(kind), str(identity), str(state)) for kind, identity, state in rows]


def completion(path: pathlib.Path) -> tuple[str, str] | None:
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1) as database:
            row = database.execute(
                "SELECT orchestrator_status, cleanup_status FROM completion"
            ).fetchone()
    except (sqlite3.Error, OSError):
        return None
    return None if row is None else (str(row[0]), str(row[1]))


def tagged_resources() -> list[dict]:
    payload = aws_json(
        "resourcegroupstaggingapi",
        "get-resources",
        "--tag-filters",
        f"Key=purpose,Values={provider_harness.PURPOSE}",
    )
    mappings = payload.get("ResourceTagMappingList", [])
    if not isinstance(mappings, list):
        raise AssertionError("AWS tag inventory returned an invalid response")
    return mappings


def delete_owned_rows(rows: Sequence[tuple[str, str, str]]) -> None:
    """Best effort test trap using only exact identities from the durable ledger."""

    for kind, identity, _state in reversed(rows):
        if kind == "kind_cluster" and identity.startswith(provider_harness.OWNED_PREFIX):
            run(["kind", "delete", "cluster", "--name", identity], timeout=180)
    for kind, identity, _state in reversed(rows):
        if kind == "secretsmanager" and identity.startswith(provider_harness.OWNED_PREFIX):
            aws(
                "secretsmanager",
                "delete-secret",
                "--secret-id",
                identity,
                "--force-delete-without-recovery",
            )
        elif kind == "iam_role_policy":
            role, policy = identity.split("/", 1)
            if role.startswith(provider_harness.OWNED_PREFIX):
                aws(
                    "iam",
                    "delete-role-policy",
                    "--role-name",
                    role,
                    "--policy-name",
                    policy,
                )
        elif kind == "iam_role" and identity.startswith(provider_harness.OWNED_PREFIX):
            aws("iam", "delete-role", "--role-name", identity)
        elif kind == "iam_oidc_provider" and identity.startswith("arn:"):
            aws(
                "iam",
                "delete-open-id-connect-provider",
                "--open-id-connect-provider-arn",
                identity,
            )
        elif kind == "s3_bucket" and identity.startswith(provider_harness.OWNED_PREFIX):
            for key in (".well-known/openid-configuration", "openid/v1/jwks"):
                aws("s3api", "delete-object", "--bucket", identity, "--key", key)
            aws("s3api", "delete-bucket-policy", "--bucket", identity)
            aws("s3api", "delete-bucket", "--bucket", identity)
        elif kind == "docker_image" and identity.startswith(provider_harness.OWNED_PREFIX):
            run(["docker", "image", "rm", "--force", identity])


def direct_aws_resource_is_absent(kind: str, identity: str) -> bool:
    if kind == "secretsmanager":
        result = aws("secretsmanager", "describe-secret", "--secret-id", identity)
        return result.returncode != 0 and "ResourceNotFoundException" in result.stderr
    if kind == "iam_role_policy":
        role, policy = identity.split("/", 1)
        result = aws(
            "iam",
            "get-role-policy",
            "--role-name",
            role,
            "--policy-name",
            policy,
        )
        return result.returncode != 0 and "NoSuchEntity" in result.stderr
    if kind == "iam_role":
        result = aws("iam", "get-role", "--role-name", identity)
        return result.returncode != 0 and "NoSuchEntity" in result.stderr
    if kind == "iam_oidc_provider":
        result = aws(
            "iam",
            "get-open-id-connect-provider",
            "--open-id-connect-provider-arn",
            identity,
        )
        return result.returncode != 0 and "NoSuchEntity" in result.stderr
    if kind == "s3_bucket":
        result = aws("s3api", "head-bucket", "--bucket", identity)
        return result.returncode != 0 and any(
            code in result.stderr for code in ("(404)", "NoSuchBucket")
        )
    raise AssertionError(f"unsupported AWS ledger kind: {kind}")


class ProviderHarnessLive(unittest.TestCase):
    def test_actual_owned_kind_cluster_is_refused_without_deletion(self):
        prior = kind_clusters()
        with tempfile.TemporaryDirectory() as directory:
            case = new_case(pathlib.Path(directory), real_aws=False)
            created = False
            try:
                self.assertNotIn(case.cluster, prior)
                result = run(
                    ["kind", "create", "cluster", "--name", case.cluster, "--wait", "120s"],
                    timeout=180,
                )
                self.assertEqual(result.returncode, 0, "owned kind cluster creation failed")
                created = True
                with self.assertRaisesRegex(
                    provider_harness.HarnessError, "owned kind cluster already exists"
                ):
                    case.preflight()
                self.assertIn(case.cluster, kind_clusters())
            finally:
                if created or case.cluster in kind_clusters():
                    run(["kind", "delete", "cluster", "--name", case.cluster], timeout=180)
                wait_for("owned kind test cluster deletion", lambda: kind_clusters() == prior)
                shutil.rmtree(case.work, ignore_errors=True)

    def test_actual_tagged_secret_is_refused_and_exactly_removed(self):
        # CreateSecret tagging and forced deletion follow the provider API contracts:
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_CreateSecret.html
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_DeleteSecret.html
        prior_clusters = kind_clusters()
        self.assertEqual(tagged_resources(), [], "owned AWS tag inventory must start empty")
        with tempfile.TemporaryDirectory() as directory:
            case = new_case(pathlib.Path(directory), real_aws=True)
            created_secret = False
            value_path = provider_harness.write_private_file(
                case.work / "preflight-secret.json", '{"STATIC_KEY":"synthetic"}'
            )
            try:
                self.assertTrue(
                    direct_aws_resource_is_absent("secretsmanager", case.primary_name),
                    "generated test secret name already exists",
                )
                created = aws(
                    "secretsmanager",
                    "create-secret",
                    "--name",
                    case.primary_name,
                    "--secret-string",
                    f"file://{value_path}",
                    "--tags",
                    f"Key=purpose,Value={provider_harness.PURPOSE}",
                )
                self.assertEqual(created.returncode, 0, "test owned secret creation failed")
                created_secret = True
                with self.assertRaisesRegex(
                    provider_harness.HarnessError,
                    "preexisting tagged AWS resources must be removed first",
                ):
                    case.preflight()
                self.assertEqual(
                    aws(
                        "secretsmanager", "describe-secret", "--secret-id", case.primary_name
                    ).returncode,
                    0,
                    "preflight deleted the test owned secret",
                )
                self.assertEqual(kind_clusters(), prior_clusters)
            finally:
                if created_secret:
                    aws(
                        "secretsmanager",
                        "delete-secret",
                        "--secret-id",
                        case.primary_name,
                        "--force-delete-without-recovery",
                    )
                    wait_for(
                        "test owned secret deletion",
                        lambda: direct_aws_resource_is_absent("secretsmanager", case.primary_name),
                    )
                wait_for("empty owned AWS tag inventory", lambda: tagged_resources() == [])
                self.assertEqual(kind_clusters(), prior_clusters)
                shutil.rmtree(case.work, ignore_errors=True)

    def test_actual_kind_set_mismatch_is_detected(self):
        prior = kind_clusters()
        with tempfile.TemporaryDirectory() as directory:
            case = new_case(pathlib.Path(directory), real_aws=False)
            case.prior_clusters = prior
            original_wait = provider_harness.wait_until
            created_cluster = False

            def bounded_wait(action, predicate, timeout=180, interval=2.0):
                return original_wait(action, predicate, timeout=min(timeout, 2), interval=0.1)

            try:
                self.assertNotIn(case.cluster, prior)
                created = run(
                    ["kind", "create", "cluster", "--name", case.cluster, "--wait", "120s"],
                    timeout=180,
                )
                self.assertEqual(created.returncode, 0, "owned mismatch cluster creation failed")
                created_cluster = True
                provider_harness.wait_until = bounded_wait
                with self.assertRaisesRegex(
                    provider_harness.HarnessError,
                    "timed out while waiting for owned kind cluster deletion",
                ):
                    case.verify_cleanup([])
            finally:
                provider_harness.wait_until = original_wait
                if created_cluster:
                    run(["kind", "delete", "cluster", "--name", case.cluster], timeout=180)
                wait_for("owned mismatch cluster deletion", lambda: kind_clusters() == prior)
                shutil.rmtree(case.work, ignore_errors=True)

    def test_real_aws_sigterm_cleans_every_recorded_resource(self):
        # Direct absence checks use each provider's documented read operation:
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_DescribeSecret.html
        # https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetRole.html
        # https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetOpenIDConnectProvider.html
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
        candidate = pathlib.Path(os.environ.get("CURIE_BIN", ""))
        self.assertTrue(candidate.is_file() and os.access(candidate, os.X_OK), "set CURIE_BIN")
        candidate = candidate.resolve()
        prior_clusters = kind_clusters()
        self.assertEqual(tagged_resources(), [], "owned AWS tag inventory must start empty")

        process: subprocess.Popen[bytes] | None = None
        ledger_path: pathlib.Path | None = None
        rows: list[tuple[str, str, str]] = []
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            stdout_path = root / "stdout.log"
            stderr_path = root / "stderr.log"
            try:
                with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                    process = subprocess.Popen(
                        [str(candidate), "dev", "secrets-e2e", "--real-aws"],
                        cwd=REPO_ROOT,
                        stdin=subprocess.DEVNULL,
                        stdout=stdout,
                        stderr=stderr,
                        start_new_session=True,
                    )

                def discover_ledger() -> bool:
                    nonlocal ledger_path
                    text = stdout_path.read_text(encoding="utf-8", errors="replace")
                    match = re.search(r"(?m)^provider-harness-ledger: (.+)$", text)
                    if match is not None:
                        ledger_path = pathlib.Path(match.group(1))
                    return ledger_path is not None and ledger_path.is_file()

                wait_for("durable ledger path", discover_ledger, timeout=900)
                assert ledger_path is not None
                wait_for(
                    "created IAM role ledger row",
                    lambda: any(
                        kind == "iam_role" and state == "created"
                        for kind, _identity, state in ledger_rows(ledger_path)
                    ),
                    timeout=1800,
                )
                os.killpg(process.pid, signal.SIGTERM)
                cleanup_log = ledger_path.parent / "cleanup.log"
                wait_for(
                    "cleanup signal mode",
                    lambda: (
                        cleanup_log.is_file()
                        and "cleanup start" in cleanup_log.read_text(encoding="utf-8")
                    ),
                    timeout=180,
                )
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                status = process.wait(timeout=180)
                self.assertNotEqual(status, 0)
                wait_for(
                    "durable interrupted completion",
                    lambda: completion(ledger_path) is not None,
                    timeout=900,
                )
                self.assertEqual(completion(ledger_path), ("interrupted", "complete"))
                rows = ledger_rows(ledger_path)
                self.assertTrue(
                    any(kind in AWS_KINDS and state == "created" for kind, _name, state in rows)
                )

                self.assertEqual(kind_clusters(), prior_clusters)
                for kind, identity, _state in rows:
                    if kind in AWS_KINDS:
                        wait_for(
                            f"direct absence of {kind}",
                            lambda kind=kind, identity=identity: direct_aws_resource_is_absent(
                                kind, identity
                            ),
                        )
                    elif kind == "docker_image":
                        inspected = run(["docker", "image", "inspect", identity])
                        self.assertNotEqual(inspected.returncode, 0)
                    elif kind == "kind_cluster":
                        self.assertNotIn(identity, kind_clusters())
                self.assertEqual(tagged_resources(), [])
            finally:
                if process is not None and process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=30)
                if ledger_path is not None:
                    rows = ledger_rows(ledger_path) or rows
                    delete_owned_rows(rows)
                    wait_for("fallback kind cleanup", lambda: kind_clusters() == prior_clusters)
                    for kind, identity, _state in rows:
                        if kind in AWS_KINDS:
                            wait_for(
                                f"fallback direct absence of {kind}",
                                lambda kind=kind, identity=identity: direct_aws_resource_is_absent(
                                    kind, identity
                                ),
                            )
                    wait_for(
                        "fallback empty owned AWS tag inventory",
                        lambda: tagged_resources() == [],
                    )
                if stderr_path.exists():
                    private_match = re.search(
                        r"(?m)^provider-harness-private-diagnostics: (.+)$",
                        stderr_path.read_text(encoding="utf-8", errors="replace"),
                    )
                    if private_match is not None:
                        shutil.rmtree(pathlib.Path(private_match.group(1)), ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
