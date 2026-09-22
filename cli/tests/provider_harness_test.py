"""Focused contracts for the provider harness safety boundary."""

import hashlib
import importlib.util
import json
import pathlib
import signal
import sqlite3
import stat
import sys
import tempfile
import unittest

HARNESS_PATH = pathlib.Path(__file__).parents[1] / "scripts" / "provider_harness.py"
SPEC = importlib.util.spec_from_file_location("provider_harness", HARNESS_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load provider harness from {HARNESS_PATH}")
provider_harness = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = provider_harness
SPEC.loader.exec_module(provider_harness)


class ProviderHarnessContracts(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
