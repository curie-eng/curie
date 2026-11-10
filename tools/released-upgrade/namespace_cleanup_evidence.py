"""Read only CI evidence. @spec NAMESPACE-CLEANUP-EVIDENCE-1 to NAMESPACE-CLEANUP-EVIDENCE-4."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from collections import Counter
from typing import Any

_NAMESPACE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z", re.ASCII)
_RESOURCE = re.compile(r"[a-z][a-z0-9-]*(?:\.[a-z0-9][a-z0-9-]*)*\Z", re.ASCII)
_SYMBOL = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}\Z", re.ASCII)
_PROJECTION = (
    "[{{range $i,$item := .items}}{{if $i}},{{end}}"
    '{"finalizers":[{{range $j,$f := $item.metadata.finalizers}}'
    '{{if $j}},{{end}}{{printf "%q" $f}}{{end}}],'
    '"deleting":{{if $item.metadata.deletionTimestamp}}true{{else}}false{{end}},'
    '"owner_kinds":[{{range $k,$o := $item.metadata.ownerReferences}}'
    '{{if $k}},{{end}}{{printf "%q" $o.kind}}{{end}}]}{{end}}]'
)


def _run(command: list[str], timeout_s: float) -> subprocess.CompletedProcess[str]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-3."""
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_s,
        check=False,
    )


def _symbol(value: str) -> str:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    return value if _SYMBOL.fullmatch(value) else "unrecognized"


def _object(value: Any) -> dict[str, Any]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    if not isinstance(value, dict):
        raise ValueError("invalid_projection")
    return value


def _list(value: Any, *, limit: int = 64) -> list[Any]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
    if not isinstance(value, list) or len(value) > limit:
        raise ValueError("invalid_projection")
    return value


def _text(value: Any) -> str:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    if not isinstance(value, str):
        raise ValueError("invalid_projection")
    return _symbol(value)


def _symbols(value: Any) -> list[str]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    return [_text(item) for item in _list(value)]


def _condition(value: Any) -> dict[str, str]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    item = _object(value)
    return {key: _text(item.get(key, "Unknown")) for key in ("type", "status", "reason")}


def _namespace(value: Any, result: dict[str, Any]) -> None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    item = _object(value)
    metadata = _object(item.get("metadata"))
    spec, status = _object(item.get("spec", {})), _object(item.get("status"))
    # Parse everything before publishing any part of a possibly malformed observation.
    phase = _text(status.get("phase"))
    finalizers = _symbols(metadata.get("finalizers", [])) + _symbols(spec.get("finalizers", []))
    conditions = [_condition(condition) for condition in _list(status.get("conditions", []))]
    timestamp = metadata.get("deletionTimestamp")
    if timestamp is not None and not isinstance(timestamp, str):
        raise ValueError("invalid_projection")
    deleting = timestamp is not None
    result.update(
        phase=phase, deleting=deleting, finalizers=sorted(set(finalizers)), conditions=conditions
    )


def _resources(value: Any, kind: str) -> dict[str, Any] | None:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    items = _list(value, limit=1024)
    finalizers: Counter[str] = Counter()
    owners: Counter[str] = Counter()
    deleting = 0
    for value in items:
        item = _object(value)
        if (
            set(item) != {"finalizers", "deleting", "owner_kinds"}
            or type(item["deleting"]) is not bool
        ):
            raise ValueError("invalid_projection")
        finalizers.update(_symbols(item["finalizers"]))
        owners.update(_symbols(item["owner_kinds"]))
        deleting += item["deleting"]
    if not items:
        return None
    return dict(
        kind=kind,
        count=len(items),
        deleting=deleting,
        finalizers=dict(sorted(finalizers.items())),
        owner_kinds=dict(sorted(owners.items())),
    )


def _services(value: Any) -> list[dict[str, str]]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
    result = []
    for entry in _list(_object(value).get("items"), limit=1024):
        item = _object(entry)
        name = _text(_object(item.get("metadata")).get("name"))
        status = _object(item.get("status", {}))
        conditions = [_condition(value) for value in _list(status.get("conditions", []))]
        available = [condition for condition in conditions if condition["type"] == "Available"]
        if len(available) > 1:
            raise ValueError("invalid_projection")
        condition = available[0] if available else {"status": "Unknown", "reason": "Unknown"}
        if condition["status"] != "True":
            result.append(dict(name=name, status=condition["status"], reason=condition["reason"]))
    return result


def _reject_constant(value: str) -> Any:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-2."""
    raise ValueError("invalid_projection")


class _Collector:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-3."""

    def __init__(self, context: str, budget_s: float) -> None:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-3."""
        self.context = context
        self.deadline = time.monotonic() + budget_s
        self.report: dict[str, Any] = dict(
            schema_version=1,
            complete=True,
            namespaces=[],
            api_services=[],
            errors=[],
        )

    def error(self, errors: list[Any], operation: str, reason: str) -> None:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-2 NAMESPACE-CLEANUP-EVIDENCE-3."""
        self.report["complete"] = False
        error = dict(operation=operation, reason=reason)
        if error not in errors:
            errors.append(error)

    def query(
        self, arguments: list[str], errors: list[Any], operation: str, *, raw: bool = False
    ) -> Any:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1 to NAMESPACE-CLEANUP-EVIDENCE-3."""
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            self.error(errors, operation, "budget_exhausted")
            return None
        try:
            completed = _run(["kubectl", "--context", self.context, *arguments], min(5, remaining))
            if completed.returncode != 0:
                reason = "not_found" if "(NotFound)" in completed.stderr else "command_error"
                self.error(errors, operation, reason)
                return None
            output = completed.stdout
            if not isinstance(output, str):
                raise ValueError("invalid_projection")
            if len(output.encode("utf-8")) > 1024 * 1024:
                self.error(errors, operation, "oversized_projection")
                return None
            if raw:
                return output
            value = json.loads(output, parse_constant=_reject_constant)
            if value is None:
                raise ValueError("invalid_projection")
            return value
        except subprocess.TimeoutExpired:
            self.error(errors, operation, "timeout")
        except (OSError, subprocess.SubprocessError, UnicodeError):
            self.error(errors, operation, "command_error")
        except (ValueError, RecursionError):
            self.error(errors, operation, "invalid_projection")
        return None

    def discover(self) -> list[str]:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-3."""
        errors = self.report["errors"]
        output = self.query(
            ["api-resources", "--namespaced=true", "--verbs=list", "-o", "name"],
            errors,
            "discovery",
            raw=True,
        )
        if output is None:
            return []
        names = output.splitlines()
        if len(names) > 128:
            self.error(errors, "discovery", "resource_limit")
        valid = []
        for name in names[:128]:
            if len(name) > 256 or not _RESOURCE.fullmatch(name) or name in valid:
                self.error(errors, "discovery", "invalid_projection")
            else:
                valid.append(name)
        return valid

    def namespace(self, name: str, kinds: list[str]) -> None:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1 to NAMESPACE-CLEANUP-EVIDENCE-3."""
        result: dict[str, Any] = dict(
            namespace=name,
            phase="Unknown",
            deleting=None,
            finalizers=[],
            conditions=[],
            resources=[],
            errors=[],
        )
        self.report["namespaces"].append(result)
        errors = result["errors"]
        value = self.query(["get", "namespace", name, "-o", "json"], errors, "namespace")
        if value is not None:
            try:
                _namespace(value, result)
            except (ValueError, RecursionError):
                self.error(errors, "namespace", "invalid_projection")
        for kind in kinds:
            if time.monotonic() >= self.deadline:
                self.error(errors, "resources", "budget_exhausted")
                break
            value = self.query(
                ["get", kind, "-n", name, "-o", "go-template=" + _PROJECTION],
                errors,
                "resources",
            )
            if value is not None:
                try:
                    summary = _resources(value, kind)
                    if summary is not None:
                        result["resources"].append(summary)
                except (ValueError, RecursionError):
                    self.error(errors, "resources", "invalid_projection")


def collect_cleanup_evidence(
    context: str,
    namespaces: list[str],
    *,
    budget_s: float = 40,
) -> dict[str, Any]:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 to NAMESPACE-CLEANUP-EVIDENCE-3."""
    if (
        not isinstance(context, str)
        or not context.strip()
        or not isinstance(namespaces, (list, tuple))
        or not 1 <= len(namespaces) <= 4
        or any(not isinstance(name, str) or not _NAMESPACE.fullmatch(name) for name in namespaces)
        or len(set(namespaces)) != len(namespaces)
        or type(budget_s) not in (int, float)
        or not 1 <= budget_s <= 60
    ):
        raise ValueError("invalid_cleanup_evidence_input") from None
    collector = _Collector(context, budget_s)
    kinds = collector.discover()
    for name in namespaces:
        collector.namespace(name, kinds)
    errors = collector.report["errors"]
    value = collector.query(["get", "apiservices", "-o", "json"], errors, "api_services")
    if value is not None:
        try:
            collector.report["api_services"] = _services(value)
        except (ValueError, RecursionError):
            collector.error(errors, "api_services", "invalid_projection")
    return collector.report


class _Parser(argparse.ArgumentParser):
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1."""

    def error(self, message: str) -> Any:
        """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-2."""
        raise ValueError("invalid_cleanup_evidence_input") from None


def main(argv: list[str] | None = None) -> int:
    """@spec NAMESPACE-CLEANUP-EVIDENCE-1 NAMESPACE-CLEANUP-EVIDENCE-4."""
    parser = _Parser(description="Read only namespace cleanup evidence")
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", action="append", required=True)
    parser.add_argument("--budget-seconds", type=float, default=40)
    try:
        arguments = parser.parse_args(argv)
        report = collect_cleanup_evidence(
            arguments.context,
            arguments.namespace,
            budget_s=arguments.budget_seconds,
        )
    except ValueError:
        report = dict(
            schema_version=1,
            complete=False,
            namespaces=[],
            api_services=[],
            errors=[dict(operation="input", reason="invalid_cleanup_evidence_input")],
        )
    print(json.dumps(report, sort_keys=True))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
