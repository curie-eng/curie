"""Opt-in Helm values. @spec SRE-SCHEDULED-JOBS c1, c2, c3, c4, c5"""

from __future__ import annotations

import argparse
import json
import re


def scheduled_job_group(namespace: str, description: str) -> dict:
    """@spec SRE-SCHEDULED-JOBS c1, c2, c3"""
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace):
        raise ValueError("namespace must be a Kubernetes DNS label")
    if not description.strip():
        raise ValueError("description must state the impact of an unhealthy job")
    selector = '{namespace="' + namespace + '"}'
    schedule = "kube_cronjob_status_last_schedule_time" + selector
    success = "kube_cronjob_status_last_successful_time" + selector
    rules = []
    for name, expression, hold, summary in (
        (
            "CurieScheduledJobSuspended",
            "kube_cronjob_spec_suspend" + selector + " == 1",
            "1m",
            "Scheduled job {{ $labels.cronjob }} is suspended",
        ),
        (
            "CurieScheduledJobFailing",
            f"({schedule} - {success}) > 0 or ({schedule} unless {success})",
            "15m",
            "Scheduled job {{ $labels.cronjob }}'s last run did not succeed",
        ),
    ):
        rules.append(
            {
                "alert": name,
                "expr": expression,
                "for": hold,
                "labels": {"severity": "page", "component": "scheduled-job"},
                "annotations": {"summary": summary, "description": description},
            }
        )
    return {"name": "curie-scheduled-jobs", "rules": rules}


def main() -> int:
    """@spec SRE-SCHEDULED-JOBS c3, c4, c5"""
    parser = argparse.ArgumentParser(
        description="Render opt-in scheduled job alerts as Helm values"
    )
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--description", required=True)
    args = parser.parse_args()
    try:
        group = scheduled_job_group(args.namespace, args.description)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps({"serverFiles": {"alerts": {"groups": [group]}}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
