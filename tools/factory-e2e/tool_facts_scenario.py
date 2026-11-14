"""Black-box selected-image observations in the owned scripted install."""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote, urlencode


def assert_tools(answer: dict[str, Any], expected: dict[str, str]) -> None:
    tools = {entry["name"]: entry for entry in answer["executables"]}
    if set(tools) != {*expected, "acme-outside-catalog"}:
        raise RuntimeError("facts returned a different executable request")
    for name, state in expected.items():
        entry = tools[name]
        reason = "unreadable" if state == "unknown" else None
        basis = None if state == "unknown" else "selected_image_probe"
        if (entry["state"], entry["reason"], entry["basis"]) != (state, reason, basis):
            raise RuntimeError("facts disagreed with the selected-image lookup")
    outside = tools["acme-outside-catalog"]
    if (outside["state"], outside["reason"]) != ("unknown", "outside_catalog"):
        raise RuntimeError("facts turned an outside-catalog name into an observation")
    if answer["admission"] != "not_requested":
        raise RuntimeError("facts started a factory execution")


def run_observations(driver: Any) -> None:
    for name in driver.facts_runner_images:
        driver.facts_api(
            "POST",
            "/agents",
            {
                "name": name,
                "repo_full_name": "acme-corp/acme-bot",
                "channel": {
                    "kind": "github",
                    "address": "acme-corp/acme-bot",
                    "trigger_label": name,
                },
            },
        )
    references = {}
    for name, image in driver.facts_runner_images.items():
        pod = name + "-reference"
        script = (
            "for name in jq sh; do "
            'if command -v "$name" >/dev/null 2>&1; then state=present; else state=absent; fi; '
            'printf "%s=%s\\n" "$name" "$state"; done'
        )
        driver.kubectl(
            "-n",
            driver.namespace,
            "run",
            pod,
            "--restart=Never",
            "--image",
            image,
            "--command",
            "--",
            "/bin/sh",
            "-c",
            script,
        )
        driver.kubectl(
            "-n",
            driver.namespace,
            "wait",
            "--for=jsonpath={.status.phase}=Succeeded",
            f"pod/{pod}",
            "--timeout=120s",
        )
        output = driver.kubectl("-n", driver.namespace, "logs", pod)
        entries = dict(line.split("=", 1) for line in output.splitlines())
        expected = {"jq": "absent" if name.endswith("absent") else "present", "sh": "present"}
        if entries != expected:
            raise RuntimeError("reference images do not provide the positive and negative controls")
        references[name] = entries
        driver.kubectl("-n", driver.namespace, "delete", "pod", pod, "--wait=true")
    with watch_claims(driver) as observed_claims:
        driver.kubectl("-n", driver.namespace, "scale", "deployment/curie-worker", "--replicas=1")
        driver.kubectl(
            "-n", driver.namespace, "rollout", "status", "deployment/curie-worker", "--timeout=300s"
        )
        while time.monotonic() < driver.deadline:
            counts = driver.sql(
                "SELECT json_build_object('observed',count(DISTINCT agent_id)) "
                "FROM curie.factory_fact_observations WHERE kind='tool' AND expires_at > now()"
            )
            if counts["observed"] == 3:
                break
            time.sleep(1)
        else:
            raise RuntimeError("worker did not observe every factory subject")
        answers = {}
        for label, expected in [
            *references.items(),
            ("factory", {"jq": "unknown", "sh": "unknown"}),
        ]:
            for _ in range(3):
                query = urlencode(
                    {
                        "repository": "acme-corp/acme-bot",
                        "trigger_label": label,
                        "executables": "jq,sh,acme-outside-catalog",
                    }
                )
                answer = driver.facts_api("GET", "/factory/facts?" + query)
                assert_tools(answer, expected)
            answers[label] = answer
        rows = driver.sql(
            "SELECT json_build_object('positive_tag_rows',count(*) FILTER "
            "(WHERE a.name='acme-facts' AND o.state IN ('present','absent')),"
            "'probe_identities',count(DISTINCT o.subject_digest) FILTER "
            "(WHERE o.state IN ('present','absent'))) "
            "FROM curie.factory_fact_observations o JOIN curie.agents a ON a.id=o.agent_id "
            "WHERE o.kind='tool'"
        )
        if rows != {"positive_tag_rows": 0, "probe_identities": 2}:
            raise RuntimeError("tool observations were not bound to the two declared images")
        pods = json.loads(driver.kubectl("-n", driver.namespace, "get", "pods", "-o", "json"))
        claims = json.loads(
            driver.kubectl("-n", driver.namespace, "get", "sandboxclaims", "-o", "json")
        )
        if claims["items"] or any(
            pod["metadata"]["name"].startswith("curie-tool-observer-") for pod in pods["items"]
        ):
            raise RuntimeError("observation claim release was not confirmed")
        work = driver.sql(
            "SELECT json_build_object('items',(SELECT count(*) FROM curie.work_items),"
            "'requests',(SELECT count(*) FROM curie.execution_requests))"
        )
        if (
            work != {"items": 0, "requests": 0}
            or driver.stub.unknown_requests
            or driver.model is not None
        ):
            raise RuntimeError("tool facts crossed the no-execution boundary")
    if len(observed_claims) != 2:
        raise RuntimeError("requests changed the number of selected-image probes")
    driver.evidence.update(
        tool_facts=answers,
        reference_lookups=references,
        probe_subjects=len(observed_claims),
        tag_only_positive_rows=0,
        model_requests=[],
    )


@contextmanager
def watch_claims(driver: Any) -> Iterator[set[str]]:
    current = json.loads(
        driver.kubectl("-n", driver.namespace, "get", "sandboxclaims", "-o", "json")
    )
    version = current["metadata"]["resourceVersion"]
    watch_path = (
        "/apis/extensions.agents.x-k8s.io/v1alpha1/namespaces/"
        + quote(driver.namespace, safe="")
        + "/sandboxclaims?"
        + urlencode({"watch": "true", "resourceVersion": version})
    )
    output = driver.workdir / "probe-watch.json"
    errors = driver.workdir / "probe-watch-errors.txt"
    seen: set[str] = set()
    with output.open("w") as stdout, errors.open("w") as stderr:
        watcher = subprocess.Popen(
            [
                "kubectl",
                "--context",
                driver.context,
                "get",
                "--raw",
                watch_path,
                f"--request-timeout={max(1, int(driver.deadline - time.monotonic()))}s",
            ],
            env=driver.env,
            stdout=stdout,
            stderr=stderr,
        )
        try:
            yield seen
        finally:
            stopped_early = watcher.poll() is not None
            watcher.terminate()
            try:
                watcher.wait(timeout=10)
            except subprocess.TimeoutExpired:
                watcher.kill()
                watcher.wait(timeout=10)
                raise RuntimeError("claim observation watch did not stop") from None
    if stopped_early:
        raise RuntimeError("claim observation watch ended before verification")
    raw = output.read_text()
    decoder = json.JSONDecoder()
    while raw.strip():
        event, end = decoder.raw_decode(raw.lstrip())
        raw = raw.lstrip()[end:]
        if event.get("type") == "ERROR":
            raise RuntimeError("claim observation watch lost its event history")
        name = event.get("object", {}).get("metadata", {}).get("name", "")
        if event.get("type") == "ADDED" and name.startswith("curie-tool-observer-"):
            seen.add(name)
