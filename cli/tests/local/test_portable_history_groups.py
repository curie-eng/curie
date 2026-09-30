"""Local fix pin: real candidate SDK transport, scripted external provider only.

Issue #3628: observed SDK0.2.159 / CLI2.1.281 outgoing native model requests
lost interleaved results absent shared assistant message.id. Fixture uses the
observed message_start.message.id and exact tool_use/tool_result wire fields.
No fabricated AssistantMessage attribute or normalization algorithm stands in
for the candidate session capture and native CLI's first resume request.
"""

import hashlib
import json
import pathlib
import subprocess
import uuid

import pytest

REPO = pathlib.Path(__file__).parents[3]
FIXTURES = pathlib.Path(__file__).parent


def _run(argv, timeout=300):
    return subprocess.run(
        argv, cwd=REPO, text=True, capture_output=True, timeout=timeout, check=False
    )


def _require(result, purpose):
    if result.returncode:
        raise RuntimeError(f"{purpose}: exit {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def _source_identity():
    return {
        str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in [
            REPO / "uv.lock",
            REPO / "runner/Dockerfile",
            REPO / "runner/export_dependency_pins.py",
            *sorted((REPO / "runner/src").rglob("*.py")),
        ]
    }


@pytest.fixture
def native_runtime(tmp_path):
    label = f"curie-check-3628-{uuid.uuid4().hex[:12]}"
    image = f"{label}:candidate"
    network = label
    provider = f"{label}-provider"
    candidate = f"{label}-candidate"
    proof = tmp_path / "proof"
    proof.mkdir(mode=0o777)
    proof.chmod(0o777)
    owned = []
    network_id = None
    image_id = None
    try:
        _require(_run(["docker", "info"]), "Docker prerequisite")
        sources = _source_identity()
        # Always build this checkout; an unrelated pre-existing image cannot make
        # the fix pin green. Source hashes are recorded alongside the image ID.
        _require(
            _run(["docker", "build", "-f", "runner/Dockerfile", "-t", image, "."], timeout=1200),
            "source runner build",
        )
        image_id = _require(
            _run(["docker", "image", "inspect", image, "--format", "{{.Id}}"]),
            "candidate image identity",
        )
        network_id = _require(
            _run(
                ["docker", "network", "create", "--internal", "--label", f"task={label}", network]
            ),
            "owned internal network",
        )
        shared = [
            "--label",
            f"task={label}",
            "--network",
            network,
            "--mount",
            f"type=bind,src={FIXTURES},dst=/fixture,readonly",
            "--mount",
            f"type=bind,src={proof},dst=/proof",
        ]
        owned.append(
            _require(
                _run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        provider,
                        "--network-alias",
                        "acme-provider",
                        *shared,
                        "--entrypoint",
                        "/app/.venv/bin/python",
                        image,
                        "/fixture/portable_groups_provider.py",
                    ]
                ),
                "provider startup",
            )
        )
        owned.append(
            _require(
                _run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        candidate,
                        *shared,
                        "--entrypoint",
                        "/bin/sleep",
                        image,
                        "infinity",
                    ]
                ),
                "candidate startup",
            )
        )
        _require(
            _run(
                [
                    "docker",
                    "exec",
                    candidate,
                    "python",
                    "-c",
                    "import urllib.request,time\n"
                    "for n in range(30):\n"
                    " try: urllib.request.urlopen('http://acme-provider:18579',timeout=2);break\n"
                    " except OSError: time.sleep(.2)\n"
                    "else: raise RuntimeError('provider unavailable')",
                ]
            ),
            "provider readiness",
        )
        if _source_identity() != sources:
            raise RuntimeError("candidate source changed during the build/run")
        installed = json.loads(
            _require(
                _run(
                    [
                        "docker",
                        "exec",
                        candidate,
                        "python",
                        "-c",
                        "import pathlib,hashlib,json,curie_runner;"
                        "root=pathlib.Path(curie_runner.__file__).parent;"
                        "print(json.dumps({'runner/src/curie_runner/'+str(p.relative_to(root)):"
                        "hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*.py')}))",
                    ]
                ),
                "installed candidate source provenance",
            )
        )
        expected_source = {k: v for k, v in sources.items() if k.startswith("runner/src/")}
        if installed != expected_source:
            raise RuntimeError("installed image source differs from candidate checkout")
        proof.joinpath("provenance.json").write_text(
            json.dumps(
                {
                    "image_id": image_id,
                    "sources": sources,
                    "source_head": _require(_run(["git", "rev-parse", "HEAD"]), "source HEAD"),
                }
            )
        )
        _require(
            _run(
                ["docker", "exec", candidate, "python", "/fixture/portable_groups_probe.py"],
                timeout=240,
            ),
            "actual candidate SDK capture/replay",
        )
        data = json.loads(proof.joinpath("proof.json").read_text())
        if data["sdk"] != "0.2.159" or data["cli"] != "2.1.281":
            raise RuntimeError("native fixture SDK/CLI version differs from measured pins")
        messages = data["portable"]["messages"]
        expected = _results(messages)
        if (
            len(_result_blocks(messages)) != 3
            or set(expected) != {"call-acme-1", "call-acme-2", "call-acme-3"}
            or not all(
                "meaningful portable evidence" in json.dumps(result)
                and result.get("is_error") is not True
                for result in expected.values()
            )
        ):
            raise RuntimeError(
                "actual MCP fixture did not return all three successful result bytes"
            )
        sequence = [
            (b["type"], b.get("id", b.get("tool_use_id")))
            for m in messages
            if isinstance(m["content"], list)
            for b in m["content"]
            if b["type"] in {"tool_use", "tool_result"}
        ]
        if sequence.index(("tool_result", "call-acme-1")) >= sequence.index(
            ("tool_use", "call-acme-3")
        ):
            raise RuntimeError("native capture failed to exercise actual result/call interleaving")
        requests = [
            json.loads(line) for line in proof.joinpath("requests.jsonl").read_text().splitlines()
        ]
        yield data, requests, expected
    finally:
        # Register cleanup before any startup. Every process gets fresh state;
        # errors here are fixture errors, never skips or green behavior claims.
        errors = []
        # A failed docker run can leave a created container before returning its
        # ID. The unique task label owns that partial startup too.
        listed = _run(["docker", "ps", "-aq", "--filter", f"label=task={label}"])
        if listed.returncode:
            errors.append(listed.stderr)
        else:
            owned.extend(
                identifier for identifier in listed.stdout.splitlines() if identifier not in owned
            )
        for container_id in reversed(owned):
            result = _run(["docker", "rm", "-f", container_id])
            if result.returncode:
                errors.append(result.stderr)
        if network_id:
            result = _run(["docker", "network", "rm", network_id])
            if result.returncode:
                errors.append(result.stderr)
        remaining = _require(
            _run(["docker", "ps", "-aq", "--filter", f"label=task={label}"]),
            "cleanup container verification",
        )
        remaining_networks = _require(
            _run(["docker", "network", "ls", "-q", "--filter", f"label=task={label}"]),
            "cleanup network verification",
        )
        if remaining or remaining_networks:
            errors.append(f"owned resources remain: {remaining} {remaining_networks}")
        if image_id:
            result = _run(["docker", "image", "rm", image])
            if result.returncode:
                errors.append(result.stderr)
        if errors:
            raise RuntimeError("owned fixture cleanup failed: " + "; ".join(errors))


def _request_for(requests, text):
    for row in requests:
        if "count_tokens" in row["path"]:
            continue
        messages = row["request"]["messages"]
        if text in json.dumps(messages[-1]):
            return messages
    raise AssertionError(f"no native provider request for {text}")


def _result_blocks(messages):
    return [
        block
        for message in messages
        if isinstance(message.get("content"), list)
        for block in message["content"]
        if block.get("type") == "tool_result"
    ]


def _results(messages):
    return {block["tool_use_id"]: block for block in _result_blocks(messages)}


def _assert_three_unique_results(messages):
    blocks = _result_blocks(messages)
    assert len(blocks) == 3
    assert sorted(block["tool_use_id"] for block in blocks) == [
        "call-acme-1",
        "call-acme-2",
        "call-acme-3",
    ]


def test_fresh_native_resume_preserves_interleaved_tool_results(native_runtime):
    proof, requests, expected = native_runtime
    assert proof["portable"]["harness_replay"] is None
    ordered = _request_for(requests, "acme resume ordered")
    _assert_three_unique_results(ordered)
    assert _results(ordered) == expected
    grouped = _request_for(requests, "acme resume grouped")
    _assert_three_unique_results(grouped)
    assert _results(grouped) == expected
    assert "[Tool result missing due to internal error]" not in json.dumps(grouped)
    # @spec RUNNER-HISTORY-GROUP-4: without its groups the turn is text only.
    assert proof["stripped_rows"] >= 1 and proof["stripped_tool_rows"] == 0
