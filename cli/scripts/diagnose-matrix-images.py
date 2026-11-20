"""Capture CRI references for the matrix observer's preserved refusal sample."""

import json
import subprocess
import sys
from pathlib import Path


def repository(reference: str) -> str:
    value = reference.removeprefix("docker-pullable://").split("@", 1)[0]
    first, _, last = value.rpartition("/")
    name = last.split(":", 1)[0]
    if not first:
        return f"docker.io/library/{name}"
    registry = first.split("/", 1)[0]
    if "." not in registry and ":" not in registry and registry != "localhost":
        first = f"docker.io/{first}"
    return f"{first}/{name}"


def run(*args: str) -> str:
    return subprocess.run(args, capture_output=True, check=True, text=True, timeout=5).stdout


def main() -> None:
    evidence = Path(sys.argv[1])
    cluster = sys.argv[2]
    upgrade = json.loads((evidence / "last-upgrade.json").read_text())
    convergence = upgrade.get("convergence", {})
    if upgrade.get("phase") != "converge" or convergence.get("images") is not False:
        return
    marker = "matrix-image-observation "
    lines = (evidence / "last-upgrade.err").read_text().splitlines()
    samples = [json.loads(line[len(marker) :]) for line in lines if line.startswith(marker)]
    result = {"observer_sample": samples[-1] if samples else None, "cri": {}}
    path = evidence / "pre-teardown-image-binding.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    if not samples:
        return
    owned_nodes = run("kind", "get", "nodes", "--name", cluster).splitlines()
    for node in owned_nodes:
        pod_images = [row for row in samples[-1]["images"] if row.get("pod_node") == node]
        if not pod_images:
            continue
        refs = {
            row[key]
            for row in pod_images
            for key in ("target_image", "status_image", "image_id")
            if row.get(key)
        }
        repos = {repository(ref) for ref in refs}
        digests = {ref.partition("@")[2] for ref in refs if "@" in ref}
        cri = json.loads(run("docker", "exec", node, "crictl", "images", "-o", "json"))
        rows = []
        for image in cri["images"]:
            tags = image.get("repoTags", [])
            repo_digests = image.get("repoDigests", [])
            if not any(
                repository(ref) in repos or ref.partition("@")[2] in digests
                for ref in tags + repo_digests
            ):
                continue
            rows.append({
                "id": image["id"],
                "repo_tags": tags[:20],
                "repo_digests": repo_digests[:20],
                "size_bytes": image["size"],
            })
        result["cri"][node] = {"image_row_count": len(cri["images"]), "rows": rows[:50]}
        path.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"image evidence unavailable ({type(error).__name__})", file=sys.stderr)
        sys.exit(1)
