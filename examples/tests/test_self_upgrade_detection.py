"""The self-upgrade job's two pure decisions, tested without a network.

Both are cheap to get subtly wrong in the direction that reports success:

* "am I behind" has three answers, not two. A version deployed by hand from a
  working copy records no commit, and that must read as UNKNOWN rather than as
  up to date -- otherwise the job goes quiet forever on exactly the install where
  someone deployed once by hand.
* re-taring a subdirectory out of a repository tarball has to drop everything
  outside it, including symlinks, which are how a tar extraction escapes.
"""

import io
import json
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sre-bot" / "self-upgrade"))

from redeploy import (  # noqa: E402
    BUNDLE_PREFIX,
    SelfUpgradeError,
    bundle_from_repo_tarball,
    deploy,
    deployed_commit,
    member_of,
    pin_build_connectors,
    replace_member,
    upgrade_disposition,
)


def _repo_tarball(files: dict[str, bytes], root: str = "curie-eng-curie-abc1234") -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(f"{root}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


def _names(bundle: bytes) -> list[str]:
    with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as tar:
        return sorted(m.name for m in tar.getmembers())


def test_the_bundle_is_lifted_out_of_the_repository_tarball() -> None:
    bundle = bundle_from_repo_tarball(
        _repo_tarball(
            {
                f"{BUNDLE_PREFIX}/.claude-plugin/plugin.json": b"{}",
                f"{BUNDLE_PREFIX}/skills/sre-bot/SKILL.md": b"# skill",
                f"{BUNDLE_PREFIX}/evals/cases.json": b"[]",
            }
        )
    )
    assert _names(bundle) == [
        ".claude-plugin/plugin.json",
        "evals/cases.json",
        "skills/sre-bot/SKILL.md",
    ]


def test_everything_outside_the_bundle_is_left_behind() -> None:
    # The repository is a monorepo: the platform's own source sits beside the
    # bundle and must not be packaged into an agent version.
    bundle = bundle_from_repo_tarball(
        _repo_tarball(
            {
                f"{BUNDLE_PREFIX}/.claude-plugin/plugin.json": b"{}",
                "apps/api/src/curie_api/main.py": b"# not the bundle",
                "README.md": b"# not the bundle",
                "examples/weather/connectors.yaml": b"# another bundle",
            }
        )
    )
    assert _names(bundle) == [".claude-plugin/plugin.json"]


def test_the_sha_carrying_root_is_discovered_not_assumed() -> None:
    # GitHub names the top-level directory after the ref, so it cannot be hardcoded.
    bundle = bundle_from_repo_tarball(
        _repo_tarball(
            {f"{BUNDLE_PREFIX}/.claude-plugin/plugin.json": b"{}"},
            root="curie-eng-curie-deadbeefcafe",
        )
    )
    assert _names(bundle) == [".claude-plugin/plugin.json"]


def test_an_empty_result_is_an_error_rather_than_an_empty_bundle() -> None:
    # Uploading an empty bundle would succeed and leave the agent with nothing.
    with pytest.raises(SelfUpgradeError, match="no bundle files"):
        bundle_from_repo_tarball(_repo_tarball({"README.md": b"# only this"}))


def test_a_symlink_inside_the_bundle_is_dropped() -> None:
    out = io.BytesIO()
    root = "curie-eng-curie-abc1234"
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        info = tarfile.TarInfo(f"{root}/{BUNDLE_PREFIX}/.claude-plugin/plugin.json")
        info.size = 2
        tar.addfile(info, io.BytesIO(b"{}"))
        link = tarfile.TarInfo(f"{root}/{BUNDLE_PREFIX}/skills/escape")
        link.type = tarfile.SYMTYPE
        link.linkname = "../../../../etc/passwd"
        tar.addfile(link)
    assert _names(bundle_from_repo_tarball(out.getvalue())) == [".claude-plugin/plugin.json"]


# --- pinning images from the commit being deployed ---------------------------
#
# The repository declares `build:` for its connectors, which records a LOCAL
# image id the cluster tier refuses. `release.yaml` publishes each connector on
# every release-branch push tagged `sha-<commit>`, so the images that belong with
# a bundle are derivable from the bundle's own commit.
#
# The second substitution carries runtime connector environment across the
# immutable rebuild rather than reverting it to repository defaults.


def _bundle(files: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        for name, body in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return out.getvalue()


def _read(bundle: bytes) -> dict[str, bytes]:
    with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as tar:
        return {
            member.name: tar.extractfile(member).read()
            for member in tar.getmembers()
            if member.isfile()
        }


DECLARATION = b"""connectors:
  kubernetes:
    image: ghcr.io/containers/kubernetes-mcp-server@sha256:aaa
  tempo:
    build:
      context: connectors/tempo
    env:
      TEMPO_URL: http://tempo.observability.svc.cluster.local:3200
"""


@pytest.fixture
def offline_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """No registry in a unit test; the resolution itself is exercised live."""
    import redeploy

    monkeypatch.setattr(redeploy, "resolve_digest", lambda _c, _commit: "sha256:fixture")


def test_a_build_connector_is_pinned_to_the_commits_published_image(
    offline_registry: None,
) -> None:
    parsed = yaml.safe_load(pin_build_connectors(DECLARATION, "c" * 40, {"tempo": {}}))
    tempo = parsed["connectors"]["tempo"]
    assert "build" not in tempo, "a build declaration cannot reach a cluster deploy"
    assert tempo["image"] == ("ghcr.io/curie-eng/curie-sre-bot-tempo@sha256:fixture")
    # An already-pinned connector is left alone rather than re-resolved.
    assert parsed["connectors"]["kubernetes"]["image"].endswith("@sha256:aaa")


def test_the_running_connector_environment_survives_the_upgrade(
    offline_registry: None,
) -> None:
    parsed = yaml.safe_load(
        pin_build_connectors(
            DECLARATION,
            "c" * 40,
            {"tempo": {"TEMPO_URL": "http://tempo.custom.svc.cluster.local:3200"}},
        )
    )
    assert parsed["connectors"]["tempo"]["env"]["TEMPO_URL"] == (
        "http://tempo.custom.svc.cluster.local:3200"
    )


REVOKE = b"""connectors:
  self-upgrade:
    build:
      context: connectors/self-upgrade
    env:
      SELF_UPGRADE_CRONJOB: sre-bot-self-upgrade
      PLATFORM_UPGRADE_CRONJOB: ""
      K8S_WRITE_ALLOWLIST: ns-a/deploy-a
"""


def test_a_carried_env_cannot_override_an_incoming_empty_value(
    offline_registry: None,
) -> None:
    """A commit that revokes a grant by resetting env to empty must land.

    connectors.yaml expresses "not granted" as an empty string. Carrying the
    running value over that unconditionally made upgrade_self unable to
    narrow a ceiling (curie#2292).
    """

    parsed = yaml.safe_load(
        pin_build_connectors(
            REVOKE,
            "c" * 40,
            {
                "self-upgrade": {
                    "PLATFORM_UPGRADE_CRONJOB": "sre-bot-platform-upgrade",
                    "K8S_WRITE_ALLOWLIST": "ns-a/deploy-a,ns-b/deploy-b",
                }
            },
        )
    )
    env = parsed["connectors"]["self-upgrade"]["env"]
    assert env["PLATFORM_UPGRADE_CRONJOB"] == "", (
        "a carried grant must not override an incoming empty value; empty is "
        "how the declaration revokes the capability"
    )
    assert env["K8S_WRITE_ALLOWLIST"] == "ns-a/deploy-a", (
        "a carried allowlist must not override a narrower incoming ceiling"
    )
    assert env["SELF_UPGRADE_CRONJOB"] == "sre-bot-self-upgrade"


def test_replacing_one_member_leaves_the_others_byte_for_byte() -> None:
    original = _bundle({"connectors.yaml": b"old", "skills/sre-bot/SKILL.md": b"the skill"})
    swapped = _read(replace_member(original, "connectors.yaml", b"new"))
    assert swapped["connectors.yaml"] == b"new"
    assert swapped["skills/sre-bot/SKILL.md"] == b"the skill"


def test_a_missing_member_is_an_error_rather_than_an_empty_file() -> None:
    with pytest.raises(SelfUpgradeError):
        member_of(_bundle({"skills/sre-bot/SKILL.md": b"x"}), "connectors.yaml")


# --- the version being SERVED, not the newest row -----------------------------
#
# Those are different facts. A version can be created and never deployed, and
# reading that one made the job ask for a connector surface that does not exist
# -- "no bundle stored for this version", which reads like a broken agent rather
# than a question asked about the wrong row.


class _FakeApi:
    """Answers the three GETs deployed_commit makes, in order."""

    def __init__(self, deployments: list[dict], versions: list[dict]) -> None:
        self.routes = {
            "/agents": [{"id": "agent-1", "name": "sre-bot"}],
            "/deployments": deployments,
            "/agents/agent-1/versions": versions,
        }

    def __call__(self, request, timeout=0):  # noqa: ANN001 - urlopen's shape
        path = request.full_url.replace("http://api", "").split("?", 1)[0]
        import io as _io

        return _io.BytesIO(json.dumps(self.routes[path]).encode())


def _patched(monkeypatch: pytest.MonkeyPatch, api: _FakeApi) -> None:
    import redeploy

    class _Ctx:
        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self.body

        def __exit__(self, *_):
            return False

    monkeypatch.setattr(redeploy.urllib.request, "urlopen", lambda r, timeout=0: _Ctx(api(r)))


def test_the_served_version_wins_over_a_newer_undeployed_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _FakeApi(
        deployments=[
            {
                "agent_id": "agent-1",
                "version_id": "served",
                "status": "active",
                "environment": "prod",
                "deployed_at": "2026-01-01T00:00:00",
                "commit_sha": None,
            }
        ],
        versions=[
            {"id": "served", "commit_sha": "a" * 40, "created_at": "2026-01-01T00:00:00"},
            # Newer, and never deployed: exactly the row that used to win.
            {"id": "never-deployed", "commit_sha": "b" * 40, "created_at": "2026-06-01T00:00:00"},
        ],
    )
    _patched(monkeypatch, api)

    agent_id, commit, version_id = deployed_commit("http://api", "k", "sre-bot")

    assert version_id == "served"
    assert commit == "a" * 40


def test_no_active_deployment_reads_as_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    # Never "up to date". A job that cannot tell must not report success.
    api = _FakeApi(deployments=[], versions=[{"id": "v", "commit_sha": "c" * 40}])
    _patched(monkeypatch, api)
    _agent, commit, version_id = deployed_commit("http://api", "k", "sre-bot")
    assert commit is None and version_id is None


def test_the_prod_deployment_wins_over_a_newer_active_dev_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple active rows are normal; newest-across-environments is not prod.

    create_deployment_row never stops the previous row, so an old active prod
    row and a newer active dev row coexist. deployed_commit must match what
    deploy() posts to (environment=prod), not the newest active row
    (curie#2292).
    """

    api = _FakeApi(
        deployments=[
            {
                "agent_id": "agent-1",
                "version_id": "prod-v",
                "status": "active",
                "environment": "prod",
                "deployed_at": "2026-01-01T00:00:00",
                "commit_sha": "a" * 40,
            },
            {
                "agent_id": "agent-1",
                "version_id": "dev-v",
                "status": "active",
                "environment": "dev",
                "deployed_at": "2026-09-01T00:00:00",
                "commit_sha": "b" * 40,
            },
        ],
        versions=[
            {"id": "prod-v", "commit_sha": "a" * 40},
            {"id": "dev-v", "commit_sha": "b" * 40},
        ],
    )
    _patched(monkeypatch, api)

    agent_id, commit, version_id = deployed_commit("http://api", "k", "sre-bot")

    assert agent_id == "agent-1"
    assert version_id == "prod-v"
    assert commit == "a" * 40


def test_an_active_dev_deployment_is_not_read_as_prod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The falsifiable negative: no prod row must not report the dev commit."""

    api = _FakeApi(
        deployments=[
            {
                "agent_id": "agent-1",
                "version_id": "dev-v",
                "status": "active",
                "environment": "dev",
                "deployed_at": "2026-09-01T00:00:00",
                "commit_sha": "b" * 40,
            }
        ],
        versions=[{"id": "dev-v", "commit_sha": "b" * 40}],
    )
    _patched(monkeypatch, api)
    _agent, commit, version_id = deployed_commit("http://api", "k", "sre-bot")
    assert commit is None and version_id is None


# --- the bundle upload is a form, not a body ---------------------------------
#
# The endpoint is an upload rather than a document write. A raw PUT is refused
# with a 422 naming a field the caller never knew about:
#     {"loc": ["body", "file"], "msg": "Field required"}
# It failed exactly there on the live install, after the job had already fetched
# the bundle, resolved three image digests and created the version -- so the cost
# of getting this wrong is a version row with no bundle behind it.


def test_the_bundle_is_uploaded_as_a_multipart_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import redeploy

    seen: list[dict] = []

    def _fake(api_url, api_key, path, *, method="GET", body=None, content_type="application/json"):
        seen.append({"path": path, "method": method, "body": body, "content_type": content_type})
        if path.endswith("/versions"):
            return json.dumps({"id": "v-1"}).encode()
        return b"{}"

    monkeypatch.setattr(redeploy, "_api", _fake)
    deploy("http://api", "k", "agent-1", b"BUNDLEBYTES", "d" * 40)

    upload = next(c for c in seen if c["path"].endswith("/bundle"))
    assert upload["content_type"].startswith("multipart/form-data; boundary=")
    boundary = upload["content_type"].split("boundary=", 1)[1]
    assert upload["body"].startswith(f"--{boundary}\r\n".encode())
    assert b'name="file"; filename="bundle.tar.gz"' in upload["body"]
    assert b"BUNDLEBYTES" in upload["body"], "the bundle itself must survive the framing"
    assert upload["body"].endswith(f"\r\n--{boundary}--\r\n".encode())

    # And the order still holds: create, upload, then deploy -- so a failure
    # never leaves a version marked active with no bundle behind it.
    assert [c["path"].rsplit("/", 1)[-1] for c in seen] == ["versions", "bundle", "deployments"]


def test_a_version_with_no_commit_is_still_upgradable() -> None:
    """The regression that stuck: an installer-created version could never move.

    `curie example sre-bot install` records no commit, so this job refused --
    and told the reader to deploy through the installer, which is what produced
    the state. An install deployed the supported way could never be upgraded by
    the supported job (#2128).

    Losing the commit loses the COMPARISON, not the ability to deploy.
    """
    assert upgrade_disposition(None, "a-version-id") == "deploy-unknown"
    assert upgrade_disposition("", "a-version-id") == "deploy-unknown"


def test_no_version_is_still_refused() -> None:
    """The half that must stay fatal.

    Without a version there is no running connector env to carry forward, so a
    deploy ships the bundle's placeholder ceilings -- a bot that refuses every
    write and looks exactly like one that chose not to act.
    """
    assert upgrade_disposition("c" * 40, None) == "refuse"
    assert upgrade_disposition(None, None) == "refuse"


def test_a_fully_known_version_deploys_normally() -> None:
    assert upgrade_disposition("c" * 40, "a-version-id") == "deploy"
