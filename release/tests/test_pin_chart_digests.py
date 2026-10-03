"""Contract tests for release/pin_chart_digests.py (#3843).

The release chart job pins every first-party image the chart references to the
signed index digest it just verified, so the packaged chart deploys exactly the
bytes this release published instead of a mutable tag. The script edits the
packaged copy of charts/curie/values.yaml in place and fails closed on every
input that could ship a wrong or partial pin. The source tree keeps
`digest: ""` everywhere; only the packaged chart is pinned.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "release" / "pin_chart_digests.py"
CHART_DIR = REPO_ROOT / "charts" / "curie"
SOURCE_VALUES = CHART_DIR / "values.yaml"

REPOSITORY_PREFIX = "ghcr.io/curie-eng/curie-"

# Published image name -> path of its digest field inside values.yaml.
DIGEST_PATHS = {
    "api": ("api", "image", "digest"),
    "dispatcher": ("dispatcher", "image", "digest"),
    "mail-adapter": ("mailAdapter", "image", "digest"),
    "worker": ("worker", "image", "digest"),
    "ui": ("ui", "image", "digest"),
    "runner": ("agentSandbox", "runner", "digest"),
}

DIGESTS = {
    "api": "sha256:" + "a" * 64,
    "dispatcher": "sha256:" + "b" * 64,
    "mail-adapter": "sha256:" + "c" * 64,
    "worker": "sha256:" + "d" * 64,
    "ui": "sha256:" + "e" * 64,
    "runner": "sha256:" + "0123456789abcdef" * 4,
}


def image_arg(name: str, digest: str | None = None, repository: str | None = None) -> str:
    repo = repository if repository is not None else REPOSITORY_PREFIX + name
    return f"{name}={repo}@{digest if digest is not None else DIGESTS[name]}"


def all_image_args(**overrides: str) -> list[str]:
    args: list[str] = []
    for name in DIGESTS:
        args += ["--image", overrides.get(name, image_arg(name))]
    return args


def lookup(values: dict, path: tuple[str, ...]):
    node = values
    for key in path:
        node = node[key]
    return node


def run_pin(values_path: Path, image_args: list[str]) -> subprocess.CompletedProcess:
    assert SCRIPT.is_file(), f"{SCRIPT} does not exist"
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--values", str(values_path), *image_args],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def values_copy(tmp_path: Path) -> Path:
    target = tmp_path / "values.yaml"
    shutil.copyfile(SOURCE_VALUES, target)
    return target


def assert_refused(values_path: Path, image_args: list[str], *, mentions: str) -> None:
    before = values_path.read_bytes()
    result = run_pin(values_path, image_args)
    assert result.returncode != 0, (
        f"expected a refusal, got exit 0\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert result.stderr.strip(), "a refusal must explain itself on stderr"
    assert mentions in result.stderr, (
        f"refusal should name {mentions!r}; stderr was:\n{result.stderr}"
    )
    assert values_path.read_bytes() == before, "a refusal must leave the values file byte-identical"


class TestSourceTreeIsUnpinned:
    def test_chart_first_party_images_are_exactly_the_six_published_names(self):
        values = yaml.safe_load(SOURCE_VALUES.read_text(encoding="utf-8"))
        found: dict[str, str] = {}

        def walk(node):
            if isinstance(node, dict):
                for child in node.values():
                    walk(child)
            elif isinstance(node, list):
                for child in node:
                    walk(child)
            elif isinstance(node, str) and node.startswith(REPOSITORY_PREFIX):
                found[node.removeprefix(REPOSITORY_PREFIX)] = node

        walk(values)
        assert set(found) == set(DIGEST_PATHS)

    def test_every_first_party_digest_in_the_source_values_is_empty(self):
        # Liveness for the success path below, and the guard that a pin never
        # lands in the source tree: only the packaged chart carries digests.
        values = yaml.safe_load(SOURCE_VALUES.read_text(encoding="utf-8"))
        for name, path in DIGEST_PATHS.items():
            assert lookup(values, path) == "", f"{name} digest is pinned in the source tree"


class TestPinSucceeds:
    def test_all_six_digests_are_pinned_and_nothing_else_changes(self, values_copy: Path):
        original = values_copy.read_text(encoding="utf-8")
        result = run_pin(values_copy, all_image_args())
        assert result.returncode == 0, result.stderr

        pinned_text = values_copy.read_text(encoding="utf-8")
        pinned = yaml.safe_load(pinned_text)
        for name, path in DIGEST_PATHS.items():
            assert lookup(pinned, path) == DIGESTS[name], name

        before_lines = original.splitlines(keepends=True)
        after_lines = pinned_text.splitlines(keepends=True)
        assert len(before_lines) == len(after_lines), "pinning must be a line-for-line edit"
        changed = [
            i for i, (a, b) in enumerate(zip(before_lines, after_lines, strict=True)) if a != b
        ]
        assert len(changed) == len(DIGEST_PATHS), [after_lines[i] for i in changed]

        supplied = set(DIGESTS.values())
        for i in changed:
            before, after = before_lines[i], after_lines[i]
            assert before.strip() == 'digest: ""', before
            indent = before[: len(before) - len(before.lstrip())]
            match = re.fullmatch(
                rf'{re.escape(indent)}digest: (["\']?)(sha256:[0-9a-f]{{64}})\1\n?', after
            )
            assert match, after
            supplied.discard(match.group(2))
        assert not supplied, f"digests never written: {supplied}"

    def test_argument_order_does_not_matter(self, values_copy: Path):
        args = all_image_args()
        pairs = [args[i : i + 2] for i in range(0, len(args), 2)]
        reversed_args = [token for pair in reversed(pairs) for token in pair]
        result = run_pin(values_copy, reversed_args)
        assert result.returncode == 0, result.stderr
        pinned = yaml.safe_load(values_copy.read_text(encoding="utf-8"))
        for name, path in DIGEST_PATHS.items():
            assert lookup(pinned, path) == DIGESTS[name], name

    @pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not on PATH")
    def test_pinned_chart_renders_every_first_party_image_by_digest(self, tmp_path: Path):
        chart = tmp_path / "curie"
        shutil.copytree(CHART_DIR, chart)

        def render() -> str:
            result = subprocess.run(
                ["helm", "template", "t", str(chart)],
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, result.stderr
            return result.stdout

        unpinned = render()
        tag_form = {
            name: re.compile(rf"{re.escape(REPOSITORY_PREFIX + name)}:[A-Za-z0-9._-]+")
            for name in DIGEST_PATHS
        }
        rendered_by_default = {
            name for name, pattern in tag_form.items() if pattern.search(unpinned)
        }
        # The default render must exercise both the Deployment path and the
        # sandbox runner path, or this test proves nothing about either.
        assert {"api", "worker", "ui", "runner"} <= rendered_by_default, rendered_by_default

        result = run_pin(chart / "values.yaml", all_image_args())
        assert result.returncode == 0, result.stderr
        pinned = render()

        for name in rendered_by_default:
            assert f"{REPOSITORY_PREFIX}{name}@{DIGESTS[name]}" in pinned, name
        for name, pattern in tag_form.items():
            assert not pattern.search(pinned), f"{name} still renders by tag"

        # Images the default render does not deploy are still pinned in values,
        # so enabling them in the packaged chart deploys the release digest.
        values = yaml.safe_load((chart / "values.yaml").read_text(encoding="utf-8"))
        for name in set(DIGEST_PATHS) - rendered_by_default:
            assert lookup(values, DIGEST_PATHS[name]) == DIGESTS[name], name


class TestPinRefuses:
    def test_a_chart_image_with_no_supplied_digest(self, values_copy: Path):
        args = [
            token
            for name in DIGESTS
            if name != "ui"
            for token in ("--image", image_arg(name))
        ]
        assert_refused(values_copy, args, mentions="ui")

    def test_an_image_the_chart_does_not_reference(self, values_copy: Path):
        args = all_image_args() + [
            "--image",
            image_arg("sre-bot-tempo", digest="sha256:" + "f" * 64),
        ]
        assert_refused(values_copy, args, mentions="sre-bot-tempo")

    def test_a_repository_that_is_not_the_charts(self, values_copy: Path):
        args = all_image_args(
            api=image_arg("api", repository="ghcr.io/someone-else/curie-api")
        )
        assert_refused(values_copy, args, mentions="api")

    @pytest.mark.parametrize(
        "bad_reference",
        [
            pytest.param(REPOSITORY_PREFIX + "api@sha256:" + "A" * 64, id="uppercase-hex"),
            pytest.param(REPOSITORY_PREFIX + "api@sha256:" + "a" * 63, id="63-hex"),
            pytest.param(REPOSITORY_PREFIX + "api@sha256:" + "a" * 65, id="65-hex"),
            pytest.param(REPOSITORY_PREFIX + "api@" + "a" * 64, id="missing-sha256-prefix"),
            pytest.param(REPOSITORY_PREFIX + "api@sha512:" + "a" * 64, id="wrong-algorithm"),
            pytest.param(REPOSITORY_PREFIX + "api:0.13.0", id="tag-not-digest"),
            pytest.param(REPOSITORY_PREFIX + "api", id="bare-repository"),
        ],
    )
    def test_a_malformed_digest(self, values_copy: Path, bad_reference: str):
        args = all_image_args(api=f"api={bad_reference}")
        assert_refused(values_copy, args, mentions="api")

    def test_a_duplicate_image_name(self, values_copy: Path):
        args = all_image_args() + [
            "--image",
            image_arg("api", digest="sha256:" + "9" * 64),
        ]
        assert_refused(values_copy, args, mentions="api")

    def test_an_identical_duplicate_image_name(self, values_copy: Path):
        args = all_image_args() + ["--image", image_arg("api")]
        assert_refused(values_copy, args, mentions="api")

    def test_a_target_digest_that_is_already_set(self, values_copy: Path):
        text = values_copy.read_text(encoding="utf-8")
        marker = "    repository: ghcr.io/curie-eng/curie-api\n"
        start = text.index(marker)
        digest_at = text.index('digest: ""', start)
        preset = "sha256:" + "7" * 64
        text = text[:digest_at] + f'digest: "{preset}"' + text[digest_at + len('digest: ""') :]
        values_copy.write_text(text, encoding="utf-8")
        assert yaml.safe_load(text)["api"]["image"]["digest"] == preset

        assert_refused(values_copy, all_image_args(), mentions="api")

    def test_an_image_argument_without_a_name(self, values_copy: Path):
        args = all_image_args()[2:] + ["--image", REPOSITORY_PREFIX + "api@" + DIGESTS["api"]]
        assert_refused(values_copy, args, mentions="api")
