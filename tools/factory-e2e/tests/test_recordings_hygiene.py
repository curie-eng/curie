"""Recorded SaaS responses carry no credentials and no real tenant names (#4398).

Follow-up changes commit recorded GitHub, GitLab, Bitbucket, Jira and
Confluence responses under tools/factory-e2e/ and apps/api/tests/. A recording
taken from a live account carries that account's identity: a token in a
header, the Atlassian site, the Bitbucket workspace, the GitLab group, the
Atlassian cloud id. This module fails when any file under those two roots
carries one of them, unless the value is an obvious placeholder.

The tree scan reads `git ls-files` (tracked plus untracked files that are not
ignored), so caches and generated output stay out of it. A finding names the
path, the line and the rule, and shows only a short prefix of the value.

The planted values in the cases below are assembled at runtime from fragments
and seeded random characters, so this file never holds a complete real-format
token or tenant name itself.
"""

from __future__ import annotations

import random
import re
import shutil
import string
import subprocess
from itertools import pairwise
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCANNED_ROOTS = ("tools/factory-e2e", "apps/api/tests")

# A token body is a placeholder when one of these words is anchored in it and
# the rest is filler; see `_token_placeholder` for the exact rule.
PLACEHOLDER_WORDS = (
    "example",
    "fixture",
    "placeholder",
    "fake",
    "dummy",
    "test",
    "sample",
    "redacted",
    "xxxx",
)
PLACEHOLDER_WORD_RE = re.compile("|".join(PLACEHOLDER_WORDS), re.IGNORECASE)
# Characters that delimit a word inside a token body.
SEPARATORS = frozenset("_-.=+/")
# A remainder this short around an anchored placeholder word is filler.
SHORT_FILLER = 16

# Site, workspace and group names a reader is meant to replace.
PLACEHOLDER_NAMES = frozenset(
    {
        "example",
        "acme",
        "acme-corp",
        "your-site",
        "your-domain",
        "test",
        "sample",
        "fixture",
        "placeholder",
        "company",
        "mycompany",
        "demo",
    }
)

# First path segments on gitlab.com and bitbucket.org that are product routes,
# not a group or workspace.
GITLAB_ROUTES = frozenset({"api", "help", "explore", "users", "dashboard", "oauth", "search", "-"})
BITBUCKET_ROUTES = frozenset({"account", "site", "product", "dashboard", "repo"})

# Each token rule: (rule id, pattern, length of the fixed prefix). The body
# after the prefix is what the placeholder check reads.
TOKEN_RULES: tuple[tuple[str, re.Pattern[str], int], ...] = (
    ("github-token", re.compile(r"(?<![A-Za-z0-9_])gh[pousr]_[A-Za-z0-9]{36}(?![A-Za-z0-9_])"), 4),
    (
        "github-fine-grained-token",
        re.compile(r"(?<![A-Za-z0-9_])github_pat_[A-Za-z0-9_]{82}(?![A-Za-z0-9_])"),
        11,
    ),
    ("gitlab-token", re.compile(r"(?<![A-Za-z0-9_-])glpat-[A-Za-z0-9_-]{20,}"), 6),
    ("atlassian-api-token", re.compile(r"(?<![A-Za-z0-9_])ATATT[A-Za-z0-9_=-]{60,}"), 5),
    ("bitbucket-token", re.compile(r"(?<![A-Za-z0-9_])(?:ATBB|ATCTT)[A-Za-z0-9_=-]{24,}"), 4),
    (
        "slack-token",
        re.compile(
            r"(?<![A-Za-z0-9_])xox[abprs]-[0-9]{8,}-[0-9]{8,}(?:-[0-9]{8,})?-[A-Za-z0-9]{24,}"
        ),
        5,
    ),
    (
        "anthropic-key",
        re.compile(r"(?<![A-Za-z0-9_-])sk-ant-(?:api03|oat01)-[A-Za-z0-9_-]{80,}"),
        13,
    ),
    ("openrouter-key", re.compile(r"(?<![A-Za-z0-9_-])sk-or-v1-[0-9a-f]{64}(?![0-9a-f])"), 9),
    ("aws-access-key-id", re.compile(r"(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])"), 4),
)

# A private key is the header followed by at least one base64 body line,
# across real newlines or escaped `\n` inside a string literal. A lone header
# used as a test literal is not a key.
PEM_RULE = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[ \t]*(?:\r?\n|\\n)[ \t]*"
    r"((?:[A-Za-z0-9+/=]{40,}[ \t]*(?:\r?\n|\\n)?[ \t]*)+)"
)

_NAME = r"([A-Za-z0-9][A-Za-z0-9_.-]*)"
_UUID = r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"

# Each site rule: (rule id, pattern whose group 1 is the name, product routes).
SITE_RULES: tuple[tuple[str, re.Pattern[str], frozenset[str]], ...] = (
    (
        "atlassian-site",
        re.compile(r"(?<![A-Za-z0-9_-])([A-Za-z0-9][A-Za-z0-9-]*)\.atlassian\.net\b"),
        frozenset(),
    ),
    (
        "bitbucket-workspace",
        re.compile(r"\bapi\.bitbucket\.org/2\.0/(?:repositories|workspaces)/" + _NAME),
        frozenset(),
    ),
    (
        "bitbucket-workspace",
        re.compile(r"(?<![A-Za-z0-9_.-])bitbucket\.org/" + _NAME),
        BITBUCKET_ROUTES,
    ),
    ("gitlab-group", re.compile(r"\bgitlab\.com/api/v4/groups/" + _NAME), frozenset()),
    ("gitlab-group", re.compile(r"(?<![A-Za-z0-9_.-])gitlab\.com/" + _NAME), GITLAB_ROUTES),
)

CLOUD_ID_RULES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bapi\.atlassian\.com/ex/(?:jira|confluence)/" + _UUID),
    re.compile(r"(?i)\bcloud_?id[\"']?\s*[:=]\s*[\"']?" + _UUID),
)

PREFIX_SHOWN = 6


def _shown(value: str) -> str:
    """A short prefix of a matched value; never the whole thing."""
    return value[: min(PREFIX_SHOWN, max(1, len(value) // 2))] + "..."


def _low_entropy(body: str) -> bool:
    if len(set(body)) <= 5:
        return True
    for size in range(1, min(16, len(body) // 2) + 1):
        unit = body[:size]
        if (unit * (len(body) // size + 1))[: len(body)] == body:
            return True
    return False


def _sequential(text: str) -> bool:
    """Mostly runs like `0123` or `abcd`: at least 3 in 4 neighbours step by one."""
    if len(text) < 2:
        return True
    steps = sum(1 for left, right in pairwise(text) if ord(right) - ord(left) == 1)
    return steps * 4 >= (len(text) - 1) * 3


def _filler(text: str) -> bool:
    """What a person types around a placeholder word, not a random secret.

    Short (at most 16 characters, like the documented AWS example key's
    `IOSFODNN7`), low-entropy, a repeated handful of characters (no more than
    one distinct character per four), or a sequential run.
    """
    return (
        len(text) <= SHORT_FILLER
        or _low_entropy(text)
        or len(set(text)) <= max(5, len(text) // 4)
        or _sequential(text)
    )


def _anchored_word(lowered: str, word: str) -> bool:
    """`word` starts the body, ends it, or has a separator on both sides."""
    start = lowered.find(word)
    while start != -1:
        end = start + len(word)
        if start == 0 or end == len(lowered):
            return True
        if lowered[start - 1] in SEPARATORS and lowered[end] in SEPARATORS:
            return True
        start = lowered.find(word, start + 1)
    return False


def _token_placeholder(body: str) -> bool:
    """A token body that is obviously not a real credential.

    Either the body is low-entropy as a whole, or both of these hold:

    1. A placeholder word is anchored in it: the word starts the body, ends
       it, or sits between separators (`_example_`, `-test-`). A word glued to
       other characters on both sides mid-body is the kind of thing a random
       body contains by chance, so it exempts nothing.
    2. What is left once every placeholder word and separator is removed is
       filler (`_filler`), not a random string.

    A real high-entropy token that happens to contain `test`, `fake` or
    `example` fails condition 1 when the word is mid-body and condition 2 when
    it is not.
    """
    if _low_entropy(body):
        return True
    if not any(_anchored_word(body.lower(), word) for word in PLACEHOLDER_WORDS):
        return False
    # The remainder keeps its original case, so `A1b2` stays four symbols.
    rest = PLACEHOLDER_WORD_RE.sub("", body)
    return _filler("".join(char for char in rest if char not in SEPARATORS))


def _cloud_id_placeholder(value: str) -> bool:
    digits = value.replace("-", "").lower()
    return len(set(digits)) == 1 or digits.startswith("123456781234")


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def findings(text: str) -> list[str]:
    """Every non-placeholder credential or tenant identifier in `text`.

    Each entry is `line N: <rule>: <prefix>...`.
    """
    found: list[tuple[int, str]] = []

    def report(offset: int, rule: str, value: str) -> None:
        found.append((offset, f"line {_line_of(text, offset)}: {rule}: {_shown(value)}"))

    for rule, pattern, prefix in TOKEN_RULES:
        for match in pattern.finditer(text):
            if not _token_placeholder(match.group(0)[prefix:]):
                report(match.start(), rule, match.group(0))

    for match in PEM_RULE.finditer(text):
        body = re.sub(r"\s|\\n", "", match.group(1))
        if not _token_placeholder(body):
            report(match.start(), "private-key", match.group(0))

    for rule, pattern, routes in SITE_RULES:
        for match in pattern.finditer(text):
            name = match.group(1).rstrip("._-").lower()
            if name.endswith(".git"):
                name = name[: -len(".git")]
            if name in PLACEHOLDER_NAMES or name in routes:
                continue
            report(match.start(1), rule, name)

    for pattern in CLOUD_ID_RULES:
        for match in pattern.finditer(text):
            if not _cloud_id_placeholder(match.group(1)):
                report(match.start(1), "atlassian-cloud-id", match.group(1))

    # Both cloud id patterns can see the same value; report it once.
    unique = dict.fromkeys(entry for _, entry in sorted(found))
    return list(unique)


def _listed_files(repo_root: Path, roots: tuple[str, ...]) -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", *roots],
        cwd=repo_root,
        capture_output=True,
        check=True,
    ).stdout.decode()
    return sorted({path for path in listed.split("\0") if path})


def scan_tree(repo_root: Path, roots: tuple[str, ...] = SCANNED_ROOTS) -> list[str]:
    """`path:line: rule: prefix...` for every finding in the listed text files."""
    report: list[str] = []
    for relative in _listed_files(repo_root, roots):
        path = repo_root / relative
        if not path.is_file():
            continue
        raw = path.read_bytes()
        if b"\0" in raw[:8192]:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        report.extend(f"{relative}:{entry.removeprefix('line ')}" for entry in findings(text))
    return report


# Planted values. Built from fragments and seeded random characters so this
# file holds no complete real-format value.
_RNG = random.Random(4398)
_ALNUM = string.ascii_letters + string.digits
_UPPER = string.ascii_uppercase + string.digits


def _rand(alphabet: str, size: int) -> str:
    while True:
        value = "".join(_RNG.choice(alphabet) for _ in range(size))
        if not _token_placeholder(value):
            return value


def _tenant() -> str:
    return "".join(_RNG.choice(string.ascii_lowercase) for _ in range(9)) + "corp"


def _uuid() -> str:
    while True:
        value = "".join(_RNG.choice("0123456789abcdef") for _ in range(32))
        if not _cloud_id_placeholder(value):
            return "-".join((value[:8], value[8:12], value[12:16], value[16:20], value[20:]))


_PEM_HEAD = "-----" + "BEGIN RSA " + "PRIVATE KEY-----"
_PEM_TAIL = "-----" + "END RSA " + "PRIVATE KEY-----"

# case -> (rule expected to fire, planted text)
PLANTED: dict[str, tuple[str, str]] = {
    "github-token": (
        "github-token",
        '"Authorization": "token ' + "gh" + "p_" + _rand(_ALNUM, 36) + '"',
    ),
    "github-token-server": ("github-token", "gh" + "s_" + _rand(_ALNUM, 36)),
    "github-fine-grained": ("github-fine-grained-token", "github" + "_pat_" + _rand(_ALNUM, 82)),
    "gitlab": ("gitlab-token", "PRIVATE-TOKEN: " + "gl" + "pat-" + _rand(_ALNUM, 20)),
    "atlassian-api": ("atlassian-api-token", "ATA" + "TT3xFfGF0" + _rand(_ALNUM, 180)),
    "bitbucket-app-password": ("bitbucket-token", "ATB" + "B" + _rand(_ALNUM, 32)),
    "bitbucket-access": ("bitbucket-token", "ATC" + "TT3xFfGN0" + _rand(_ALNUM, 180)),
    "slack-bot": (
        "slack-token",
        "xo"
        + "xb-"
        + _rand(string.digits, 12)
        + "-"
        + _rand(string.digits, 13)
        + "-"
        + _rand(_ALNUM, 24),
    ),
    "anthropic-api": ("anthropic-key", "sk-" + "ant-api03-" + _rand(_ALNUM + "_-", 93) + "AA"),
    "anthropic-oauth": ("anthropic-key", "sk-" + "ant-oat01-" + _rand(_ALNUM + "_-", 93) + "AA"),
    "openrouter": ("openrouter-key", "sk-" + "or-v1-" + _rand("0123456789abcdef", 64)),
    "aws-long-term": ("aws-access-key-id", "AK" + "IA" + _rand(_UPPER, 16)),
    "aws-session": ("aws-access-key-id", "AS" + "IA" + _rand(_UPPER, 16)),
    "pem-lines": (
        "private-key",
        _PEM_HEAD + "\n" + "\n".join(_rand(_ALNUM + "+/", 64) for _ in range(3)) + "\n" + _PEM_TAIL,
    ),
    "pem-escaped": (
        "private-key",
        '"' + _PEM_HEAD + "\\n" + _rand(_ALNUM + "+/", 64) + "\\n" + _PEM_TAIL + '"',
    ),
    "atlassian-site": (
        "atlassian-site",
        '"self": "https://' + _tenant() + ".atlassian" + '.net/rest/api/3/issue/10012"',
    ),
    "bitbucket-api": (
        "bitbucket-workspace",
        "https://api.bit" + "bucket.org/2.0/repositories/" + _tenant() + "/app",
    ),
    "bitbucket-web": (
        "bitbucket-workspace",
        "https://" + "bitbucket.org/" + _tenant() + "/app.git",
    ),
    "gitlab-web": ("gitlab-group", "https://" + "gitlab.com/" + _tenant() + "/platform/app"),
    "gitlab-api": (
        "gitlab-group",
        "https://" + "gitlab.com/api/v4/groups/" + _tenant() + "/projects",
    ),
    "cloud-id-url": (
        "atlassian-cloud-id",
        "https://api." + "atlassian.com/ex/jira/" + _uuid() + "/rest/api/3/myself",
    ),
    "cloud-id-camel": ("atlassian-cloud-id", '{"cloudId": "' + _uuid() + '"}'),
    "cloud-id-snake": ("atlassian-cloud-id", "cloud_" + "id = '" + _uuid() + "'"),
    # A real high-entropy body that merely contains a placeholder word is
    # still a credential: mid-body, glued to random characters, ...
    "github-token-test-mid-body": (
        "github-token",
        "gh" + "p_" + _rand(_ALNUM, 14) + "test" + _rand(_ALNUM, 18),
    ),
    "anthropic-key-fake-mid-body": (
        "anthropic-key",
        "sk-" + "ant-api03-" + _rand(_ALNUM, 45) + "fake" + _rand(_ALNUM, 44) + "AA",
    ),
    "atlassian-token-example-mid-body": (
        "atlassian-api-token",
        "ATA" + "TT3xFfGF0" + _rand(_ALNUM, 90) + "Example" + _rand(_ALNUM, 90),
    ),
    # ... and anchored, when what surrounds the word is random, not filler.
    "github-fine-grained-example-prefix": (
        "github-fine-grained-token",
        "github" + "_pat_" + "EXAMPLE" + _rand(_ALNUM, 75),
    ),
    "gitlab-test-delimited": (
        "gitlab-token",
        "gl" + "pat-" + _rand(_ALNUM, 12) + "-test-" + _rand(_ALNUM, 12),
    ),
}


@pytest.mark.parametrize("case", sorted(PLANTED))
def test_a_planted_value_is_found_without_echoing_it(case: str) -> None:
    rule, value = PLANTED[case]
    text = "first line\n" + value + "\n"

    result = findings(text)

    assert len(result) == 1, result
    assert result[0].startswith(f"line 2: {rule}: ")
    shown = result[0].split(": ", 2)[2].removesuffix("...")
    assert len(shown) <= PREFIX_SHOWN
    for credential in re.findall(r"[A-Za-z0-9_+/=-]{12,}", value):
        assert credential not in result[0]


PLACEHOLDERS: dict[str, str] = {
    "github-token-repeated": "gh" + "p_" + "x" * 36,
    "github-token-unit": "gh" + "p_" + "abc123" * 6,
    "github-token-word": "gh" + "o_" + "EXAMPLE" + "0123456789abcdefghijABCDEFGHI",
    "github-fine-grained-word": "github" + "_pat_" + "placeholder" + "A1b2C3d4E5" * 7 + "q",
    "gitlab-token-word": "gl" + "pat-" + "fixture" + "Ab1Cd2Ef3Gh4K",
    "atlassian-token-word": "ATA" + "TT" + "redacted" + _ALNUM + _ALNUM[:10],
    "bitbucket-token-low": "ATB" + "B" + "0" * 32,
    "slack-token-word": "xo" + "xb-" + "1" * 12 + "-" + "2" * 13 + "-" + "fakefakefakefakefakefake",
    "anthropic-key-word": "sk-" + "ant-api03-" + "dummy" + _ALNUM + _ALNUM[:20],
    "openrouter-key-low": "sk-" + "or-v1-" + "0" * 64,
    "aws-access-key-id-word": "AK" + "IA" + "IOSFODNN7" + "EXAMPLE",
    "aws-access-key-id-low": "AK" + "IA" + "A" * 16,
    "private-key-header-only": '"' + _PEM_HEAD + '"',
    "private-key-low-body": _PEM_HEAD + "\n" + "A" * 64 + "\n" + _PEM_TAIL,
    "atlassian-site-acme": "https://" + "acme.atlassian" + ".net/rest/api/3/issue/10012",
    "atlassian-site-your-domain": "your-domain.atlassian" + ".net",
    "atlassian-site-templated": "https://{site}.atlassian" + ".net",
    "bitbucket-workspace-acme-corp": "https://api.bit" + "bucket.org/2.0/workspaces/" + "acme-corp",
    "bitbucket-web-route": "https://" + "bitbucket.org/" + "account/settings",
    "gitlab-group-example": "https://" + "gitlab.com/" + "example/app",
    "gitlab-api-projects": "https://" + "gitlab.com/api/v4/projects/" + "42",
    "gitlab-group-api-demo": "https://" + "gitlab.com/api/v4/groups/" + "demo",
    "cloud-id-zeros": '{"cloudId": "' + "00000000-0000-0000-0000-000000000000" + '"}',
    "cloud-id-repeated": "api."
    + "atlassian.com/ex/confluence/"
    + "-".join("a" * size for size in (8, 4, 4, 4, 12)),
    "cloud-id-sequence": "cloud_" + "id: " + "12345678-1234-1234-1234-123456789012",
}


@pytest.mark.parametrize("case", sorted(PLACEHOLDERS))
def test_a_placeholder_value_passes(case: str) -> None:
    assert findings(PLACEHOLDERS[case]) == []


def test_the_scanned_roots_carry_no_credentials_or_tenant_names() -> None:
    listed = _listed_files(REPO_ROOT, SCANNED_ROOTS)
    # Liveness: the scan really walks both roots, including this file.
    assert "tools/factory-e2e/tests/test_recordings_hygiene.py" in listed
    assert any(path.startswith("apps/api/tests/") for path in listed)

    assert scan_tree(REPO_ROOT) == []


def test_the_tree_scan_reports_a_planted_file_by_path_and_line(tmp_path: Path) -> None:
    root = "tools/factory-e2e"
    for relative in _listed_files(REPO_ROOT, (root,)):
        source = REPO_ROOT / relative
        if source.is_file():
            target = tmp_path / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True)
    assert scan_tree(tmp_path, (root,)) == []

    planted = tmp_path / root / "fixtures" / "recorded" / "jira-issue.json"
    planted.parent.mkdir(parents=True)
    planted.write_text(
        "{\n"
        f"  {PLANTED['atlassian-site'][1]},\n"
        f'  "token": "{PLANTED["github-token-server"][1]}"\n'
        "}\n"
    )
    # An ignored file is out of scope, like a cache would be.
    (tmp_path / ".gitignore").write_text("ignored.txt\n")
    (tmp_path / root / "ignored.txt").write_text(PLANTED["aws-long-term"][1] + "\n")

    report = scan_tree(tmp_path, (root,))

    path = f"{root}/fixtures/recorded/jira-issue.json"
    assert len(report) == 2, report
    assert report[0].startswith(f"{path}:2: atlassian-site: ")
    assert report[1].startswith(f"{path}:3: github-token: ")
    assert PLANTED["github-token-server"][1] not in "\n".join(report)
