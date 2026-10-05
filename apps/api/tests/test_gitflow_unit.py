"""Unit tests for git-flow signature, ref mapping, and clone-url guarding."""

import base64
import hashlib
import hmac
import io
import json
import logging
import subprocess
import tarfile
from pathlib import Path
from unittest import mock

import pytest
from curie_api.config import Settings, get_settings
from curie_api.gitflow import (
    CloneOriginMismatch,
    GitFlowError,
    clone_and_archive,
    environment_for_ref,
    verify_signature,
)
from curie_api.models import Environment
from fastapi.testclient import TestClient

SECRET = "top-secret"
_VALID_SHA1 = "a" * 40
_VALID_SHA256 = "a" * 64
# The repository binding these tests are pinned to, and a clone base whose
# derived origin is `_ALLOWED_URL`. Every call site supplies `repo_full_name`,
# because the trusted origin is derived from the stored binding plus config and
# the payload's clone URL is only ever compared against it (#1122).
_REPO = "octo/demo-agent"
_DEV_REF = "refs/heads/dev"
_LOCAL_BASE = "file:///tmp/gitflow-none"
_ALLOWED_URL = f"{_LOCAL_BASE}/{_REPO}.git"
_GITHUB_URL = f"https://github.com/{_REPO}.git"


def _local_settings() -> Settings:
    """Settings whose derived origin for `_REPO` is exactly `_ALLOWED_URL`."""

    return Settings(github_clone_base=_LOCAL_BASE)


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def test_verify_signature_accepts_a_correct_digest() -> None:
    body = b'{"ref":"refs/heads/dev"}'
    assert verify_signature(SECRET, body, _sign(body)) is True


def test_verify_signature_rejects_tampered_body() -> None:
    good = _sign(b"original")
    assert verify_signature(SECRET, b"tampered", good) is False


def test_verify_signature_rejects_missing_or_malformed_header() -> None:
    assert verify_signature(SECRET, b"x", None) is False
    assert verify_signature(SECRET, b"x", "not-a-sig") is False


def test_environment_for_ref_maps_dev_and_prod_branches() -> None:
    settings = Settings()
    assert environment_for_ref("refs/heads/dev", settings) is Environment.dev
    assert environment_for_ref("refs/heads/main", settings) is Environment.prod
    assert environment_for_ref("refs/heads/feature-x", settings) is None
    assert environment_for_ref(None, settings) is None


def test_environment_for_ref_requires_exact_head_ref() -> None:
    # A tag or a nested branch that merely ends in dev/main must not deploy.
    settings = Settings()
    assert environment_for_ref("refs/tags/main", settings) is None
    assert environment_for_ref("refs/heads/feature/dev", settings) is None
    assert environment_for_ref("refs/heads/topic/main", settings) is None


def test_clone_and_archive_refuses_disallowed_scheme() -> None:
    # ext:: is git's arbitrary-command transport; it must be refused before any
    # subprocess runs, regardless of the allowlist. The scheme allowlist runs
    # BEFORE the origin comparator, so this must still fail on the scheme --
    # asserting the error is not a CloneOriginMismatch pins that ordering.
    settings = _local_settings()
    with pytest.raises(GitFlowError) as err:
        clone_and_archive(
            "ext::sh -c whoami",
            "0" * 40,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )
    assert not isinstance(err.value, CloneOriginMismatch)
    assert "scheme" in str(err.value)


@pytest.mark.parametrize(
    "bad_sha",
    [
        "--foo",  # git-option injection via a leading dash
        "--upload-pack=touch /tmp/pwned",  # a real option-injection payload
        "-o",  # short option flag
        "deadbeef",  # too short (8 hex chars, not 40/64)
        "z" * 40,  # right length, non-hex chars
        "A" * 40,  # uppercase hex is rejected (regex is lowercase only)
        "a" * 39,  # one short of SHA-1
        "a" * 41,  # one over SHA-1 (and not SHA-256)
        "a" * 63,  # one short of SHA-256
        "a" * 65,  # one over SHA-256
        "a" * 40 + "\n",  # valid hex with a trailing newline ($ regex leak)
        "a" * 40 + "\r",  # valid hex with a trailing carriage return
        "a" * 40 + "\n--foo",  # embedded newline smuggling a git option
        "",  # empty
    ],
)
def test_clone_and_archive_rejects_invalid_sha_before_any_subprocess(
    bad_sha: str,
) -> None:
    # An invalid ref must be refused by the format gate BEFORE git ever runs, so
    # a leading-dash sha can never reach `git archive` as an injected option.
    # The clone URL here matches the derived origin, so the sha gate is provably
    # what rejects, not the origin comparator.
    settings = _local_settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        with pytest.raises(GitFlowError):
            clone_and_archive(
                _ALLOWED_URL,
                bad_sha,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    run.assert_not_called()


def _completed(
    stdout: bytes = b"", *, returncode: int = 0, stderr: bytes = b""
) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


def _real_git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def test_clone_and_archive_ignores_replacement_objects(tmp_path: Path) -> None:
    base = tmp_path / "remotes"
    work = tmp_path / "work"
    bare = base / f"{_REPO}.git"
    work.mkdir()
    bare.parent.mkdir(parents=True)

    _real_git(work, "init", "-q", "-b", "dev")
    marker = work / "marker.txt"
    marker.write_text("approved tree\n")
    _real_git(work, "add", marker.name)
    _real_git(work, "commit", "-q", "-m", "approved")
    approved_sha = _real_git(work, "rev-parse", "HEAD")

    marker.write_text("replacement tree\n")
    _real_git(work, "add", marker.name)
    replacement_tree = _real_git(work, "write-tree")
    replacement_sha = _real_git(work, "commit-tree", replacement_tree, "-m", "replacement")
    _real_git(work, "replace", approved_sha, replacement_sha)
    _real_git(tmp_path, "clone", "--quiet", "--mirror", str(work), str(bare))

    archive = clone_and_archive(
        f"file://{bare}",
        approved_sha,
        Settings(github_clone_base=f"file://{base}"),
        repo_full_name=_REPO,
        ref=_DEV_REF,
    )

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as contents:
        archived_marker = contents.extractfile("marker.txt")
        assert archived_marker is not None
        assert archived_marker.read() == b"approved tree\n"


def test_clone_and_archive_allows_a_commit_reachable_from_the_payload_ref() -> None:
    settings = _local_settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.side_effect = [
            _completed(),
            _completed(returncode=0),
            _completed(b"tar-bytes"),
        ]
        result = clone_and_archive(
            _ALLOWED_URL,
            _VALID_SHA1,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    assert result == b"tar-bytes"
    merge_base_argv = run.call_args_list[1].args[0]
    assert merge_base_argv[-4:] == [
        "merge-base",
        "--is-ancestor",
        _VALID_SHA1,
        _DEV_REF,
    ]


def test_clone_and_archive_rejects_a_commit_unreachable_from_the_payload_ref() -> None:
    from curie_api.gitflow import CommitNotOnBranch

    settings = _local_settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.side_effect = [_completed(), _completed(returncode=1)]
        with pytest.raises(CommitNotOnBranch):
            clone_and_archive(
                _ALLOWED_URL,
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )

    assert len(run.call_args_list) == 2


def test_clone_and_archive_treats_other_merge_base_failures_as_operational() -> None:
    from curie_api.gitflow import CommitNotOnBranch

    settings = _local_settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.side_effect = [
            _completed(),
            _completed(returncode=2, stderr=b"fatal: invalid object"),
        ]
        with pytest.raises(GitFlowError) as err:
            clone_and_archive(
                _ALLOWED_URL,
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )

    assert not isinstance(err.value, CommitNotOnBranch)
    assert len(run.call_args_list) == 2


@pytest.mark.parametrize("good_sha", [_VALID_SHA1, _VALID_SHA256])
def test_clone_and_archive_accepts_valid_hex_and_inserts_dash_dash(
    good_sha: str,
) -> None:
    # A full lowercase-hex SHA-1 or SHA-256 passes the format gate, and the
    # `git archive` argv must place a `--` separator immediately before the sha
    # so a value can never be parsed as a git option.
    settings = _local_settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        result = clone_and_archive(
            _ALLOWED_URL,
            good_sha,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    assert result == b"tar-bytes"

    # Locate the `git archive` invocation among the subprocess calls.
    archive_argv = next(call.args[0] for call in run.call_args_list if "archive" in call.args[0])
    assert "--" in archive_argv, archive_argv
    dash_index = archive_argv.index("--")
    assert archive_argv[dash_index + 1] == good_sha, archive_argv


def test_clone_refuses_a_foreign_host_before_any_subprocess() -> None:
    # The ticket's exact attack (#1122): a correctly signed payload names a
    # registered repository and an attacker-controlled clone URL. Before this
    # guard the platform GitHub token was scoped to -- and therefore delivered
    # to -- that attacker's host on the very first request.
    settings = Settings(github_token="ghs-secret-token")
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        with pytest.raises(CloneOriginMismatch):
            clone_and_archive(
                f"https://evil.example/{_REPO}.git",
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    run.assert_not_called()


@pytest.mark.parametrize(
    "payload_url",
    [
        # Userinfo of any kind: a credential in the payload URL is never honored.
        f"https://x-access-token:t@github.com/{_REPO}.git",
        # The classic reviewer-fooling URL: hostname is evil.example, the
        # "github.com" is the username. Fails on both host and userinfo.
        f"https://github.com@evil.example/{_REPO}.git",
        # A non-default port reaches a different service on the same host.
        f"https://github.com:8443/{_REPO}.git",
        # Scheme downgrade: a cleartext clone of a substituted bundle is still a
        # deploy of attacker code under a registered agent's identity.
        f"http://github.com/{_REPO}.git",
        # Path prefix. Whole-key equality, never startswith -- this is the single
        # most likely implementation bug in the change.
        "https://github.com/octo/demo-agent-evil.git",
        # Path suffix.
        "https://github.com/octo/demo-agent/evil.git",
        # Different owner.
        "https://github.com/evil/demo-agent.git",
        # IDN homograph: Cyrillic small i (U+0456) in "github". hostname
        # lowercases but does not IDNA-normalize, so the strings simply differ.
        f"https://gіthub.com/{_REPO}.git",
        # Percent-encoded host: urlsplit does not decode escapes in the netloc.
        f"https://%67ithub.com/{_REPO}.git",
        # Trailing-dot host: resolves identically in DNS, mismatches here. Over
        # strict on purpose; no real GitHub payload carries it.
        f"https://github.com./{_REPO}.git",
        # Query and fragment are both in the comparison key.
        f"https://github.com/{_REPO}.git?x=1",
        f"https://github.com/{_REPO}.git#x",
        # Unparseable port: urlsplit().port raises ValueError, which must read as
        # a mismatch rather than escaping the threadpool as a 500.
        f"https://github.com:notaport/{_REPO}.git",
        # Malformed IPv6 literal: urlsplit itself raises ValueError.
        f"https://[::1/{_REPO}.git",
    ],
)
def test_clone_refuses_every_mismatch_dimension_before_any_subprocess(
    payload_url: str,
) -> None:
    # Every dimension AC1 names -- host, user information, port, normalized path
    # -- plus the parse failures, rejected before tempfile.mkdtemp and before any
    # subprocess. Anything not provably identical to the derived origin is a
    # mismatch; that default is the design.
    settings = Settings(github_token="ghs-secret-token")
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        with pytest.raises(CloneOriginMismatch):
            clone_and_archive(
                payload_url,
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    run.assert_not_called()


@pytest.mark.parametrize(
    "payload_url",
    [
        # GitHub's push payload sets clone_url with .git and url without it.
        f"https://github.com/{_REPO}.git",
        f"https://github.com/{_REPO}",
        # Trailing slash, with and without .git before it. Stripping order
        # matters: slashes, then one .git, then slashes again.
        f"https://github.com/{_REPO}.git/",
        f"https://github.com/{_REPO}/",
        # An explicit default port is the same origin.
        f"https://github.com:443/{_REPO}.git",
        # urlsplit().hostname lowercases ASCII hosts.
        f"https://GitHub.com/{_REPO}.git",
    ],
)
def test_clone_accepts_the_registered_origin_in_its_equivalent_forms(
    payload_url: str,
) -> None:
    # A legitimate push must not break. Each accepted form still results in git
    # being handed the derived origin, never the payload string.
    settings = Settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        result = clone_and_archive(
            payload_url,
            _VALID_SHA1,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    assert result == b"tar-bytes"
    assert _GITHUB_URL in run.call_args_list[0].args[0]


def test_clone_refuses_a_mixed_case_scheme() -> None:
    # `HTTPS://` is rejected by the PRE-EXISTING case-sensitive scheme allowlist
    # at gitflow.py:131 (`clone_url.startswith(settings.git_allowed_schemes)`),
    # which runs BEFORE the origin comparator -- not by the comparator, whose own
    # scheme .lower() never gets the chance to accept this form. Asserting the
    # error is not a CloneOriginMismatch is what pins that.
    #
    # This is deliberately a rejection, not an acceptance: no real GitHub webhook
    # payload carries a mixed-case scheme, and case-folding that allowlist to
    # make an accept-case go green would be widening a security gate to pass a
    # test. Nobody may do that.
    settings = Settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        with pytest.raises(GitFlowError) as err:
            clone_and_archive(
                f"HTTPS://github.com/{_REPO}.git",
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    assert not isinstance(err.value, CloneOriginMismatch)
    run.assert_not_called()


@pytest.mark.parametrize(
    "clone_base",
    [
        "github.com",  # no scheme at all
        "ssh://git@github.com",  # a scheme outside the allowlist
    ],
)
def test_clone_refuses_a_configured_base_outside_the_scheme_allowlist(
    clone_base: str,
) -> None:
    # The scheme allowlist validates the PAYLOAD string, which is compared and
    # then discarded. The string git actually receives is the derived
    # `trusted_url`, and it is never checked against `git_allowed_schemes`. That
    # is safe today only by transitivity through the origin comparison, and the
    # design's own argument is that the comparison is not the protection.
    #
    # No payload can be constructed that both passes the payload allowlist AND
    # matches a derived URL whose scheme is disallowed: the comparison key holds
    # the scheme, so matching would require the payload to carry the disallowed
    # scheme too, which the payload allowlist already refuses. That impossibility
    # is exactly why the derived URL needs a check of its own, and it is why the
    # assertion that matters here is the error TYPE. A misconfigured base must
    # fail as a configuration error, not be mistaken for a forged push.
    settings = Settings(github_token="ghs-secret-token", github_clone_base=clone_base)
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        with pytest.raises(GitFlowError) as err:
            clone_and_archive(
                _GITHUB_URL,
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    assert not isinstance(err.value, CloneOriginMismatch)
    run.assert_not_called()


def test_clone_refuses_a_configured_base_git_would_read_as_an_option() -> None:
    # A `github_clone_base` beginning with a dash reaches argv construction as a
    # positional that git parses as an option. `--upload-pack=` is the live
    # payload: git clone runs it as a command against the remote. Operator
    # supplied config is not attacker supplied, but a value that turns into a
    # git option must be refused before any subprocess, exactly as a leading dash
    # sha already is.
    settings = Settings(
        github_token="ghs-secret-token",
        github_clone_base="--upload-pack=touch /tmp/pwned",
    )
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        with pytest.raises(GitFlowError) as err:
            clone_and_archive(
                _GITHUB_URL,
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    assert not isinstance(err.value, CloneOriginMismatch)
    run.assert_not_called()


def test_clone_argv_inserts_dash_dash_before_the_url() -> None:
    # Belt to the previous test's braces, and the half that survives someone
    # relaxing the scheme gate later: `git clone` accepts `--` before its
    # positional, so the URL can never be parsed as an option regardless of what
    # configuration produced it. `git archive` already does this for the sha.
    settings = Settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        clone_and_archive(
            _GITHUB_URL,
            _VALID_SHA1,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    clone_argv = next(call.args[0] for call in run.call_args_list if "clone" in call.args[0])
    assert "--" in clone_argv, clone_argv
    dash_index = clone_argv.index("--")
    assert clone_argv[dash_index + 1] == _GITHUB_URL, clone_argv
    assert dash_index > clone_argv.index("clone"), clone_argv


def test_clone_accepts_a_well_formed_https_base() -> None:
    # The control on the two rejection tests above: a guard that refused every
    # configured base would satisfy them and break every real deploy. A
    # well-formed https base must still reach git, carrying the derived URL.
    base = "https://ghe.corp.example"
    derived = f"{base}/{_REPO}.git"
    settings = Settings(github_clone_base=base)
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        result = clone_and_archive(
            derived,
            _VALID_SHA1,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    assert result == b"tar-bytes"
    assert derived in run.call_args_list[0].args[0]


def test_clone_hands_git_the_derived_origin_not_the_payload_url() -> None:
    # The load-bearing property: there is no code path from the payload's
    # clone_url to subprocess.run. The payload value is compared and discarded;
    # git receives a URL computed from configuration plus the stored binding, so
    # even a normalization bug that wrongly ACCEPTS a hostile URL cannot leak the
    # credential. This fails if an implementer keeps clone_url in the argv even
    # with a correct comparison, which is the likeliest partial implementation.
    payload_url = f"https://github.com/{_REPO}/"  # accepted, but not byte-equal
    settings = Settings(github_token="ghs-secret-token")
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        clone_and_archive(
            payload_url,
            _VALID_SHA1,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    argv = run.call_args_list[0].args[0]
    assert _GITHUB_URL in argv
    assert payload_url not in argv
    assert payload_url not in " ".join(argv)


@pytest.mark.parametrize(
    ("clone_base", "expected_key"),
    [
        ("https://github.com", "http.https://github.com/.extraheader"),
        ("https://ghe.corp.example", "http.https://ghe.corp.example/.extraheader"),
    ],
)
def test_clone_credential_is_keyed_to_the_derived_origin(
    clone_base: str, expected_key: str
) -> None:
    # A private repo is the normal case for a bundle -- it names internal hosts
    # and services -- so an unauthenticated clone made git-flow unusable (#1058).
    # The credential must ride in git config env, never in argv: argv is visible
    # in `ps` and is echoed verbatim by CalledProcessError.
    #
    # The GHE case is the half that proves the header is keyed to the DERIVED
    # origin rather than coincidentally to github.com: change the configured
    # base and the extraheader key follows it.
    repo = "acme/private-bundle"
    derived = f"{clone_base}/{repo}.git"
    settings = Settings(github_token="ghs-secret-token", github_clone_base=clone_base)
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        clone_and_archive(
            derived,
            _VALID_SHA1,
            settings,
            repo_full_name=repo,
            ref=_DEV_REF,
        )

    clone_call = run.call_args_list[0]
    argv = clone_call.args[0]
    env = clone_call.kwargs["env"]

    assert "ghs-secret-token" not in " ".join(argv)
    assert derived in argv  # the derived URL, unmodified -- no embedded credential
    assert env["GIT_CONFIG_COUNT"] == "1"
    assert env["GIT_CONFIG_KEY_0"] == expected_key
    assert env["GIT_CONFIG_VALUE_0"].startswith("Authorization: Basic ")
    decoded = base64.b64decode(env["GIT_CONFIG_VALUE_0"].split()[-1]).decode()
    assert decoded == "x-access-token:ghs-secret-token"


def test_clone_pins_redirect_following_off() -> None:
    # git 2.43.0. `git help config` on http.followRedirects: "If set to initial,
    # git will follow redirects only for the initial request to a remote, but not
    # for subsequent follow-up HTTP requests. [...] The default is initial." The
    # request that carries the extraheader IS that initial request, so the
    # default would let a redirect target receive it. `-c` must precede the
    # `clone` subcommand to take effect.
    #
    # This is an argv-shape check and explicitly NOT the AC4 proof;
    # test_gitflow_redirect.py proves it by executing git. This test exists to
    # localize the failure when that one goes red.
    settings = _local_settings()
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        clone_and_archive(
            _ALLOWED_URL,
            _VALID_SHA1,
            settings,
            repo_full_name=_REPO,
            ref=_DEV_REF,
        )

    argv = run.call_args_list[0].args[0]
    assert "-c" in argv, argv
    flag_index = argv.index("-c")
    assert argv[flag_index + 1] == "http.followRedirects=false", argv
    assert flag_index + 1 < argv.index("clone"), argv


def test_clone_sends_no_credential_when_none_is_configured() -> None:
    settings = Settings(github_token="")
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        clone_and_archive(
            "https://github.com/acme/public.git",
            _VALID_SHA1,
            settings,
            repo_full_name="acme/public",
            ref=_DEV_REF,
        )
    env = run.call_args_list[0].kwargs["env"]
    assert "GIT_CONFIG_COUNT" not in env


def test_clone_does_not_send_the_credential_over_plain_http() -> None:
    # http:// is in the transport allowlist for local/test remotes; a bearer
    # token must not be handed to a cleartext endpoint. This is also why the AC4
    # redirect proof must not assert on an absent Authorization header: over
    # loopback http there was never going to be one.
    settings = Settings(
        github_token="ghs-secret-token", github_clone_base="http://insecure.example"
    )
    with mock.patch("curie_api.gitflow.subprocess.run") as run:
        run.return_value = _completed(b"tar-bytes")
        clone_and_archive(
            "http://insecure.example/acme/repo.git",
            _VALID_SHA1,
            settings,
            repo_full_name="acme/repo",
            ref=_DEV_REF,
        )
    assert "GIT_CONFIG_COUNT" not in run.call_args_list[0].kwargs["env"]


def test_clone_failure_reports_git_stderr_not_the_exit_code() -> None:
    # The old message interpolated the exception, so an operator saw only
    # "returned non-zero exit status 128" while the actual reason -- captured in
    # stderr -- was discarded. That cost real debugging time on a private repo.
    settings = _local_settings()
    failure = subprocess.CalledProcessError(
        128, ["git", "clone"], stderr=b"remote: Repository not found.\nfatal: could not read\n"
    )
    with mock.patch("curie_api.gitflow.subprocess.run", side_effect=failure):
        with pytest.raises(GitFlowError) as err:
            clone_and_archive(
                _ALLOWED_URL,
                _VALID_SHA1,
                settings,
                repo_full_name=_REPO,
                ref=_DEV_REF,
            )
    assert "Repository not found" in str(err.value)


# --------------------------------------------------------------------------- #
# The network-free half of the clone's authorization, on its own (#1211)
# --------------------------------------------------------------------------- #
# A promote of an already-stored bundle no longer clones, so every check that
# used to live inside `clone_and_archive` and cost nothing -- the payload scheme
# allowlist, the sha format gate, the `repo_full_name` derivation, the derived
# URL's own scheme allowlist, and whole-key origin equality -- has to run
# somewhere both paths reach. `verify_push_origin` is that somewhere, and the
# tests below call it DIRECTLY: exercising it only through `clone_and_archive`
# would test it exclusively on the path that no longer always runs.
#
# The exhaustive fourteen-case mismatch battery stays where it is, still driven
# through `clone_and_archive`; what is repeated here is a representative subset,
# because two copies of fourteen cases rot into two different fourteen cases.
#
# Note what is absent: no `subprocess.run` patch anywhere below. That is not an
# omission, it is the property -- this function starts no process at all, which
# is what lets it be hoisted in front of the branch and run on a delivery that
# will never touch the network. A version of it that needed a mock would have
# smuggled the clone back in.
_MISMATCH_SUBSET = [
    # Userinfo of any kind: a credential in the payload URL is never honored.
    f"https://x-access-token:t@github.com/{_REPO}.git",
    # Hostname is evil.example; the "github.com" is the username.
    f"https://github.com@evil.example/{_REPO}.git",
    # A non-default port reaches a different service on the same host.
    f"https://github.com:8443/{_REPO}.git",
    # Scheme downgrade.
    f"http://github.com/{_REPO}.git",
    # Path prefix. Whole-key equality, never startswith.
    "https://github.com/octo/demo-agent-evil.git",
    # Unparseable port: urlsplit().port raises ValueError, which must read as a
    # mismatch rather than escaping as a 500.
    f"https://github.com:notaport/{_REPO}.git",
    # Malformed IPv6 literal: urlsplit itself raises ValueError.
    f"https://[::1/{_REPO}.git",
]


@pytest.mark.parametrize("payload_url", _MISMATCH_SUBSET)
def test_verify_push_origin_rejects_a_mismatched_origin(payload_url: str) -> None:
    from curie_api.gitflow import verify_push_origin

    settings = Settings(github_token="ghs-secret-token")
    with pytest.raises(CloneOriginMismatch):
        verify_push_origin(payload_url, _VALID_SHA1, settings, repo_full_name=_REPO)


@pytest.mark.parametrize(
    "payload_url",
    [
        # GitHub sets clone_url with .git and url without it.
        f"https://github.com/{_REPO}.git",
        f"https://github.com/{_REPO}",
        # Trailing slash, with and without .git before it.
        f"https://github.com/{_REPO}.git/",
        f"https://github.com/{_REPO}/",
        # An explicit default port is the same origin.
        f"https://github.com:443/{_REPO}.git",
        # urlsplit().hostname lowercases ASCII hosts.
        f"https://GitHub.com/{_REPO}.git",
    ],
)
def test_verify_push_origin_returns_the_derived_url_for_every_accepted_form(
    payload_url: str,
) -> None:
    # Asserting the RETURN VALUE, not merely "it did not raise". The derived URL
    # is the only string that may ever reach git, and a function that accepted
    # the payload and handed its caller the payload back would satisfy a
    # does-not-raise assertion while re-opening #1122 for whoever uses it next.
    from curie_api.gitflow import verify_push_origin

    assert (
        verify_push_origin(payload_url, _VALID_SHA1, Settings(), repo_full_name=_REPO)
        == _GITHUB_URL
    )


def test_verify_push_origin_refuses_a_mixed_case_scheme_as_a_config_error() -> None:
    # `HTTPS://` is refused by the case-sensitive PAYLOAD scheme allowlist, which
    # runs BEFORE the origin comparator. The error TYPE is the assertion: a
    # scheme refusal is not a forged-push report, and the two produce different
    # operator-facing codes (`git.archive_failed` vs `git.origin_mismatch`).
    # Case-folding that allowlist to turn this into an accept would be widening a
    # security gate to pass a test. Nobody may do that.
    from curie_api.gitflow import verify_push_origin

    with pytest.raises(GitFlowError) as err:
        verify_push_origin(
            f"HTTPS://github.com/{_REPO}.git", _VALID_SHA1, Settings(), repo_full_name=_REPO
        )
    assert not isinstance(err.value, CloneOriginMismatch)


@pytest.mark.parametrize("clone_base", ["github.com", "ssh://git@github.com"])
def test_verify_push_origin_refuses_a_configured_base_outside_the_allowlist(
    clone_base: str,
) -> None:
    # The DERIVED url gets its own scheme check, because it -- not the payload --
    # is what git would be handed. A misconfigured `github_clone_base` must fail
    # as a deployment configuration error, never be reported as a forged push,
    # so again the assertion that carries the meaning is the error type.
    from curie_api.gitflow import verify_push_origin

    settings = Settings(github_token="ghs-secret-token", github_clone_base=clone_base)
    with pytest.raises(GitFlowError) as err:
        verify_push_origin(_GITHUB_URL, _VALID_SHA1, settings, repo_full_name=_REPO)
    assert not isinstance(err.value, CloneOriginMismatch)


def test_verify_push_origin_propagates_an_invalid_repo_full_name() -> None:
    # #1140. `InvalidRepoFullName` is a distinct exception rather than a
    # GitFlowError precisely so `process_push` can map it to
    # `git.invalid_repository`, and it must escape this function unwrapped or the
    # code changes shape depending on whether the delivery cloned. This is why
    # `git.invalid_repository` needs no duplicate integration test per path: one
    # function raises it for both.
    from curie_api.gitflow import verify_push_origin
    from curie_api.repo_full_name import InvalidRepoFullName

    with pytest.raises(InvalidRepoFullName):
        verify_push_origin(_ALLOWED_URL, _VALID_SHA1, _local_settings(), repo_full_name="octo/..")


@pytest.mark.parametrize(
    "bad_sha",
    [
        "--upload-pack=touch /tmp/pwned",  # a real option-injection payload
        "deadbeef",  # too short
        "A" * 40,  # uppercase hex is rejected
        "a" * 40 + "\n--foo",  # embedded newline smuggling a git option
        "",  # empty
    ],
)
def test_verify_push_origin_rejects_a_malformed_sha(bad_sha: str) -> None:
    # #65's option-injection gate. It travels with the origin check rather than
    # staying behind in the clone, so a delivery that never runs `git archive`
    # still refuses a sha that is not a full lowercase-hex object id -- and the
    # refusal is a plain GitFlowError, not a mismatch report.
    from curie_api.gitflow import verify_push_origin

    with pytest.raises(GitFlowError) as err:
        verify_push_origin(_ALLOWED_URL, bad_sha, _local_settings(), repo_full_name=_REPO)
    assert not isinstance(err.value, CloneOriginMismatch)


def test_verify_push_origin_accepts_a_local_file_origin() -> None:
    # The control on every rejection above: a function that refused everything
    # would satisfy them all and break every real deploy. `file://` is the shape
    # the integration suite's bare repositories use, so this is also the form the
    # end-to-end tests depend on.
    from curie_api.gitflow import verify_push_origin

    assert (
        verify_push_origin(_ALLOWED_URL, _VALID_SHA1, _local_settings(), repo_full_name=_REPO)
        == _ALLOWED_URL
    )


def test_rejected_push_is_logged_loudly() -> None:
    # A rejected push still returns 200 -- GitHub would retry a non-2xx and the
    # push will not succeed on a retry. The cost is that every dashboard reports
    # success while nothing was deployed, so the platform must say so itself.
    # Moved from the webhook router into gitflow (#1268) so the polling lane
    # shares it. Same behaviour; the lane is now named in the record.
    from curie_api.gitflow import log_push_outcome
    from curie_api.schemas.deployments import WebhookResult

    result = WebhookResult(
        status="rejected",
        errors=[{"code": "git.archive_failed", "message": "Repository not found"}],
    )
    payload = {
        "ref": "refs/heads/dev",
        "after": _VALID_SHA1,
        "repository": {"full_name": "acme/private-bundle"},
    }
    with mock.patch("curie_api.gitflow.logger") as log:
        log_push_outcome(result, payload, source="github webhook")
    log.warning.assert_called_once()
    rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
    assert "acme/private-bundle" in rendered
    assert "git.archive_failed" in rendered


def test_a_rejected_webhook_deploy_warns_from_the_router(
    client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The reporter's direct test cannot detect a missing router call. Drive a
    # signed push through the real API and git flow so the webhook's warning is
    # checked even though GitHub receives a successful HTTP acknowledgment.
    repo = "acme-corp/acme-bot"
    created = client.post(
        "/agents",
        json={
            "name": "acme-bot",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
            "repo_full_name": repo,
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    settings = get_settings()
    payload = {
        "ref": f"refs/heads/{settings.dev_branch}",
        "after": _VALID_SHA1,
        "repository": {
            "full_name": repo.upper(),
            "clone_url": f"https://github.com/{repo}.git",
        },
    }
    body = json.dumps(payload).encode()
    signature = "sha256=" + hmac.new(
        settings.github_webhook_secret.encode(), body, hashlib.sha256
    ).hexdigest()
    with caplog.at_level(logging.WARNING, logger="curie_api.gitflow"):
        response = client.post(
            "/github/webhook",
            content=body,
            headers={
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "00000000-0000-4000-8000-000000000001",
                "X-Hub-Signature-256": signature,
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "rejected", result
    assert [error["code"] for error in result["errors"]] == ["git.repository_case_mismatch"]
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.name == "curie_api.gitflow" and record.levelno == logging.WARNING
    ]
    assert any(
        "github webhook" in message and "git.repository_case_mismatch" in message
        for message in warnings
    ), warnings


def test_successful_push_does_not_warn() -> None:
    # Moved from the webhook router into gitflow (#1268) so the polling lane
    # shares it. Same behaviour; the lane is now named in the record.
    from curie_api.gitflow import log_push_outcome
    from curie_api.schemas.deployments import WebhookResult

    with mock.patch("curie_api.gitflow.logger") as log:
        log_push_outcome(WebhookResult(status="deployed"), {}, source="github webhook")
        log_push_outcome(WebhookResult(status="ignored"), {}, source="github webhook")
    log.warning.assert_not_called()
