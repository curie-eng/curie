#!/usr/bin/env bash
# Guards on a pull request body and its changed path metadata.
#
# 1. Reject a literal "\\n" escape. GitHub displays that escape as text rather
#    than a line break, so a following `Closes #<issue>` is not parsed as a
#    closing keyword.
# 2. Reject AI attribution, delegating to check-commit-messages.sh
#    --message-file so both surfaces share ONE matcher. AGENTS.md forbids
#    attribution in commit messages and pull request bodies alike, but only the
#    commit half was enforced, so agent-authored bodies kept arriving with the
#    robot-emoji footer and were merged (#2225). The body is the half a human
#    reviewer sees first and the half no rebase can rewrite.
# 3. Reject a patch-release PR (title `Prepare the vX.Y.Z release` with Z not
#    0) whose Trigger lacks issue numbers, or whose visible Live proof lacks
#    this-pr-ci, a run URL, or an explicit waiver (#2251). The v0.8.x patch
#    PRs shipped as release mechanics with every product tier marked n/a.
# 4. Require visible tier commands and outcomes for mapped changed paths, or a
#    discovery waiver backed by an open issue in this repository (#3816).
#
# Usage:
#   scripts/check-pr-body.sh <body-file> --changed-files-file <file> --open-issues-file <file> [--title-file <file>]
#   scripts/check-pr-body.sh --self-test
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
COMMIT_GATE="$SCRIPT_DIR/check-commit-messages.sh"

usage() {
    echo "Usage: scripts/check-pr-body.sh <body-file> --changed-files-file <file> --open-issues-file <file> [--title-file <file>]" >&2
    echo "       scripts/check-pr-body.sh --self-test" >&2
}

# True when the title is a patch release: "Prepare the vX.Y.Z release" and Z is
# not 0. Feature and major cuts (Z == 0) are not this gate.
is_patch_release_title() {
    local title="$1"
    title="${title//$'\r'/}"
    title="${title#"${title%%[![:space:]]*}"}"
    title="${title%"${title##*[![:space:]]}"}"
    if [[ "$title" =~ ^Prepare\ the\ v([0-9]+)\.([0-9]+)\.([0-9]+)\ release$ ]]; then
        [[ "${BASH_REMATCH[3]}" != "0" ]]
        return
    fi
    return 1
}

# Strip HTML comments, including ones that span lines, then drop blank lines.
visible_text() {
    awk '
        BEGIN { in_comment = 0 }
        {
            line = $0
            out = ""
            while (length(line) > 0) {
                if (in_comment) {
                    idx = index(line, "-->")
                    if (idx == 0) { line = ""; break }
                    line = substr(line, idx + 3)
                    in_comment = 0
                } else {
                    idx = index(line, "<!--")
                    if (idx == 0) { out = out line; break }
                    out = out substr(line, 1, idx - 1)
                    line = substr(line, idx + 4)
                    in_comment = 1
                }
            }
            print out
        }
    ' | sed 's/\r$//;s/^[[:space:]]*//;s/[[:space:]]*$//' | sed '/^$/d'
}

# Print the body of `## <heading>` until the next ATX H2. Exact heading match
# after collapsing space, case-insensitive.
extract_section() {
    local heading="$1"
    awk -v heading="$heading" '
        {
            line = $0
            sub(/\r$/, "", line)
        }
        /^##[[:space:]]+/ {
            title = line
            sub(/^##[[:space:]]+/, "", title)
            gsub(/[[:space:]]+/, " ", title)
            sub(/[[:space:]]+$/, "", title)
            if (capturing) {
                exit
            }
            heading_l = heading
            title_l = title
            # tolower is POSIX; Ubuntu awk (mawk) has no IGNORECASE.
            if (tolower(title_l) == tolower(heading_l)) {
                capturing = 1
                next
            }
        }
        capturing { print line }
    '
}

section_visible() {
    local heading="$1"
    local body_file="$2"
    extract_section "$heading" <"$body_file" | visible_text
}

trigger_lists_issue_numbers() {
    grep -qE '#[0-9]+' <<<"$1"
}

live_proof_names_ci_url_or_waiver() {
    local text="$1"
    # A standalone marker defers proof to this PR's current-head CI.
    if grep -qx 'this-pr-ci' <<<"$text"; then
        return 0
    fi
    if grep -qiE 'https?://' <<<"$text"; then
        return 0
    fi
    grep -qiE '(^|[[:space:]])waiver:[[:space:]]*[^[:space:]]' <<<"$text"
}

check_patch_release_sections() {
    local body_file="$1"
    local title="$2"
    local trigger_text proof_text

    if ! is_patch_release_title "$title"; then
        return 0
    fi

    trigger_text="$(section_visible "Trigger" "$body_file" || true)"
    if [[ -z "$trigger_text" ]] || ! trigger_lists_issue_numbers "$trigger_text"; then
        echo "PR body check failed: patch release PRs must have a non-empty Trigger section listing issue numbers." >&2
        return 1
    fi

    proof_text="$(section_visible "Live proof" "$body_file" || true)"
    if [[ -z "$proof_text" ]] || ! live_proof_names_ci_url_or_waiver "$proof_text"; then
        echo "PR body check failed: patch release PRs must have a non-empty Live proof section naming standalone this-pr-ci, a run URL, or an explicit waiver." >&2
        return 1
    fi
    return 0
}

# Keep the normal guard offline. CI supplies both snapshots through files;
# validation and path classification are part of this same command boundary.
check_discovery_tiers() {
    python3 - "$1" "$2" "$3" <<'PYTHON'
import json
import re
import sys
from pathlib import Path, PurePosixPath


def fail(message):
    print(f"PR body check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def read_json(filename, label):
    try:
        value = json.loads(Path(filename).read_text(encoding="utf8"))
    except (OSError, UnicodeError, ValueError):
        fail(f"{label} metadata must be a readable JSON array")
    if not isinstance(value, list):
        fail(f"{label} metadata must be a JSON array")
    return value


body_file, changed_files_file, open_issues_file = sys.argv[1:]
changed_files = read_json(changed_files_file, "changed files")
if any(
    not isinstance(path, str)
    or not path
    or "\x00" in path
    or path.startswith("/")
    or any(part in ("", ".", "..") for part in path.split("/"))
    for path in changed_files
):
    fail("changed files metadata must contain relative filename strings")
open_issues = read_json(open_issues_file, "open issues")
if any(type(number) is not int or number <= 0 for number in open_issues):
    fail("open issues metadata must contain positive integer issue numbers")
open_issues = set(open_issues)


def path_tiers(path):
    tiers = set()
    name = PurePosixPath(path).name
    stem = PurePosixPath(path).stem
    runtime_file = PurePosixPath(path).suffix in {
        ".py",
        ".sh",
        ".c",
        ".rs",
        ".ts",
        ".tsx",
        ".js",
        ".jsx",
        ".css",
        ".html",
        ".json",
    }
    if runtime_file and path.startswith(
        (
            "apps/api/src/",
            "apps/worker/src/",
            "apps/dispatcher/src/",
            "apps/mail-adapter/src/",
            "apps/ui/src/",
            "cli/src/",
        )
    ):
        tiers.add("local")
    if runtime_file and path.startswith(
        ("runner/src/", "packages/aci-protocol/src/", "packages/plugin-format/src/")
    ):
        tiers.add("skill")
    if (
        path.startswith("charts/curie/templates/")
        and not name.endswith(".md")
        or path.startswith("charts/curie/")
        and "/" not in path.removeprefix("charts/curie/")
        and (name == "Chart.yaml" or re.fullmatch(r"values(?:[.-].+)?\.ya?ml", name))
        or runtime_file
        and path.startswith("apps/worker/src/curie_worker/sandbox/")
    ):
        tiers.add("cluster")
    if path.startswith("compose/") or (
        "/" not in path and re.fullmatch(r"compose(?:[.-].+)?\.ya?ml", path)
    ):
        tiers.add("local")
    if (
        path.startswith("compose/")
        and "release" in stem
        or path
        in {
            "compose.release.yaml",
            "charts/curie/Chart.yaml",
            "charts/curie/values.yaml",
            "pyproject.toml",
            "uv.lock",
            "cli/Cargo.toml",
            "cli/Cargo.lock",
            "runner/pyproject.toml",
            "VERSION",
        }
        or path.startswith("scripts/")
        and ("release" in stem or "install" in stem or stem == "check-version-consistency")
        or path == ".github/workflows/release.yaml"
        or path.startswith("cli/src/")
        and stem in {"installation", "release_accept"}
    ):
        tiers.add("local-release")
    if runtime_file and path.startswith("apps/api/src/curie_api/"):
        if (
            stem.startswith(("factory_", "github_factory", "publication", "workitem"))
            or "/routers/" in path
            and stem.startswith(("work_item", "factory_status"))
        ):
            tiers.add("factory")
        if stem.startswith("publication"):
            tiers.update(("live provider", "external integration"))
        if (
            stem.startswith("github_factory")
            or stem == "repository_auth"
            or "/routers/" in path
            and stem == "hooks"
        ):
            tiers.add("external integration")
    if runtime_file and path.startswith("apps/worker/src/curie_worker/"):
        if stem.startswith(("workitem", "work_item", "publication")) or stem in {
            "kernel",
            "turn_progress",
        }:
            tiers.add("factory")
        if stem.startswith("publication"):
            tiers.update(("live provider", "external integration"))
    if runtime_file and path.startswith("runner/src/curie_runner/"):
        if (
            name.endswith(".py")
            and (
                stem.startswith("mcp")
                or stem
                in {
                    "hooks",
                    "__main__",
                    "session",
                    "plugin",
                    "sdk_auth",
                    "approval",
                    "state",
                    "issue_read",
                    "progress",
                    "turn_progress",
                    "tool_access",
                    "connectors",
                    "workspace_snapshot",
                }
            )
            or stem == "bash_credential_prelude"
        ):
            tiers.update(("live provider", "external integration"))
        if stem in {
            "__main__",
            "hooks",
            "progress",
            "turn_progress",
            "publication_precheck",
            "verification",
            "workspace_snapshot",
        }:
            tiers.add("factory")
        if stem in {"budget", "usage_report"}:
            tiers.add("live provider")
    if runtime_file and path.startswith(("apps/dispatcher/src/", "apps/mail-adapter/src/")):
        tiers.add("external integration")
    if runtime_file and path.startswith("cli/src/") and stem.startswith("factory_"):
        tiers.add("factory")
    if runtime_file and path.startswith("cli/src/") and stem in {"modelpin", "providers"}:
        tiers.add("live provider")
    if path.startswith("examples/dark-factory/"):
        tiers.update(("factory", "live provider", "external integration"))
    return tiers


required_tiers = set().union(*(path_tiers(path) for path in changed_files))
try:
    body = Path(body_file).read_text(encoding="utf8")
except (OSError, UnicodeError):
    fail("body must be readable UTF8 text")

# Match the workflow's visibility rules: comments and code blocks cannot supply
# evidence or waiver issue references. An unterminated fence stays hidden.
body = re.sub(r"<!--.*?(?:-->|$)", "", body, flags=re.S)
visible_lines = []
fence = None
for line in body.splitlines():
    if fence:
        if re.fullmatch(r" {0,3}" + re.escape(fence[0]) + r"{" + str(fence[1]) + r",}\s*", line):
            fence = None
        visible_lines.append("")
        continue
    opening = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
    if opening:
        fence = (opening[1][0], len(opening[1]))
        visible_lines.append("")
    elif re.match(r"^(?: {4}| {0,3}\t)", line):
        visible_lines.append("")
    else:
        visible_lines.append(line)


def cells(line):
    # Escaped pipes belong to the cell. Unescaped pipes delimit GFM cells,
    # including within code spans, so malformed tables cannot supply evidence.
    if "|" not in line:
        return None
    parts = re.split(r"(?<!\\)\|", line.strip())
    if not parts[0]:
        parts.pop(0)
    if parts and not parts[-1]:
        parts.pop()
    return [part.strip().replace(r"\|", "|") for part in parts]


def plain(text):
    return re.sub(r"\s+", " ", text.replace("*", "").replace("_", "").strip()).casefold()


def without_code(text):
    return re.sub(r"(`+).*?\1", "", text)


def issue_numbers(text):
    # Require bare same-repository #N references. A linked or qualified foreign
    # reference must not borrow an open issue with the same number here.
    text = re.sub(r"\[[^\]]*\]\([^)]*\)", "", without_code(text))
    return {int(number) for number in re.findall(r"(?<![\w/])#([1-9][0-9]*)\b", text)}


def waiver(text):
    match = re.search(r"(?:^|\s)Discovery waiver:\s*(.+)$", without_code(text), flags=re.I)
    if not match:
        return False
    reason = re.sub(r"(?<![\w/])#[0-9]+\b", "", match[1])
    reason = re.sub(r"\[[^\]]*\]\([^)]*\)", "", reason)
    reason = re.sub(r"<[^>]*>", "", reason)
    return bool(re.search(r"[A-Za-z]{2,}", reason)) and bool(issue_numbers(match[1]) & open_issues)


tiers = {
    "skill",
    "local",
    "local-release",
    "cluster",
    "live provider",
    "external integration",
    "factory",
}
expected_header = [
    "tier",
    "required / n/a",
    "reason",
    "mode (fake / live)",
    "command and observed outcome",
]
rows = {}
table_lines = set()
index = 0
while index + 1 < len(visible_lines):
    header, delimiter = cells(visible_lines[index]), cells(visible_lines[index + 1])
    if (
        not header
        or not delimiter
        or len(header) != len(delimiter)
        or not all(re.fullmatch(r":?-{3,}:?", cell) for cell in delimiter)
    ):
        index += 1
        continue
    table_lines.update((index, index + 1))
    discovery_table = [plain(cell) for cell in header] == expected_header
    index += 2
    while index < len(visible_lines):
        row = cells(visible_lines[index])
        if not row:
            break
        table_lines.add(index)
        combined = " ".join(row)
        if re.search(r"\bfollow[\s-]*ups?\b", combined, flags=re.I) and not issue_numbers(combined):
            fail("every table follow up must include an issue number")
        if discovery_table and plain(row[0]) in tiers:
            tier = plain(row[0])
            if tier in rows:
                fail(f"duplicate {tier} tier rows are not allowed")
            if len(row) != len(expected_header):
                fail(f"{tier} tier row must have all five table cells")
            rows[tier] = row
        index += 1

global_waiver = any(
    index not in table_lines
    and re.match(r"^\s*Discovery waiver:", line, flags=re.I)
    and waiver(line)
    for index, line in enumerate(visible_lines)
)
required_tiers.update(tier for tier, row in rows.items() if plain(row[1]) == "required")
for tier in sorted(required_tiers):
    row = rows.get(tier)
    if row and plain(row[1]) != "required":
        fail(f"changed paths require {tier} to be marked required")
    if global_waiver or row and any(waiver(cell) for cell in row):
        continue
    if not row:
        fail(f"changed paths require the {tier} tier row with command and observed outcome")
    mode, evidence = row[3], row[4]
    outcome = without_code(evidence)
    if re.search(r"\b(?:blocked|not[\s-]+run|fake)\b", outcome, flags=re.I) or re.search(
        r"\b(?:blocked|not[\s-]+run)\b", mode, flags=re.I
    ):
        fail(f"{tier} evidence is unproved; supply a Discovery waiver naming an open issue")
    if tier in {"live provider", "external integration", "factory"} and plain(mode) != "live":
        fail(f"{tier} requires live evidence or a Discovery waiver naming an open issue")
    commands = re.findall(r"(`+)(.*?)\1", evidence)
    if not any(command.strip() for _, command in commands):
        fail(f"{tier} evidence must include the exact command in backticks")
    outcome = re.sub(r"<[^>]*>", "", outcome).strip(" *;:,.()")
    if not re.search(r"[A-Za-z]{2,}", outcome) or plain(outcome) in {
        "n/a",
        "none",
        "tbd",
        "pending",
        "outcome",
        "observed outcome",
        "result",
        "results",
    }:
        fail(f"{tier} evidence must include an observed outcome outside the command")
PYTHON
}

check_body_file() {
    local body_file="$1"
    local title_file="$2"
    local changed_files_file="$3"
    local open_issues_file="$4"
    local grep_status title=""
    local patch_release=0

    if [[ ! -f "$body_file" ]]; then
        echo "PR body check failed: '$body_file' is not a regular file" >&2
        return 1
    fi
    if [[ ! -r "$body_file" ]]; then
        echo "PR body check failed: '$body_file' is not readable" >&2
        return 1
    fi

    # Single quotes deliberately preserve the two literal bytes (backslash and
    # n). Real line-feed bytes do not match this fixed string.
    if LC_ALL=C grep -F -q '\n' -- "$body_file"; then
        echo "PR body check failed: found a literal \\n escape sequence." >&2
        echo "Replace each literal \\n with a real newline before opening or editing the PR." >&2
        return 1
    else
        grep_status=$?
        if ((grep_status != 1)); then
            echo "PR body check failed: could not read '$body_file' while checking for literal \\n escapes" >&2
            return 1
        fi
    fi

    if [[ ! -x "$COMMIT_GATE" && ! -r "$COMMIT_GATE" ]]; then
        echo "PR body check failed: cannot read '$COMMIT_GATE'" >&2
        return 1
    fi
    if ! bash "$COMMIT_GATE" --message-file "$body_file" >/dev/null; then
        echo "PR body check failed: the body claims AI authorship." >&2
        echo "AGENTS.md forbids AI attribution in commit messages and PR bodies alike." >&2
        return 1
    fi

    if [[ -n "$title_file" ]]; then
        if [[ ! -f "$title_file" ]]; then
            echo "PR body check failed: '$title_file' is not a regular file" >&2
            return 1
        fi
        if [[ ! -r "$title_file" ]]; then
            echo "PR body check failed: '$title_file' is not readable" >&2
            return 1
        fi
        title="$(head -n 1 -- "$title_file")"
        if ! check_patch_release_sections "$body_file" "$title"; then
            return 1
        fi
        if is_patch_release_title "$title"; then
            patch_release=1
        fi
    fi

    if ! check_discovery_tiers "$body_file" "$changed_files_file" "$open_issues_file"; then
        return 1
    fi

    if ((patch_release)); then
        echo "PR body check passed: real newlines, no AI attribution, patch release Trigger and Live proof and required tier evidence are present"
    else
        echo "PR body check passed: real newlines, no AI attribution, required tier evidence is present"
    fi
}

self_test() {
    local temp_dir real_newline_body literal_escape_body attributed_body footer_no_newline_body
    local empty_patch_body filled_patch_body comment_trigger_body empty_proof_body marker_patch_body
    local patch_title feature_title patch10_title
    local changed_files_file open_issues_file discovery_body discovery_header
    local local_row skill_row cluster_row factory_row provider_row integration_row release_row
    local factory_files publication_files runner_files chart_files docs_files
    local case_name expected body_text files_text issues_text status index
    local mode indent indented_body fenced_examples
    local path missing_tier complete_rows refusal_rows follow_text unreadable_metadata_file
    local -a metadata_args patch_release_cases discovery_cases mapping_cases

    temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/curie-pr-body-check.XXXXXX")"
    trap 'rm -rf -- "$temp_dir"' RETURN
    real_newline_body="$temp_dir/real-newline.md"
    literal_escape_body="$temp_dir/literal-escape.md"
    attributed_body="$temp_dir/attributed.md"
    footer_no_newline_body="$temp_dir/footer-no-newline.md"
    empty_patch_body="$temp_dir/empty-patch.md"
    filled_patch_body="$temp_dir/filled-patch.md"
    comment_trigger_body="$temp_dir/comment-trigger.md"
    empty_proof_body="$temp_dir/empty-proof.md"
    marker_patch_body="$temp_dir/marker-patch.md"
    patch_title="$temp_dir/patch-title.txt"
    feature_title="$temp_dir/feature-title.txt"
    patch10_title="$temp_dir/patch-10-title.txt"
    changed_files_file="$temp_dir/changed-files.json"
    open_issues_file="$temp_dir/open-issues.json"
    discovery_body="$temp_dir/discovery.md"
    printf '[]\n' >"$changed_files_file"
    printf '[]\n' >"$open_issues_file"
    metadata_args=(--changed-files-file "$changed_files_file" --open-issues-file "$open_issues_file")

    printf 'Describe a fix.\n\nCloses #1713\n' >"$real_newline_body"
    printf 'Describe a fix.\\n\\nCloses #1713\n' >"$literal_escape_body"
    # The exact footer that reached #2225, and the same footer with no trailing
    # newline, which is the shape a body pasted from a tool actually has.
    printf 'Describe a fix.\n\n\xf0\x9f\xa4\x96 Generated with [Claude Code](https://claude.com/claude-code)\n' \
        >"$attributed_body"
    printf 'Describe a fix.\n\n\xf0\x9f\xa4\x96 Generated with [Claude Code](https://claude.com/claude-code)' \
        >"$footer_no_newline_body"

    printf 'Prepare the v0.8.4 release\n' >"$patch_title"
    printf 'Prepare the v0.9.0 release\n' >"$feature_title"
    printf 'Prepare the v0.8.10 release\n' >"$patch10_title"

    printf '%s\n' \
        '## Summary' \
        '' \
        'Prepare the frozen v0.8.4 patch release.' \
        '' \
        '## Related issue' \
        '' \
        'Milestone: v0.8.4' \
        >"$empty_patch_body"

    printf '%s\n' \
        '## Summary' \
        '' \
        'Prepare the frozen v0.8.4 patch release.' \
        '' \
        '## Trigger' \
        '' \
        '#2202, #2203, #2205, #2194' \
        '' \
        '## Live proof' \
        '' \
        'https://github.com/curie-eng/curie/actions/runs/1' \
        >"$filled_patch_body"

    printf '%s\n' \
        '## Trigger' \
        '' \
        '<!-- List the issue numbers of the defects that triggered this patch. -->' \
        '' \
        '## Live proof' \
        '' \
        'https://github.com/curie-eng/curie/actions/runs/1' \
        >"$comment_trigger_body"

    printf '%s\n' \
        '## Trigger' \
        '' \
        '#2202' \
        '' \
        '## Live proof' \
        '' \
        '<!-- Name a run URL or an explicit waiver. -->' \
        >"$empty_proof_body"

    if ! bash "$SCRIPT_PATH" "$real_newline_body" "${metadata_args[@]}" >/dev/null; then
        echo "PR body check self-test failed: real newlines were rejected" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$literal_escape_body" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: literal \\n was accepted" >&2
        return 1
    fi

    if bash "$SCRIPT_PATH" "$attributed_body" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: AI attribution footer was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$footer_no_newline_body" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: unterminated AI attribution footer was accepted" >&2
        return 1
    fi

    if bash "$SCRIPT_PATH" "$empty_patch_body" --title-file "$patch_title" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: empty patch-release Trigger/Live proof were accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$comment_trigger_body" --title-file "$patch_title" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: comment-only Trigger was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$empty_proof_body" --title-file "$patch_title" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: empty Live proof was accepted" >&2
        return 1
    fi
    if ! bash "$SCRIPT_PATH" "$filled_patch_body" --title-file "$patch_title" "${metadata_args[@]}" >/dev/null; then
        echo "PR body check self-test failed: filled Trigger and Live proof were rejected" >&2
        return 1
    fi

    # Three fields per patch case: name, expected status, body. A marker must
    # supply visible proof in its own section without weakening the Trigger.
    patch_release_cases=(
        "standalone this-pr-ci Live proof is accepted" 0 $'## Trigger\n\n#2202\n\n## Live proof\n\nthis-pr-ci'
        "explicit Live proof waiver remains accepted" 0 $'## Trigger\n\n#2202\n\n## Live proof\n\nwaiver: No live provider behavior changed.'
        "misspelled this-pr-ci marker is rejected" 1 $'## Trigger\n\n#2202\n\n## Live proof\n\nthis-pr-cl'
        "prose mentioning this-pr-ci is rejected" 1 $'## Trigger\n\n#2202\n\n## Live proof\n\nThe this-pr-ci marker will be added after validation.'
        "comment-only this-pr-ci marker is rejected" 1 $'## Trigger\n\n#2202\n\n## Live proof\n\n<!-- this-pr-ci -->'
        "multiline comment-only this-pr-ci marker is rejected" 1 $'## Trigger\n\n#2202\n\n## Live proof\n\n<!--\nthis-pr-ci\n-->'
        "this-pr-ci outside Live proof is rejected" 1 $'## Summary\n\nthis-pr-ci\n\n## Trigger\n\n#2202\n\n## Live proof\n\nPending proof.'
        "this-pr-ci after Live proof is rejected" 1 $'## Trigger\n\n#2202\n\n## Live proof\n\nPending proof.\n\n## Follow-ups\n\nthis-pr-ci'
        "this-pr-ci without Trigger is rejected" 1 $'## Live proof\n\nthis-pr-ci'
        "this-pr-ci with comment-only Trigger is rejected" 1 $'## Trigger\n\n<!-- #2202 -->\n\n## Live proof\n\nthis-pr-ci'
    )
    for ((index = 0; index < ${#patch_release_cases[@]}; index += 3)); do
        case_name="${patch_release_cases[index]}"
        expected="${patch_release_cases[index + 1]}"
        body_text="${patch_release_cases[index + 2]}"
        printf '%s\n' "$body_text" >"$marker_patch_body"
        if bash "$SCRIPT_PATH" "$marker_patch_body" --title-file "$patch_title" "${metadata_args[@]}" >/dev/null 2>&1; then
            status=0
        else
            status=$?
        fi
        if ((status != expected)); then
            echo "PR body check self-test failed: $case_name (expected $expected, observed $status)" >&2
            return 1
        fi
    done

    if ! bash "$SCRIPT_PATH" "$empty_patch_body" --title-file "$feature_title" "${metadata_args[@]}" >/dev/null; then
        echo "PR body check self-test failed: a feature-release title was gated as a patch" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$empty_patch_body" --title-file "$patch10_title" "${metadata_args[@]}" >/dev/null 2>&1; then
        echo "PR body check self-test failed: two-digit patch version skipped the gate" >&2
        return 1
    fi

    if bash "$SCRIPT_PATH" "$real_newline_body" >/dev/null 2>&1; then
        echo "PR body check self-test failed: missing discovery metadata was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$real_newline_body" --open-issues-file "$open_issues_file" >/dev/null 2>&1; then
        echo "PR body check self-test failed: missing changed files metadata was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$real_newline_body" --changed-files-file "$changed_files_file" >/dev/null 2>&1; then
        echo "PR body check self-test failed: missing open issues metadata was accepted" >&2
        return 1
    fi
    unreadable_metadata_file="$temp_dir/unreadable-metadata.json"
    printf '[]\n' >"$unreadable_metadata_file"
    chmod 000 "$unreadable_metadata_file"
    if bash "$SCRIPT_PATH" "$real_newline_body" --changed-files-file "$unreadable_metadata_file" --open-issues-file "$open_issues_file" >/dev/null 2>&1; then
        chmod 600 "$unreadable_metadata_file"
        echo "PR body check self-test failed: unreadable changed files metadata was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$real_newline_body" --changed-files-file "$changed_files_file" --open-issues-file "$unreadable_metadata_file" >/dev/null 2>&1; then
        chmod 600 "$unreadable_metadata_file"
        echo "PR body check self-test failed: unreadable open issues metadata was accepted" >&2
        return 1
    fi
    chmod 600 "$unreadable_metadata_file"

    discovery_header=$'## End-to-end verification\n\n| Tier | Required / n/a | Reason | Mode (fake / live) | Command and observed outcome |\n| --- | --- | --- | --- | --- |'
    local_row='| local | required | API runtime changed | live | `CURIE_E2E_TIERS=local curie dev e2e-ladder` exited 0; the local loop passed |'
    skill_row='| skill | required | Runner runtime changed | fake | `CURIE_E2E_TIERS=skill curie dev e2e-ladder` exited 0; all skill cases passed |'
    cluster_row='| cluster | required | Chart runtime changed | live | `CURIE_E2E_TIERS=cluster curie dev e2e-ladder` exited 0; all cluster cases passed |'
    factory_row='| factory | required | Factory admission changed | live | `curie dev factory-e2e preflight` exited 0; the work item was admitted |'
    provider_row='| live provider | required | Provider tool projection changed | live | `CURIE_E2E_LIVE=1 CURIE_E2E_TIERS=skill curie dev e2e-ladder` exited 0; the provider returned a tool receipt |'
    integration_row='| external integration | required | External tool dispatch changed | live | `CURIE_E2E_LIVE=1 CURIE_E2E_TIERS=skill curie dev e2e-ladder` exited 0; the external service received the tool call |'
    release_row='| local-release | required | Released install identity changed | live | `uv run python3 scripts/check-released-upgrade.py` exited 0; the released upgrade passed |'
    factory_files='["apps/api/src/curie_api/factory_ci.py"]'
    publication_files='["apps/api/src/curie_api/publication_policy.py"]'
    runner_files='["runner/src/curie_runner/mcp_tool_capability.py"]'
    chart_files='["charts/curie/templates/worker.yaml"]'
    docs_files='["docs/contributing.md"]'
    fenced_examples=$'```markdown\n'"$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n```\n\n```text\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123\n```'

    # Each case invokes the script itself with the same metadata contract as CI.
    # Five fields per case: name, expected status, body, changed paths, open issues.
    discovery_cases=(
        "unrelated docs need no tier rows" 0 'Document wording changed.' "$docs_files" '[]'
        "factory changes require a table" 1 'Factory admission changed.' "$factory_files" '[]'
        "factory changes require the factory row" 1 "$discovery_header"$'\n'"$local_row" "$factory_files" '[]'
        "blank factory classification is refused" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | | | | |' "$factory_files" '[]'
        "factory n/a is refused" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | n/a | Unit checks cover admission | live | `curie dev factory-e2e preflight` exited 0 |' "$factory_files" '[]'
        "factory command without outcome is refused" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | required | Factory admission changed | live | `curie dev factory-e2e preflight` |' "$factory_files" '[]'
        "factory outcome without command is refused" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | required | Factory admission changed | live | The work item was admitted |' "$factory_files" '[]'
        "factory rows with command and outcome pass" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row" "$factory_files" '[]'
        "factory changes also require local" 1 "$discovery_header"$'\n'"$factory_row" "$factory_files" '[]'
        "worker kernel requires factory" 1 "$discovery_header"$'\n'"$local_row" '["apps/worker/src/curie_worker/kernel.py"]' '[]'
        "worker kernel local and factory evidence pass" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row" '["apps/worker/src/curie_worker/kernel.py"]' '[]'
        "worker turn progress requires factory" 1 "$discovery_header"$'\n'"$local_row" '["apps/worker/src/curie_worker/turn_progress.py"]' '[]'
        "worker turn progress local and factory evidence pass" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row" '["apps/worker/src/curie_worker/turn_progress.py"]' '[]'
        "API factory progress requires factory" 1 "$discovery_header"$'\n'"$local_row" '["apps/api/src/curie_api/factory_progress.py"]' '[]'
        "API factory progress complete evidence passes" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row" '["apps/api/src/curie_api/factory_progress.py"]' '[]'
        "publication router requires factory" 1 "$discovery_header"$'\n'"$local_row"$'\n'"$provider_row"$'\n'"$integration_row" '["apps/api/src/curie_api/routers/publications.py"]' '[]'
        "publication router complete evidence passes" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" '["apps/api/src/curie_api/routers/publications.py"]' '[]'
        "factory examples require factory" 1 "$discovery_header"$'\n'"$provider_row"$'\n'"$integration_row" '["examples/dark-factory/hooks/verify.sh"]' '[]'
        "factory examples complete evidence passes" 0 "$discovery_header"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" '["examples/dark-factory/hooks/verify.sh"]' '[]'
        "worker work item dispatch requires factory" 1 "$discovery_header"$'\n'"$local_row" '["apps/worker/src/curie_worker/workitem_dispatch.py"]' '[]'
        "worker work item dispatch complete evidence passes" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row" '["apps/worker/src/curie_worker/workitem_dispatch.py"]' '[]'
        "runner progress requires factory" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"$integration_row" '["runner/src/curie_runner/progress.py"]' '[]'
        "runner progress complete evidence passes" 0 "$discovery_header"$'\n'"$skill_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" '["runner/src/curie_runner/progress.py"]' '[]'
        "runner publication precheck requires factory" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"$integration_row" '["runner/src/curie_runner/publication_precheck.py"]' '[]'
        "runner publication precheck complete evidence passes" 0 "$discovery_header"$'\n'"$skill_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" '["runner/src/curie_runner/publication_precheck.py"]' '[]'
        "missing table has an open issue waiver" 0 $'Factory admission changed.\n\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123' "$factory_files" '[123]'
        "closed issue cannot waive missing table" 1 $'Factory admission changed.\n\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123' "$factory_files" '[]'
        "unknown issue cannot waive missing table" 1 $'Factory admission changed.\n\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #456' "$factory_files" '[123]'
        "waiver requires a reason" 1 $'Factory admission changed.\n\nDiscovery waiver: #123' "$factory_files" '[123]'
        "waiver requires an issue number" 1 $'Factory admission changed.\n\nDiscovery waiver: Disposable infrastructure unavailable' "$factory_files" '[123]'
        "runner MCP rows with live evidence pass" 0 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"$integration_row" "$runner_files" '[]'
        "runner MCP requires skill" 1 "$discovery_header"$'\n'"$provider_row"$'\n'"$integration_row" "$runner_files" '[]'
        "runner MCP requires live provider" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$integration_row" "$runner_files" '[]'
        "runner MCP requires external integration" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row" "$runner_files" '[]'
        "runner verification requires factory" 1 "$discovery_header"$'\n'"$skill_row" '["runner/src/curie_runner/verification.py"]' '[]'
        "runner verification skill and factory evidence pass" 0 "$discovery_header"$'\n'"$skill_row"$'\n'"$factory_row" '["runner/src/curie_runner/verification.py"]' '[]'
        "runner approval requires live provider" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$integration_row" '["runner/src/curie_runner/approval.py"]' '[]'
        "runner approval full evidence passes" 0 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"$integration_row" '["runner/src/curie_runner/approval.py"]' '[]'
        "runner tool access requires live provider" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$integration_row" '["runner/src/curie_runner/tool_access.py"]' '[]'
        "runner tool access full evidence passes" 0 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"$integration_row" '["runner/src/curie_runner/tool_access.py"]' '[]'
        "ordinary skill permits fake mode" 0 "$discovery_header"$'\n'"$skill_row" '["runner/src/curie_runner/translate.py"]' '[]'
        "ordinary skill command may name fake model" 0 "$discovery_header"$'\n'"${skill_row/curie dev e2e-ladder/curie dev e2e-ladder --fake-model}" '["runner/src/curie_runner/translate.py"]' '[]'
        "voluntary required skill with completed evidence passes" 0 "$discovery_header"$'\n'"$skill_row" "$docs_files" '[]'
        "voluntary required skill needs an observed outcome" 1 "$discovery_header"$'\n| skill | required | Documentation example verified | fake | `CURIE_E2E_TIERS=skill curie dev e2e-ladder` |' "$docs_files" '[]'
        "voluntary required skill refuses fake evidence" 1 "$discovery_header"$'\n| skill | required | Documentation example verified | fake | fake |' "$docs_files" '[]'
        "publication rows with all required tiers pass" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" "$publication_files" '[]'
        "publication requires local" 1 "$discovery_header"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" "$publication_files" '[]'
        "publication requires factory" 1 "$discovery_header"$'\n'"$local_row"$'\n'"$provider_row"$'\n'"$integration_row" "$publication_files" '[]'
        "publication requires live provider" 1 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n'"$integration_row" "$publication_files" '[]'
        "publication requires external integration" 1 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n'"$provider_row" "$publication_files" '[]'
        "chart changes require cluster" 1 'Chart runtime changed.' "$chart_files" '[]'
        "chart cluster evidence passes" 0 "$discovery_header"$'\n'"$cluster_row" "$chart_files" '[]'
        "follow up in any row requires an issue" 1 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n| local-release | n/a | Follow up after infrastructure is available | | |' "$factory_files" '[]'
        "follow up with an issue passes" 0 "$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n| local-release | n/a | Follow up in #123 after infrastructure is available | | |' "$factory_files" '[]'
        "comment only table cannot prove required tiers" 1 $'<!--\n'"$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n-->' "$factory_files" '[]'
        "fenced table cannot prove required tiers" 1 $'```markdown\n'"$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n```' "$factory_files" '[]'
        "fenced waiver cannot excuse required tiers" 1 $'```text\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123\n```' "$factory_files" '[123]'
        "visible table alongside fenced examples passes" 0 "$fenced_examples"$'\n\n'"$discovery_header"$'\n'"$local_row"$'\n'"$factory_row" "$factory_files" '[123]'
        "duplicate required row is refused" 1 "$discovery_header"$'\n'"$local_row"$'\n'"$local_row"$'\n'"$factory_row" "$factory_files" '[]'
        "row waiver accepts only its required tier" 0 "$discovery_header"$'\n'"$local_row"$'\n| factory | required | Discovery waiver: Disposable infrastructure unavailable; tracked in #123 | live | blocked |' "$factory_files" '[123]'
        "closed issue cannot waive a required row" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | required | Discovery waiver: Disposable infrastructure unavailable; tracked in #123 | live | blocked |' "$factory_files" '[]'
        "unknown issue cannot waive a required row" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | required | Discovery waiver: Disposable infrastructure unavailable; tracked in #456 | live | blocked |' "$factory_files" '[123]'
        "n/a row waiver cannot waive mapped factory tier" 1 "$discovery_header"$'\n'"$local_row"$'\n| factory | n/a | Discovery waiver: Disposable infrastructure unavailable; tracked in #123 | live | blocked |' "$factory_files" '[123]'
        "factory row waiver cannot waive missing local" 1 "$discovery_header"$'\n| factory | required | Discovery waiver: Disposable infrastructure unavailable; tracked in #123 | live | blocked |' "$factory_files" '[123]'
        "changed files must be a JSON filename array" 1 'Document wording changed.' '[123]' '[]'
        "open issues must be a JSON integer array" 1 'Document wording changed.' "$docs_files" '["123"]'
    )

    for status in blocked 'not run' fake; do
        body_text="$discovery_header"$'\n'"$local_row"$'\n| factory | required | Factory admission changed | live | '"$status"' |'
        discovery_cases+=(
            "factory $status evidence is refused" 1 "$body_text" "$factory_files" '[]'
            "factory $status evidence has an open issue waiver" 0 "$body_text"$'\n\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123' "$factory_files" '[123]'
        )
    done
    body_text="$discovery_header"$'\n'"$local_row"$'\n'"${factory_row/| live |/| fake |}"
    discovery_cases+=(
        "required factory refuses fake mode" 1 "$body_text" "$factory_files" '[]'
        "fake factory mode has an open issue waiver" 0 "$body_text"$'\n\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123' "$factory_files" '[123]'
        "ordinary skill refuses fake evidence" 1 "$discovery_header"$'\n| skill | required | Runner runtime changed | fake | fake |' '["runner/src/curie_runner/translate.py"]' '[]'
        "ordinary local refuses fake evidence" 1 "$discovery_header"$'\n| local | required | API runtime changed | fake | fake |' '["apps/api/src/curie_api/schemas.py"]' '[]'
        "ordinary local permits fake mode with observed outcome" 0 "$discovery_header"$'\n'"${local_row/| live |/| fake |}" '["apps/api/src/curie_api/schemas.py"]' '[]'
    )
    body_text="$discovery_header"$'\n'"$skill_row"$'\n'"${provider_row/| live |/| fake |}"$'\n'"$integration_row"
    discovery_cases+=(
        "required live provider refuses fake mode" 1 "$body_text" "$runner_files" '[]'
        "fake provider evidence has an open issue waiver" 0 "$body_text"$'\n\nDiscovery waiver: Provider access unavailable; tracked in #123' "$runner_files" '[123]'
    )
    body_text="$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"${integration_row/| live |/| fake |}"
    discovery_cases+=(
        "required external integration refuses fake mode" 1 "$body_text" "$runner_files" '[]'
        "fake integration evidence has an open issue waiver" 0 "$body_text"$'\n\nDiscovery waiver: External service access unavailable; tracked in #123' "$runner_files" '[123]'
    )

    discovery_cases+=(
        "factory refuses blank mode" 1 "$discovery_header"$'\n'"$local_row"$'\n'"${factory_row/| live |/| |}" "$factory_files" '[]'
        "live provider refuses blank mode" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"${provider_row/| live |/| |}"$'\n'"$integration_row" "$runner_files" '[]'
        "external integration refuses blank mode" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"${integration_row/| live |/| |}" "$runner_files" '[]'
        "factory refuses mock mode" 1 "$discovery_header"$'\n'"$local_row"$'\n'"${factory_row/| live |/| mock |}" "$factory_files" '[]'
        "factory refuses replay mode" 1 "$discovery_header"$'\n'"$local_row"$'\n'"${factory_row/| live |/| replay |}" "$factory_files" '[]'
        "live provider refuses fixture mode" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"${provider_row/| live |/| fixture |}"$'\n'"$integration_row" "$runner_files" '[]'
        "external integration refuses kind mode" 1 "$discovery_header"$'\n'"$skill_row"$'\n'"$provider_row"$'\n'"${integration_row/| live |/| kind |}" "$runner_files" '[]'
        "invalid live tier modes have an open issue waiver" 0 "$discovery_header"$'\n'"$local_row"$'\n'"${factory_row/| live |/| mock |}"$'\n'"${provider_row/| live |/| |}"$'\n'"${integration_row/| live |/| kind |}"$'\n\nDiscovery waiver: Required live resources unavailable; tracked in #123' "$publication_files" '[123]'
        "ordinary skill refuses blocked mode despite completed outcome" 1 "$discovery_header"$'\n'"${skill_row/| fake |/| blocked |}" '["runner/src/curie_runner/translate.py"]' '[]'
        "blocked skill mode has an open issue waiver" 0 "$discovery_header"$'\n'"${skill_row/| fake |/| blocked |}"$'\n\nDiscovery waiver: Required runner unavailable; tracked in #123' '["runner/src/curie_runner/translate.py"]' '[123]'
        "ordinary local refuses not run mode despite completed outcome" 1 "$discovery_header"$'\n'"${local_row/| live |/| not run |}" '["apps/api/src/curie_api/schemas.py"]' '[]'
        "not run local mode has an open issue waiver" 0 "$discovery_header"$'\n'"${local_row/| live |/| not run |}"$'\n\nDiscovery waiver: Required local stack unavailable; tracked in #123' '["apps/api/src/curie_api/schemas.py"]' '[123]'
    )
    for indent in '    ' $'\t'; do
        if [[ "$indent" == $'\t' ]]; then
            mode="tab"
        else
            mode="four space"
        fi
        body_text="$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"
        indented_body="$indent${body_text//$'\n'/$'\n'"$indent"}"
        discovery_cases+=(
            "$mode indented table cannot prove required tiers" 1 "$indented_body" "$factory_files" '[]'
            "$mode indented waiver cannot excuse required tiers" 1 "${indent}Discovery waiver: Disposable infrastructure unavailable; tracked in #123" "$factory_files" '[123]'
        )
    done

    for follow_text in 'Follow-ups' 'Follow ups' Followups; do
        body_text="$discovery_header"$'\n'"$local_row"$'\n'"$factory_row"$'\n| local-release | n/a | '"$follow_text"': tracked separately | | |'
        discovery_cases+=(
            "$follow_text without an issue is refused" 1 "$body_text" "$factory_files" '[]'
            "$follow_text with an issue passes" 0 "${body_text/tracked separately/tracked in #123}" "$factory_files" '[]'
        )
    done
    discovery_cases+=(
        "source Markdown near factory mapping needs no tiers" 0 'Document the factory admission contract.' '["apps/api/src/curie_api/factory_notes.md"]' '[]'
        "malformed changed files JSON is refused" 1 'Document wording changed.' '[' '[]'
        "malformed open issues JSON is refused" 1 'Document wording changed.' "$docs_files" '['
        "zero issue number is refused" 1 'Document wording changed.' "$docs_files" '[0]'
        "negative issue number is refused" 1 'Document wording changed.' "$docs_files" '[-1]'
        "boolean issue number is refused" 1 'Document wording changed.' "$docs_files" '[true]'
        "absolute changed path is refused" 1 'Document wording changed.' '["/docs/contributing.md"]' '[]'
        "parent relative changed path is refused" 1 'Document wording changed.' '["apps/../docs/contributing.md"]' '[]'
        "empty changed path is refused" 1 'Document wording changed.' '[""]' '[]'
        "CRLF table evidence passes" 0 "${discovery_header//$'\n'/$'\r\n'}"$'\r\n'"$local_row"$'\r\n'"$factory_row"$'\r' "$factory_files" '[]'
        "waiver with several references accepts an open issue" 0 'Discovery waiver: Disposable infrastructure unavailable; tracked in #456 and #123' "$factory_files" '[123]'
        "waiver with several references needs an open issue" 1 'Discovery waiver: Disposable infrastructure unavailable; tracked in #456 and #123' "$factory_files" '[789]'
        "global waiver cannot excuse unnumbered follow up" 1 "$discovery_header"$'\n| local-release | n/a | Follow up after infrastructure is available | | |\n\nDiscovery waiver: Disposable infrastructure unavailable; tracked in #123' "$factory_files" '[123]'
        "completed live denial and 401 observation passes" 0 "$discovery_header"$'\n'"$skill_row"$'\n| live provider | required | Provider authorization changed | live | `CURIE_E2E_LIVE=1 CURIE_E2E_TIERS=skill curie dev e2e-ladder` exited 0; the unauthorized request was denied with HTTP 401 |\n'"$integration_row" "$runner_files" '[]'
    )

    # Four fields per mapping: path, missing tier, complete rows, refusal rows.
    mapping_cases=(
        'VERSION' 'local-release' "$release_row" ''
        'compose/generate_release_compose.py' 'local-release' "$local_row"$'\n'"$release_row" "$local_row"
        'charts/curie/Chart.yaml' 'local-release' "$cluster_row"$'\n'"$release_row" "$cluster_row"
        'scripts/install.sh' 'local-release' "$release_row" ''
        'cli/src/installation.rs' 'local-release' "$local_row"$'\n'"$release_row" "$local_row"
        '.github/workflows/release.yaml' 'local-release' "$release_row" ''
        'compose.dev.yaml' 'local' "$local_row" ''
        'apps/worker/src/curie_worker/sandbox/k8s.py' 'cluster' "$local_row"$'\n'"$cluster_row" "$local_row"
        'apps/api/src/curie_api/github_factory.py' 'factory' "$local_row"$'\n'"$factory_row"$'\n'"$integration_row" "$local_row"$'\n'"$integration_row"
        'apps/api/src/curie_api/repository_auth.py' 'external integration' "$local_row"$'\n'"$integration_row" "$local_row"
        'apps/api/src/curie_api/routers/hooks.py' 'external integration' "$local_row"$'\n'"$integration_row" "$local_row"
        'apps/api/src/curie_api/routers/work_items.py' 'factory' "$local_row"$'\n'"$factory_row" "$local_row"
        'apps/api/src/curie_api/routers/factory_status.py' 'factory' "$local_row"$'\n'"$factory_row" "$local_row"
        'apps/worker/src/curie_worker/publication_dispatch.py' 'factory' "$local_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" "$local_row"$'\n'"$provider_row"$'\n'"$integration_row"
        'runner/src/curie_runner/hooks.py' 'factory' "$skill_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" "$skill_row"$'\n'"$provider_row"$'\n'"$integration_row"
        'runner/src/curie_runner/__main__.py' 'factory' "$skill_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" "$skill_row"$'\n'"$provider_row"$'\n'"$integration_row"
        'runner/src/curie_runner/workspace_snapshot.py' 'factory' "$skill_row"$'\n'"$factory_row"$'\n'"$provider_row"$'\n'"$integration_row" "$skill_row"$'\n'"$provider_row"$'\n'"$integration_row"
        'apps/dispatcher/src/curie_dispatcher/app.py' 'external integration' "$local_row"$'\n'"$integration_row" "$local_row"
        'apps/mail-adapter/src/curie_mail_adapter/server.py' 'external integration' "$local_row"$'\n'"$integration_row" "$local_row"
        'apps/ui/src/App.tsx' 'local' "$local_row" ''
        'cli/src/status.rs' 'local' "$local_row" ''
        'cli/src/factory_status.rs' 'factory' "$local_row"$'\n'"$factory_row" "$local_row"
        'cli/src/providers.rs' 'live provider' "$local_row"$'\n'"$provider_row" "$local_row"
        'runner/src/curie_runner/budget.py' 'live provider' "$skill_row"$'\n'"$provider_row" "$skill_row"
        'runner/src/curie_runner/usage_report.py' 'live provider' "$skill_row"$'\n'"$provider_row" "$skill_row"
        'packages/aci-protocol/src/aci_protocol/session.py' 'skill' "$skill_row" ''
        'packages/plugin-format/src/plugin_format/manifest.py' 'skill' "$skill_row" ''
    )
    for ((index = 0; index < ${#mapping_cases[@]}; index += 4)); do
        path="${mapping_cases[index]}"
        missing_tier="${mapping_cases[index + 1]}"
        complete_rows="${mapping_cases[index + 2]}"
        refusal_rows="${mapping_cases[index + 3]}"
        files_text='["'"$path"'"]'
        discovery_cases+=(
            "$path requires $missing_tier" 1 "$discovery_header"$'\n'"$refusal_rows" "$files_text" '[]'
            "$path complete evidence passes" 0 "$discovery_header"$'\n'"$complete_rows" "$files_text" '[]'
        )
    done

    for ((index = 0; index < ${#discovery_cases[@]}; index += 5)); do
        case_name="${discovery_cases[index]}"
        expected="${discovery_cases[index + 1]}"
        body_text="${discovery_cases[index + 2]}"
        files_text="${discovery_cases[index + 3]}"
        issues_text="${discovery_cases[index + 4]}"
        printf '%s\n' "$body_text" >"$discovery_body"
        printf '%s\n' "$files_text" >"$changed_files_file"
        printf '%s\n' "$issues_text" >"$open_issues_file"
        if bash "$SCRIPT_PATH" "$discovery_body" "${metadata_args[@]}" >/dev/null 2>&1; then
            status=0
        else
            status=$?
        fi
        if ((status != expected)); then
            echo "PR body check self-test failed: $case_name (expected $expected, observed $status)" >&2
            return 1
        fi
    done

    echo "PR body check self-test passed: real newlines accepted, literal \\n and AI attribution rejected, empty patch release Trigger and Live proof rejected, discovery tier evidence and waivers checked"
}

if [[ "${1:-}" == "--self-test" ]]; then
    if (($# != 1)); then
        usage
        exit 2
    fi
    self_test
    exit 0
fi

body_file=""
title_file=""
changed_files_file=""
open_issues_file=""
while (($#)); do
    case "$1" in
        --title-file|--changed-files-file|--open-issues-file)
            if (($# < 2)); then
                usage
                exit 2
            fi
            case "$1" in
                --title-file)
                    [[ -z "$title_file" ]] || { usage; exit 2; }
                    title_file="$2"
                    ;;
                --changed-files-file)
                    [[ -z "$changed_files_file" ]] || { usage; exit 2; }
                    changed_files_file="$2"
                    ;;
                --open-issues-file)
                    [[ -z "$open_issues_file" ]] || { usage; exit 2; }
                    open_issues_file="$2"
                    ;;
            esac
            shift 2
            ;;
        --*)
            usage
            exit 2
            ;;
        *)
            if [[ -n "$body_file" ]]; then
                usage
                exit 2
            fi
            body_file="$1"
            shift
            ;;
    esac
done

if [[ -z "$body_file" || -z "$changed_files_file" || -z "$open_issues_file" ]]; then
    usage
    exit 2
fi

check_body_file "$body_file" "$title_file" "$changed_files_file" "$open_issues_file"
