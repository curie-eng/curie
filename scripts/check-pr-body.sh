#!/usr/bin/env bash
# Three guards on a pull request body.
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
#    0) whose Trigger or Live proof section is missing, comment-only, or empty
#    of issue numbers / a run URL or explicit waiver (#2251). The v0.8.x patch
#    PRs shipped as release mechanics with every product tier marked n/a.
#
# Usage:
#   scripts/check-pr-body.sh <body-file> [--title-file <title-file>]
#   scripts/check-pr-body.sh --self-test
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
COMMIT_GATE="$SCRIPT_DIR/check-commit-messages.sh"

usage() {
    echo "Usage: scripts/check-pr-body.sh <body-file> [--title-file <title-file>]" >&2
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

live_proof_names_url_or_waiver() {
    local text="$1"
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
    if [[ -z "$proof_text" ]] || ! live_proof_names_url_or_waiver "$proof_text"; then
        echo "PR body check failed: patch release PRs must have a non-empty Live proof section naming a run URL or an explicit waiver." >&2
        return 1
    fi
    return 0
}

check_body_file() {
    local body_file="$1"
    local title_file="${2:-}"
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

    if ((patch_release)); then
        echo "PR body check passed: real newlines, no AI attribution, patch release Trigger and Live proof are present"
    else
        echo "PR body check passed: real newlines, no AI attribution"
    fi
}

self_test() {
    local temp_dir real_newline_body literal_escape_body attributed_body footer_no_newline_body
    local empty_patch_body filled_patch_body comment_trigger_body empty_proof_body
    local patch_title feature_title patch10_title

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
    patch_title="$temp_dir/patch-title.txt"
    feature_title="$temp_dir/feature-title.txt"
    patch10_title="$temp_dir/patch-10-title.txt"

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

    if ! bash "$SCRIPT_PATH" "$real_newline_body" >/dev/null; then
        echo "PR body check self-test failed: real newlines were rejected" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$literal_escape_body" >/dev/null 2>&1; then
        echo "PR body check self-test failed: literal \\n was accepted" >&2
        return 1
    fi

    if bash "$SCRIPT_PATH" "$attributed_body" >/dev/null 2>&1; then
        echo "PR body check self-test failed: AI attribution footer was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$footer_no_newline_body" >/dev/null 2>&1; then
        echo "PR body check self-test failed: unterminated AI attribution footer was accepted" >&2
        return 1
    fi

    if bash "$SCRIPT_PATH" "$empty_patch_body" --title-file "$patch_title" >/dev/null 2>&1; then
        echo "PR body check self-test failed: empty patch-release Trigger/Live proof were accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$comment_trigger_body" --title-file "$patch_title" >/dev/null 2>&1; then
        echo "PR body check self-test failed: comment-only Trigger was accepted" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$empty_proof_body" --title-file "$patch_title" >/dev/null 2>&1; then
        echo "PR body check self-test failed: empty Live proof was accepted" >&2
        return 1
    fi
    if ! bash "$SCRIPT_PATH" "$filled_patch_body" --title-file "$patch_title" >/dev/null; then
        echo "PR body check self-test failed: filled Trigger and Live proof were rejected" >&2
        return 1
    fi
    if ! bash "$SCRIPT_PATH" "$empty_patch_body" --title-file "$feature_title" >/dev/null; then
        echo "PR body check self-test failed: a feature-release title was gated as a patch" >&2
        return 1
    fi
    if bash "$SCRIPT_PATH" "$empty_patch_body" --title-file "$patch10_title" >/dev/null 2>&1; then
        echo "PR body check self-test failed: two-digit patch version skipped the gate" >&2
        return 1
    fi

    echo "PR body check self-test passed: clean body accepted, literal \\n and AI attribution rejected, empty patch-release Trigger and Live proof rejected"
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
while (($#)); do
    case "$1" in
        --title-file)
            if (($# < 2)); then
                usage
                exit 2
            fi
            title_file="$2"
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

if [[ -z "$body_file" ]]; then
    usage
    exit 2
fi

check_body_file "$body_file" "$title_file"
