#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: verify-fix-pin.sh <change> <selector>" >&2
  exit 2
}

fail() {
  echo "$*" >&2
  exit 1
}

if [[ $# -ne 2 ]]; then
  usage
fi

change=$1
selector=$2
repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || fail "not inside a git repository"

temp_root=$(mktemp -d)
scratch="$temp_root/worktree"
patch_file="$temp_root/change.patch"
names_file="$temp_root/changed-files"
baseline_stdout="$temp_root/baseline.stdout"
baseline_stderr="$temp_root/baseline.stderr"
reversed_stdout="$temp_root/reversed.stdout"
reversed_stderr="$temp_root/reversed.stderr"
reversed_junit="$temp_root/reversed.junit.xml"
selector_old="$temp_root/selector.old"
rust_old_node="$temp_root/rust-old-node"
rust_new_node="$temp_root/rust-new-node"
worktree_added=0

cleanup() {
  local status=$?
  local cleanup_status=0
  trap - EXIT

  if (( worktree_added )); then
    git -C "$repo_root" worktree remove --force "$scratch" >/dev/null 2>&1 || cleanup_status=1
  fi
  rm -rf -- "$temp_root" || cleanup_status=1

  if (( cleanup_status != 0 )); then
    echo "failed to clean up the verification worktree" >&2
    exit 1
  fi
  exit "$status"
}
trap cleanup EXIT

changed_files=()
change_kind=pull_request
if [[ ! "$change" =~ ^[0-9]+$ && ! "$change" =~ ^https://github\.com/[^/]+/[^/]+/pull/[0-9]+/?$ ]] && \
  commit=$(git -C "$repo_root" rev-parse --verify "${change}^{commit}" 2>/dev/null); then
  change_kind=commit
fi

if [[ "$change_kind" == commit ]]; then
  parent=$(git -C "$repo_root" rev-parse --verify "${commit}^1" 2>/dev/null) || \
    fail "commit $commit has no first parent"
  git -C "$repo_root" diff --binary "$parent" "$commit" -- >"$patch_file"
  git -C "$repo_root" diff --name-only -z "$parent" "$commit" -- >"$names_file"
  while IFS= read -r -d '' path; do
    changed_files+=("$path")
  done <"$names_file"
else
  gh pr view "$change" >/dev/null || \
    fail "change is neither a commit nor a pull request: $change"
  gh pr diff "$change" >"$patch_file" || \
    fail "could not read pull request patch: $change"
  gh pr diff "$change" --name-only >"$names_file" || \
    fail "could not read pull request files: $change"
  while IFS= read -r path || [[ -n "$path" ]]; do
    [[ -n "$path" ]] && changed_files+=("$path")
  done <"$names_file"
fi

if [[ ${#changed_files[@]} -eq 0 ]]; then
  fail "change contains no files"
fi

is_test_path() {
  case "$1" in
    apps/*/tests/* | packages/*/tests/* | runner/tests/* | cli/tests/* | charts/curie/ci/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

test_files=()
product_files=()
for path in "${changed_files[@]}"; do
  if is_test_path "$path"; then
    test_files+=("$path")
  else
    product_files+=("$path")
  fi
done

if [[ ${#test_files[@]} -eq 0 ]]; then
  fail "change contains no test files"
fi
if [[ ${#product_files[@]} -eq 0 ]]; then
  fail "change contains no product files to reverse"
fi

selector_file=
selector_kind=
rust_target=
rust_test=
case "$selector" in
  cli/tests/local/test_*.py::*)
    selector_file=${selector%%::*}
    if [[ ! "$selector" =~ ^cli/tests/local/test_[A-Za-z0-9_-]+\.py::([A-Za-z_][A-Za-z0-9_]*::)*test[A-Za-z0-9_]*(\[[A-Za-z0-9_.-]+\])?$ ]]; then
      fail "unsupported selector: $selector"
    fi
    selector_kind=python
    ;;
  apps/*/tests/*.py::* | packages/*/tests/*.py::* | runner/tests/*.py::*)
    selector_file=${selector%%::*}
    if [[ "$selector" == "$selector_file" || -z "${selector#*::}" ]]; then
      fail "unsupported selector: $selector"
    fi
    selector_kind=python
    ;;
  cli/tests/*.rs::*)
    selector_file=${selector%%::*}
    rust_test=${selector#*::}
    if [[ -z "$rust_test" || "$rust_test" == *::* || ! "$rust_test" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      fail "unsupported selector: $selector"
    fi
    rust_target=${selector_file##*/}
    rust_target=${rust_target%.rs}
    selector_kind=rust
    ;;
  charts/curie/ci/*.sh)
    selector_file=$selector
    selector_kind=chart
    ;;
  *)
    fail "unsupported selector: $selector"
    ;;
esac

selector_changed=0
for path in "${test_files[@]}"; do
  if [[ "$path" == "$selector_file" ]]; then
    selector_changed=1
    break
  fi
done
if (( selector_changed == 0 )); then
  fail "selector file was not changed by $change: $selector_file"
fi

head_commit=$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}') || \
  fail "could not resolve current HEAD"
git -C "$repo_root" worktree add --detach "$scratch" "$head_commit" >/dev/null
worktree_added=1

run_selector() {
  local phase=$1

  case "$selector_kind" in
    python)
      if [[ "$phase" == reversed ]]; then
        (
          cd "$scratch"
          uv run --python 3.13 pytest "$selector" --junitxml "$reversed_junit"
        )
      else
        (cd "$scratch" && uv run --python 3.13 pytest "$selector")
      fi
      ;;
    rust)
      (
        cd "$scratch"
        cargo test --manifest-path cli/Cargo.toml --test "$rust_target" -- --exact "$rust_test"
      )
      ;;
    chart)
      (cd "$scratch" && bash "$selector")
      ;;
  esac
}

replay_selector_output() {
  cat "$1"
  cat "$2" >&2
}

extract_rust_test_node() {
  local output_kind=$2

  python3 - "$1" "$rust_test" "$output_kind" <<'PY'
import pathlib
import re
import sys


def invalid() -> None:
    raise SystemExit(1)


try:
    source = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
except (OSError, UnicodeError):
    invalid()

name = sys.argv[2]
output_kind = sys.argv[3]
if output_kind not in {"node", "range"}:
    invalid()

masked = list(source)


def hide(start: int, end: int) -> None:
    for index in range(start, end):
        if source[index] not in "\r\n":
            masked[index] = " "


def raw_string_end(start: int) -> int | None:
    if start > 0 and (source[start - 1].isalnum() or source[start - 1] == "_"):
        return None
    for prefix in ("br", "cr", "r"):
        if not source.startswith(prefix, start):
            continue
        cursor = start + len(prefix)
        while cursor < len(source) and source[cursor] == "#":
            cursor += 1
        if cursor >= len(source) or source[cursor] != '"':
            continue
        terminator = '"' + "#" * (cursor - start - len(prefix))
        end = source.find(terminator, cursor + 1)
        if end < 0:
            invalid()
        return end + len(terminator)
    return None


index = 0
while index < len(source):
    if source.startswith("//", index):
        end = source.find("\n", index + 2)
        end = len(source) if end < 0 else end
        hide(index, end)
        index = end
        continue
    if source.startswith("/*", index):
        depth = 1
        end = index + 2
        while end < len(source) and depth:
            if source.startswith("/*", end):
                depth += 1
                end += 2
            elif source.startswith("*/", end):
                depth -= 1
                end += 2
            else:
                end += 1
        if depth:
            invalid()
        hide(index, end)
        index = end
        continue
    raw_end = raw_string_end(index)
    if raw_end is not None:
        hide(index, raw_end)
        index = raw_end
        continue
    if source[index] == '"':
        end = index + 1
        while end < len(source):
            if source[end] == "\\":
                end += 2
                continue
            if source[end] == '"':
                end += 1
                break
            end += 1
        else:
            invalid()
        if end > len(source):
            invalid()
        hide(index, end)
        index = end
        continue
    if source[index] == "'":
        end = index + 1
        if end < len(source) and source[end] == "\\":
            escape = end + 1
            if escape >= len(source):
                invalid()
            if source[escape] in "\\'\"nrt0":
                end = escape + 1
            elif source[escape] == "x":
                digits = source[escape + 1 : escape + 3]
                if len(digits) != 2 or any(character not in "0123456789abcdefABCDEF" for character in digits):
                    invalid()
                end = escape + 3
            elif source[escape] == "u" and escape + 2 < len(source) and source[escape + 1] == "{":
                close = source.find("}", escape + 2)
                digits = source[escape + 2 : close] if close >= 0 else ""
                if not digits or any(
                    character not in "0123456789abcdefABCDEF_" for character in digits
                ):
                    invalid()
                end = close + 1
            else:
                invalid()
            if end >= len(source) or source[end] != "'":
                invalid()
            end += 1
        elif end + 1 < len(source) and source[end + 1] == "'":
            end += 2
        else:
            index += 1
            continue
        hide(index, end)
        index = end
        continue
    index += 1

code = "".join(masked)
pairs: dict[int, int] = {}
stack: list[tuple[str, int]] = []
matching = {")": "(", "]": "[", "}": "{"}
for position, character in enumerate(code):
    if character in "([{":
        stack.append((character, position))
    elif character in matching:
        if not stack or stack[-1][0] != matching[character]:
            invalid()
        _, opening = stack.pop()
        pairs[opening] = position
if stack:
    invalid()


def identifier_end(start: int) -> int:
    end = start
    while end < len(code) and (code[end] == "_" or code[end].isalnum()):
        end += 1
    return end


functions: list[tuple[int, int]] = []
attributes: list[tuple[int, int]] = []
stack = []
index = 0
while index < len(code):
    character = code[index]
    if character == "#" and not stack and index + 1 < len(code) and code[index + 1] == "[":
        attributes.append((index, pairs[index + 1] + 1))
    if character == "r" and index + 2 < len(code) and code[index + 1] == "#" and (
        code[index + 2] == "_" or code[index + 2].isalpha()
    ):
        index = identifier_end(index + 2)
        continue
    if character == "_" or character.isalpha():
        end = identifier_end(index)
        if code[index:end] == "fn" and not stack:
            selected_start = end
            while selected_start < len(code) and code[selected_start].isspace():
                selected_start += 1
            selected_end = identifier_end(selected_start)
            if selected_end > selected_start and code[selected_start:selected_end] == name:
                functions.append((index, selected_end))
        index = end
        continue
    if character in "([{":
        stack.append(character)
    elif character in matching:
        stack.pop()
    index += 1

if len(functions) != 1:
    invalid()

function_start, name_end = functions[0]
signature_stack: list[str] = []
body_start = None
index = name_end
while index < len(code):
    character = code[index]
    if character in "([":
        signature_stack.append(character)
    elif character in ")]":
        if not signature_stack or signature_stack[-1] != matching[character]:
            invalid()
        signature_stack.pop()
    elif character == "{" and not signature_stack:
        body_start = index
        break
    elif character in ";}" and not signature_stack:
        invalid()
    index += 1
if body_start is None:
    invalid()
body_end = pairs[body_start]

node_start = function_start
for attribute_start, attribute_end in reversed(attributes):
    if attribute_end > node_start:
        continue
    gap = code[attribute_end:node_start]
    if node_start == function_start:
        words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", gap)
        remainder = re.sub(r"[A-Za-z_][A-Za-z0-9_]*|[\s():]", "", gap)
        if remainder or any(
            word not in {"pub", "crate", "self", "super", "in", "const", "async", "unsafe", "extern", "safe", "default"}
            for word in words
        ):
            break
    elif gap.strip():
        break
    node_start = attribute_start

if output_kind == "node":
    sys.stdout.write(source[node_start : body_end + 1])
else:
    start_line = source.count("\n", 0, node_start) + 1
    end_line = source.count("\n", 0, body_end) + 1
    print(f"{start_line}:{end_line}")
PY
}

if run_selector baseline >"$baseline_stdout" 2>"$baseline_stderr"; then
  replay_selector_output "$baseline_stdout" "$baseline_stderr"
else
  replay_selector_output "$baseline_stdout" "$baseline_stderr"
  fail "baseline selector is red: $selector"
fi

if [[ "$selector_kind" != chart ]]; then
  if ! git -C "$scratch" apply --check --reverse "--include=$selector_file" "$patch_file"; then
    fail "could not reconstruct the original selector: $selector"
  fi
  git -C "$scratch" apply --reverse "--include=$selector_file" "$patch_file"
  if [[ -f "$scratch/$selector_file" ]]; then
    cp "$scratch/$selector_file" "$selector_old"
  else
    : >"$selector_old"
  fi
  if ! git -C "$scratch" apply --check "--include=$selector_file" "$patch_file"; then
    fail "could not restore the selected test: $selector"
  fi
  git -C "$scratch" apply "--include=$selector_file" "$patch_file"
fi

if [[ "$selector_kind" == python ]]; then
  if ! (
    cd "$scratch"
    uv run --python 3.13 python - "$selector_old" "$selector_file" "${selector#*::}" <<'PY'
import ast
import pathlib
import sys


def selected_source(path: str, selector: str) -> str | None:
    source = pathlib.Path(path).read_text()
    tree = ast.parse(source)
    parts = selector.split("::")
    parts[-1] = parts[-1].split("[", 1)[0]
    body = tree.body
    node = None
    for index, name in enumerate(parts):
        allowed = (ast.FunctionDef, ast.AsyncFunctionDef)
        if index < len(parts) - 1:
            allowed = (ast.ClassDef,)
        node = next((item for item in body if isinstance(item, allowed) and item.name == name), None)
        if node is None:
            return None
        body = node.body
    start = min(
        [node.lineno, *(decorator.lineno for decorator in node.decorator_list)]
    )
    return "".join(source.splitlines(keepends=True)[start - 1 : node.end_lineno])


old = selected_source(sys.argv[1], sys.argv[3])
new = selected_source(sys.argv[2], sys.argv[3])
if new is None or old == new:
    raise SystemExit(1)
PY
  ); then
    fail "selected Python test node was not changed by $change: $selector"
  fi
fi

if [[ "$selector_kind" == rust ]]; then
  if ! extract_rust_test_node "$scratch/$selector_file" node >"$rust_new_node"; then
    fail "could not locate the selected Rust test function: $selector"
  fi
  if ! rust_range=$(extract_rust_test_node "$scratch/$selector_file" range); then
    fail "could not locate the selected Rust test function: $selector"
  fi
  if extract_rust_test_node "$selector_old" node >"$rust_old_node" && \
    cmp -s "$rust_old_node" "$rust_new_node"; then
    fail "selected Rust test node was not changed by $change: $selector"
  fi
fi

include_args=()
for path in "${product_files[@]}"; do
  include_args+=("--include=$path")
done

if ! git -C "$scratch" apply --check --reverse "${include_args[@]}" "$patch_file"; then
  echo "could not reverse the changed product files:" >&2
  printf '  %s\n' "${product_files[@]}" >&2
  exit 1
fi
git -C "$scratch" apply --reverse "${include_args[@]}" "$patch_file"

if run_selector reversed >"$reversed_stdout" 2>"$reversed_stderr"; then
  replay_selector_output "$reversed_stdout" "$reversed_stderr"
  echo "UNPINNED"
  exit 1
fi

replay_selector_output "$reversed_stdout" "$reversed_stderr"

case "$selector_kind" in
  python)
    if ! (
      cd "$scratch"
      uv run --python 3.13 python - "$reversed_junit" "$selector_file" "${selector#*::}" <<'PY'
import pathlib
import sys
import xml.etree.ElementTree as ET


report = pathlib.Path(sys.argv[1])
selector_file = sys.argv[2]
selector_parts = sys.argv[3].split("::")
expected_name = selector_parts.pop()
expected_classname = selector_file.removesuffix(".py").replace("/", ".")
if selector_parts:
    expected_classname += "." + ".".join(selector_parts)

try:
    root = ET.parse(report).getroot()
except (ET.ParseError, OSError):
    raise SystemExit(1)

testcases = root.findall(".//testcase")
selected = [
    testcase
    for testcase in testcases
    if testcase.get("classname") == expected_classname
    and testcase.get("name") == expected_name
]
failures = root.findall(".//failure")
errors = root.findall(".//error")
if (
    len(selected) != 1
    or len(failures) != 1
    or errors
    or selected[0].find("failure") is None
):
    raise SystemExit(1)
PY
    ); then
      fail "reversed failure was not attributed to the selected Python test: $selector"
    fi
    ;;
  rust)
    if grep -Fq "test $rust_test ... FAILED" "$reversed_stdout" "$reversed_stderr"; then
      :
    else
      if [[ ! "$rust_range" =~ ^([0-9]+):([0-9]+)$ ]]; then
        fail "could not locate the selected Rust test function: $selector"
      fi
      rust_start=${BASH_REMATCH[1]}
      rust_end=${BASH_REMATCH[2]}
      rust_diagnostic_path=${selector_file#cli/}
      diagnostic_in_selected_test=0
      diagnostic_level=
      while IFS= read -r diagnostic; do
        if [[ "$diagnostic" =~ ^error(\[[^]]+\])?:[[:space:]] ]]; then
          diagnostic_level=error
        elif [[ "$diagnostic" =~ ^warning(\[[^]]+\])?:[[:space:]] ]]; then
          diagnostic_level=warning
        elif [[ "$diagnostic_level" == error && \
          "$diagnostic" =~ ^[[:space:]]*--\>[[:space:]]+([^:]+):([0-9]+):[0-9]+$ ]] && \
          [[ "${BASH_REMATCH[1]}" == "$rust_diagnostic_path" ]] && \
          (( BASH_REMATCH[2] >= rust_start && BASH_REMATCH[2] <= rust_end )); then
          diagnostic_in_selected_test=1
          break
        fi
      done < <(cat "$reversed_stdout" "$reversed_stderr")
      if (( diagnostic_in_selected_test == 0 )); then
        fail "reversed failure was not attributed to the selected Rust test: $selector"
      fi
    fi
    ;;
  chart)
    ;;
esac

echo "PINNED"
