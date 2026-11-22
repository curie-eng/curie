#!/usr/bin/env bash
# Pin the github-link no-log locations from issue #5827.
# With no arguments, apps/ui/Dockerfile must contain the exact POST location
# and the slash-bounded callback location, and must not keep the unsuffixed
# prefix. This path does not run Docker.
# With --nginx-t, read nginx -T on stdin. Both locations must set
# access_log off and error_log /dev/null, the POST and callback must select
# those locations, and /api/identity/me/github-link-extra must fall outside
# them onto a location that still logs.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
dockerfile="${root}/apps/ui/Dockerfile"

if [[ "${1:-}" == "--nginx-t" ]]; then
  if [[ $# -ne 1 ]]; then
    echo "usage: github-link-nginx.sh [--nginx-t]" >&2
    exit 2
  fi
  python3 -c "$(cat <<'PY'
import re
import sys

text = sys.stdin.read()
if not text.strip():
    sys.exit("nginx -T dump is empty")

LOCATION = re.compile(
    r"^\s*location\s+(?P<mod>=|~\*|~|\^~)?\s*(?P<arg>\S+)\s*\{"
)
POST = "/api/identity/me/github-link"
CALLBACK = "/api/identity/me/github-link/callback"
EXTRA = "/api/identity/me/github-link-extra"
EXACT = ("=", POST)
BOUNDED = ("^~", POST + "/")


def blocks(src: str) -> list[tuple[str | None, str, str]]:
    lines = src.splitlines()
    found: list[tuple[str | None, str, str]] = []
    index = 0
    while index < len(lines):
        code = lines[index].split("#", 1)[0]
        match = LOCATION.match(code)
        if match is None:
            index += 1
            continue
        depth = code.count("{") - code.count("}")
        body = [lines[index]]
        index += 1
        while index < len(lines) and depth > 0:
            body.append(lines[index])
            part = lines[index].split("#", 1)[0]
            depth += part.count("{") - part.count("}")
            index += 1
        if depth != 0:
            sys.exit("nginx -T location block is not closed")
        found.append((match.group("mod"), match.group("arg"), "\n".join(body)))
    return found


def logging_off(body: str) -> bool:
    access = False
    error = False
    for line in body.splitlines():
        code = line.split("#", 1)[0].strip().rstrip(";")
        if code == "access_log off" or code.startswith("access_log off "):
            access = True
        if code == "error_log /dev/null" or code.startswith("error_log /dev/null "):
            error = True
    return access and error


def matches(mod: str | None, arg: str, path: str) -> bool:
    if mod == "=":
        return path == arg
    if mod == "~":
        return re.search(arg, path) is not None
    if mod == "~*":
        return re.search(arg, path, re.IGNORECASE) is not None
    return path.startswith(arg)


def winner(
    locs: list[tuple[str | None, str, str]], path: str
) -> tuple[str | None, str, str] | None:
    exact = [loc for loc in locs if loc[0] == "=" and matches("=", loc[1], path)]
    if len(exact) > 1:
        sys.exit(f"more than one exact location matches {path}")
    if len(exact) == 1:
        return exact[0]
    prefixes = [
        loc for loc in locs if loc[0] in (None, "^~") and matches(loc[0], loc[1], path)
    ]
    longest: tuple[str | None, str, str] | None = None
    for loc in prefixes:
        if longest is None or len(loc[1]) > len(longest[1]):
            longest = loc
    if longest is not None and longest[0] == "^~":
        return longest
    for loc in locs:
        if loc[0] in ("~", "~*") and matches(loc[0], loc[1], path):
            return loc
    return longest


def describe(loc: tuple[str | None, str, str] | None) -> str:
    if loc is None:
        return "no location"
    return f"{loc[0] or 'prefix'} {loc[1]}"


locs = blocks(text)
for mod, arg, _body in locs:
    if mod == "^~" and arg == POST:
        sys.exit("unsuffixed github-link prefix is present in nginx -T")


def require(pair: tuple[str, str]) -> None:
    hits = [loc for loc in locs if (loc[0], loc[1]) == pair]
    if len(hits) != 1:
        sys.exit(f"expected one location {pair[0]} {pair[1]}, found {len(hits)}")
    if not logging_off(hits[0][2]):
        sys.exit(
            f"location {pair[1]} is missing access_log off or error_log /dev/null"
        )


require(EXACT)
require(BOUNDED)

post = winner(locs, POST)
if post is None or (post[0], post[1]) != EXACT:
    sys.exit(f"POST route selected {describe(post)}")
callback = winner(locs, CALLBACK)
if callback is None or (callback[0], callback[1]) != BOUNDED:
    sys.exit(f"callback route selected {describe(callback)}")
extra = winner(locs, EXTRA)
if extra is None or (extra[0], extra[1]) in (EXACT, BOUNDED) or extra[1] == POST:
    sys.exit(f"github-link-extra is inside {describe(extra)}")
if logging_off(extra[2]):
    sys.exit(f"github-link-extra location {extra[1]} disables access logging")
print("OK nginx -T keeps github-link-extra outside the no-log locations")
PY
)"
  exit 0
fi

if [[ $# -ne 0 ]]; then
  echo "usage: github-link-nginx.sh [--nginx-t]" >&2
  exit 2
fi

exact='location = /api/identity/me/github-link {'
bounded='location ^~ /api/identity/me/github-link/ {'
if grep -E -q \
  '^[[:space:]]*location[[:space:]]+\^~[[:space:]]+/api/identity/me/github-link[[:space:]]*\{' \
  "$dockerfile"; then
  echo "unsuffixed github-link prefix location is still present" >&2
  exit 1
fi
if ! grep -F -q "$exact" "$dockerfile"; then
  echo "missing exact github-link location" >&2
  exit 1
fi
if ! grep -F -q "$bounded" "$dockerfile"; then
  echo "missing slash-bounded github-link location" >&2
  exit 1
fi
echo "OK github-link nginx locations are bounded"
