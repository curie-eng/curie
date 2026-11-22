#!/usr/bin/env bash
# Pin the github-link no-log locations from issue #5827.
# With no arguments, apps/ui/Dockerfile must contain the exact POST location
# and the slash-bounded callback location, and must not keep the unsuffixed
# prefix. This path does not run Docker.
# With --nginx-t, read nginx -T on stdin. Assertions use only the server that
# listens on 8080 and proxies /api/. Inside that server, the POST and callback
# locations must each set access_log off and error_log /dev/null. The sibling
# path /api/identity/me/github-link-extra must select a different location that
# sets neither of those suppressions. A required block that exists only in
# another server does not count.
# With --self-test, run those controls through this checker.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
dockerfile="${root}/apps/ui/Dockerfile"
script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

if [[ "${1:-}" == "--self-test" ]]; then
  if [[ $# -ne 1 ]]; then
    echo "usage: github-link-nginx.sh [--nginx-t|--self-test]" >&2
    exit 2
  fi
  work="$(mktemp)"
  trap 'rm -f "$work"' EXIT
  expect_reject() {
    local name="$1"
    local status=0
    bash "$script_path" --nginx-t < "$work" || status=$?
    if [[ "$status" -eq 0 ]]; then
      echo "self-test ${name} unexpectedly passed" >&2
      exit 1
    fi
    echo "self-test ${name} rejected"
  }
  expect_accept() {
    local name="$1"
    bash "$script_path" --nginx-t < "$work"
    echo "self-test ${name} accepted"
  }
  cat > "$work" <<'EOF'
server {
    listen 8080;
    server_name _;
    location = /api/identity/me/github-link {
        access_log off;
        error_log /dev/null;
    }
    location ^~ /api/identity/me/github-link/ {
        access_log off;
        error_log /dev/null;
    }
    location /api/ {
        gzip off;
    }
    location / {
    }
}
EOF
  expect_accept "bounded UI server"
  cat > "$work" <<'EOF'
server {
    listen 8080;
    server_name _;
    location = /api/identity/me/github-link {
        access_log off;
        error_log /dev/null;
    }
    location ^~ /api/identity/me/github-link/ {
        access_log off;
        error_log /dev/null;
    }
    location /api/ {
        access_log off;
        gzip off;
    }
    location / {
    }
}
EOF
  expect_reject "sibling access_log off alone"
  cat > "$work" <<'EOF'
server {
    listen 8080;
    server_name _;
    location = /api/identity/me/github-link {
        access_log off;
        error_log /dev/null;
    }
    location ^~ /api/identity/me/github-link/ {
        access_log off;
        error_log /dev/null;
    }
    location /api/ {
        error_log /dev/null;
        gzip off;
    }
    location / {
    }
}
EOF
  expect_reject "sibling error_log /dev/null alone"
  cat > "$work" <<'EOF'
server {
    listen 8080;
    server_name _;
    location /api/ {
        gzip off;
    }
    location / {
    }
}
server {
    listen 80;
    location = /api/identity/me/github-link {
        access_log off;
        error_log /dev/null;
    }
    location ^~ /api/identity/me/github-link/ {
        access_log off;
        error_log /dev/null;
    }
}
EOF
  expect_reject "required block only on a second server"
  bash "$script_path"
  bash "$script_path" --nginx-t < "$dockerfile"
  echo "OK github-link nginx self-test"
  exit 0
fi

if [[ "${1:-}" == "--nginx-t" ]]; then
  if [[ $# -ne 1 ]]; then
    echo "usage: github-link-nginx.sh [--nginx-t|--self-test]" >&2
    exit 2
  fi
  python3 -c "$(cat <<'PY'
import re
import sys

text = sys.stdin.read()
if not text.strip():
    sys.exit("nginx -T dump is empty")

SERVER = re.compile(r"^\s*server\s*\{")
LOCATION = re.compile(
    r"^\s*location\s+(?P<mod>=|~\*|~|\^~)?\s*(?P<arg>\S+)\s*\{"
)
LISTEN_8080 = re.compile(r"(?:^|[\s:])8080(?:\s|$)")
POST = "/api/identity/me/github-link"
CALLBACK = "/api/identity/me/github-link/callback"
EXTRA = "/api/identity/me/github-link-extra"
EXACT = ("=", POST)
BOUNDED = ("^~", POST + "/")


def capture(src: str, header: re.Pattern[str]) -> list[tuple[re.Match[str] | None, str]]:
    lines = src.splitlines()
    found: list[tuple[re.Match[str] | None, str]] = []
    index = 0
    while index < len(lines):
        code = lines[index].split("#", 1)[0]
        match = header.match(code)
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
            sys.exit("nginx -T block is not closed")
        found.append((match, "\n".join(body)))
    return found


def locations(src: str) -> list[tuple[str | None, str, str]]:
    found: list[tuple[str | None, str, str]] = []
    for match, body in capture(src, LOCATION):
        if match is None:
            continue
        found.append((match.group("mod"), match.group("arg"), body))
    return found


def directives(body: str) -> list[str]:
    found: list[str] = []
    for line in body.splitlines():
        code = line.split("#", 1)[0].strip().rstrip(";")
        if code:
            found.append(code)
    return found


def access_off(body: str) -> bool:
    return any(
        item == "access_log off" or item.startswith("access_log off ")
        for item in directives(body)
    )


def error_off(body: str) -> bool:
    return any(
        item == "error_log /dev/null" or item.startswith("error_log /dev/null ")
        for item in directives(body)
    )


def listens_8080(server: str) -> bool:
    for line in server.splitlines():
        code = line.split("#", 1)[0].strip().rstrip(";")
        if code.startswith("listen ") and LISTEN_8080.search(code):
            return True
    return False


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


servers = [body for _match, body in capture(text, SERVER)]
ui_servers = [
    server
    for server in servers
    if listens_8080(server)
    and any(mod is None and arg == "/api/" for mod, arg, _body in locations(server))
]
if len(ui_servers) != 1:
    sys.exit(
        "expected one UI server listening on 8080 with location /api/, "
        f"found {len(ui_servers)}"
    )
locs = locations(ui_servers[0])
for mod, arg, _body in locs:
    if mod == "^~" and arg == POST:
        sys.exit("unsuffixed github-link prefix is present in the UI server")


def require(pair: tuple[str, str]) -> None:
    hits = [loc for loc in locs if (loc[0], loc[1]) == pair]
    if len(hits) != 1:
        sys.exit(f"expected one location {pair[0]} {pair[1]} in the UI server, found {len(hits)}")
    missing: list[str] = []
    if not access_off(hits[0][2]):
        missing.append("access_log off")
    if not error_off(hits[0][2]):
        missing.append("error_log /dev/null")
    if missing:
        sys.exit(f"location {pair[1]} is missing {' and '.join(missing)}")


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
suppressed: list[str] = []
if access_off(extra[2]):
    suppressed.append("access_log off")
if error_off(extra[2]):
    suppressed.append("error_log /dev/null")
if suppressed:
    sys.exit(
        f"github-link-extra location {extra[1]} suppresses logging with "
        + " and ".join(suppressed)
    )
print("OK nginx -T keeps github-link-extra outside the no-log locations")
PY
)"
  exit 0
fi

if [[ $# -ne 0 ]]; then
  echo "usage: github-link-nginx.sh [--nginx-t|--self-test]" >&2
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
