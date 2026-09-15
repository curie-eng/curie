# Agent install

This runbook lets a coding agent install the released Curie CLI without a
source checkout, verify its signature and checksum, create an isolated bundle,
and report the result without human input.

Use this canonical raw URL when instructing a coding agent:

<!-- doclint:ignore-line -->
`https://raw.githubusercontent.com/curie-eng/curie/main/docs/agent-install.md`

The calling environment must already hold one real, nonempty
`CURIE_CREDENTIALS`, `CLAUDE_CODE_OAUTH_TOKEN`, or `ANTHROPIC_API_KEY` value.
This runbook never creates a credential, substitutes a placeholder, or prints a
credential value. Curie reports only the credential name.

## Execute

Run this block unchanged in one Bash session.

```bash
set -euo pipefail

step=preflight
finish() {
  status=$?
  if [ "$status" -ne 0 ]; then
    printf '{"INSTALL_RESULT":{"status":"failed","version":null,"doctor_ready":false,"summary":null,"step":"%s","exit_code":%s}}\n' "$step" "$status"
  fi
}
trap finish EXIT

# Step 1: preflight
case "$(uname -s)/$(uname -m)" in
  Linux/x86_64|Darwin/arm64|Darwin/aarch64) ;;
  *)
    printf 'unsupported platform: %s/%s\n' "$(uname -s)" "$(uname -m)" >&2
    exit 1
    ;;
esac
command -v curl >/dev/null
command -v cosign >/dev/null
command -v docker >/dev/null
command -v jq >/dev/null
if ! command -v sha256sum >/dev/null && ! command -v shasum >/dev/null; then
  printf 'missing sha256sum or shasum\n' >&2
  exit 1
fi
docker info >/dev/null
CREDENTIAL_NAME=
for candidate in CURIE_CREDENTIALS CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_API_KEY; do
  if [ -n "${!candidate:-}" ]; then
    CREDENTIAL_NAME="$candidate"
    break
  fi
done
if [ -z "$CREDENTIAL_NAME" ]; then
  printf 'missing supported model credential in the environment\n' >&2
  exit 1
fi
RUN_ROOT="$(mktemp -d)"
printf 'step=preflight result=success workspace=%s credential_name=%s\n' "$RUN_ROOT" "$CREDENTIAL_NAME"

# Step 2: download, verify, and install the release
step=install
curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | CURIE_REQUIRE_COSIGN=1 bash 2>&1 | tee "$RUN_ROOT/install.log"
grep -Fq '==> cosign verify-blob' "$RUN_ROOT/install.log"
grep -Fq '==> verifying sha256' "$RUN_ROOT/install.log"
if [ -w /usr/local/bin ]; then
  CURIE_BIN=/usr/local/bin/curie
  export PATH="/usr/local/bin:$PATH"
else
  CURIE_BIN="$HOME/.local/bin/curie"
  export PATH="$HOME/.local/bin:$PATH"
fi
test -x "$CURIE_BIN"
printf 'step=install result=success binary=%s\n' "$CURIE_BIN"

# Step 3: prove the installed binary is executable
step=version
CURIE_VERSION="$(curie --version)"
printf 'step=version result=success curie_version=%s\n' "$CURIE_VERSION"

# Step 4: create a clean bundle for the doctor check
step=bundle
cd "$RUN_ROOT"
curie init agent-install-check
cd agent-install-check
test -f .claude-plugin/plugin.json
printf 'step=bundle result=success bundle_dir=%s\n' "$PWD"

# Step 5: require Curie doctor to report ready
step=doctor
KUBECONFIG="$RUN_ROOT/no-kubeconfig" curie --json doctor | tee doctor.json
jq -e '.ready == true and ([.checks[] | select(.state == "missing")] | length == 0)' doctor.json >/dev/null
DOCTOR_SUMMARY="$(jq -er '.summary' doctor.json)"
jq -cn \
  --arg version "$CURIE_VERSION" \
  --arg summary "$DOCTOR_SUMMARY" \
  --arg workspace "$RUN_ROOT" \
  '{INSTALL_RESULT:{status:"success",version:$version,doctor_ready:true,summary:$summary,workspace:$workspace}}'
```

## Exit signals

1. Preflight succeeds only when it prints
   `step=preflight result=success`. An unsupported platform, missing command,
   stopped Docker daemon, or absent credential exits nonzero and emits an
   `INSTALL_RESULT` line with `status` set to `failed` and `step` set to
   `preflight`.
2. Installation succeeds only when the shell pipeline exits zero and the
   transcript contains both the cosign and sha256 verification lines. Cosign is
   mandatory because `CURIE_REQUIRE_COSIGN=1` is set. Any download, signature,
   checksum, install, or transcript assertion failure exits nonzero with
   `step` set to `install`.
3. Binary verification succeeds only when `curie --version` exits zero and
   prints `step=version result=success`. Any failure exits nonzero with `step`
   set to `version`.
4. Bundle setup succeeds only when `curie init agent-install-check` exits zero,
   the manifest exists, and the block prints `step=bundle result=success`.
   Any failure exits nonzero with `step` set to `bundle`.
5. Doctor verification succeeds only when the isolated laptop rung produces
   valid JSON, `ready` is `true`, and no check has state `missing`. It finishes
   by printing one compact `INSTALL_RESULT` JSON line. A false readiness value,
   missing check, invalid JSON, or command failure exits nonzero with `step`
   set to `doctor`.

Return the final `INSTALL_RESULT` line unchanged with the command transcript.

## Runtime repository allowlist

Managed `/workspace` checkouts are selected by one allowed root GitHub URL in
a thread's message, written in plain words; Curie's reply names the repository
it inferred. The chart default `api.githubRepoAllowlist: []` denies
every selection. Set it when you install or upgrade:

```bash
curie cluster up --set 'api.githubRepoAllowlist[0]=acme-corp/acme-bot'
```

`owner/*` allows every repository under that owner. `curie cluster deploy
--workspace` warns when the allowlist is empty. The `--workspace` flag itself
is a deprecated compatibility no-op; the allowlist is the real control. The
SRE bot installer also accepts `--workspace-repo owner/repo` (repeatable).
