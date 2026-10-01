"""Environment for Bash and bundle hook subprocesses.

The runner process keeps platform credentials for its own clients and for the
Claude CLI parent, which needs the model key to call the provider. Hook
commands and other runner subprocesses receive a copy without those
credentials. The Bash tool inherits the CLI process, so a ``BASH_ENV`` prelude
unsets the same names before the command runs.

The runner process locks its environ, and the CLI parent env loads the
constructor library because exec clears the lock. The prelude unsets names
from the shell export list.

Declared connector secrets are not platform credentials. ADR-0009 remote
``${VAR}`` expansion still reads them from this env.
"""

from __future__ import annotations

import ctypes
import subprocess
import tempfile
from collections.abc import Mapping, MutableMapping
from pathlib import Path

from .sdk_auth import (
    API_KEY_ENV,
    AUTH_TOKEN_ENV,
    CREDENTIALS_ENV,
    OAUTH_TOKEN_ENV,
    InvalidEnvKeyError,
    resolve_credential_env_keys,
)

# Names the Claude CLI parent reads. Hooks never receive them. The Bash tool
# drops them by sourcing BASH_CREDENTIAL_PRELUDE before the command. That
# file ships in the image, which is root owned, so the sandbox user cannot
# replace it. CLAUDE_CODE_SUBPROCESS_ENV_SCRUB is not used: it also forces a
# sandbox this runner does not ship, and Bash then fails closed.
BASH_CREDENTIAL_PRELUDE = Path(__file__).with_name("bash_credential_prelude.sh")

CLI_PARENT_MODEL_KEYS = frozenset(
    {
        API_KEY_ENV,
        AUTH_TOKEN_ENV,
        OAUTH_TOKEN_ENV,
        "ANTHROPIC_FOUNDRY_API_KEY",
        "ANTHROPIC_CUSTOM_HEADERS",
    }
)


def platform_credential_names(source: Mapping[str, str]) -> frozenset[str]:
    """Env names a shell or hook must not receive.

    ``CURIE_*TOKEN*`` covers runner, state, memory, history, progress, and
    caller tokens, including indexed names. Model credential variables and
    any name ``CURIE_MODEL_ENV_KEY`` points at are included too.
    """

    names: set[str] = set(CLI_PARENT_MODEL_KEYS)
    names.add(CREDENTIALS_ENV)
    names.update(key for key in source if key.startswith("CURIE_") and "TOKEN" in key)
    try:
        names.update(resolve_credential_env_keys(dict(source)))
    except (InvalidEnvKeyError, ValueError):
        pass
    return frozenset(names)


def shell_and_hook_env(
    source: Mapping[str, str],
    *,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy ``source`` without platform credentials.

    ``extra`` is applied last (hook ``CLAUDE_PLUGIN_ROOT``) and cannot put a
    platform credential back.
    """

    # Union both maps. An override in ``extra`` must not hide a credential
    # name that ``source`` already declared (a bundle MCP env block can set
    # CURIE_MODEL_ENV_KEY to empty or to a different name).
    denied = platform_credential_names(source)
    if extra:
        denied = denied | platform_credential_names({**source, **extra})
    child = {key: value for key, value in source.items() if key not in denied}
    if extra:
        for key, value in extra.items():
            if key not in denied:
                child[key] = value
    return child


def release_platform_credentials(env: MutableMapping[str, str]) -> None:
    """Drop platform credentials the CLI parent does not need.

    Model SDK variables stay so the parent can authenticate. ``CURIE_*TOKEN*``,
    ``CURIE_CREDENTIALS``, and a declared provider credential name do not.
    """

    denied = platform_credential_names(env) - CLI_PARENT_MODEL_KEYS
    for key in list(env):
        if key in denied:
            env.pop(key, None)


def lock_process_environ() -> None:
    """Same-uid peers cannot read this process environ."""

    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(4, 0, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno())


def proc_dumpable_library(source: Mapping[str, str]) -> str:
    """Constructor library the CLI parent loads. exec clears the environ lock.

    The path comes from ``source``. ``connect`` clears ``os.environ`` before
    building the parent env, and the image path is root owned. A missing
    configured path fails closed. Hosts without the image variable compile a
    fresh library in a private temp directory, not a stable home cache.
    """

    configured = source.get("CURIE_PROC_DUMPABLE_PRELOAD", "").strip()
    if configured:
        if Path(configured).is_file():
            return configured
        raise FileNotFoundError(configured)
    library_dir = Path(tempfile.mkdtemp(prefix="curie-proc-dumpable-"))
    library = library_dir / "libproc_dumpable.so"
    c_source = Path(__file__).with_name("proc_dumpable.c")
    # connect has already cleared os.environ. gcc needs PATH to find cc1.
    compile_env = dict(source)
    compile_env.setdefault("PATH", "/usr/bin:/bin")
    subprocess.run(
        ["gcc", "-shared", "-fPIC", "-O2", "-o", str(library), str(c_source)],
        check=True,
        env=compile_env,
    )
    return str(library)


def cli_parent_env(source: Mapping[str, str]) -> dict[str, str]:
    """Env to install for the moment the CLI is spawned.

    Same as the shell env, plus model SDK variables that were already set, plus
    ``BASH_ENV`` pointing at the credential prelude. ``LD_PRELOAD`` loads the
    constructor library because exec clears the environ lock. Callers restore
    the previous process env after spawn. The CLI copies this mapping at start.
    """

    parent = shell_and_hook_env(source)
    for key in CLI_PARENT_MODEL_KEYS:
        value = source.get(key)
        if value:
            parent[key] = value
    parent["BASH_ENV"] = str(BASH_CREDENTIAL_PRELUDE)
    library = proc_dumpable_library(source)
    existing = source.get("LD_PRELOAD", "")
    kept = [entry for entry in existing.split(":") if entry and entry != library]
    parent["LD_PRELOAD"] = ":".join([library, *kept])
    return parent
