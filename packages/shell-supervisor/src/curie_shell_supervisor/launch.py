"""Consume this claim's private environment, then execute its runner command."""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

from .protocol import CLAIM_ENV_FILE_ENV


def claim_environment(environ: dict[str, str]) -> dict[str, str]:
    path = Path(environ[CLAIM_ENV_FILE_ENV])
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o400
            or info.st_uid != os.getuid()
        ):
            raise ValueError(
                "claim environment must be a runner-owned regular file with mode 0400"
            )
        value = json.load(stream)
    if not isinstance(value, dict) or not all(
        isinstance(k, str)
        and k
        and "=" not in k
        and "\0" not in k
        and isinstance(v, str)
        and "\0" not in v
        for k, v in value.items()
    ):
        raise ValueError("claim environment must contain environment names and string values")
    env = {k: v for k, v in environ.items() if k != CLAIM_ENV_FILE_ENV}
    path.unlink()
    return {**env, **value}


def main() -> None:
    try:
        env = claim_environment(dict(os.environ))
        if not sys.argv[1:]:
            raise ValueError("claim has no runner command")
    except (KeyError, OSError, UnicodeError, ValueError):
        sys.exit("claim environment refused; provide a readable runner-owned claim file")
    os.execvpe(sys.argv[1], sys.argv[1:], env)


if __name__ == "__main__":
    main()
