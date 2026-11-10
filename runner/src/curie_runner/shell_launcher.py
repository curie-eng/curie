"""Start Bash with a credential-free environment before it parses user code."""

from __future__ import annotations

import os
import sys

from .subprocess_env import shell_and_hook_env


def main() -> None:
    """Replace the isolated launcher with Bash, preserving arguments and signals."""

    os.execve("/bin/bash", ["/bin/bash", *sys.argv[1:]], shell_and_hook_env(os.environ))


if __name__ == "__main__":
    main()
