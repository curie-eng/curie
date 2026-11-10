#!/bin/sh
# Noninteractive sh does not read Bash startup files. Isolate Python imports
# before removing credentials and starting the actual shell.
if [ -z "${CURIE_SHELL_PYTHON-}" ]; then
  printf '%s\n' 'The runner shell is unavailable.' >&2
  exit 126
fi
exec "$CURIE_SHELL_PYTHON" -I -m curie_runner.shell_launcher "$@"
