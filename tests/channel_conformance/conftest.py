"""Put the suite's helper modules on ``sys.path``.

The root suite runs pytest with ``--import-mode=importlib``, under which a
sibling module is not importable by name, so this directory goes on the path
the way ``apps/mail-adapter/tests/conftest.py`` does it. The mail subject also
drives that suite's fake AgentMail and fake platform ingress, so its directory
goes on the path too: reusing those fakes keeps one model of the provider
instead of a second one that could drift from it.
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MAIL_TESTS = _HERE.parents[1] / "apps" / "mail-adapter" / "tests"

for _path in (_HERE, _MAIL_TESTS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
