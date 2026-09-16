"""Keep the foreign-repository fixture tree out of this repository's collection.

``fixtures/repo_toolchain/tests/test_rates.py`` is a **deliberately failing**
stand-in for a foreign repository's own test suite: the red -> green -> red proof
in ``test_repo_toolchain_proof.py`` depends on it failing against the seeded
defect. It is executed only inside the runner container, against the fixture's
own virtualenv, and must never be imported or run by this repository's pytest.
"""

from __future__ import annotations

collect_ignore_glob = ["fixtures/**"]
