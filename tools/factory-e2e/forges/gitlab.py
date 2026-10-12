"""The GitLab entry of the forge-neutral factory loop: reserved, not installed.

`run --scenario forge-loop --forge gitlab` and every acceptance pairing that
needs this forge are refused with "not installed" before anything is
installed. The driver that implements ForgeDriver (forges/base.py) replaces
this file.
"""

from __future__ import annotations

import factory_e2e as fe

fe.register_forge(
    fe.ForgeEntry(name="gitlab", driver=fe.not_installed_driver("gitlab"), installed=False)
)
