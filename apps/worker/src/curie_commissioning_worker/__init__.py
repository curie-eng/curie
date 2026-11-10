"""Standalone commissioning worker composition, @spec PROTECTED-COMMISSIONING-2/4.

A top-level package shipped in the worker image beside ``curie_worker`` and not
a module of it: no ordinary composition imports it, so the production worker
keeps importing no commissioning module, and it imports nothing of the API.
It reads a provisioner-owned directory, authenticates as the campaign's
candidate worker and consumes only the campaign lane the permit derives.
``entry`` opens it; ``consumer`` verifies and parks each claim.
"""
