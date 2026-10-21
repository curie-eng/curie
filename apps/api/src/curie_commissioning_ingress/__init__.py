"""Standalone commissioning ingress composition, @spec PROTECTED-COMMISSIONING-2/4.

A top-level package shipped beside ``curie_api`` and not a module of it: the
ordinary API process composition never imports or mounts it, and an
import-linter contract keeps it that way. It reads a provisioner-owned
directory, authenticates as the campaign's candidate enqueue principal and adds
signed synthetic deliveries to the campaign lane only after the refusal order of
the commissioning specification passes. ``directory`` reads the files,
``ingress`` decides and ``app`` is the explicit HTTP constructor.
"""
