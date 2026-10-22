"""The caller proxy in front of each hosted connector (ADR-0168 decision 7).

A top-level package shipped in the worker image, and not a module of
``curie_worker``: importing any ``curie_worker`` module imports its package
``__init__``, which loads the whole kernel (Kubernetes client, SQLAlchemy,
Valkey), and this runs beside a connector that holds a production credential.
``caller`` checks the token; ``server`` is the HTTP side.

``resolver`` is the shared connector resolver (ADR 0239): in a shared
application's proxy container it admits each provider call against Postgres
under the application's own role and releases that caller's credential over a
pod-local Unix socket. It follows the same import boundary.
"""
