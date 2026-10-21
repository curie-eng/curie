"""The shared connector store and its administration (ADR 0239).

``store`` holds applications, recipients, members and bindings;
``database_roles`` creates and rotates each application's restricted Postgres
role; ``sealing`` seals to an application's recipient key. The routers in
``routers/shared_connectors.py`` are the only callers.
"""
