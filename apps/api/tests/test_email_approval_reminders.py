"""A completed mail turn keeps its original approval answer authority. ADR 0273."""

from __future__ import annotations

import importlib

from apps.api.tests.test_http_email_approvals import email_case as email_case
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

mail_support = importlib.import_module("apps.mail-adapter.tests._support")
verifying = importlib.import_module("apps.mail-adapter.tests._verifying_provider")
