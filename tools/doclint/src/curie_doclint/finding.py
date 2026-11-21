"""The one finding type the linter emits.

A ``Finding`` carries at least the repo-relative doc path, the offending
citation or field, and a human reason, per the public contract. The printed
line combines all three so the test suite (which reads exit code and message
text only) can assert on any of them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class Finding:
    """A single lint finding.

    ``doc`` is the repo-relative path of the file the failure was found in.
    ``citation`` is the offending token: a citation string, a front-matter
    field name, a kind value, a coordinate, or a generated-marker name.
    ``reason`` explains the failure. ``line`` is the 1-based line number when
    known, else ``None``. ``severity`` is ``error`` (fails the gate) or
    ``warning`` (printed in logs, exit 0).
    """

    doc: str
    citation: str
    reason: str
    line: int | None = None
    severity: Severity = "error"

    def render(self) -> str:
        """One printable line naming the doc, the citation, and the reason."""
        location = self.doc if self.line is None else f"{self.doc}:{self.line}"
        prefix = "warning: " if self.severity == "warning" else ""
        return f"{prefix}{location}: {self.citation}: {self.reason}"
