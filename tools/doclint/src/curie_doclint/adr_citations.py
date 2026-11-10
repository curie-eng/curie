"""ADR number citations in the linted docs (#3844).

A doc cites a decision by number in the shapes this tree uses: ``ADR-0007``,
``ADR 0007``, a plural list (``ADRs 0125 and 0126``), and a link whose target is
``.../adr/0007-<slug>.md``. Each cited number must be the number prefix of a
file in ``docs/adr/``.

Like the raw line-ban rule this scans the raw text, fenced blocks included: a
diagram that cites ``ADR-0010`` is still a citation. Template numbers
(``ADR-NNNN``) have no digits and never match. A link into another
repository's ``adr/`` directory is not a citation of this tree's ADRs, so an
absolute URL counts only when it points at this repository.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass

_SINGLE = re.compile(r"\bADR[- ](\d{4})(?!\d)")
_PLURAL = re.compile(r"\bADRs (\d{4}(?:(?:,? and |,? or |, | through | to )\d{4})*)(?!\d)")
_LINK = re.compile(r"[^\s()<>\[\]\"'`]*\badr/(\d{4})-")
_NUMBER = re.compile(r"\d{4}")

# This repository under its current and former name (AGENTS.md).
_THIS_REPO = ("github.com/curie-eng/curie/", "github.com/curie-eng/agentos/")

@dataclass(frozen=True)
class AdrCitation:
    number: str
    line: int

    @property
    def label(self) -> str:
        return f"ADR-{self.number}"

def _link_numbers(line: str) -> Iterator[str]:
    for match in _LINK.finditer(line):
        target = match.group(0)

        if "://" in target and not any(repo in target for repo in _THIS_REPO):
            continue
        
        yield match.group(1)

def scan_adr_citations(text: str) -> Iterator[AdrCitation]:
    """Yield each distinct ADR number cited on each line (before suppression)."""

    for index, line in enumerate(text.splitlines(), start=1):
        numbers = [match.group(1) for match in _SINGLE.finditer(line)]

        for match in _PLURAL.finditer(line):
            numbers.extend(_NUMBER.findall(match.group(1)))

        numbers.extend(_link_numbers(line))

        for number in dict.fromkeys(numbers):
            yield AdrCitation(number=number, line=index)