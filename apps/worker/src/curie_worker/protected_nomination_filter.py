"""Private nomination fences at the output boundary, @spec AUTOMATED-REMEDIATION-5/6.

This filter preserves malformed and oversized candidate text for downstream
validation. It does not authorize, parse nominations, or submit to an API.
"""

from __future__ import annotations

from dataclasses import dataclass, field

_OPEN = "```curie-remediation"
_CLOSE = "```"


@dataclass(frozen=True, slots=True)
class FilteredNomination:
    """Final public reply and private candidate, @spec AUTOMATED-REMEDIATION-6."""

    reply: str = field(repr=False)
    submission: str | None = field(repr=False)


class NominationLineFilter:
    """Hold partial lines so split fences never leak, @spec AUTOMATED-REMEDIATION-6.

    Only LF separates lines. Fence matching is exact: indentation, spaces, CR
    and extra backticks remain meaningful. Captured fragments are retained
    verbatim, including malformed, multiple and unclosed blocks.
    """

    __slots__ = ("__pending", "__captured", "__inside", "__finished")

    def __init__(self) -> None:
        """@spec AUTOMATED-REMEDIATION-6."""
        self.__pending: list[str] = []
        self.__captured: list[str] = []
        self.__inside = False
        self.__finished = False

    def __line(self, line: str) -> str:
        """Classify one exact line without normalizing its bytes, @spec AUTOMATED-REMEDIATION-6."""
        logical = line[:-1] if line.endswith("\n") else line
        if not self.__inside and logical == _OPEN:
            self.__inside = True
        if self.__inside:
            self.__captured.append(line)
            if logical == _CLOSE:
                self.__inside = False
            return ""
        return line

    def feed(self, text: str) -> str:
        """Emit only completed public lines, @spec AUTOMATED-REMEDIATION-6."""
        if self.__finished:
            raise ValueError("nomination filter already finished")
        lines = text.split("\n")
        emitted: list[str] = []
        for part in lines[:-1]:
            self.__pending.append(part)
            line = "".join(self.__pending) + "\n"
            self.__pending.clear()
            emitted.append(self.__line(line))
        if lines[-1]:
            self.__pending.append(lines[-1])
        return "".join(emitted)

    def finish(self) -> str:
        """Flush an ordinary tail once; withhold a private tail, @spec AUTOMATED-REMEDIATION-6."""
        if self.__finished:
            return ""
        self.__finished = True
        tail = "".join(self.__pending)
        self.__pending.clear()
        return self.__line(tail) if tail else ""

    def submission(self, status: str) -> str | None:
        """After finish, done alone permits exact captured text, @spec AUTOMATED-REMEDIATION-6."""
        if status != "done":
            return None
        if not self.__finished:
            raise ValueError("nomination filter not finished")
        if not self.__captured:
            return None
        return "".join(self.__captured)


def filter_final(output: str, status: str) -> FilteredNomination:
    """Apply the same grammar to a final reply, @spec AUTOMATED-REMEDIATION-6."""
    filter_ = NominationLineFilter()
    reply = filter_.feed(output) + filter_.finish()
    return FilteredNomination(reply, filter_.submission(status))
