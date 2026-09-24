"""The guardrails of ADR 0169 d7, in code rather than in the prompt."""

import re

from mean_tester_probes.config import Config
from mean_tester_probes.observe import PROBE_MARK

_MENTION = re.compile(r"<[@!][^>]+>")


class GuardRefusal(ValueError):
    """A probe this connector will not post."""


def refuse_unlisted(config: Config, channel: str) -> None:
    if channel not in config.channels:
        raise GuardRefusal(f"{channel} is not an operator-listed channel")


class ProbeGuard:
    def __init__(self, config: Config) -> None:
        self._config = config

    def check(
        self, channel: str, channel_info: dict, texts: list[str], target_user: str
    ) -> list[str]:
        refuse_unlisted(self._config, channel)
        # Only an explicit `false` for both flags counts as unshared: a channel
        # Slack did not describe is treated as shared rather than trusted.
        if channel_info.get("is_ext_shared") is not False or (
            channel_info.get("is_shared") is not False
        ):
            raise GuardRefusal(
                f"{channel} is externally shared, or Slack did not say it is not; "
                "probes are never sent there"
            )
        cap = self._config.max_probes
        if not 1 <= len(texts) <= cap:
            raise GuardRefusal(f"a round sends at most {cap} probes, got {len(texts)}")
        out = []
        for text in texts:
            body = text.strip()
            if not body:
                raise GuardRefusal("a probe is empty")
            if len(body) > self._config.max_probe_chars:
                raise GuardRefusal(f"a probe is over {self._config.max_probe_chars} characters")
            if _MENTION.search(body):
                raise GuardRefusal(
                    "a probe may mention only the target, and the connector adds that"
                )
            out.append(f"{PROBE_MARK} <@{target_user}> {body}")
        return out
