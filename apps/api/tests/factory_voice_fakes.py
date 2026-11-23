"""Deterministic recognizer for the database and adapter tests."""

from curie_api.factory_voice_contract import Refused


class FixtureVoiceRecognizer:
    def __init__(self, transcripts: dict[bytes, str], *, available: bool = True):
        self._transcripts = transcripts
        self._available = available

    async def available(self) -> bool:
        return self._available

    async def recognize(self, declared_type: str, data: bytes) -> str | Refused:
        if not self._available:
            return Refused(reason="disabled")
        return self._transcripts.get(data, Refused(reason="invalid_media"))
