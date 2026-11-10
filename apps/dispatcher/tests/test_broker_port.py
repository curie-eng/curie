"""The producer-side broker port: redis-py structurally satisfies StreamPublisher.

Structural-conformance only -- no Valkey behavior is mocked (the real enqueue /
dedupe behavior is covered against real Valkey in test_queue.py). This pins that
the one backing today (redis.Redis) is a drop-in for the port, so a second broker
that implements the same verbs is too.
"""

import redis
from curie_internal.turn_queue import StreamPublisher


def test_redis_client_satisfies_publisher_port() -> None:
    # Construction does not connect; no network call is made here.
    client = redis.Redis(host="localhost", port=6379)
    assert isinstance(client, StreamPublisher)


def test_minimal_second_broker_satisfies_port() -> None:
    class FakeBroker:
        def xadd(self, name: object, fields: object) -> str:
            return "0-1"

        def set(self, name: object, value: object, *, nx: bool = False, ex: object = None) -> bool:
            return True

        def delete(self, *names: object) -> int:
            return 1

    assert isinstance(FakeBroker(), StreamPublisher)
