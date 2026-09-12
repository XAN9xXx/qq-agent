from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path


@contextmanager
def open_database(
    path: Path,
) -> Iterator[sqlite3.Connection]:
    """Yield a sqlite3 connection that is always closed on exit.

    Using ``with sqlite3.connect(...)`` directly is a trap: the
    connection's own context manager commits or rolls back the
    transaction but leaves the connection *open*. The lingering file
    handle is invisible on Linux, where an open file can still be
    unlinked, but on Windows it makes TemporaryDirectory.cleanup()
    fail with WinError 32.
    """
    with closing(sqlite3.connect(path)) as db:
        yield db


def make_private_payload(
    *,
    message_id: int,
    text: str,
) -> dict:
    return {
        "post_type": "message",
        "message_type": "private",
        "self_id": 10001,
        "user_id": 20002,
        "message_id": message_id,
        "time": 1700000000 + message_id,
        "raw_message": text,
        "message": [
            {
                "type": "text",
                "data": {
                    "text": text,
                },
            }
        ],
    }


def make_group_payload(
    *,
    message_id: int,
    text: str,
    mention_bot: bool = False,
) -> dict:
    segments: list[dict] = []

    if mention_bot:
        segments.append(
            {
                "type": "at",
                "data": {
                    "qq": "10001",
                },
            }
        )

    segments.append(
        {
            "type": "text",
            "data": {
                "text": text,
            },
        }
    )

    return {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10001,
        "user_id": 20002,
        "group_id": 30003,
        "message_id": message_id,
        "time": 1700000000 + message_id,
        "raw_message": text,
        "message": segments,
    }
