from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

from app.adapter.onebot.normalizer import normalize_message_event
from app.store.database import Database


PAYLOAD = {
    "post_type": "message",
    "message_type": "private",
    "self_id": 10001,
    "user_id": 20002,
    "message_id": 40001,
    "time": 1700004001,
    "raw_message": "CRASH_01",
    "message": [
        {
            "type": "text",
            "data": {
                "text": "CRASH_01",
            },
        }
    ],
}


async def run(
    database_path: Path,
) -> None:
    database = Database(
        database_path
    )

    await database.start()

    event = normalize_message_event(
        PAYLOAD
    )

    if event is None:
        raise RuntimeError(
            "Failed to normalize crash-test event"
        )

    result = (
        await database.ingest_message_event(
            event
        )
    )

    if not result.accepted:
        raise RuntimeError(
            "Crash-test event was not accepted"
        )

    claimed = (
        await database.claim_inbound_event(
            event.dedup_key
        )
    )

    if not claimed:
        raise RuntimeError(
            "Crash-test event was not claimed"
        )

    # Do NOT close the database.
    #
    # Simulate an abrupt process death after the
    # received -> processing transaction committed.
    os.kill(
        os.getpid(),
        signal.SIGKILL,
    )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "usage: python -m tests.crash_worker "
            "<database-path>"
        )

    asyncio.run(
        run(
            Path(sys.argv[1])
        )
    )


if __name__ == "__main__":
    main()
