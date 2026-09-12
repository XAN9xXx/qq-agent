from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.adapter.onebot.normalizer import normalize_message_event
from app.store.database import Database
from app.store.migrate import migrate
from tests.support import open_database


PAYLOAD = {
    "post_type": "message",
    "message_type": "private",
    "self_id": 10001,
    "user_id": 20002,
    "message_id": 30003,
    "time": 1700000000,
    "raw_message": "DEDUP_TEST",
    "message": [
        {
            "type": "text",
            "data": {
                "text": "DEDUP_TEST",
            },
        }
    ],
}


class IngestDedupTest(
    unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self) -> None:
        self.temp_dir = (
            tempfile.TemporaryDirectory()
        )

        self.database_path = (
            Path(self.temp_dir.name)
            / "agent-test.db"
        )

        migrate(self.database_path)

        self.database = Database(
            self.database_path
        )

        await self.database.start()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.temp_dir.cleanup()

    async def test_same_event_is_accepted_once(
        self,
    ) -> None:
        event = normalize_message_event(
            PAYLOAD
        )

        self.assertIsNotNone(
            event
        )

        assert event is not None

        first = (
            await self.database
            .ingest_message_event(
                event
            )
        )

        second = (
            await self.database
            .ingest_message_event(
                event
            )
        )

        self.assertTrue(
            first.accepted
        )

        self.assertFalse(
            second.accepted
        )

        with open_database(
            self.database_path
        ) as db:
            inbound_count = db.execute(
                """
                SELECT COUNT(*)
                FROM inbound_events
                """
            ).fetchone()[0]

            actor_count = db.execute(
                """
                SELECT COUNT(*)
                FROM actors
                """
            ).fetchone()[0]

            scope_count = db.execute(
                """
                SELECT COUNT(*)
                FROM scopes
                """
            ).fetchone()[0]

        self.assertEqual(
            inbound_count,
            1,
        )

        self.assertEqual(
            actor_count,
            1,
        )

        self.assertEqual(
            scope_count,
            1,
        )


if __name__ == "__main__":
    unittest.main()
