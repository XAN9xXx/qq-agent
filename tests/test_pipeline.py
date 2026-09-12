from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.adapter.onebot.normalizer import normalize_message_event
from app.dispatcher.dispatcher import DispatchItem
from app.dispatcher.pipeline import MessagePipeline
from app.store.database import Database
from app.store.migrate import migrate
from tests.support import (
    make_group_payload,
    make_private_payload,
    open_database,
)


class PipelineTest(
    unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

        self.database_path = (
            Path(self.temp_dir.name)
            / "agent-test.db"
        )

        migrate(self.database_path)

        self.database = Database(
            self.database_path
        )

        await self.database.start()

        self.pipeline = MessagePipeline(
            self.database
        )

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.temp_dir.cleanup()

    async def ingest_and_process(
        self,
        payload: dict,
    ):
        event = normalize_message_event(
            payload
        )

        self.assertIsNotNone(event)
        assert event is not None

        result = (
            await self.database
            .ingest_message_event(event)
        )

        self.assertTrue(
            result.accepted
        )

        await self.pipeline(
            DispatchItem(
                scope_id=result.scope_id,
                actor_id=result.actor_id,
                payload=event,
            )
        )

        return event, result

    async def test_private_message_is_persisted(
        self,
    ) -> None:
        event, result = (
            await self.ingest_and_process(
                make_private_payload(
                    message_id=1001,
                    text="hello private",
                )
            )
        )

        with open_database(
            self.database_path
        ) as db:
            state = db.execute(
                """
                SELECT state
                FROM inbound_events
                WHERE dedup_key = ?
                """,
                (event.dedup_key,),
            ).fetchone()[0]

            conversation_count = db.execute(
                """
                SELECT COUNT(*)
                FROM conversations
                """
            ).fetchone()[0]

            message = db.execute(
                """
                SELECT
                    direction,
                    sender_actor_id,
                    external_message_id,
                    source_event_key,
                    text
                FROM messages
                """
            ).fetchone()

        self.assertEqual(
            state,
            "completed",
        )

        self.assertEqual(
            conversation_count,
            1,
        )

        self.assertEqual(
            message[0],
            "inbound",
        )

        self.assertEqual(
            message[1],
            result.actor_id,
        )

        self.assertEqual(
            message[2],
            event.external_message_id,
        )

        self.assertEqual(
            message[3],
            event.dedup_key,
        )

        self.assertEqual(
            message[4],
            "hello private",
        )

    async def test_plain_group_message_is_ignored(
        self,
    ) -> None:
        event, _ = (
            await self.ingest_and_process(
                make_group_payload(
                    message_id=1002,
                    text="普通群聊",
                    mention_bot=False,
                )
            )
        )

        with open_database(
            self.database_path
        ) as db:
            state = db.execute(
                """
                SELECT state
                FROM inbound_events
                WHERE dedup_key = ?
                """,
                (event.dedup_key,),
            ).fetchone()[0]

            conversation_count = db.execute(
                """
                SELECT COUNT(*)
                FROM conversations
                """
            ).fetchone()[0]

            message_count = db.execute(
                """
                SELECT COUNT(*)
                FROM messages
                """
            ).fetchone()[0]

        self.assertEqual(
            state,
            "completed",
        )

        self.assertEqual(
            conversation_count,
            0,
        )

        self.assertEqual(
            message_count,
            0,
        )

    async def test_group_mention_enters_conversation(
        self,
    ) -> None:
        event, _ = (
            await self.ingest_and_process(
                make_group_payload(
                    message_id=1003,
                    text="@bot hello",
                    mention_bot=True,
                )
            )
        )

        with open_database(
            self.database_path
        ) as db:
            state = db.execute(
                """
                SELECT state
                FROM inbound_events
                WHERE dedup_key = ?
                """,
                (event.dedup_key,),
            ).fetchone()[0]

            conversation_count = db.execute(
                """
                SELECT COUNT(*)
                FROM conversations
                """
            ).fetchone()[0]

            message_count = db.execute(
                """
                SELECT COUNT(*)
                FROM messages
                """
            ).fetchone()[0]

        self.assertEqual(
            state,
            "completed",
        )

        self.assertEqual(
            conversation_count,
            1,
        )

        self.assertEqual(
            message_count,
            1,
        )

    async def test_startup_abandons_unfinished_events(
        self,
    ) -> None:
        received_event = (
            normalize_message_event(
                make_private_payload(
                    message_id=1004,
                    text="left received",
                )
            )
        )

        processing_event = (
            normalize_message_event(
                make_private_payload(
                    message_id=1005,
                    text="left processing",
                )
            )
        )

        self.assertIsNotNone(
            received_event
        )
        self.assertIsNotNone(
            processing_event
        )

        assert received_event is not None
        assert processing_event is not None

        first = (
            await self.database
            .ingest_message_event(
                received_event
            )
        )

        second = (
            await self.database
            .ingest_message_event(
                processing_event
            )
        )

        self.assertTrue(
            first.accepted
        )
        self.assertTrue(
            second.accepted
        )

        claimed = (
            await self.database
            .claim_inbound_event(
                processing_event.dedup_key
            )
        )

        self.assertTrue(
            claimed
        )

        await self.database.close()

        self.database = Database(
            self.database_path
        )

        await self.database.start()

        with open_database(
            self.database_path
        ) as db:
            states = dict(
                db.execute(
                    """
                    SELECT
                        external_message_id,
                        state
                    FROM inbound_events
                    """
                ).fetchall()
            )

        self.assertEqual(
            states[
                received_event.external_message_id
            ],
            "abandoned",
        )

        self.assertEqual(
            states[
                processing_event.external_message_id
            ],
            "abandoned",
        )

    async def test_existing_conversation_updates_updated_at(
        self,
    ) -> None:
        await self.ingest_and_process(
            make_private_payload(
                message_id=1006,
                text="first message",
            )
        )

        with open_database(
            self.database_path
        ) as db:
            row = db.execute(
                """
                SELECT id
                FROM conversations
                """
            ).fetchone()

            self.assertIsNotNone(row)

            conversation_id = row[0]

            db.execute(
                """
                UPDATE conversations
                SET updated_at = 1
                WHERE id = ?
                """,
                (conversation_id,),
            )

            db.commit()

        await self.ingest_and_process(
            make_private_payload(
                message_id=1007,
                text="second message",
            )
        )

        with open_database(
            self.database_path
        ) as db:
            conversation_count = db.execute(
                """
                SELECT COUNT(*)
                FROM conversations
                """
            ).fetchone()[0]

            message_count = db.execute(
                """
                SELECT COUNT(*)
                FROM messages
                """
            ).fetchone()[0]

            row = db.execute(
                """
                SELECT id, updated_at
                FROM conversations
                """
            ).fetchone()

        self.assertEqual(
            conversation_count,
            1,
        )

        self.assertEqual(
            message_count,
            2,
        )

        self.assertEqual(
            row[0],
            conversation_id,
        )

        self.assertGreater(
            row[1],
            1,
        )

if __name__ == "__main__":
    unittest.main()
