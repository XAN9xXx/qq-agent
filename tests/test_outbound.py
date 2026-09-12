from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from app.adapter.onebot.message_port import (
    SendResult,
    SendStatus,
)
from app.adapter.onebot.normalizer import normalize_message_event
from app.dispatcher.dispatcher import DispatchItem
from app.dispatcher.pipeline import MessagePipeline
from app.store.database import Database
from app.store.migrate import migrate
from tests.support import (
    make_private_payload,
    open_database,
)


class StubMessagePort:
    """Stands in for MessagePort without touching a WebSocket."""

    def __init__(
        self,
        *,
        result: SendResult | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._result = result
        self._error = error
        self.calls: list[tuple[str, str, str]] = []

    async def send_text(
        self,
        *,
        scope_type: str,
        scope_external_id: str,
        text: str,
    ) -> SendResult:
        self.calls.append(
            (scope_type, scope_external_id, text)
        )

        if self._error is not None:
            raise self._error

        assert self._result is not None

        return self._result


class OutboundAttemptTest(
    unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

        self.database_path = (
            Path(self.temp_dir.name)
            / "agent-outbound-test.db"
        )

        migrate(self.database_path)

        self.database = Database(self.database_path)

        await self.database.start()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.temp_dir.cleanup()

    async def process(
        self,
        pipeline: MessagePipeline,
        *,
        message_id: int,
        text: str,
    ):
        event = normalize_message_event(
            make_private_payload(
                message_id=message_id,
                text=text,
            )
        )

        assert event is not None

        result = await self.database.ingest_message_event(event)

        self.assertTrue(result.accepted)

        await pipeline(
            DispatchItem(
                scope_id=result.scope_id,
                actor_id=result.actor_id,
                payload=event,
            )
        )

        return event

    def attempts(self) -> list[tuple]:
        with open_database(self.database_path) as db:
            return db.execute(
                """
                SELECT
                    status,
                    external_message_id,
                    retcode,
                    source_event_key,
                    text
                FROM outbound_attempts
                ORDER BY created_at, id
                """
            ).fetchall()

    def outbound_messages(self) -> list[tuple]:
        with open_database(self.database_path) as db:
            return db.execute(
                """
                SELECT
                    external_message_id,
                    source_event_key,
                    text,
                    sender_actor_id,
                    segments_json
                FROM messages
                WHERE direction = 'outbound'
                """
            ).fetchall()

    def inbound_state(self, dedup_key: str) -> str:
        with open_database(self.database_path) as db:
            return db.execute(
                """
                SELECT state
                FROM inbound_events
                WHERE dedup_key = ?
                """,
                (dedup_key,),
            ).fetchone()[0]

    async def test_sent_reply_is_recorded_as_a_message(
        self,
    ) -> None:
        port = StubMessagePort(
            result=SendResult(
                status=SendStatus.SENT,
                external_message_id="88888",
                retcode=0,
            )
        )

        pipeline = MessagePipeline(self.database, port)

        event = await self.process(
            pipeline,
            message_id=2001,
            text="/ping",
        )

        self.assertEqual(
            port.calls,
            [("private", "20002", "pong")],
        )

        self.assertEqual(
            self.attempts(),
            [
                (
                    "sent",
                    "88888",
                    0,
                    event.dedup_key,
                    "pong",
                )
            ],
        )

        self.assertEqual(
            self.outbound_messages(),
            [
                (
                    "88888",
                    event.dedup_key,
                    "pong",
                    None,
                    None,
                )
            ],
        )

    async def test_explicit_failure_records_retcode_and_no_message(
        self,
    ) -> None:
        port = StubMessagePort(
            result=SendResult(
                status=SendStatus.EXPLICIT_FAILURE,
                retcode=1200,
            )
        )

        pipeline = MessagePipeline(self.database, port)

        await self.process(
            pipeline,
            message_id=2002,
            text="/ping",
        )

        (attempt,) = self.attempts()

        self.assertEqual(attempt[0], "explicit_failure")
        self.assertIsNone(attempt[1])
        self.assertEqual(attempt[2], 1200)

        self.assertEqual(self.outbound_messages(), [])

    async def test_missing_message_port_records_not_dispatched(
        self,
    ) -> None:
        pipeline = MessagePipeline(self.database, None)

        await self.process(
            pipeline,
            message_id=2003,
            text="/ping",
        )

        (attempt,) = self.attempts()

        self.assertEqual(attempt[0], "not_dispatched")

        self.assertEqual(self.outbound_messages(), [])

    async def test_unexpected_send_error_records_unknown_outcome(
        self,
    ) -> None:
        port = StubMessagePort(
            error=RuntimeError("transport exploded")
        )

        pipeline = MessagePipeline(self.database, port)

        with self.assertRaises(RuntimeError):
            await self.process(
                pipeline,
                message_id=2004,
                text="/ping",
            )

        (attempt,) = self.attempts()

        # Unknown, not a failure: nothing may later infer from this row
        # that the reply is safe to send again.
        self.assertEqual(attempt[0], "unknown_outcome")

        self.assertEqual(self.outbound_messages(), [])

    async def test_inbound_stays_completed_when_the_reply_fails(
        self,
    ) -> None:
        port = StubMessagePort(
            error=RuntimeError("transport exploded")
        )

        pipeline = MessagePipeline(self.database, port)

        event = normalize_message_event(
            make_private_payload(
                message_id=2005,
                text="/ping",
            )
        )

        assert event is not None

        result = await self.database.ingest_message_event(event)

        with self.assertRaises(RuntimeError):
            await pipeline(
                DispatchItem(
                    scope_id=result.scope_id,
                    actor_id=result.actor_id,
                    payload=event,
                )
            )

        # The inbound message really was persisted; only the reply
        # failed. The two outcomes are recorded separately.
        self.assertEqual(
            self.inbound_state(event.dedup_key),
            "completed",
        )

    async def test_non_command_message_creates_no_attempt(
        self,
    ) -> None:
        port = StubMessagePort(
            result=SendResult(status=SendStatus.SENT)
        )

        pipeline = MessagePipeline(self.database, port)

        await self.process(
            pipeline,
            message_id=2006,
            text="just chatting",
        )

        self.assertEqual(port.calls, [])
        self.assertEqual(self.attempts(), [])
        self.assertEqual(self.outbound_messages(), [])

    async def test_message_clocks_are_separated(
        self,
    ) -> None:
        port = StubMessagePort(
            result=SendResult(
                status=SendStatus.SENT,
                external_message_id="66666",
                retcode=0,
            )
        )

        pipeline = MessagePipeline(self.database, port)

        before = int(time.time())

        event = await self.process(
            pipeline,
            message_id=2008,
            text="/ping",
        )

        after = int(time.time())

        with open_database(self.database_path) as db:
            clocks = {
                direction: (created_at, occurred_at)
                for direction, created_at, occurred_at in db.execute(
                    """
                    SELECT
                        direction,
                        created_at,
                        occurred_at
                    FROM messages
                    """
                )
            }

        inbound_created, inbound_occurred = clocks["inbound"]
        outbound_created, outbound_occurred = clocks["outbound"]

        # occurred_at keeps the platform clock...
        self.assertEqual(
            inbound_occurred,
            event.occurred_at,
        )

        # ...while created_at is the local persistence clock. The
        # fixture dates the event in 2023, so equal values here would
        # mean the two clocks are still conflated.
        self.assertNotEqual(
            inbound_created,
            inbound_occurred,
        )

        self.assertGreaterEqual(inbound_created, before)
        self.assertLessEqual(inbound_created, after)

        # An outbound message has no platform time of its own.
        self.assertIsNone(outbound_occurred)

        self.assertGreaterEqual(outbound_created, before)
        self.assertLessEqual(outbound_created, after)

        # The whole point of the split: ordering a conversation by
        # created_at can never put the reply before the message it
        # answers.
        self.assertLessEqual(
            inbound_created,
            outbound_created,
        )

    async def test_restart_marks_a_pending_attempt_unknown(
        self,
    ) -> None:
        port = StubMessagePort(
            result=SendResult(
                status=SendStatus.SENT,
                external_message_id="77777",
                retcode=0,
            )
        )

        pipeline = MessagePipeline(self.database, port)

        await self.process(
            pipeline,
            message_id=2007,
            text="/ping",
        )

        with open_database(self.database_path) as db:
            conversation_id = db.execute(
                "SELECT id FROM conversations"
            ).fetchone()[0]

        # An attempt that never reaches finish_outbound_attempt is what
        # a crash between dispatch and bookkeeping leaves behind.
        await self.database.begin_outbound_attempt(
            conversation_id=conversation_id,
            source_event_key=None,
            text="interrupted",
        )

        await self.database.close()

        self.database = Database(self.database_path)

        await self.database.start()

        statuses = {
            row[4]: row[0] for row in self.attempts()
        }

        self.assertEqual(statuses["pong"], "sent")
        self.assertEqual(
            statuses["interrupted"],
            "unknown_outcome",
        )

        # Recovery must not invent an outbound message for an attempt
        # whose delivery was never confirmed.
        self.assertEqual(
            len(self.outbound_messages()),
            1,
        )


if __name__ == "__main__":
    unittest.main()
