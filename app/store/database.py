from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

from app.adapter.onebot.models import NormalizedMessageEvent
from app.config.settings import DATABASE_PATH


logger = logging.getLogger(
    "qq-agent.database"
)


@dataclass(frozen=True, slots=True)
class IngestResult:
    accepted: bool
    actor_id: str
    scope_id: str


class Database:
    def __init__(
        self,
        path: Path = DATABASE_PATH,
    ) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._db is not None:
            raise RuntimeError(
                "Database has already been started"
            )

        db = await aiosqlite.connect(
            self._path
        )

        self._db = db

        try:
            await db.execute(
                "PRAGMA journal_mode=WAL;"
            )

            await db.execute(
                "PRAGMA foreign_keys=ON;"
            )

            await db.execute(
                "PRAGMA busy_timeout=5000;"
            )

            now = int(
                time.time()
            )

            await db.execute(
                "BEGIN IMMEDIATE"
            )

            try:
                inbound_cursor = (
                    await db.execute(
                        """
                        UPDATE inbound_events
                        SET
                            state = 'abandoned',
                            state_updated_at = ?
                        WHERE state IN (
                            'received',
                            'processing'
                        )
                        """,
                        (now,),
                    )
                )

                outbound_cursor = (
                    await db.execute(
                        """
                        UPDATE outbound_attempts
                        SET
                            status = 'unknown_outcome',
                            updated_at = ?
                        WHERE status = 'pending'
                        """,
                        (now,),
                    )
                )

                await db.commit()

            except Exception:
                await db.rollback()
                raise

            if inbound_cursor.rowcount > 0:
                logger.info(
                    "Recovered %d abandoned "
                    "inbound event(s)",
                    inbound_cursor.rowcount,
                )

            if outbound_cursor.rowcount > 0:
                logger.info(
                    "Recovered %d unknown "
                    "outbound attempt(s)",
                    outbound_cursor.rowcount,
                )

        except Exception:
            try:
                await db.close()
            finally:
                self._db = None

            raise

    async def close(self) -> None:
        db = self._db

        if db is None:
            return

        self._db = None

        await db.close()

    def _require_db(
        self,
    ) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError(
                "Database has not been started"
            )

        return self._db

    async def ingest_message_event(
        self,
        event: NormalizedMessageEvent,
    ) -> IngestResult:
        db = self._require_db()

        async with self._write_lock:
            await db.execute(
                "BEGIN IMMEDIATE"
            )

            try:
                actor_id = await self._resolve_actor(
                    db,
                    platform=event.platform,
                    external_id=(
                        event.actor_external_id
                    ),
                )

                scope_id = await self._resolve_scope(
                    db,
                    platform=event.platform,
                    scope_type=event.scope_type,
                    external_id=(
                        event.scope_external_id
                    ),
                )

                now = int(
                    time.time()
                )

                cursor = await db.execute(
                    """
                    INSERT OR IGNORE
                    INTO inbound_events (
                        dedup_key,
                        event_type,
                        external_message_id,
                        scope_id,
                        actor_id,
                        occurred_at,
                        received_at,
                        state,
                        state_updated_at,
                        raw_json
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?
                    )
                    """,
                    (
                        event.dedup_key,
                        "message",
                        event.external_message_id,
                        scope_id,
                        actor_id,
                        event.occurred_at,
                        now,
                        "received",
                        now,
                        json.dumps(
                            event.raw_payload,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    ),
                )

                accepted = (
                    cursor.rowcount == 1
                )

                await db.commit()

            except Exception:
                await db.rollback()
                raise

        return IngestResult(
            accepted=accepted,
            actor_id=actor_id,
            scope_id=scope_id,
        )

    async def claim_inbound_event(
        self,
        dedup_key: str,
    ) -> bool:
        db = self._require_db()

        async with self._write_lock:
            cursor = await db.execute(
                """
                UPDATE inbound_events
                SET
                    state = 'processing',
                    state_updated_at = ?
                WHERE dedup_key = ?
                  AND state = 'received'
                """,
                (
                    int(time.time()),
                    dedup_key,
                ),
            )

            await db.commit()

            return (
                cursor.rowcount == 1
            )

    async def complete_inbound_event(
        self,
        dedup_key: str,
    ) -> None:
        db = self._require_db()

        async with self._write_lock:
            cursor = await db.execute(
                """
                UPDATE inbound_events
                SET
                    state = 'completed',
                    state_updated_at = ?
                WHERE dedup_key = ?
                  AND state = 'processing'
                """,
                (
                    int(time.time()),
                    dedup_key,
                ),
            )

            if cursor.rowcount != 1:
                await db.rollback()

                raise RuntimeError(
                    "Cannot complete inbound event: "
                    f"{dedup_key}"
                )

            await db.commit()

    async def mark_inbound_failed(
        self,
        dedup_key: str,
    ) -> bool:
        db = self._require_db()

        async with self._write_lock:
            cursor = await db.execute(
                """
                UPDATE inbound_events
                SET
                    state = 'failed',
                    state_updated_at = ?
                WHERE dedup_key = ?
                  AND state IN (
                      'received',
                      'processing'
                  )
                """,
                (
                    int(time.time()),
                    dedup_key,
                ),
            )

            await db.commit()

            return (
                cursor.rowcount == 1
            )

    async def persist_inbound_message(
        self,
        event: NormalizedMessageEvent,
        *,
        actor_id: str,
        scope_id: str,
    ) -> str:
        db = self._require_db()

        async with self._write_lock:
            await db.execute(
                "BEGIN IMMEDIATE"
            )

            try:
                row = await (
                    await db.execute(
                        """
                        SELECT state
                        FROM inbound_events
                        WHERE dedup_key = ?
                        """,
                        (
                            event.dedup_key,
                        ),
                    )
                ).fetchone()

                if row is None:
                    raise RuntimeError(
                        "Inbound event not found: "
                        f"{event.dedup_key}"
                    )

                if row[0] != "processing":
                    raise RuntimeError(
                        "Inbound event is not "
                        "processing: "
                        f"{event.dedup_key} "
                        f"state={row[0]!r}"
                    )

                conversation_id = (
                    await self._ensure_conversation(
                        db,
                        scope_id=scope_id,
                        actor_id=actor_id,
                    )
                )

                now = int(
                    time.time()
                )

                await db.execute(
                    """
                    INSERT INTO messages (
                        conversation_id,
                        direction,
                        sender_actor_id,
                        external_message_id,
                        source_event_key,
                        text,
                        segments_json,
                        created_at,
                        occurred_at
                    )
                    VALUES (
                        ?,
                        'inbound',
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?
                    )
                    """,
                    (
                        conversation_id,
                        actor_id,
                        event.external_message_id,
                        event.dedup_key,
                        event.text,
                        json.dumps(
                            event.segments,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        now,
                        event.occurred_at,
                    ),
                )

                await db.execute(
                    """
                    UPDATE conversations
                    SET updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        now,
                        conversation_id,
                    ),
                )

                cursor = await db.execute(
                    """
                    UPDATE inbound_events
                    SET
                        state = 'completed',
                        state_updated_at = ?
                    WHERE dedup_key = ?
                      AND state = 'processing'
                    """,
                    (
                        now,
                        event.dedup_key,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Failed to complete inbound "
                        "event: "
                        f"{event.dedup_key}"
                    )

                await db.commit()

                return conversation_id

            except Exception:
                await db.rollback()
                raise

    async def begin_outbound_attempt(
        self,
        *,
        conversation_id: str,
        source_event_key: str | None,
        text: str,
    ) -> str:
        db = self._require_db()

        attempt_id = str(
            uuid.uuid4()
        )

        now = int(
            time.time()
        )

        async with self._write_lock:
            await db.execute(
                """
                INSERT INTO outbound_attempts (
                    id,
                    conversation_id,
                    source_event_key,
                    text,
                    status,
                    external_message_id,
                    retcode,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    'pending',
                    NULL,
                    NULL,
                    ?,
                    ?
                )
                """,
                (
                    attempt_id,
                    conversation_id,
                    source_event_key,
                    text,
                    now,
                    now,
                ),
            )

            await db.commit()

        return attempt_id

    async def finish_outbound_attempt(
        self,
        attempt_id: str,
        *,
        status: str,
        external_message_id: str | None = None,
        retcode: int | None = None,
    ) -> None:
        allowed_statuses = {
            "sent",
            "not_dispatched",
            "explicit_failure",
            "unknown_outcome",
        }

        if status not in allowed_statuses:
            raise ValueError(
                "Invalid final outbound status: "
                f"{status!r}"
            )

        db = self._require_db()

        async with self._write_lock:
            await db.execute(
                "BEGIN IMMEDIATE"
            )

            try:
                row = await (
                    await db.execute(
                        """
                        SELECT
                            conversation_id,
                            source_event_key,
                            text,
                            status
                        FROM outbound_attempts
                        WHERE id = ?
                        """,
                        (
                            attempt_id,
                        ),
                    )
                ).fetchone()

                if row is None:
                    raise RuntimeError(
                        "Outbound attempt not found: "
                        f"{attempt_id}"
                    )

                (
                    conversation_id,
                    source_event_key,
                    text,
                    current_status,
                ) = row

                if current_status != "pending":
                    raise RuntimeError(
                        "Outbound attempt is not "
                        "pending: "
                        f"{attempt_id} "
                        f"status={current_status!r}"
                    )

                now = int(
                    time.time()
                )

                cursor = await db.execute(
                    """
                    UPDATE outbound_attempts
                    SET
                        status = ?,
                        external_message_id = ?,
                        retcode = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND status = 'pending'
                    """,
                    (
                        status,
                        external_message_id,
                        retcode,
                        now,
                        attempt_id,
                    ),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Failed to finalize "
                        "outbound attempt: "
                        f"{attempt_id}"
                    )

                if status == "sent":
                    await db.execute(
                        """
                        INSERT INTO messages (
                            conversation_id,
                            direction,
                            sender_actor_id,
                            external_message_id,
                            source_event_key,
                            text,
                            segments_json,
                            created_at,
                            occurred_at
                        )
                        VALUES (
                            ?,
                            'outbound',
                            NULL,
                            ?,
                            ?,
                            ?,
                            NULL,
                            ?,
                            NULL
                        )
                        """,
                        (
                            conversation_id,
                            external_message_id,
                            source_event_key,
                            text,
                            now,
                        ),
                    )

                    await db.execute(
                        """
                        UPDATE conversations
                        SET updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            now,
                            conversation_id,
                        ),
                    )

                await db.commit()

            except Exception:
                await db.rollback()
                raise

    async def _ensure_conversation(
        self,
        db: aiosqlite.Connection,
        *,
        scope_id: str,
        actor_id: str,
    ) -> str:
        row = await (
            await db.execute(
                """
                SELECT id
                FROM conversations
                WHERE scope_id = ?
                  AND actor_id = ?
                """,
                (
                    scope_id,
                    actor_id,
                ),
            )
        ).fetchone()

        if row is not None:
            return str(
                row[0]
            )

        now = int(
            time.time()
        )

        conversation_id = str(
            uuid.uuid4()
        )

        await db.execute(
            """
            INSERT INTO conversations (
                id,
                scope_id,
                actor_id,
                context_reset_at,
                created_at,
                updated_at
            )
            VALUES (
                ?,
                ?,
                ?,
                NULL,
                ?,
                ?
            )
            """,
            (
                conversation_id,
                scope_id,
                actor_id,
                now,
                now,
            ),
        )

        return conversation_id

    async def _resolve_actor(
        self,
        db: aiosqlite.Connection,
        *,
        platform: str,
        external_id: str,
    ) -> str:
        row = await (
            await db.execute(
                """
                SELECT id
                FROM actors
                WHERE platform = ?
                  AND external_id = ?
                """,
                (
                    platform,
                    external_id,
                ),
            )
        ).fetchone()

        if row is not None:
            return str(
                row[0]
            )

        actor_id = str(
            uuid.uuid4()
        )

        await db.execute(
            """
            INSERT INTO actors (
                id,
                platform,
                external_id,
                created_at
            )
            VALUES (
                ?,
                ?,
                ?,
                ?
            )
            """,
            (
                actor_id,
                platform,
                external_id,
                int(time.time()),
            ),
        )

        return actor_id

    async def _resolve_scope(
        self,
        db: aiosqlite.Connection,
        *,
        platform: str,
        scope_type: str,
        external_id: str,
    ) -> str:
        row = await (
            await db.execute(
                """
                SELECT id
                FROM scopes
                WHERE platform = ?
                  AND scope_type = ?
                  AND external_id = ?
                """,
                (
                    platform,
                    scope_type,
                    external_id,
                ),
            )
        ).fetchone()

        if row is not None:
            return str(
                row[0]
            )

        scope_id = str(
            uuid.uuid4()
        )

        await db.execute(
            """
            INSERT INTO scopes (
                id,
                platform,
                scope_type,
                external_id,
                created_at
            )
            VALUES (
                ?,
                ?,
                ?,
                ?,
                ?
            )
            """,
            (
                scope_id,
                platform,
                scope_type,
                external_id,
                int(time.time()),
            ),
        )

        return scope_id
