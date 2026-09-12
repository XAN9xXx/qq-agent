from __future__ import annotations

import logging

from app.adapter.onebot.message_port import (
    MessagePort,
    SendStatus,
)
from app.adapter.onebot.models import (
    NormalizedMessageEvent,
)
from app.dispatcher.dispatcher import (
    DispatchItem,
)
from app.store.database import Database


logger = logging.getLogger(
    "qq-agent.pipeline"
)


def should_enter_agent(
    event: NormalizedMessageEvent,
) -> bool:
    if event.scope_type == "private":
        return True

    if event.text.lstrip().startswith("/"):
        return True

    for segment in event.segments:
        if segment.get("type") != "at":
            continue

        data = segment.get("data")

        if not isinstance(data, dict):
            continue

        if str(data.get("qq")) == event.self_id:
            return True

    return False


def extract_plain_text(
    event: NormalizedMessageEvent,
) -> str:
    parts: list[str] = []

    for segment in event.segments:
        if segment.get("type") != "text":
            continue

        data = segment.get("data")

        if not isinstance(data, dict):
            continue

        text = data.get("text")

        if text is not None:
            parts.append(
                str(text)
            )

    if parts:
        return "".join(
            parts
        ).strip()

    return event.text.strip()


class MessagePipeline:
    def __init__(
        self,
        database: Database,
        message_port: MessagePort | None = None,
    ) -> None:
        self._database = database
        self._message_port = message_port

    async def __call__(
        self,
        item: DispatchItem,
    ) -> None:
        event = item.payload

        if not isinstance(
            event,
            NormalizedMessageEvent,
        ):
            raise TypeError(
                "DispatchItem payload must be "
                "NormalizedMessageEvent"
            )

        claimed = (
            await self._database
            .claim_inbound_event(
                event.dedup_key
            )
        )

        if not claimed:
            logger.warning(
                "Inbound event could not be claimed: "
                "dedup_key=%s",
                event.dedup_key,
            )
            return

        try:
            if not should_enter_agent(
                event
            ):
                await (
                    self._database
                    .complete_inbound_event(
                        event.dedup_key
                    )
                )

                logger.info(
                    "Ignored group message: "
                    "message_id=%s",
                    event.external_message_id,
                )

                return

            conversation_id = (
                await self._database
                .persist_inbound_message(
                    event,
                    actor_id=item.actor_id,
                    scope_id=item.scope_id,
                )
            )

            logger.info(
                "Inbound message completed: "
                "message_id=%s "
                "conversation=%s",
                event.external_message_id,
                conversation_id,
            )

            await self._handle_local_command(
                event,
                conversation_id=(
                    conversation_id
                ),
            )

        except Exception:
            await (
                self._database
                .mark_inbound_failed(
                    event.dedup_key
                )
            )

            raise

    async def _handle_local_command(
        self,
        event: NormalizedMessageEvent,
        *,
        conversation_id: str,
    ) -> None:
        command = extract_plain_text(
            event
        )

        if command != "/ping":
            return

        await self._send_reply(
            event,
            conversation_id=conversation_id,
            text="pong",
        )

    async def _send_reply(
        self,
        event: NormalizedMessageEvent,
        *,
        conversation_id: str,
        text: str,
    ) -> None:
        attempt_id = (
            await self._database
            .begin_outbound_attempt(
                conversation_id=conversation_id,
                source_event_key=(
                    event.dedup_key
                ),
                text=text,
            )
        )

        if self._message_port is None:
            logger.warning(
                "Reply not dispatched: "
                "MessagePort is unavailable, "
                "attempt=%s",
                attempt_id,
            )

            await self._finish_attempt(
                attempt_id,
                SendStatus.NOT_DISPATCHED,
            )

            return

        try:
            result = (
                await self._message_port.send_text(
                    scope_type=event.scope_type,
                    scope_external_id=(
                        event.scope_external_id
                    ),
                    text=text,
                )
            )

        except Exception:
            # An unexpected error leaves the outcome unprovable, so
            # record it as unknown rather than as a failure that
            # something could later decide to retry.
            await self._finish_attempt(
                attempt_id,
                SendStatus.UNKNOWN_OUTCOME,
            )

            raise

        await self._finish_attempt(
            attempt_id,
            result.status,
            external_message_id=(
                result.external_message_id
            ),
            retcode=result.retcode,
        )

        if result.status == SendStatus.SENT:
            logger.info(
                "Reply sent: "
                "attempt=%s "
                "external_message_id=%s",
                attempt_id,
                result.external_message_id,
            )

            return

        logger.error(
            "Reply failed: "
            "attempt=%s "
            "status=%s "
            "retcode=%s",
            attempt_id,
            result.status.value,
            result.retcode,
        )

    async def _finish_attempt(
        self,
        attempt_id: str,
        status: SendStatus,
        *,
        external_message_id: str | None = None,
        retcode: int | None = None,
    ) -> None:
        await (
            self._database
            .finish_outbound_attempt(
                attempt_id,
                status=status.value,
                external_message_id=(
                    external_message_id
                ),
                retcode=retcode,
            )
        )
