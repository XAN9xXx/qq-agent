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
                event
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
    ) -> None:
        command = extract_plain_text(
            event
        )

        if command != "/ping":
            return

        if self._message_port is None:
            logger.warning(
                "/ping received but "
                "MessagePort is unavailable"
            )
            return

        result = (
            await self._message_port.send_text(
                scope_type=event.scope_type,
                scope_external_id=(
                    event.scope_external_id
                ),
                text="pong",
            )
        )

        if result.status == SendStatus.SENT:
            logger.info(
                "/ping reply sent: "
                "external_message_id=%s",
                result.external_message_id,
            )
            return

        logger.error(
            "/ping reply failed: "
            "status=%s retcode=%s",
            result.status.value,
            result.retcode,
        )
