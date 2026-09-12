from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from app.adapter.onebot.action_channel import (
    ActionNotDispatched,
    ActionOutcomeUnknown,
    OneBotActionChannel,
)


ScopeType = Literal[
    "private",
    "group",
]


class SendStatus(
    str,
    Enum,
):
    SENT = "sent"
    NOT_DISPATCHED = "not_dispatched"
    EXPLICIT_FAILURE = "explicit_failure"
    UNKNOWN_OUTCOME = "unknown_outcome"


@dataclass(frozen=True, slots=True)
class SendResult:
    status: SendStatus
    external_message_id: str | None = None
    retcode: int | None = None


class MessagePort:
    def __init__(
        self,
        channel: OneBotActionChannel,
    ) -> None:
        self._channel = channel

    async def send_text(
        self,
        *,
        scope_type: ScopeType,
        scope_external_id: str,
        text: str,
    ) -> SendResult:
        if scope_type == "private":
            action = "send_private_msg"

            params = {
                "user_id": int(
                    scope_external_id
                ),
                "message": text,
                "auto_escape": True,
            }

        elif scope_type == "group":
            action = "send_group_msg"

            params = {
                "group_id": int(
                    scope_external_id
                ),
                "message": text,
                "auto_escape": True,
            }

        else:
            raise ValueError(
                "Unsupported scope_type: "
                f"{scope_type!r}"
            )

        try:
            response = await self._channel.call(
                action,
                params,
            )

        except ActionNotDispatched:
            return SendResult(
                status=(
                    SendStatus.NOT_DISPATCHED
                )
            )

        except ActionOutcomeUnknown:
            return SendResult(
                status=(
                    SendStatus.UNKNOWN_OUTCOME
                )
            )

        if (
            response.status != "ok"
            or response.retcode != 0
        ):
            return SendResult(
                status=(
                    SendStatus.EXPLICIT_FAILURE
                ),
                retcode=response.retcode,
            )

        external_message_id = None

        if isinstance(
            response.data,
            dict,
        ):
            value = response.data.get(
                "message_id"
            )

            if value is not None:
                external_message_id = str(
                    value
                )

        return SendResult(
            status=SendStatus.SENT,
            external_message_id=(
                external_message_id
            ),
            retcode=response.retcode,
        )
