from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from typing import Any, Protocol


class WebSocketLike(Protocol):
    @property
    def closed(self) -> bool:
        ...

    async def send_str(
        self,
        data: str,
    ) -> None:
        ...


class ActionNotDispatched(
    RuntimeError
):
    pass


class ActionOutcomeUnknown(
    RuntimeError
):
    pass


@dataclass(frozen=True, slots=True)
class ActionResponse:
    status: str
    retcode: int
    data: Any
    echo: str
    raw: dict[str, Any]


class OneBotActionChannel:
    def __init__(
        self,
        *,
        response_timeout: float = 10.0,
    ) -> None:
        if response_timeout <= 0:
            raise ValueError(
                "response_timeout must be greater than zero"
            )

        self._response_timeout = response_timeout

        self._ws: WebSocketLike | None = None

        self._pending: dict[
            str,
            asyncio.Future[dict[str, Any]],
        ] = {}

    @property
    def connected(self) -> bool:
        ws = self._ws

        return (
            ws is not None
            and not ws.closed
        )

    def attach(
        self,
        ws: WebSocketLike,
    ) -> None:
        old_ws = self._ws

        if (
            old_ws is not None
            and old_ws is not ws
        ):
            self._fail_pending(
                ActionOutcomeUnknown(
                    "OneBot WebSocket replaced "
                    "while actions were pending"
                )
            )

        self._ws = ws

    def detach(
        self,
        ws: WebSocketLike,
    ) -> None:
        if self._ws is not ws:
            return

        self._ws = None

        self._fail_pending(
            ActionOutcomeUnknown(
                "OneBot WebSocket disconnected "
                "after action dispatch"
            )
        )

    def handle_response(
        self,
        payload: dict[str, Any],
    ) -> bool:
        if "echo" not in payload:
            return False

        if (
            "status" not in payload
            and "retcode" not in payload
        ):
            return False

        echo = str(
            payload.get("echo")
        )

        future = self._pending.pop(
            echo,
            None,
        )

        if future is not None:
            if not future.done():
                future.set_result(
                    payload
                )

        # It is still an action response even if the
        # matching future already timed out/disappeared.
        return True

    async def call(
        self,
        action: str,
        params: dict[str, Any],
    ) -> ActionResponse:
        ws = self._ws

        if (
            ws is None
            or ws.closed
        ):
            raise ActionNotDispatched(
                "No active OneBot WebSocket"
            )

        echo = uuid.uuid4().hex

        loop = (
            asyncio.get_running_loop()
        )

        future: asyncio.Future[
            dict[str, Any]
        ] = loop.create_future()

        self._pending[echo] = future

        request = {
            "action": action,
            "params": params,
            "echo": echo,
        }

        encoded = json.dumps(
            request,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        try:
            await ws.send_str(
                encoded
            )
        except Exception as exc:
            self._pending.pop(
                echo,
                None,
            )

            if not future.done():
                future.cancel()

            # Once send_str() has been attempted we cannot
            # safely prove that NapCat did not receive it.
            raise ActionOutcomeUnknown(
                "OneBot action send outcome "
                "is unknown"
            ) from exc

        try:
            payload = await asyncio.wait_for(
                asyncio.shield(
                    future
                ),
                timeout=(
                    self._response_timeout
                ),
            )

        except TimeoutError as exc:
            self._pending.pop(
                echo,
                None,
            )

            raise ActionOutcomeUnknown(
                "Timed out waiting for "
                "OneBot action response"
            ) from exc

        except ActionOutcomeUnknown:
            self._pending.pop(
                echo,
                None,
            )
            raise

        finally:
            self._pending.pop(
                echo,
                None,
            )

        return ActionResponse(
            status=str(
                payload.get(
                    "status",
                    "",
                )
            ),
            retcode=int(
                payload.get(
                    "retcode",
                    -1,
                )
            ),
            data=payload.get(
                "data"
            ),
            echo=echo,
            raw=payload,
        )

    def _fail_pending(
        self,
        exc: Exception,
    ) -> None:
        pending = list(
            self._pending.values()
        )

        self._pending.clear()

        for future in pending:
            if future.done():
                continue

            future.set_exception(
                exc
            )
