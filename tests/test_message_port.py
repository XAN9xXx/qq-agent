from __future__ import annotations

import json
import unittest
from collections.abc import Awaitable, Callable

from app.adapter.onebot.action_channel import (
    OneBotActionChannel,
)
from app.adapter.onebot.message_port import (
    MessagePort,
    SendStatus,
)


OnSend = Callable[
    [str],
    Awaitable[None],
]


class FakeWebSocket:
    def __init__(
        self,
        on_send: OnSend | None = None,
    ) -> None:
        self.closed = False
        self.sent: list[str] = []
        self.on_send = on_send

    async def send_str(
        self,
        data: str,
    ) -> None:
        self.sent.append(
            data
        )

        if self.on_send is not None:
            await self.on_send(
                data
            )


class MessagePortTest(
    unittest.IsolatedAsyncioTestCase
):
    async def test_no_connection_is_not_dispatched(
        self,
    ) -> None:
        channel = OneBotActionChannel(
            response_timeout=0.1
        )

        port = MessagePort(
            channel
        )

        result = await port.send_text(
            scope_type="private",
            scope_external_id="20002",
            text="hello",
        )

        self.assertEqual(
            result.status,
            SendStatus.NOT_DISPATCHED,
        )

    async def test_successful_send_is_sent(
        self,
    ) -> None:
        channel = OneBotActionChannel(
            response_timeout=0.1
        )

        async def on_send(
            data: str,
        ) -> None:
            request = json.loads(
                data
            )

            handled = (
                channel.handle_response(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {
                            "message_id": 12345,
                        },
                        "echo": request["echo"],
                    }
                )
            )

            self.assertTrue(
                handled
            )

        ws = FakeWebSocket(
            on_send
        )

        channel.attach(
            ws
        )

        port = MessagePort(
            channel
        )

        result = await port.send_text(
            scope_type="private",
            scope_external_id="20002",
            text="hello",
        )

        self.assertEqual(
            result.status,
            SendStatus.SENT,
        )

        self.assertEqual(
            result.external_message_id,
            "12345",
        )

        self.assertEqual(
            len(ws.sent),
            1,
        )

        request = json.loads(
            ws.sent[0]
        )

        self.assertEqual(
            request["action"],
            "send_private_msg",
        )

        self.assertEqual(
            request["params"]["user_id"],
            20002,
        )

        self.assertEqual(
            request["params"]["message"],
            "hello",
        )

        self.assertTrue(
            request["params"]["auto_escape"]
        )

    async def test_explicit_failure_is_reported(
        self,
    ) -> None:
        channel = OneBotActionChannel(
            response_timeout=0.1
        )

        async def on_send(
            data: str,
        ) -> None:
            request = json.loads(
                data
            )

            channel.handle_response(
                {
                    "status": "failed",
                    "retcode": 100,
                    "data": None,
                    "echo": request["echo"],
                }
            )

        ws = FakeWebSocket(
            on_send
        )

        channel.attach(
            ws
        )

        port = MessagePort(
            channel
        )

        result = await port.send_text(
            scope_type="group",
            scope_external_id="30003",
            text="hello",
        )

        self.assertEqual(
            result.status,
            SendStatus.EXPLICIT_FAILURE,
        )

        self.assertEqual(
            result.retcode,
            100,
        )

        self.assertEqual(
            len(ws.sent),
            1,
        )

        request = json.loads(
            ws.sent[0]
        )

        self.assertEqual(
            request["action"],
            "send_group_msg",
        )

        self.assertEqual(
            request["params"]["group_id"],
            30003,
        )

        self.assertEqual(
            request["params"]["message"],
            "hello",
        )

        self.assertTrue(
            request["params"]["auto_escape"]
        )

    async def test_disconnect_after_dispatch_is_unknown(
        self,
    ) -> None:
        channel = OneBotActionChannel(
            response_timeout=0.1
        )

        ws: FakeWebSocket

        async def on_send(
            data: str,
        ) -> None:
            channel.detach(
                ws
            )

        ws = FakeWebSocket(
            on_send
        )

        channel.attach(
            ws
        )

        port = MessagePort(
            channel
        )

        result = await port.send_text(
            scope_type="private",
            scope_external_id="20002",
            text="hello",
        )

        self.assertEqual(
            result.status,
            SendStatus.UNKNOWN_OUTCOME,
        )

        self.assertEqual(
            len(ws.sent),
            1,
        )

        request = json.loads(
            ws.sent[0]
        )

        self.assertEqual(
            request["action"],
            "send_private_msg",
        )

        self.assertEqual(
            request["params"]["user_id"],
            20002,
        )

        self.assertEqual(
            request["params"]["message"],
            "hello",
        )

        self.assertTrue(
            request["params"]["auto_escape"]
        )


if __name__ == "__main__":
    unittest.main()
