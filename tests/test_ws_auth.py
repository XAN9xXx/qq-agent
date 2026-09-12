from __future__ import annotations

import unittest
import weakref

from aiohttp import WSServerHandshakeError, web
from aiohttp.test_utils import TestClient, TestServer

from app.adapter.onebot.action_channel import (
    OneBotActionChannel,
)
from app.main import (
    ACCESS_TOKEN,
    ACTION_CHANNEL,
    WEBSOCKETS,
    onebot_ws_handler,
)


TOKEN = "correct-horse-battery-staple"

PATH = "/onebot/v11"


def build_app() -> web.Application:
    """Wire only what onebot_ws_handler touches.

    start_services() is deliberately not used: it opens the real
    database, which this test has no business creating.
    """
    app = web.Application()

    app[ACCESS_TOKEN] = TOKEN
    app[ACTION_CHANNEL] = OneBotActionChannel()
    app[WEBSOCKETS] = weakref.WeakSet()

    app.router.add_get(PATH, onebot_ws_handler)

    return app


class WebSocketAuthTest(
    unittest.IsolatedAsyncioTestCase
):
    async def asyncSetUp(self) -> None:
        self.client = TestClient(
            TestServer(build_app())
        )

        await self.client.start_server()

    async def asyncTearDown(self) -> None:
        await self.client.close()

    async def test_missing_token_is_rejected(self) -> None:
        response = await self.client.get(PATH)

        self.assertEqual(response.status, 401)
        self.assertEqual(
            response.headers.get("WWW-Authenticate"),
            "Bearer",
        )

    async def test_wrong_token_is_rejected(self) -> None:
        response = await self.client.get(
            PATH,
            headers={"Authorization": "Bearer wrong"},
        )

        self.assertEqual(response.status, 401)

    async def test_non_bearer_scheme_is_rejected(self) -> None:
        response = await self.client.get(
            PATH,
            headers={"Authorization": f"Token {TOKEN}"},
        )

        self.assertEqual(response.status, 401)

    async def test_empty_bearer_value_is_rejected(self) -> None:
        response = await self.client.get(
            PATH,
            headers={"Authorization": "Bearer   "},
        )

        self.assertEqual(response.status, 401)

    async def test_bearer_header_is_accepted(self) -> None:
        ws = await self.client.ws_connect(
            PATH,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )

        try:
            self.assertFalse(ws.closed)
        finally:
            await ws.close()

    async def test_query_parameter_is_accepted(self) -> None:
        ws = await self.client.ws_connect(
            f"{PATH}?access_token={TOKEN}"
        )

        try:
            self.assertFalse(ws.closed)
        finally:
            await ws.close()

    async def test_header_takes_precedence_over_query(self) -> None:
        # A present Authorization header must be authoritative, so a
        # valid query parameter cannot rescue a bad header.
        response = await self.client.get(
            f"{PATH}?access_token={TOKEN}",
            headers={"Authorization": "Bearer wrong"},
        )

        self.assertEqual(response.status, 401)

    async def test_second_connection_is_refused(self) -> None:
        first = await self.client.ws_connect(
            PATH,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )

        try:
            with self.assertRaises(
                WSServerHandshakeError
            ) as caught:
                await self.client.ws_connect(
                    PATH,
                    headers={
                        "Authorization": f"Bearer {TOKEN}"
                    },
                )

            self.assertEqual(caught.exception.status, 409)
        finally:
            await first.close()

    async def test_unauthenticated_client_learns_nothing_about_state(
        self,
    ) -> None:
        # An already-occupied channel must still answer 401 rather than
        # 409 to an unauthenticated caller.
        first = await self.client.ws_connect(
            PATH,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )

        try:
            response = await self.client.get(PATH)

            self.assertEqual(response.status, 401)
        finally:
            await first.close()


if __name__ == "__main__":
    unittest.main()
