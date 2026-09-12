from __future__ import annotations

import asyncio
import unittest

from app.dispatcher.dispatcher import (
    ConversationDispatcher,
    DispatchItem,
    QueueFullError,
)


class DispatcherTest(
    unittest.IsolatedAsyncioTestCase
):
    async def test_same_conversation_is_serial_fifo(
        self,
    ) -> None:
        order: list[int] = []

        running = 0
        max_running = 0

        async def handler(
            item: DispatchItem,
        ) -> None:
            nonlocal running
            nonlocal max_running

            running += 1

            max_running = max(
                max_running,
                running,
            )

            await asyncio.sleep(0.01)

            order.append(
                item.payload
            )

            running -= 1

        dispatcher = ConversationDispatcher(
            handler,
            queue_maxsize=8,
            worker_idle_timeout=1.0,
        )

        try:
            for number in range(5):
                await dispatcher.enqueue(
                    DispatchItem(
                        scope_id="scope-a",
                        actor_id="actor-a",
                        payload=number,
                    )
                )

            await dispatcher.join()

            self.assertEqual(
                order,
                [0, 1, 2, 3, 4],
            )

            self.assertEqual(
                max_running,
                1,
            )

        finally:
            await dispatcher.close()

    async def test_different_conversations_are_parallel(
        self,
    ) -> None:
        release = asyncio.Event()
        both_started = asyncio.Event()

        running = 0
        max_running = 0

        async def handler(
            item: DispatchItem,
        ) -> None:
            nonlocal running
            nonlocal max_running

            running += 1

            max_running = max(
                max_running,
                running,
            )

            if running == 2:
                both_started.set()

            await release.wait()

            running -= 1

        dispatcher = ConversationDispatcher(
            handler,
            queue_maxsize=8,
            worker_idle_timeout=1.0,
        )

        try:
            await dispatcher.enqueue(
                DispatchItem(
                    scope_id="scope-a",
                    actor_id="actor-a",
                    payload="A",
                )
            )

            await dispatcher.enqueue(
                DispatchItem(
                    scope_id="scope-b",
                    actor_id="actor-b",
                    payload="B",
                )
            )

            await asyncio.wait_for(
                both_started.wait(),
                timeout=1.0,
            )

            self.assertEqual(
                max_running,
                2,
            )

            release.set()

            await dispatcher.join()

        finally:
            release.set()
            await dispatcher.close()

    async def test_queue_is_bounded(
        self,
    ) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def handler(
            item: DispatchItem,
        ) -> None:
            started.set()
            await release.wait()

        dispatcher = ConversationDispatcher(
            handler,
            queue_maxsize=1,
            worker_idle_timeout=1.0,
        )

        try:
            await dispatcher.enqueue(
                DispatchItem(
                    scope_id="scope-a",
                    actor_id="actor-a",
                    payload=1,
                )
            )

            await asyncio.wait_for(
                started.wait(),
                timeout=1.0,
            )

            await dispatcher.enqueue(
                DispatchItem(
                    scope_id="scope-a",
                    actor_id="actor-a",
                    payload=2,
                )
            )

            with self.assertRaises(
                QueueFullError
            ):
                await dispatcher.enqueue(
                    DispatchItem(
                        scope_id="scope-a",
                        actor_id="actor-a",
                        payload=3,
                    )
                )

            release.set()

            await dispatcher.join()

        finally:
            release.set()
            await dispatcher.close()

    async def test_idle_worker_is_reaped(
        self,
    ) -> None:
        async def handler(
            item: DispatchItem,
        ) -> None:
            return

        dispatcher = ConversationDispatcher(
            handler,
            queue_maxsize=8,
            worker_idle_timeout=0.05,
        )

        try:
            await dispatcher.enqueue(
                DispatchItem(
                    scope_id="scope-a",
                    actor_id="actor-a",
                    payload=None,
                )
            )

            await dispatcher.join()

            self.assertEqual(
                dispatcher.active_worker_count,
                1,
            )

            await asyncio.sleep(0.1)

            self.assertEqual(
                dispatcher.active_worker_count,
                0,
            )

        finally:
            await dispatcher.close()


if __name__ == "__main__":
    unittest.main()
