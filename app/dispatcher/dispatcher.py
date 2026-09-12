from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger("qq-agent.dispatcher")


ConversationKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class DispatchItem:
    scope_id: str
    actor_id: str
    payload: Any

    @property
    def key(self) -> ConversationKey:
        return (
            self.scope_id,
            self.actor_id,
        )


@dataclass(slots=True)
class _WorkerState:
    queue: asyncio.Queue[DispatchItem]
    task: asyncio.Task[None]


class QueueFullError(RuntimeError):
    def __init__(
        self,
        key: ConversationKey,
        maxsize: int,
    ) -> None:
        self.key = key
        self.maxsize = maxsize

        super().__init__(
            f"Conversation queue is full: "
            f"key={key!r} maxsize={maxsize}"
        )


Handler = Callable[
    [DispatchItem],
    Awaitable[None],
]


class ConversationDispatcher:
    def __init__(
        self,
        handler: Handler,
        *,
        queue_maxsize: int = 32,
        worker_idle_timeout: float = 60.0,
    ) -> None:
        if queue_maxsize <= 0:
            raise ValueError(
                "queue_maxsize must be greater than zero"
            )

        if worker_idle_timeout <= 0:
            raise ValueError(
                "worker_idle_timeout must be greater than zero"
            )

        self._handler = handler
        self._queue_maxsize = queue_maxsize
        self._worker_idle_timeout = worker_idle_timeout

        self._workers: dict[
            ConversationKey,
            _WorkerState,
        ] = {}

        self._lock = asyncio.Lock()
        self._closing = False

    @property
    def active_worker_count(self) -> int:
        return len(self._workers)

    @property
    def queue_maxsize(self) -> int:
        return self._queue_maxsize

    async def enqueue(
        self,
        item: DispatchItem,
    ) -> None:
        if self._closing:
            raise RuntimeError(
                "Dispatcher is closing"
            )

        async with self._lock:
            if self._closing:
                raise RuntimeError(
                    "Dispatcher is closing"
                )

            state = self._workers.get(
                item.key
            )

            if state is None:
                queue: asyncio.Queue[
                    DispatchItem
                ] = asyncio.Queue(
                    maxsize=self._queue_maxsize
                )

                task = asyncio.create_task(
                    self._worker(
                        item.key,
                        queue,
                    ),
                    name=(
                        "conversation-worker:"
                        f"{item.scope_id}:"
                        f"{item.actor_id}"
                    ),
                )

                state = _WorkerState(
                    queue=queue,
                    task=task,
                )

                self._workers[item.key] = state

            try:
                state.queue.put_nowait(item)
            except asyncio.QueueFull as exc:
                raise QueueFullError(
                    item.key,
                    self._queue_maxsize,
                ) from exc

    async def join(self) -> None:
        while True:
            async with self._lock:
                states = list(
                    self._workers.values()
                )

            if not states:
                return

            await asyncio.gather(
                *(
                    state.queue.join()
                    for state in states
                )
            )

            async with self._lock:
                if all(
                    state.queue.empty()
                    for state in self._workers.values()
                ):
                    return

    async def close(self) -> None:
        async with self._lock:
            if self._closing:
                return

            self._closing = True

            states = list(
                self._workers.values()
            )

        if states:
            await asyncio.gather(
                *(
                    state.queue.join()
                    for state in states
                )
            )

        async with self._lock:
            tasks = [
                state.task
                for state in self._workers.values()
            ]

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        async with self._lock:
            self._workers.clear()

    async def _worker(
        self,
        key: ConversationKey,
        queue: asyncio.Queue[DispatchItem],
    ) -> None:
        current_task = asyncio.current_task()

        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        queue.get(),
                        timeout=self._worker_idle_timeout,
                    )
                except TimeoutError:
                    async with self._lock:
                        state = self._workers.get(
                            key
                        )

                        if (
                            state is not None
                            and state.task is current_task
                            and queue.empty()
                        ):
                            self._workers.pop(
                                key,
                                None,
                            )

                            logger.debug(
                                "Conversation worker retired: "
                                "key=%r",
                                key,
                            )

                            return

                    continue

                try:
                    await self._handler(item)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Unhandled conversation handler "
                        "error: key=%r",
                        key,
                    )
                finally:
                    queue.task_done()

        finally:
            async with self._lock:
                state = self._workers.get(
                    key
                )

                if (
                    state is not None
                    and state.task is current_task
                ):
                    self._workers.pop(
                        key,
                        None,
                    )
