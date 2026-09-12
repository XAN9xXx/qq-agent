from __future__ import annotations

import hmac
import json
import logging
import weakref

from aiohttp import WSCloseCode, WSMsgType, web

from app.adapter.onebot.action_channel import (
    OneBotActionChannel,
)
from app.adapter.onebot.message_port import (
    MessagePort,
)
from app.adapter.onebot.normalizer import (
    normalize_message_event,
)
from app.config.settings import (
    DATA_DIR,
    load_onebot_access_token,
)
from app.dispatcher.dispatcher import (
    ConversationDispatcher,
    DispatchItem,
    QueueFullError,
)
from app.dispatcher.pipeline import (
    MessagePipeline,
)
from app.store.database import (
    Database,
)
from app.store.migrate import (
    migrate,
)


WEBSOCKETS: web.AppKey[
    weakref.WeakSet[web.WebSocketResponse]
] = web.AppKey(
    "websockets",
    weakref.WeakSet,
)

DATABASE: web.AppKey[
    Database
] = web.AppKey(
    "database",
    Database,
)

DISPATCHER: web.AppKey[
    ConversationDispatcher
] = web.AppKey(
    "dispatcher",
    ConversationDispatcher,
)

ACTION_CHANNEL: web.AppKey[
    OneBotActionChannel
] = web.AppKey(
    "action_channel",
    OneBotActionChannel,
)

MESSAGE_PORT: web.AppKey[
    MessagePort
] = web.AppKey(
    "message_port",
    MessagePort,
)

ACCESS_TOKEN: web.AppKey[
    str
] = web.AppKey(
    "access_token",
    str,
)


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s: "
        "%(message)s"
    ),
)

logger = logging.getLogger(
    "qq-agent"
)


async def health_handler(
    request: web.Request,
) -> web.Response:
    dispatcher = request.app.get(
        DISPATCHER
    )

    action_channel = request.app.get(
        ACTION_CHANNEL
    )

    return web.json_response(
        {
            "status": "ok",
            "service": "qq-agent",
            "active_workers": (
                dispatcher.active_worker_count
                if dispatcher is not None
                else 0
            ),
            "onebot_connected": (
                action_channel.connected
                if action_channel is not None
                else False
            ),
        }
    )


async def handle_onebot_event(
    request: web.Request,
    payload: dict,
) -> None:
    logger.info(
        "OneBot event: "
        "post_type=%s "
        "message_id=%s "
        "time=%s",
        payload.get("post_type"),
        payload.get("message_id"),
        payload.get("time"),
    )

    try:
        event = normalize_message_event(
            payload
        )
    except (TypeError, ValueError):
        logger.exception(
            "Malformed OneBot message event: "
            "message_id=%s",
            payload.get("message_id"),
        )
        return

    if event is None:
        return

    database = request.app[
        DATABASE
    ]

    try:
        result = (
            await database
            .ingest_message_event(
                event
            )
        )

    except Exception:
        logger.exception(
            "Failed to ingest OneBot message: "
            "message_id=%s",
            event.external_message_id,
        )
        return

    if not result.accepted:
        logger.warning(
            "Duplicate message dropped: "
            "message_id=%s "
            "dedup_key=%s",
            event.external_message_id,
            event.dedup_key,
        )
        return

    logger.info(
        "Accepted message: "
        "message_id=%s "
        "actor=%s "
        "scope=%s "
        "dedup_key=%s",
        event.external_message_id,
        result.actor_id,
        result.scope_id,
        event.dedup_key,
    )

    dispatcher = request.app[
        DISPATCHER
    ]

    try:
        await dispatcher.enqueue(
            DispatchItem(
                scope_id=result.scope_id,
                actor_id=result.actor_id,
                payload=event,
            )
        )

    except QueueFullError:
        logger.error(
            "Conversation queue full: "
            "message_id=%s "
            "scope=%s "
            "actor=%s",
            event.external_message_id,
            result.scope_id,
            result.actor_id,
        )

        try:
            await (
                database
                .mark_inbound_failed(
                    event.dedup_key
                )
            )
        except Exception:
            logger.exception(
                "Failed to mark "
                "queue-rejected event "
                "as failed: "
                "dedup_key=%s",
                event.dedup_key,
            )

    except RuntimeError:
        logger.exception(
            "Dispatcher rejected message: "
            "message_id=%s",
            event.external_message_id,
        )

        try:
            await (
                database
                .mark_inbound_failed(
                    event.dedup_key
                )
            )
        except Exception:
            logger.exception(
                "Failed to mark rejected "
                "event as failed: "
                "dedup_key=%s",
                event.dedup_key,
            )


def _extract_access_token(
    request: web.Request,
) -> str | None:
    header = request.headers.get(
        "Authorization"
    )

    if header:
        scheme, _, value = header.partition(
            " "
        )

        if scheme.lower() != "bearer":
            return None

        return value.strip() or None

    # OneBot v11 allows the query parameter only where the client
    # cannot set request headers.
    return request.query.get(
        "access_token",
        "",
    ).strip() or None


async def onebot_ws_handler(
    request: web.Request,
) -> web.WebSocketResponse:
    peer = request.remote

    presented = _extract_access_token(
        request
    )

    expected = request.app[
        ACCESS_TOKEN
    ]

    # Both sides are encoded so that a non-ASCII token does not make
    # compare_digest() raise instead of comparing.
    if (
        presented is None
        or not hmac.compare_digest(
            presented.encode("utf-8"),
            expected.encode("utf-8"),
        )
    ):
        logger.warning(
            "Rejected OneBot WebSocket: "
            "missing or invalid access token, "
            "peer=%s",
            peer,
        )

        raise web.HTTPUnauthorized(
            headers={
                "WWW-Authenticate": "Bearer"
            }
        )

    action_channel = request.app[
        ACTION_CHANNEL
    ]

    # NapCat runs exactly one reverse WebSocket client, so a second
    # concurrent connection is never legitimate. Refusing it stops a
    # later connection from taking over the action channel.
    #
    # Cost of this choice: a half-open socket keeps `closed` False
    # until the 30s heartbeat reaps it, so an ungraceful drop can make
    # NapCat's first reconnect attempt fail before one succeeds.
    if action_channel.connected:
        logger.warning(
            "Rejected OneBot WebSocket: "
            "a connection is already active, "
            "peer=%s",
            peer,
        )

        raise web.HTTPConflict()

    ws = web.WebSocketResponse(
        heartbeat=30,
        max_msg_size=(
            4 * 1024 * 1024
        ),
    )

    await ws.prepare(
        request
    )

    request.app[
        WEBSOCKETS
    ].add(ws)

    action_channel.attach(
        ws
    )

    logger.info(
        "OneBot WebSocket connected: "
        "peer=%s",
        peer,
    )

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(
                        msg.data
                    )
                except json.JSONDecodeError:
                    logger.warning(
                        "Invalid JSON received "
                        "from OneBot: peer=%s",
                        peer,
                    )
                    continue

                if not isinstance(
                    payload,
                    dict,
                ):
                    logger.warning(
                        "Unexpected OneBot "
                        "payload type: %s",
                        type(
                            payload
                        ).__name__,
                    )
                    continue

                if action_channel.handle_response(
                    payload
                ):
                    logger.debug(
                        "OneBot action response: "
                        "echo=%s "
                        "status=%s "
                        "retcode=%s",
                        payload.get("echo"),
                        payload.get("status"),
                        payload.get("retcode"),
                    )

                    continue

                await handle_onebot_event(
                    request,
                    payload,
                )

            elif (
                msg.type
                == WSMsgType.ERROR
            ):
                logger.error(
                    "OneBot WebSocket error: "
                    "peer=%s error=%s",
                    peer,
                    ws.exception(),
                )

    finally:
        action_channel.detach(
            ws
        )

        request.app[
            WEBSOCKETS
        ].discard(ws)

        logger.warning(
            "OneBot WebSocket disconnected: "
            "peer=%s",
            peer,
        )

    return ws


async def start_services(
    app: web.Application,
) -> None:
    database = Database()

    await database.start()

    action_channel = (
        OneBotActionChannel(
            response_timeout=10.0
        )
    )

    message_port = MessagePort(
        action_channel
    )

    pipeline = MessagePipeline(
        database,
        message_port,
    )

    dispatcher = (
        ConversationDispatcher(
            pipeline,
            queue_maxsize=32,
            worker_idle_timeout=60.0,
        )
    )

    app[DATABASE] = database
    app[ACTION_CHANNEL] = (
        action_channel
    )
    app[MESSAGE_PORT] = (
        message_port
    )
    app[DISPATCHER] = dispatcher

    logger.info(
        "Database started"
    )

    logger.info(
        "Dispatcher started: "
        "queue_maxsize=%d",
        dispatcher.queue_maxsize,
    )


async def shutdown_websockets(
    app: web.Application,
) -> None:
    websockets = list(
        app[WEBSOCKETS]
    )

    if websockets:
        logger.info(
            "Closing %d active "
            "WebSocket connection(s)",
            len(websockets),
        )

    for ws in websockets:
        if ws.closed:
            continue

        try:
            await ws.close(
                code=(
                    WSCloseCode.GOING_AWAY
                ),
                message=b"Server shutdown",
            )

        except Exception:
            logger.exception(
                "Failed to close "
                "WebSocket cleanly"
            )


async def cleanup_services(
    app: web.Application,
) -> None:
    dispatcher = app.get(
        DISPATCHER
    )

    if dispatcher is not None:
        logger.info(
            "Closing dispatcher"
        )

        await dispatcher.close()

        logger.info(
            "Dispatcher closed"
        )

    database = app.get(
        DATABASE
    )

    if database is not None:
        await database.close()

        logger.info(
            "Database closed"
        )


def create_app() -> web.Application:
    access_token = (
        load_onebot_access_token()
    )

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    app = web.Application()

    app[ACCESS_TOKEN] = access_token

    app[WEBSOCKETS] = (
        weakref.WeakSet()
    )

    app.router.add_get(
        "/health",
        health_handler,
    )

    app.router.add_get(
        "/onebot/v11",
        onebot_ws_handler,
    )

    app.on_startup.append(
        start_services
    )

    app.on_shutdown.append(
        shutdown_websockets
    )

    app.on_cleanup.append(
        cleanup_services
    )

    return app


def main() -> None:
    migrate()

    web.run_app(
        create_app(),
        host="0.0.0.0",
        port=8080,
    )


if __name__ == "__main__":
    main()
