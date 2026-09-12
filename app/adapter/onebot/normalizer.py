from __future__ import annotations

import hashlib
from typing import Any

from .models import NormalizedMessageEvent


def _require(payload: dict[str, Any], key: str) -> Any:
    value = payload.get(key)

    if value is None:
        raise ValueError(f"OneBot message event missing {key!r}")

    return value


def _make_dedup_key(
    *,
    self_id: str,
    message_type: str,
    scope_external_id: str,
    actor_external_id: str,
    external_message_id: str,
) -> str:
    identity = "\x1f".join(
        (
            "qq",
            self_id,
            message_type,
            scope_external_id,
            actor_external_id,
            external_message_id,
        )
    )

    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def normalize_message_event(
    payload: dict[str, Any],
) -> NormalizedMessageEvent | None:
    if payload.get("post_type") != "message":
        return None

    message_type = str(_require(payload, "message_type"))

    if message_type not in {"private", "group"}:
        raise ValueError(
            f"Unsupported OneBot message_type: {message_type!r}"
        )

    self_id = str(_require(payload, "self_id"))
    actor_external_id = str(_require(payload, "user_id"))
    external_message_id = str(_require(payload, "message_id"))
    occurred_at = int(_require(payload, "time"))

    if message_type == "private":
        scope_type = "private"
        scope_external_id = actor_external_id
    else:
        scope_type = "group"
        scope_external_id = str(_require(payload, "group_id"))

    raw_message = payload.get("raw_message")

    if raw_message is None:
        text = ""
    else:
        text = str(raw_message)

    raw_segments = payload.get("message")

    if isinstance(raw_segments, list):
        segments = raw_segments
    else:
        segments = []

    dedup_key = _make_dedup_key(
        self_id=self_id,
        message_type=message_type,
        scope_external_id=scope_external_id,
        actor_external_id=actor_external_id,
        external_message_id=external_message_id,
    )

    return NormalizedMessageEvent(
        platform="qq",
        self_id=self_id,
        actor_external_id=actor_external_id,
        scope_type=scope_type,
        scope_external_id=scope_external_id,
        external_message_id=external_message_id,
        occurred_at=occurred_at,
        text=text,
        segments=segments,
        dedup_key=dedup_key,
        raw_payload=payload,
    )
