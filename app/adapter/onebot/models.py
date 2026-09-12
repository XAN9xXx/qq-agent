from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


ScopeType = Literal["private", "group"]


@dataclass(frozen=True, slots=True)
class NormalizedMessageEvent:
    platform: str

    self_id: str
    actor_external_id: str

    scope_type: ScopeType
    scope_external_id: str

    external_message_id: str

    occurred_at: int

    text: str
    segments: list[dict[str, Any]]

    dedup_key: str

    raw_payload: dict[str, Any]
