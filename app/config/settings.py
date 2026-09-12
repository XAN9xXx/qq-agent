from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_PATH = DATA_DIR / "agent.db"
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"


ACCESS_TOKEN_ENV = "ONEBOT_ACCESS_TOKEN"


def load_onebot_access_token() -> str:
    """Return the configured OneBot access token, or fail closed.

    Refusing to start when the variable is absent is deliberate: a
    missing token must never degrade silently into an unauthenticated
    WebSocket endpoint, because that failure has no symptom.
    """
    token = os.environ.get(ACCESS_TOKEN_ENV, "").strip()

    if not token:
        raise RuntimeError(
            f"{ACCESS_TOKEN_ENV} is not set. Set it to the same access "
            "token configured in NapCat's reverse WebSocket client "
            "before starting the agent."
        )

    return token
