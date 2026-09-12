from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from app.config.settings import (
    DATABASE_PATH,
    MIGRATIONS_DIR,
)




logger = logging.getLogger("qq-agent.migrate")


def configure_connection(db: sqlite3.Connection) -> None:
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA foreign_keys=ON;")
    db.execute("PRAGMA busy_timeout=5000;")


def migrate(database_path: Path = DATABASE_PATH) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(database_path)

    try:
        configure_connection(db)

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at INTEGER NOT NULL
            )
            """
        )
        db.commit()

        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = path.name

            already_applied = db.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (version,),
            ).fetchone()

            if already_applied:
                continue

            logger.info("Applying %s", version)

            sql = path.read_text(encoding="utf-8")
            safe_version = version.replace("'", "''")

            script = f"""
            BEGIN IMMEDIATE;

            {sql}

            INSERT INTO schema_migrations(version, applied_at)
            VALUES ('{safe_version}', unixepoch());

            COMMIT;
            """

            try:
                db.executescript(script)
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise

        logger.info("Database ready: %s", database_path)

    finally:
        db.close()


if __name__ == "__main__":
    migrate()
