from __future__ import annotations

import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_PATH = DATA_DIR / "agent.db"


def configure_connection(db: sqlite3.Connection) -> None:
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA foreign_keys=ON;")
    db.execute("PRAGMA busy_timeout=5000;")


def migrate() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(DATABASE_PATH)

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

            print(f"Applying {version}")

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

        print(f"Database ready: {DATABASE_PATH}")

    finally:
        db.close()


if __name__ == "__main__":
    migrate()
