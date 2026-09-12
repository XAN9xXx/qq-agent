from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from app.adapter.onebot.normalizer import normalize_message_event
from app.config.settings import PROJECT_ROOT
from app.store.database import Database
from app.store.migrate import migrate
from tests.crash_worker import PAYLOAD
from tests.support import open_database


@unittest.skipUnless(
    os.name == "posix",
    "SIGKILL crash test requires POSIX",
)
class CrashRecoveryProcessTest(
    unittest.IsolatedAsyncioTestCase
):
    async def test_processing_event_becomes_abandoned_after_sigkill(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = (
                Path(temp_dir)
                / "agent-crash-test.db"
            )

            migrate(database_path)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.crash_worker",
                    str(database_path),
                ],
                cwd=PROJECT_ROOT,
                check=False,
            )

            # On POSIX, subprocess returncode is negative
            # when terminated by a signal.
            self.assertEqual(
                result.returncode,
                -9,
            )

            event = normalize_message_event(
                PAYLOAD
            )

            self.assertIsNotNone(
                event
            )

            assert event is not None

            with open_database(
                database_path
            ) as db:
                state_before_restart = db.execute(
                    """
                    SELECT state
                    FROM inbound_events
                    WHERE dedup_key = ?
                    """,
                    (
                        event.dedup_key,
                    ),
                ).fetchone()[0]

            self.assertEqual(
                state_before_restart,
                "processing",
            )

            database = Database(
                database_path
            )

            await database.start()

            await database.close()

            with open_database(
                database_path
            ) as db:
                state_after_restart = db.execute(
                    """
                    SELECT state
                    FROM inbound_events
                    WHERE dedup_key = ?
                    """,
                    (
                        event.dedup_key,
                    ),
                ).fetchone()[0]

                message_count = db.execute(
                    """
                    SELECT COUNT(*)
                    FROM messages
                    WHERE source_event_key = ?
                    """,
                    (
                        event.dedup_key,
                    ),
                ).fetchone()[0]

            self.assertEqual(
                state_after_restart,
                "abandoned",
            )

            self.assertEqual(
                message_count,
                0,
            )


if __name__ == "__main__":
    unittest.main()
