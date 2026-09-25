"""Print one live PumpPortal subscribeMigration message and exit.

Uses the same socket helpers as the create-event inspector. The official
data API sends subscribeMigration on the same connection as other free
methods; this script opens one socket and sends only that method so the
frame is a migration, not a create. Nothing is parsed.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import inspect_pumpportal_schema as portal

WAIT_SECONDS = 15 * 60
READ_TIMEOUT_SECONDS = 90


def main() -> None:
    api_key = os.environ.get("PUMPPORTAL_API_KEY", "").strip()
    path = portal.PATH_WITH_KEY.format(key=api_key) if api_key else portal.PATH_WITHOUT_KEY
    print(f"connecting path={path.split('api-key=')[0]}", file=sys.stderr)
    deadline = time.monotonic() + WAIT_SECONDS
    sock = portal._connect(path)
    portal.TIMEOUT_SECONDS = READ_TIMEOUT_SECONDS
    try:
        _send = portal._send_text
        _send(sock, json.dumps({"method": "subscribeMigration"}))
        while time.monotonic() < deadline:
            try:
                message = portal._read_text(sock)
            except TimeoutError:
                print("still waiting for a migration frame", file=sys.stderr)
                continue
            print(message, flush=True)
            # The ack is {"message":"Subscribed to 'migration' events."} and
            # contains the word migration. A data frame has a mint.
            if '"mint"' in message:
                return
    finally:
        sock.close()
    raise SystemExit("no migration frame before the wait ended")


if __name__ == "__main__":
    main()
