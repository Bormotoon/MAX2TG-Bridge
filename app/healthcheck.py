"""Out-of-process readiness probe, for Docker's HEALTHCHECK.

"The process is running" was the only signal the container produced, and it
stays true while the bridge sits in an unauthorised reconnect loop with a
growing backlog (audit O03). The bridge writes a snapshot to the state
directory; this reads it and answers three questions:

* is the snapshot fresh (is the event loop even turning)?
* did MAX actually authorise us?
* is the oldest undelivered message younger than the backlog threshold?

Exit code 0 means ready, 1 means not. It prints one line either way so
``docker inspect`` shows a reason rather than a bare failure.
"""

from __future__ import annotations

import json
import os
import sys
import time

HEALTH_FILENAME = "health.json"

# How stale a snapshot may be before we conclude nothing is writing it.
MAX_SNAPSHOT_AGE_SEC = 120.0
# Backlog age that means delivery has stopped even though the link looks fine.
MAX_BACKLOG_AGE_SEC = 900.0


def health_path(state_dir: str | None = None) -> str:
    directory = state_dir or os.environ.get("STATE_DIR") or "state"
    return os.path.join(directory, HEALTH_FILENAME)


def write_snapshot(path: str, snapshot: dict) -> None:
    """Atomically publish the current snapshot for the probe to read."""
    payload = dict(snapshot)
    payload["written_at"] = time.time()
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, default=str)
    os.replace(tmp, path)


def evaluate(snapshot: dict, now: float | None = None) -> tuple[bool, str]:
    """Decide readiness from a snapshot. Pure, so it can be tested directly."""
    now = now if now is not None else time.time()
    written_at = snapshot.get("written_at")
    if not isinstance(written_at, (int, float)):
        return False, "snapshot has no timestamp"
    age = now - float(written_at)
    if age > MAX_SNAPSHOT_AGE_SEC:
        return False, f"snapshot is {age:.0f}s old — the event loop may be stuck"
    if not snapshot.get("ready"):
        state = snapshot.get("state", "unknown")
        return False, f"MAX link is {state}, not ready"
    backlog = snapshot.get("oldest_pending_age")
    if isinstance(backlog, (int, float)) and backlog > MAX_BACKLOG_AGE_SEC:
        return False, f"oldest undelivered message is {backlog:.0f}s old"
    return True, f"ready ({snapshot.get('state')})"


def main() -> int:
    path = health_path()
    try:
        with open(path, encoding="utf-8") as handle:
            snapshot = json.load(handle)
    except FileNotFoundError:
        print(f"no health snapshot at {path} yet", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"cannot read {path}: {exc}", file=sys.stderr)
        return 1

    ok, reason = evaluate(snapshot)
    print(reason, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
