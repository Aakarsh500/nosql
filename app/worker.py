"""Background projector: claims outbox events from MongoDB and MERGEs them into Neo4j.

If Neo4j is down, events stay PENDING with exponential backoff (2, 4, 8 ... 30 s) and are
dead-lettered after OUTBOX_MAX_ATTEMPTS failures. When Neo4j returns, the backlog drains."""
import logging
import signal
import socket
import time

from . import config
from .db import get_stores
from .services import Commerce

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s worker %(message)s")
log = logging.getLogger("worker")
_running = True


def _stop(*_):
    global _running
    _running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    stores = get_stores()
    stores.ensure_mongo_schema()
    service = Commerce(stores)
    worker_id = socket.gethostname()
    log.info("projector started (batch=%s, max attempts=%s)", config.OUTBOX_BATCH, config.OUTBOX_MAX_ATTEMPTS)
    while _running:
        result = {"claimed": 0, "processed": 0, "failed": 0, "error": None}
        try:
            result = service.projector.run_once()
            if result["claimed"]:
                log.info("batch %s", result)
        except Exception as exc:  # MongoDB itself unavailable: keep the loop alive
            result["error"] = f"{type(exc).__name__}: {exc}"
            log.warning("projector loop error: %s", result["error"])
        try:
            service.outbox.heartbeat(worker_id=worker_id, last_batch=result,
                                     last_error=result.get("error"))
        except Exception:
            pass
        if not result["claimed"]:
            time.sleep(config.WORKER_INTERVAL)
    log.info("projector stopped")
    stores.close()


if __name__ == "__main__":
    main()
