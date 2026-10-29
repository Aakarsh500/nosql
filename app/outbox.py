"""MongoDB transactional outbox: events written in the same transaction as the business
change, then claimed by the projector with a lease, retried with backoff, and dead-lettered
after a bounded number of attempts (Review 1, section 06)."""
from __future__ import annotations

import secrets
from datetime import timedelta

from pymongo import ASCENDING, DESCENDING, ReturnDocument

from . import config
from .serialize import from_mongo, utcnow
from .trace import op


def new_event(event_type: str, aggregate_id: str, payload: dict, order_id: str | None = None) -> dict:
    event_id = "evt_" + secrets.token_hex(6)
    now = utcnow()
    event = {"_id": event_id, "event_id": event_id, "type": event_type, "aggregate_id": aggregate_id,
             "payload": payload, "status": "PENDING", "attempts": 0, "next_attempt_at": now,
             "last_error": None, "created_at": now, "processed_at": None}
    if order_id:
        event["order_id"] = order_id
    return event


def _eligible(now):
    return {"$or": [{"status": "PENDING", "next_attempt_at": {"$lte": now}},
                    {"status": "PROCESSING", "lease_until": {"$lt": now}}]}


class Outbox:
    def __init__(self, stores):
        self.col = stores.mongo.outbox
        self.state = stores.mongo.projector_state

    def claim(self, batch: int) -> list[dict]:
        """Lease up to `batch` eligible events. Safe if several workers run concurrently."""
        now = utcnow()
        with op("mongo", "outbox.find eligible", f"limit {batch}"):
            ids = [d["_id"] for d in self.col.find(_eligible(now), {"_id": 1})
                   .sort("created_at", ASCENDING).limit(batch)]
        if not ids:
            return []
        owner = "lease_" + secrets.token_hex(6)
        with op("mongo", "outbox.updateMany -> PROCESSING", f"{len(ids)} events"):
            self.col.update_many({"_id": {"$in": ids}, **_eligible(now)},
                                 {"$set": {"status": "PROCESSING", "lease_owner": owner,
                                           "lease_until": now + timedelta(seconds=config.OUTBOX_LEASE_SECONDS)}})
            return list(self.col.find({"lease_owner": owner, "status": "PROCESSING"}).sort("created_at", ASCENDING))

    def mark_processed(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        with op("mongo", "outbox.updateMany -> PROCESSED", f"{len(event_ids)} events"):
            self.col.update_many({"_id": {"$in": event_ids}},
                                 {"$set": {"status": "PROCESSED", "processed_at": utcnow(), "last_error": None},
                                  "$unset": {"lease_owner": "", "lease_until": ""}})

    def mark_failed(self, event_id: str, error: Exception) -> str:
        with op("mongo", "outbox.findOneAndUpdate attempts+1", event_id):
            doc = self.col.find_one_and_update(
                {"_id": event_id},
                {"$inc": {"attempts": 1}, "$set": {"last_error": f"{type(error).__name__}: {error}"[:300]},
                 "$unset": {"lease_owner": "", "lease_until": ""}},
                return_document=ReturnDocument.AFTER)
        if doc is None:
            return "MISSING"
        attempts = doc["attempts"]
        if attempts >= config.OUTBOX_MAX_ATTEMPTS:
            status, update = "DEAD", {"status": "DEAD"}
        else:
            delay = min(2 ** attempts, config.OUTBOX_MAX_BACKOFF)
            status, update = "PENDING", {"status": "PENDING",
                                         "next_attempt_at": utcnow() + timedelta(seconds=delay)}
        with op("mongo", f"outbox.updateOne -> {status}", event_id):
            self.col.update_one({"_id": event_id}, {"$set": update})
        return status

    def retry(self, event_id: str | None = None) -> int:
        """Put a dead (or any non-processed) event back in the queue. None means all DEAD events."""
        query = {"_id": event_id, "status": {"$ne": "PROCESSED"}} if event_id else {"status": "DEAD"}
        with op("mongo", "outbox.updateMany -> PENDING (manual retry)", event_id or "all DEAD"):
            result = self.col.update_many(query, {"$set": {"status": "PENDING", "attempts": 0,
                                                           "next_attempt_at": utcnow()}})
        return result.modified_count

    def list(self, status: str | None = None, limit: int = 50) -> list[dict]:
        query = {"status": status} if status else {}
        with op("mongo", "outbox.find", status or "all"):
            return [from_mongo(e) for e in self.col.find(query).sort("created_at", DESCENDING).limit(limit)]

    def get(self, event_id: str) -> dict | None:
        return from_mongo(self.col.find_one({"_id": event_id}))

    def status_for_orders(self, order_ids: list[str]) -> dict[str, dict]:
        if not order_ids:
            return {}
        with op("mongo", "outbox.find by order_id", f"{len(order_ids)} orders"):
            return {e["order_id"]: {"event_id": e["_id"], "status": e["status"], "attempts": e["attempts"],
                                    "processed_at": e.get("processed_at")}
                    for e in self.col.find({"order_id": {"$in": order_ids}},
                                           {"order_id": 1, "status": 1, "attempts": 1, "processed_at": 1})}

    def summary(self) -> dict:
        now = utcnow()
        with op("mongo", "outbox.aggregate status counts"):
            counts = {d["_id"]: d["n"] for d in self.col.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}])}
        with op("mongo", "outbox.find oldest pending"):
            oldest = self.col.find_one({"status": {"$in": ["PENDING", "PROCESSING"]}},
                                       sort=[("created_at", ASCENDING)])
        with op("mongo", "outbox.find recent processed"):
            recent = list(self.col.find({"status": "PROCESSED", "processed_at": {"$ne": None}},
                                        {"created_at": 1, "processed_at": 1})
                          .sort("processed_at", DESCENDING).limit(100))
        lags = sorted((e["processed_at"] - e["created_at"]).total_seconds() for e in recent)
        oldest_age = (now - oldest["created_at"]).total_seconds() if oldest else 0.0
        state = self.state.find_one({"_id": "projector"})
        heartbeat_age = (now - state["heartbeat_at"]).total_seconds() if state and state.get("heartbeat_at") else None
        return {
            "counts": {s: counts.get(s, 0) for s in ("PENDING", "PROCESSING", "PROCESSED", "DEAD")},
            "backlog": counts.get("PENDING", 0) + counts.get("PROCESSING", 0),
            "oldest_pending_age_s": round(oldest_age, 2),
            "recent_lag_avg_s": round(sum(lags) / len(lags), 3) if lags else None,
            "recent_lag_p95_s": round(lags[int(0.95 * (len(lags) - 1))], 3) if lags else None,
            "lag_target_s": config.PROJECTION_LAG_TARGET,
            "updating": oldest_age > config.PROJECTION_LAG_TARGET,
            "worker": {**from_mongo(state), "heartbeat_age_s": round(heartbeat_age, 1)} if state else None,
        }

    def heartbeat(self, **fields) -> None:
        self.state.update_one({"_id": "projector"}, {"$set": {"heartbeat_at": utcnow(), **fields}}, upsert=True)

    def add_totals(self, processed: int, failed: int) -> None:
        if processed or failed:
            self.state.update_one({"_id": "projector"},
                                  {"$inc": {"processed_total": processed, "failed_total": failed}}, upsert=True)
