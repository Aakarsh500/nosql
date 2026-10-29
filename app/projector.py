"""Outbox -> Neo4j projector. Idempotent: replaying an event MERGEs the same facts."""
from __future__ import annotations

import logging

from neo4j.exceptions import ServiceUnavailable, SessionExpired

from . import config

log = logging.getLogger(__name__)

GRAPH_DOWN = (ServiceUnavailable, SessionExpired, ConnectionError, OSError)


class Projector:
    def __init__(self, outbox, graph, stores):
        self.outbox = outbox
        self.graph = graph
        self.stores = stores

    def _project(self, events: list[dict]) -> None:
        orders, products = [], []
        for event in events:
            if event["type"] == "ORDER_PLACED":
                orders.append({**event["payload"], "eventId": event["event_id"]})
            elif event["type"] == "PRODUCT_UPSERTED":
                products.append(event["payload"])
            else:
                raise ValueError(f"unknown event type {event['type']!r}")
        # Product facts first so a brand-new product gets its category before purchases reference it.
        self.graph.project_products(products)
        self.graph.project_orders(orders)

    def run_once(self, batch: int | None = None) -> dict:
        self.stores.ensure_graph_schema(raise_errors=False)
        events = self.outbox.claim(batch or config.OUTBOX_BATCH)
        result = {"claimed": len(events), "processed": 0, "failed": 0, "dead": 0, "error": None}
        if not events:
            return result
        try:
            self._project(events)
            self.outbox.mark_processed([e["_id"] for e in events])
            result["processed"] = len(events)
        except GRAPH_DOWN as exc:
            # Neo4j is unreachable: every claimed event goes back to PENDING with backoff.
            result["error"] = f"{type(exc).__name__}: {exc}"
            for event in events:
                status = self.outbox.mark_failed(event["_id"], exc)
                result["failed"] += 1
                result["dead"] += status == "DEAD"
        except Exception as exc:
            # A bad event must not block good ones: retry one by one to isolate the poison event.
            result["error"] = f"{type(exc).__name__}: {exc}"
            log.warning("batch projection failed, isolating events: %s", exc)
            for event in events:
                try:
                    self._project([event])
                    self.outbox.mark_processed([event["_id"]])
                    result["processed"] += 1
                except Exception as single_exc:
                    status = self.outbox.mark_failed(event["_id"], single_exc)
                    result["failed"] += 1
                    result["dead"] += status == "DEAD"
        self.outbox.add_totals(result["processed"], result["failed"])
        return result

    def drain(self, batch: int = 1000, max_rounds: int = 10_000) -> dict:
        total = {"processed": 0, "failed": 0}
        for _ in range(max_rounds):
            result = self.run_once(batch)
            total["processed"] += result["processed"]
            total["failed"] += result["failed"]
            if result["claimed"] == 0 or result["processed"] == 0:
                break
        return total
