"""Durable orders in MongoDB: the checkout source of truth."""
from __future__ import annotations

from pymongo import DESCENDING
from pymongo.errors import DuplicateKeyError

from .serialize import from_mongo
from .trace import op


class OrderStore:
    def __init__(self, stores):
        self.client = stores.mongo_client
        self.col = stores.mongo.orders
        self.outbox = stores.mongo.outbox

    def get(self, order_id: str) -> dict | None:
        with op("mongo", "orders.findOne", f"_id={order_id}"):
            return from_mongo(self.col.find_one({"_id": order_id}))

    def get_by_key(self, key: str) -> dict | None:
        with op("mongo", "orders.findOne by idempotency_key", key):
            return from_mongo(self.col.find_one({"idempotency_key": key}))

    def list_for_customer(self, customer_id: str, limit: int = 20) -> list[dict]:
        with op("mongo", "orders.find by customer (customer_history index)", customer_id):
            return [from_mongo(o) for o in self.col.find({"customer_id": customer_id})
                    .sort("created_at", DESCENDING).limit(limit)]

    def create_with_outbox(self, order: dict, event: dict) -> tuple[dict, bool]:
        """Insert the order and its ORDER_PLACED event atomically. A duplicate idempotency
        key returns the order that already exists instead of creating a second one."""
        def write(session):
            self.col.insert_one(order, session=session)
            self.outbox.insert_one(event, session=session)

        try:
            with op("mongo", "txn: orders.insertOne + outbox.insertOne", order["_id"]):
                with self.client.start_session() as session:
                    # with_transaction retries transient write conflicts (e.g. concurrent same key)
                    session.with_transaction(write)
            return from_mongo(order), True
        except DuplicateKeyError:
            existing = self.get_by_key(order["idempotency_key"])
            if existing:
                return existing, False
            raise

    def co_purchase_counts(self, product_id: str, limit: int = 10) -> list[dict]:
        """The same 'bought together by distinct customers' answer computed in MongoDB.
        Used as a correctness oracle and to contrast the aggregation with the Cypher query."""
        pipeline = [
            {"$match": {"status": "PLACED"}},
            {"$unwind": "$line_items"},
            {"$group": {"_id": "$customer_id", "products": {"$addToSet": "$line_items.product_id"}}},
            {"$match": {"products": product_id}},
            {"$unwind": "$products"},
            {"$match": {"products": {"$ne": product_id}}},
            {"$group": {"_id": "$products", "shared_buyers": {"$sum": 1}}},
            {"$sort": {"shared_buyers": -1, "_id": 1}},
            {"$limit": limit},
        ]
        with op("mongo", "orders.aggregate co-purchase", product_id):
            return [{"product_id": d["_id"], "shared_buyers": d["shared_buyers"]}
                    for d in self.col.aggregate(pipeline, allowDiskUse=True)]
