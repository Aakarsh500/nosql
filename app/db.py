"""Connections to the three stores plus their schema: Mongo validators and indexes,
Neo4j constraints. Redis is schemaless; its key-space is defined in sessions_carts.py."""
from __future__ import annotations

import logging

import redis
from neo4j import GraphDatabase
from pymongo import ASCENDING, DESCENDING, TEXT, MongoClient
from pymongo.errors import CollectionInvalid

from . import config

log = logging.getLogger(__name__)
logging.getLogger("neo4j").setLevel(logging.WARNING)  # hide "constraint already exists" notices

_INT = ["int", "long"]

PRODUCT_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "sku", "slug", "name", "category", "price", "status", "attributes",
                 "schema_version", "created_at", "updated_at"],
    "properties": {
        "_id": {"bsonType": "string", "pattern": "^p_[A-Za-z0-9_-]{2,32}$"},
        "sku": {"bsonType": "string", "minLength": 2},
        "slug": {"bsonType": "string", "minLength": 2},
        "name": {"bsonType": "string", "minLength": 2},
        "description": {"bsonType": "string"},
        "category": {"bsonType": "object", "required": ["id", "name"],
                     "properties": {"id": {"bsonType": "string"}, "name": {"bsonType": "string"}}},
        "price": {"bsonType": "object", "required": ["amount", "currency"],
                  "properties": {"amount": {"bsonType": _INT, "minimum": 0},
                                 "currency": {"enum": ["INR"]}}},
        "status": {"enum": ["ACTIVE", "INACTIVE"]},
        "attributes": {"bsonType": "object"},
        "variants": {"bsonType": "array"},
        "media": {"bsonType": "array"},
        "tags": {"bsonType": "array", "items": {"bsonType": "string"}},
        "schema_version": {"bsonType": _INT, "minimum": 1},
        "created_at": {"bsonType": "date"},
        "updated_at": {"bsonType": "date"},
    },
}

ORDER_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "customer_id", "idempotency_key", "status", "line_items", "total_amount",
                 "currency", "created_at"],
    "properties": {
        "_id": {"bsonType": "string"},
        "customer_id": {"bsonType": "string"},
        "idempotency_key": {"bsonType": "string"},
        "status": {"enum": ["PLACED", "CANCELLED"]},
        "line_items": {
            "bsonType": "array", "minItems": 1,
            "items": {"bsonType": "object",
                      "required": ["product_id", "sku", "name_snapshot", "unit_price", "quantity", "line_total"],
                      "properties": {"quantity": {"bsonType": _INT, "minimum": 1},
                                     "unit_price": {"bsonType": _INT, "minimum": 0}}},
        },
        "total_amount": {"bsonType": _INT, "minimum": 0},
        "currency": {"enum": ["INR"]},
        "created_at": {"bsonType": "date"},
    },
}

OUTBOX_SCHEMA = {
    "bsonType": "object",
    "required": ["_id", "event_id", "type", "aggregate_id", "payload", "status", "attempts", "created_at"],
    "properties": {
        "type": {"enum": ["ORDER_PLACED", "PRODUCT_UPSERTED"]},
        "status": {"enum": ["PENDING", "PROCESSING", "PROCESSED", "DEAD"]},
        "attempts": {"bsonType": _INT, "minimum": 0},
        "payload": {"bsonType": "object"},
        "created_at": {"bsonType": "date"},
    },
}

GRAPH_SCHEMA = [
    "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customerId IS UNIQUE",
    "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.productId IS UNIQUE",
    "CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.categoryId IS UNIQUE",
    "CREATE INDEX purchased_event IF NOT EXISTS FOR ()-[r:PURCHASED]-() ON (r.eventId)",
]


class Stores:
    """Owns one client per database. Business code never builds its own connections."""

    def __init__(self):
        self.redis = redis.Redis.from_url(config.REDIS_URL, decode_responses=True,
                                          socket_connect_timeout=2, socket_timeout=3)
        self.mongo_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
        self.mongo = self.mongo_client[config.MONGO_DB]
        self.graph = GraphDatabase.driver(
            config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
            connection_timeout=3, connection_acquisition_timeout=5, max_transaction_retry_time=2)
        self.graph_db = config.NEO4J_DATABASE
        self.graph_schema_ready = False

    # ---------- schema ----------
    def ensure_schema(self) -> None:
        self.ensure_mongo_schema()
        self.ensure_graph_schema(raise_errors=False)

    def _ensure_collection(self, name: str, schema: dict) -> None:
        try:
            self.mongo.create_collection(name, validator={"$jsonSchema": schema},
                                         validationLevel="strict", validationAction="error")
        except CollectionInvalid:
            self.mongo.command("collMod", name, validator={"$jsonSchema": schema},
                               validationLevel="strict", validationAction="error")

    def ensure_mongo_schema(self) -> None:
        self._ensure_collection("products", PRODUCT_SCHEMA)
        self._ensure_collection("orders", ORDER_SCHEMA)
        self._ensure_collection("outbox", OUTBOX_SCHEMA)
        p, o, e = self.mongo.products, self.mongo.orders, self.mongo.outbox
        p.create_index("sku", unique=True, name="sku_unique")
        p.create_index("slug", unique=True, name="slug_unique")
        p.create_index([("category.id", ASCENDING), ("price.amount", ASCENDING)], name="category_price")
        p.create_index([("name", TEXT), ("description", TEXT), ("tags", TEXT)],
                       weights={"name": 10, "tags": 5, "description": 1}, name="product_text")
        o.create_index("idempotency_key", unique=True, name="idempotency_key_unique")
        o.create_index([("customer_id", ASCENDING), ("created_at", DESCENDING)], name="customer_history")
        e.create_index("event_id", unique=True, name="event_id_unique")
        e.create_index([("status", ASCENDING), ("next_attempt_at", ASCENDING)], name="status_next_attempt")
        e.create_index([("status", ASCENDING), ("created_at", ASCENDING)], name="status_created")
        e.create_index("order_id", name="order_lookup", sparse=True)
        e.create_index([("status", ASCENDING), ("processed_at", DESCENDING)], name="status_processed")

    def ensure_graph_schema(self, raise_errors: bool = True) -> bool:
        if self.graph_schema_ready:
            return True
        try:
            for query in GRAPH_SCHEMA:
                self.graph.execute_query(query, database_=self.graph_db)
            self.graph_schema_ready = True
        except Exception as exc:
            if raise_errors:
                raise
            log.warning("Neo4j schema not ready yet: %s", exc)
        return self.graph_schema_ready

    def close(self) -> None:
        self.graph.close()
        self.mongo_client.close()
        self.redis.close()


_stores: Stores | None = None


def get_stores() -> Stores:
    global _stores
    if _stores is None:
        _stores = Stores()
    return _stores
