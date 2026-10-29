"""Read-only database explorer used to show each store's real contents during the review.

SCAN is used here, never KEYS, and only for diagnostics (Review 1, section 07)."""
from __future__ import annotations

import shlex
import time

from pymongo import ASCENDING, DESCENDING

from .serialize import from_mongo, jsonable
from .trace import op

MONGO_COLLECTIONS = ("products", "orders", "outbox", "projector_state")
REDIS_READ_COMMANDS = {"GET", "HGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "TTL", "PTTL", "TYPE", "EXISTS",
                       "SCAN", "DBSIZE", "INFO", "STRLEN", "MEMORY", "OBJECT"}
FORBIDDEN_MONGO_OPERATORS = {"$where", "$function", "$accumulator"}

MONGO_PRESETS = {
    "sku_lookup": {"title": "Product by SKU (unique index)", "collection": "products",
                   "filter": {"sku": "SHOE-TRAIL-42"}},
    "category_price": {"title": "Category + price range (compound index)", "collection": "products",
                       "filter": {"category.id": "footwear", "price.amount": {"$gte": 1000, "$lte": 6000}},
                       "sort": [["price.amount", 1]]},
    "text_search": {"title": "Full-text search on name/tags/description", "collection": "products",
                    "filter": {"$text": {"$search": "waterproof"}}},
    "unindexed_attribute": {"title": "Attribute filter without an index (COLLSCAN contrast)",
                            "collection": "products", "filter": {"attributes.waterproof": True}},
    "customer_history": {"title": "Customer order history (customer + date index)", "collection": "orders",
                         "filter": {"customer_id": "c_0001"}, "sort": [["created_at", -1]]},
    "pending_outbox": {"title": "Pending outbox events for the projector", "collection": "outbox",
                       "filter": {"status": "PENDING"}, "sort": [["next_attempt_at", 1]]},
}


class Explorer:
    def __init__(self, stores, commerce):
        self.stores = stores
        self.r = stores.redis
        self.db = stores.mongo
        self.commerce = commerce

    # ------------------------------------------------------------------ Redis
    def redis_overview(self, pattern: str = "*", limit: int = 200) -> dict:
        keys = []
        with op("redis", "SCAN MATCH", pattern):
            for key in self.r.scan_iter(match=pattern or "*", count=500):
                keys.append(key)
                if len(keys) >= limit:
                    break
        keys.sort()
        details = []
        if keys:
            with op("redis", "pipeline TYPE + TTL", f"{len(keys)} keys"):
                pipe = self.r.pipeline(transaction=False)
                for key in keys:
                    pipe.type(key)
                    pipe.ttl(key)
                raw = pipe.execute()
            for i, key in enumerate(keys):
                details.append({"key": key, "type": raw[2 * i], "ttl": raw[2 * i + 1],
                                "pattern": key.split(":")[0] + (":idem" if key.startswith("checkout:idem") else "")})
        with op("redis", "INFO + DBSIZE"):
            info = self.r.info()
            dbsize = self.r.dbsize()
        by_pattern: dict[str, int] = {}
        for d in details:
            by_pattern[d["pattern"]] = by_pattern.get(d["pattern"], 0) + 1
        return {"keys": details, "dbsize": dbsize, "truncated": len(keys) >= limit, "by_pattern": by_pattern,
                "server": {"redis_version": info.get("redis_version"), "used_memory_human": info.get("used_memory_human"),
                           "uptime_in_seconds": info.get("uptime_in_seconds"),
                           "aof_enabled": info.get("aof_enabled"), "connected_clients": info.get("connected_clients")}}

    def redis_key(self, key: str) -> dict:
        with op("redis", "TYPE + TTL", key):
            kind = self.r.type(key)
            ttl = self.r.ttl(key)
        if kind == "none":
            return {"key": key, "exists": False}
        with op("redis", {"hash": "HGETALL", "string": "GET"}.get(kind, "READ"), key):
            if kind == "hash":
                value, command = self.r.hgetall(key), f"HGETALL {key}"
            elif kind == "string":
                value, command = self.r.get(key), f"GET {key}"
            elif kind == "list":
                value, command = self.r.lrange(key, 0, 99), f"LRANGE {key} 0 99"
            elif kind == "set":
                value, command = sorted(self.r.smembers(key)), f"SMEMBERS {key}"
            elif kind == "zset":
                value, command = self.r.zrange(key, 0, 99, withscores=True), f"ZRANGE {key} 0 99 WITHSCORES"
            else:
                value, command = None, f"TYPE {key}"
        with op("redis", "MEMORY USAGE", key):
            memory = self.r.memory_usage(key)
        return {"key": key, "exists": True, "type": kind, "ttl": ttl, "value": jsonable(value),
                "memory_bytes": memory, "cli": [command, f"TTL {key}"]}

    def redis_command(self, command: str):
        parts = shlex.split(command)
        if not parts:
            raise ValueError("enter a command")
        name = parts[0].upper()
        if name == "KEYS":
            raise ValueError("KEYS blocks Redis and is never used by this app; use SCAN 0 MATCH <pattern>")
        if name not in REDIS_READ_COMMANDS:
            raise ValueError(f"read-only console: allowed commands are {', '.join(sorted(REDIS_READ_COMMANDS))}")
        if name in ("MEMORY", "OBJECT") and (len(parts) < 2 or parts[1].upper() not in ("USAGE", "ENCODING", "IDLETIME")):
            raise ValueError(f"{name} supports only USAGE/ENCODING/IDLETIME here")
        with op("redis", name, command):
            return jsonable(self.r.execute_command(*parts))

    # ------------------------------------------------------------------ MongoDB
    def mongo_overview(self) -> list[dict]:
        result = []
        options = {c["name"]: c.get("options", {}) for c in self.db.list_collections()}
        for name in MONGO_COLLECTIONS:
            col = self.db[name]
            with op("mongo", f"{name}: count + $collStats + listIndexes"):
                count = col.count_documents({})
                try:
                    stats = next(col.aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
                except Exception:
                    stats = {}
                indexes = [{"name": i["name"], "key": list(i["key"].items()), "unique": i.get("unique", False),
                            "sparse": i.get("sparse", False), "weights": i.get("weights")}
                           for i in col.list_indexes()]
            result.append({"name": name, "count": count, "avg_doc_bytes": stats.get("avgObjSize"),
                           "size_bytes": stats.get("size"), "storage_bytes": stats.get("storageSize"),
                           "indexes": jsonable(indexes),
                           "validator": jsonable(options.get(name, {}).get("validator"))})
        return result

    def mongo_find(self, collection: str, filter_: dict | None, sort: list | None = None,
                   limit: int = 20, skip: int = 0, explain: bool = True) -> dict:
        if collection not in MONGO_COLLECTIONS:
            raise ValueError(f"collection must be one of {', '.join(MONGO_COLLECTIONS)}")
        filter_ = filter_ or {}
        _reject_code(filter_)
        sort_spec = [(field, DESCENDING if int(direction) < 0 else ASCENDING) for field, direction in (sort or [])]
        limit = max(1, min(int(limit), 100))
        col = self.db[collection]
        start = time.perf_counter()
        with op("mongo", f"{collection}.find", str(filter_)[:100]):
            cursor = col.find(filter_)
            if sort_spec:
                cursor = cursor.sort(sort_spec)
            docs = [from_mongo(d) for d in cursor.skip(int(skip)).limit(limit)]
            total = col.count_documents(filter_)
        elapsed = round((time.perf_counter() - start) * 1000, 2)
        plan = None
        if explain:
            command = {"find": collection, "filter": filter_, "limit": limit}
            if sort_spec:
                command["sort"] = dict(sort_spec)
            with op("mongo", "explain executionStats", collection):
                plan = summarize_explain(self.db.command("explain", command, verbosity="executionStats"))
        return {"collection": collection, "filter": jsonable(filter_), "sort": sort or [], "total": total,
                "documents": docs, "elapsed_ms": elapsed, "plan": plan,
                "shell": f"db.{collection}.find({_js(filter_)})" + (f".sort({_js(dict(sort_spec))})" if sort_spec else "")
                         + f".limit({limit})"}

    def mongo_preset(self, name: str) -> dict:
        preset = MONGO_PRESETS.get(name)
        if not preset:
            raise KeyError(name)
        return {"title": preset["title"], **self.mongo_find(preset["collection"], preset["filter"],
                                                            preset.get("sort"), limit=10)}

    def mongo_co_purchase(self, product_id: str) -> dict:
        start = time.perf_counter()
        rows = self.commerce.orders.co_purchase_counts(product_id)
        return {"product_id": product_id, "rows": rows, "elapsed_ms": round((time.perf_counter() - start) * 1000, 2)}

    # ------------------------------------------------------------------ Neo4j
    def neo4j_overview(self) -> dict:
        return self.commerce.graph.stats()

    def neo4j_neighborhood(self, product_id: str, buyer_limit: int) -> dict:
        data = self.commerce.graph.neighborhood(product_id, buyer_limit)
        names = self.commerce.catalog.get_many({r["product"] for r in data["rows"]} | {product_id})
        data["products"] = {pid: {"name": p["name"], "category": p["category"]["name"]} for pid, p in names.items()}
        data["seed"] = product_id
        return data


def _reject_code(value) -> None:
    if isinstance(value, dict):
        for key, inner in value.items():
            if key in FORBIDDEN_MONGO_OPERATORS:
                raise ValueError(f"{key} is not allowed in the read-only explorer")
            _reject_code(inner)
    elif isinstance(value, list):
        for inner in value:
            _reject_code(inner)


def _js(value) -> str:
    import json
    return json.dumps(jsonable(value))


def summarize_explain(explain: dict) -> dict:
    stages, indexes = [], []

    def walk(node):
        if isinstance(node, dict):
            if "stage" in node:
                stages.append(node["stage"])
            if "indexName" in node:
                indexes.append(node["indexName"])
            for key in ("queryPlan", "inputStage", "inputStages", "winningPlan"):
                if key in node:
                    walk(node[key])
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(explain.get("queryPlanner", {}).get("winningPlan", {}))
    stats = explain.get("executionStats", {})
    return {"stages": stages, "indexes": indexes,
            "uses_index": any(s in ("IXSCAN", "TEXT_MATCH", "TEXT", "IDHACK", "EXPRESS_IXSCAN", "EXPRESS_IDHACK")
                              for s in stages),
            "collection_scan": "COLLSCAN" in stages,
            "n_returned": stats.get("nReturned"), "keys_examined": stats.get("totalKeysExamined"),
            "docs_examined": stats.get("totalDocsExamined"), "execution_ms": stats.get("executionTimeMillis")}
