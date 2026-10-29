"""Product catalog in MongoDB: heterogeneous documents with embedded attributes and variants.

Each create/update writes the product and a PRODUCT_UPSERTED outbox event in one transaction,
so the graph learns about category/active changes without the API dual-writing to Neo4j."""
from __future__ import annotations

import re

from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from .outbox import new_event
from .serialize import from_mongo, utcnow
from .trace import op

ATTRIBUTE_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,40}$")

SORTS = {
    "name": [("name", ASCENDING)],
    "price_asc": [("price.amount", ASCENDING), ("_id", ASCENDING)],
    "price_desc": [("price.amount", DESCENDING), ("_id", ASCENDING)],
    "newest": [("created_at", DESCENDING), ("_id", ASCENDING)],
}


class DuplicateProduct(Exception):
    pass


def parse_attribute_filters(raw: list[str]) -> dict:
    """['waterproof:true', 'ramGb:16'] -> {'attributes.waterproof': True, 'attributes.ramGb': 16}"""
    result = {}
    for item in raw:
        key, sep, value = item.partition(":")
        if not sep or not ATTRIBUTE_KEY.match(key):
            raise ValueError(f"attribute filter must look like key:value, got {item!r}")
        result[f"attributes.{key}"] = _typed(value)
    return result


def _typed(value: str):
    lowered = value.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            pass
    return value


def upsert_event(product: dict) -> dict:
    payload = {"productId": product["_id"], "categoryId": product["category"]["id"],
               "categoryName": product["category"]["name"], "active": product["status"] == "ACTIVE",
               "updatedAt": product["updated_at"].isoformat()}
    return new_event("PRODUCT_UPSERTED", product["_id"], payload)


class Catalog:
    def __init__(self, stores):
        self.client = stores.mongo_client
        self.col = stores.mongo.products
        self.outbox = stores.mongo.outbox

    def list(self, *, q: str | None = None, category: str | None = None, min_price: int | None = None,
             max_price: int | None = None, attributes: dict | None = None, sort: str = "name",
             limit: int = 24, offset: int = 0, include_inactive: bool = False) -> dict:
        query: dict = {} if include_inactive else {"status": "ACTIVE"}
        if category:
            query["category.id"] = category
        if min_price is not None or max_price is not None:
            query["price.amount"] = {k: v for k, v in (("$gte", min_price), ("$lte", max_price)) if v is not None}
        if attributes:
            query.update(attributes)
        projection = None
        if q:
            query["$text"] = {"$search": q}
            projection = {"score": {"$meta": "textScore"}}
        order = [("score", {"$meta": "textScore"})] if q and sort == "relevance" else SORTS.get(sort, SORTS["name"])
        with op("mongo", "products.find", _describe(query)):
            cursor = self.col.find(query, projection).sort(order).skip(offset).limit(limit)
            items = [from_mongo(p) for p in cursor]
        with op("mongo", "products.countDocuments", _describe(query)):
            total = self.col.count_documents(query)
        return {"items": items, "total": total, "limit": limit, "offset": offset, "query": _describe(query)}

    def get(self, product_id: str) -> dict | None:
        with op("mongo", "products.findOne", f"_id={product_id}"):
            return from_mongo(self.col.find_one({"_id": product_id}))

    def get_many(self, product_ids) -> dict[str, dict]:
        ids = list(dict.fromkeys(product_ids))
        if not ids:
            return {}
        with op("mongo", "products.find $in", f"{len(ids)} ids"):
            return {p["_id"]: from_mongo(p) for p in self.col.find({"_id": {"$in": ids}})}

    def create(self, product: dict) -> dict:
        now = utcnow()
        doc = {**product, "_id": product["id"], "created_at": now, "updated_at": now}
        doc.pop("id")
        try:
            with op("mongo", "txn: products.insertOne + outbox.insertOne", doc["_id"]):
                with self.client.start_session() as session, session.start_transaction():
                    self.col.insert_one(doc, session=session)
                    self.outbox.insert_one(upsert_event(doc), session=session)
        except DuplicateKeyError as exc:
            raise DuplicateProduct("product id, SKU or slug already exists") from exc
        return from_mongo(doc)

    def update(self, product_id: str, changes: dict) -> dict | None:
        """Top-level fields are replaced; `attributes` is merged key by key (None removes a key)."""
        update_set, update_unset = {}, {}
        for key, value in changes.items():
            if key == "attributes":
                for attr, attr_value in value.items():
                    if not ATTRIBUTE_KEY.match(attr):
                        raise ValueError(f"invalid attribute name {attr!r}")
                    if attr_value is None:
                        update_unset[f"attributes.{attr}"] = ""
                    else:
                        update_set[f"attributes.{attr}"] = attr_value
            else:
                update_set[key] = value
        update_set["updated_at"] = utcnow()
        update = {"$set": update_set, **({"$unset": update_unset} if update_unset else {})}
        try:
            with op("mongo", "txn: products.findOneAndUpdate + outbox.insertOne", product_id):
                with self.client.start_session() as session, session.start_transaction():
                    doc = self.col.find_one_and_update({"_id": product_id}, update,
                                                       return_document=ReturnDocument.AFTER, session=session)
                    if doc is None:
                        session.abort_transaction()
                        return None
                    self.outbox.insert_one(upsert_event(doc), session=session)
        except DuplicateKeyError as exc:
            raise DuplicateProduct("SKU or slug already exists") from exc
        return from_mongo(doc)

    def categories(self) -> list[dict]:
        """Facets per category, including which attribute keys documents in it actually use."""
        pipeline = [
            {"$match": {"status": "ACTIVE"}},
            {"$project": {"category": 1, "price": "$price.amount",
                          "attrs": {"$objectToArray": "$attributes"}}},
            {"$group": {"_id": "$category.id", "name": {"$first": "$category.name"}, "count": {"$sum": 1},
                        "min_price": {"$min": "$price"}, "max_price": {"$max": "$price"},
                        "attrs": {"$push": "$attrs.k"}}},
            {"$project": {"name": 1, "count": 1, "min_price": 1, "max_price": 1,
                          "attribute_keys": {"$setUnion": [{"$reduce": {
                              "input": "$attrs", "initialValue": [],
                              "in": {"$concatArrays": ["$$value", "$$this"]}}}]}}},
            {"$sort": {"name": 1}},
        ]
        with op("mongo", "products.aggregate category facets"):
            return [from_mongo(c) for c in self.col.aggregate(pipeline)]


def _describe(query: dict) -> str:
    return str({k: v for k, v in query.items()})[:120]
