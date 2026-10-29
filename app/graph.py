"""Neo4j purchase graph: a behavioral projection of completed MongoDB orders.

    (:Customer {customerId})-[:PURCHASED {eventId, productId, orderId, quantity, purchasedAt}]->(:Product)
    (:Product {productId, categoryId, active})-[:IN_CATEGORY]->(:Category {categoryId, name})

Product display data (name, price, attributes) stays in MongoDB; recommendations are hydrated
from the catalog, so a catalog rename never requires a graph rewrite."""
from __future__ import annotations

import re

from neo4j import RoutingControl
from neo4j.exceptions import ServiceUnavailable

from .serialize import jsonable
from .trace import op

PROJECT_ORDERS = """
UNWIND $events AS e
MERGE (c:Customer {customerId: e.customerId})
WITH c, e
UNWIND e.items AS item
MERGE (p:Product {productId: item.productId})
  ON CREATE SET p.categoryId = item.categoryId, p.active = true
MERGE (c)-[r:PURCHASED {eventId: e.eventId, productId: item.productId}]->(p)
  ON CREATE SET r.orderId = e.orderId, r.quantity = item.quantity, r.purchasedAt = datetime(e.purchasedAt)
"""

PROJECT_PRODUCTS = """
UNWIND $products AS e
MERGE (p:Product {productId: e.productId})
WITH p, e WHERE coalesce(p.catalogUpdatedAt, '') <= e.updatedAt
SET p.categoryId = e.categoryId, p.active = e.active, p.catalogUpdatedAt = e.updatedAt
MERGE (c:Category {categoryId: e.categoryId})
SET c.name = e.categoryName
MERGE (p)-[:IN_CATEGORY]->(c)
WITH p, c
MATCH (p)-[old:IN_CATEGORY]->(other:Category) WHERE other <> c
DELETE old
"""

RECOMMEND = """
MATCH (seed:Product {productId: $productId})
OPTIONAL MATCH (seed)<-[:PURCHASED]-(sb:Customer)
WITH seed, count(DISTINCT sb) AS seedBuyers
MATCH (seed)<-[:PURCHASED]-(buyer:Customer)-[:PURCHASED]->(candidate:Product)
WHERE candidate <> seed AND coalesce(candidate.active, true) = true
  AND ($sameCategory = false OR candidate.categoryId = seed.categoryId)
WITH seedBuyers, candidate, count(DISTINCT buyer) AS sharedBuyers,
     collect(DISTINCT buyer.customerId)[0..5] AS sampleBuyers
WHERE sharedBuyers >= $minSupport
MATCH (candidate)<-[:PURCHASED]-(cb:Customer)
WITH candidate, sharedBuyers, sampleBuyers, seedBuyers, count(DISTINCT cb) AS candidateBuyers
WITH candidate, sharedBuyers, sampleBuyers, seedBuyers, candidateBuyers,
     toFloat(sharedBuyers) / (seedBuyers + candidateBuyers - sharedBuyers) AS jaccard
RETURN candidate.productId AS productId, sharedBuyers, jaccard, seedBuyers, candidateBuyers, sampleBuyers
ORDER BY CASE WHEN $strategy = 'jaccard' THEN jaccard ELSE toFloat(sharedBuyers) END DESC,
         sharedBuyers DESC, productId
LIMIT $limit
""".strip()

POPULAR = """
MATCH (p:Product)<-[:PURCHASED]-(c:Customer)
WHERE NOT p.productId IN $exclude AND coalesce(p.active, true) = true
  AND ($categoryId IS NULL OR p.categoryId = $categoryId)
RETURN p.productId AS productId, count(DISTINCT c) AS buyers
ORDER BY buyers DESC, productId
LIMIT $limit
"""

_WRITE_WORDS = re.compile(r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|FOREACH|LOAD\s+CSV|TRANSACTIONS|"
                          r"dbms\.|apoc\.|GRANT|DENY|REVOKE|ALTER|TERMINATE|START|STOP)\b", re.IGNORECASE)


class ReadOnlyViolation(ValueError):
    pass


class Graph:
    def __init__(self, stores):
        self.driver = stores.graph
        self.db = stores.graph_db

    def _q(self, query, label, detail="", routing=RoutingControl.WRITE, **params):
        with op("neo4j", label, detail):
            try:
                records, summary, keys = self.driver.execute_query(query, params, database_=self.db, routing_=routing)
            except ValueError as exc:
                # When the Neo4j container is stopped its DNS name disappears and the driver raises
                # ValueError("Cannot resolve address ..."): treat it as the database being unavailable.
                if "resolve address" in str(exc):
                    raise ServiceUnavailable(str(exc)) from exc
                raise
        return records, summary, keys

    def project_orders(self, events: list[dict]) -> int:
        if not events:
            return 0
        _, summary, _ = self._q(PROJECT_ORDERS, "UNWIND MERGE Customer-PURCHASED->Product",
                                f"{len(events)} order events", events=events)
        return summary.counters.relationships_created

    def project_products(self, products: list[dict]) -> None:
        if products:
            self._q(PROJECT_PRODUCTS, "UNWIND MERGE Product-IN_CATEGORY->Category",
                    f"{len(products)} product events", products=products)

    def recommend(self, product_id: str, *, limit: int, min_support: int, strategy: str,
                  same_category: bool) -> list[dict]:
        records, _, _ = self._q(RECOMMEND, "MATCH 2-hop co-purchase", product_id, routing=RoutingControl.READ,
                                productId=product_id, limit=limit, minSupport=min_support,
                                strategy=strategy, sameCategory=same_category)
        return [dict(r) for r in records]

    def popular(self, *, category_id: str | None, exclude: list[str], limit: int) -> list[dict]:
        records, _, _ = self._q(POPULAR, "MATCH popular products", category_id or "all",
                                routing=RoutingControl.READ, categoryId=category_id, exclude=exclude, limit=limit)
        return [dict(r) for r in records]

    def purchases_for_event(self, event_id: str) -> int:
        records, _, _ = self._q("MATCH ()-[r:PURCHASED {eventId: $eventId}]->() RETURN count(r) AS n",
                                "MATCH PURCHASED by eventId", event_id, routing=RoutingControl.READ, eventId=event_id)
        return records[0]["n"]

    def customer_purchases(self, customer_id: str) -> list[dict]:
        records, _, _ = self._q(
            "MATCH (:Customer {customerId: $customerId})-[r:PURCHASED]->(p:Product) "
            "RETURN p.productId AS productId, r.orderId AS orderId, r.eventId AS eventId, r.quantity AS quantity "
            "ORDER BY r.purchasedAt DESC", "MATCH customer purchases", customer_id,
            routing=RoutingControl.READ, customerId=customer_id)
        return [dict(r) for r in records]

    def stats(self) -> dict:
        labels, _, _ = self._q("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count ORDER BY label",
                               "count nodes by label", routing=RoutingControl.READ)
        rels, _, _ = self._q("MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count ORDER BY type",
                             "count relationships by type", routing=RoutingControl.READ)
        constraints, _, _ = self._q("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties",
                                    "SHOW CONSTRAINTS", routing=RoutingControl.READ)
        indexes, _, _ = self._q("SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, state",
                                "SHOW INDEXES", routing=RoutingControl.READ)
        return {"nodes": [dict(r) for r in labels], "relationships": [dict(r) for r in rels],
                "constraints": jsonable([dict(r) for r in constraints]),
                "indexes": jsonable([dict(r) for r in indexes])}

    def neighborhood(self, product_id: str, buyer_limit: int = 15) -> dict:
        records, _, _ = self._q("""
            MATCH (seed:Product {productId: $productId})<-[:PURCHASED]-(c:Customer)
            WITH DISTINCT c LIMIT $buyerLimit
            MATCH (c)-[r:PURCHASED]->(p:Product)
            OPTIONAL MATCH (p)-[:IN_CATEGORY]->(cat:Category)
            RETURN c.customerId AS customer, p.productId AS product, p.categoryId AS categoryId,
                   cat.name AS categoryName, count(r) AS times
            """, "MATCH product neighborhood", product_id, routing=RoutingControl.READ,
            productId=product_id, buyerLimit=buyer_limit)
        return {"rows": [dict(r) for r in records]}

    def run_read_only(self, query: str, params: dict | None = None, max_rows: int = 200) -> dict:
        if _WRITE_WORDS.search(query):
            raise ReadOnlyViolation("the explorer console is read-only; write clauses are not allowed")
        records, summary, keys = self._q(query, "read-only console query", query[:80],
                                         routing=RoutingControl.READ, **(params or {}))
        return {"keys": list(keys), "rows": [jsonable(r.values()) for r in records[:max_rows]],
                "truncated": len(records) > max_rows,
                "available_after_ms": summary.result_available_after}
