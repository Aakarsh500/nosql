"""Business flows. Each store is reached only through its own module; cross-store rules
(checkout, recommendation hydration) live here and are explicit about consistency."""
from __future__ import annotations

import secrets

from neo4j.exceptions import ServiceUnavailable, SessionExpired

from . import config
from .catalog import Catalog
from .graph import RECOMMEND, Graph
from .orders import OrderStore
from .outbox import Outbox, new_event
from .projector import Projector
from .serialize import utcnow
from .sessions_carts import IDEMPOTENCY_KEY, SessionCartStore


class NotFound(KeyError):
    pass


class Conflict(Exception):
    pass


class Commerce:
    def __init__(self, stores):
        self.stores = stores
        self.carts = SessionCartStore(stores.redis)
        self.catalog = Catalog(stores)
        self.orders = OrderStore(stores)
        self.outbox = Outbox(stores)
        self.graph = Graph(stores)
        self.projector = Projector(self.outbox, self.graph, stores)

    # ------------------------------------------------------------------ catalog
    def update_product(self, product_id: str, changes: dict) -> dict:
        if not changes:
            raise ValueError("at least one field must be supplied")
        product = self.catalog.update(product_id, changes)
        if product is None:
            raise NotFound("product not found")
        return product

    # ------------------------------------------------------------------ sessions / login
    def login(self, customer_id: str, display_name: str, guest_cart_id: str | None) -> dict:
        session = self.carts.create_session(customer_id, display_name)
        cart_id, merged = self.carts.attach_to_customer(guest_cart_id, customer_id)
        return {"session": session, "cart_id": cart_id, "merged_lines": merged}

    # ------------------------------------------------------------------ carts
    def _active_product(self, product_id: str) -> dict:
        product = self.catalog.get(product_id)
        if product is None or product["status"] != "ACTIVE":
            raise ValueError("product is unavailable")
        return product

    def set_cart_item(self, cart_id: str, product_id: str, quantity: int) -> dict:
        self._active_product(product_id)
        if self.carts.change_item(cart_id, product_id, quantity=quantity) is None:
            raise NotFound("cart not found")
        return self.cart_view(cart_id)

    def add_to_cart(self, cart_id: str, product_id: str, delta: int) -> dict:
        if delta > 0:
            self._active_product(product_id)
        if self.carts.change_item(cart_id, product_id, delta=delta) is None:
            raise NotFound("cart not found")
        return self.cart_view(cart_id)

    def remove_cart_item(self, cart_id: str, product_id: str) -> dict:
        if self.carts.change_item(cart_id, product_id, quantity=0) is None:
            raise NotFound("cart not found")
        return self.cart_view(cart_id)

    def cart_view(self, cart_id: str) -> dict:
        cart = self.carts.get_cart(cart_id)
        if cart is None:
            raise NotFound("cart not found")
        if cart["meta"].get("userId"):
            self.carts.touch_user_cart(cart["meta"]["userId"])
        products = self.catalog.get_many(cart["items"].keys())
        lines, subtotal = [], 0
        for product_id, qty in sorted(cart["items"].items()):
            product = products.get(product_id)
            available = bool(product and product["status"] == "ACTIVE")
            unit = product["price"]["amount"] if product else 0
            if available:
                subtotal += unit * qty
            lines.append({"product_id": product_id, "quantity": qty, "available": available,
                          "name": product["name"] if product else product_id, "unit_price": unit,
                          "line_total": unit * qty})
        return {**cart, "lines": lines, "subtotal": subtotal, "currency": "INR",
                "note": "Redis stores only product IDs and quantities; names and prices are read live from MongoDB."}

    # ------------------------------------------------------------------ checkout
    def checkout(self, cart_id: str, customer_id: str, key: str) -> tuple[dict, bool]:
        if not IDEMPOTENCY_KEY.match(key or ""):
            raise ValueError("Idempotency-Key must be 8-120 characters of letters, digits, _ . : -")
        reserved, value = self.carts.reserve_checkout(key)
        if not reserved:
            # Fast duplicate suppression in Redis; MongoDB's unique index remains authoritative.
            existing = self.orders.get_by_key(key)
            if existing:
                return existing, False
            raise Conflict("a checkout with this Idempotency-Key is already in progress; retry shortly")
        try:
            existing = self.orders.get_by_key(key)
            if existing:
                self.carts.complete_checkout(key, existing["id"])
                return existing, False
            cart = self.carts.get_cart(cart_id)
            if cart is None:
                raise NotFound("cart not found")
            if not cart["items"]:
                raise ValueError("cart is empty")
            products = self.catalog.get_many(cart["items"].keys())
            line_items, total = [], 0
            for product_id, quantity in sorted(cart["items"].items()):
                product = products.get(product_id)
                if product is None or product["status"] != "ACTIVE":
                    raise ValueError(f"product {product_id} is no longer available; remove it from the cart")
                unit_price = product["price"]["amount"]  # authoritative price re-read at checkout
                line_items.append({"product_id": product_id, "sku": product["sku"],
                                   "name_snapshot": product["name"], "category_id": product["category"]["id"],
                                   "unit_price": unit_price, "quantity": quantity,
                                   "line_total": unit_price * quantity})
                total += unit_price * quantity
            now = utcnow()
            order_id = "ord_" + secrets.token_hex(6)
            order = {"_id": order_id, "customer_id": customer_id, "idempotency_key": key, "status": "PLACED",
                     "cart_id": cart_id, "line_items": line_items, "item_count": sum(i["quantity"] for i in line_items),
                     "total_amount": total, "currency": "INR", "created_at": now}
            payload = {"orderId": order_id, "customerId": customer_id, "purchasedAt": now.isoformat(),
                       "items": [{"productId": i["product_id"], "categoryId": i["category_id"],
                                  "quantity": i["quantity"]} for i in line_items]}
            event = new_event("ORDER_PLACED", order_id, payload, order_id=order_id)
            saved, created = self.orders.create_with_outbox(order, event)
        except BaseException:
            self.carts.release_checkout(key)
            raise
        try:
            self.carts.complete_checkout(key, saved["id"])
            if created:
                self.carts.clear_items(cart_id)  # best effort: the order is already authoritative
        except Exception:
            pass
        return saved, created

    def order_view(self, order: dict) -> dict:
        projection = self.outbox.status_for_orders([order["id"]]).get(order["id"])
        return {**order, "graph_projection": projection}

    def orders_for_customer(self, customer_id: str, limit: int = 20) -> list[dict]:
        orders = self.orders.list_for_customer(customer_id, limit)
        status = self.outbox.status_for_orders([o["id"] for o in orders])
        return [{**o, "graph_projection": status.get(o["id"])} for o in orders]

    # ------------------------------------------------------------------ recommendations
    def recommendations(self, product_id: str, *, limit: int = 5, min_support: int = 1,
                        strategy: str = "support", same_category: bool = False, fallback: bool = True) -> dict:
        seed = self.catalog.get(product_id)
        if seed is None:
            raise NotFound("product not found")
        params = {"productId": product_id, "limit": limit * 3, "minSupport": min_support,
                  "strategy": strategy, "sameCategory": same_category}
        degraded, rows = None, []
        try:
            rows = self.graph.recommend(product_id, limit=limit * 3, min_support=min_support,
                                        strategy=strategy, same_category=same_category)
        except (ServiceUnavailable, SessionExpired, OSError) as exc:
            degraded = f"Neo4j unavailable ({type(exc).__name__}); showing catalog fallback"
        products = self.catalog.get_many(r["productId"] for r in rows)
        items = []
        for row in rows:
            product = products.get(row["productId"])
            if not product or product["status"] != "ACTIVE":
                continue
            items.append({
                "product": product, "source": "graph", "shared_buyers": row["sharedBuyers"],
                "jaccard": round(row["jaccard"], 4), "seed_buyers": row["seedBuyers"],
                "candidate_buyers": row["candidateBuyers"], "sample_buyers": row["sampleBuyers"],
                "reason": (f"{row['sharedBuyers']} distinct customer(s) who bought {seed['name']} also bought "
                           f"{product['name']} (Jaccard {row['jaccard']:.2f})"),
            })
            if len(items) >= limit:
                break
        if fallback and len(items) < limit:
            items += self._fallback(seed, [product_id] + [i["product"]["id"] for i in items],
                                    limit - len(items), graph_ok=degraded is None)
        try:
            projection = self.outbox.summary()
        except Exception:
            projection = None
        return {"seed": {"id": seed["id"], "name": seed["name"], "category": seed["category"]},
                "strategy": strategy, "min_support": min_support, "same_category": same_category,
                "items": items, "degraded": degraded, "cypher": RECOMMEND, "params": params,
                "projection": projection}

    def _fallback(self, seed: dict, exclude: list[str], needed: int, graph_ok: bool) -> list[dict]:
        category = seed["category"]["id"]
        if graph_ok:
            try:
                rows = self.graph.popular(category_id=category, exclude=exclude, limit=needed * 2)
                products = self.catalog.get_many(r["productId"] for r in rows)
                found = []
                for row in rows:
                    product = products.get(row["productId"])
                    if product and product["status"] == "ACTIVE":
                        found.append({"product": product, "source": "fallback-graph", "buyers": row["buyers"],
                                      "reason": f"Cold-start fallback: popular in {seed['category']['name']} "
                                                f"({row['buyers']} buyers)"})
                if found:
                    return found[:needed]
            except (ServiceUnavailable, SessionExpired, OSError):
                pass
        listing = self.catalog.list(category=category, sort="newest", limit=needed + len(exclude))
        return [{"product": p, "source": "fallback-catalog",
                 "reason": f"Cold-start fallback: another {seed['category']['name']} product from the MongoDB catalog"}
                for p in listing["items"] if p["id"] not in exclude][:needed]
