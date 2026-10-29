"""HTTP API and browser pages for the polyglot persistence demonstrator."""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import redis
from fastapi import Body, Cookie, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from pydantic import BaseModel, Field
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

from . import config, trace
from .catalog import DuplicateProduct, parse_attribute_filters
from .db import get_stores
from .explorer import MONGO_PRESETS, Explorer
from .graph import ReadOnlyViolation
from .serialize import jsonable
from .services import Commerce, Conflict, NotFound
from .sessions_carts import QuantityLimitExceeded

STATIC = Path(__file__).with_name("static")
CUSTOMER_ID = r"^c_[A-Za-z0-9_-]{1,32}$"
SAFE_CORRELATION = re.compile(r"^[A-Za-z0-9_-]{4,64}$")

AttrValue = str | int | float | bool


# ---------------------------------------------------------------------- request models
class Category(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9_-]{2,40}$")
    name: str = Field(min_length=2, max_length=80)


class Price(BaseModel):
    amount: int = Field(ge=0, le=10_000_000, description="Amount in whole rupees")
    currency: Literal["INR"] = "INR"


class Variant(BaseModel, extra="allow"):
    variantId: str = Field(min_length=1, max_length=40)
    stock: int = Field(default=0, ge=0)


class Media(BaseModel):
    url: str = Field(max_length=300)
    alt: str = Field(default="", max_length=120)


class ProductInput(BaseModel):
    id: str = Field(pattern=r"^p_[A-Za-z0-9_-]{2,32}$", examples=["p_200"])
    sku: str = Field(min_length=2, max_length=80)
    slug: str = Field(pattern=r"^[a-z0-9-]{2,80}$")
    name: str = Field(min_length=2, max_length=120)
    description: str = Field(default="", max_length=2000)
    category: Category
    price: Price
    status: Literal["ACTIVE", "INACTIVE"] = "ACTIVE"
    attributes: dict[str, AttrValue] = Field(default_factory=dict)
    variants: list[Variant] = Field(default_factory=list)
    media: list[Media] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    schema_version: int = Field(default=1, ge=1)


class ProductPatch(BaseModel):
    sku: str | None = Field(default=None, min_length=2, max_length=80)
    slug: str | None = Field(default=None, pattern=r"^[a-z0-9-]{2,80}$")
    name: str | None = Field(default=None, min_length=2, max_length=120)
    description: str | None = Field(default=None, max_length=2000)
    category: Category | None = None
    price: Price | None = None
    status: Literal["ACTIVE", "INACTIVE"] | None = None
    attributes: dict[str, AttrValue | None] | None = Field(
        default=None, description="Merged key by key; a null value removes that attribute")
    variants: list[Variant] | None = None
    tags: list[str] | None = None


class CartItemInput(BaseModel):
    quantity: int = Field(ge=1, le=config.MAX_ITEM_QUANTITY)


class CartDeltaInput(BaseModel):
    delta: int = Field(default=1, ge=-config.MAX_ITEM_QUANTITY, le=config.MAX_ITEM_QUANTITY)


class SessionInput(BaseModel):
    customer_id: str = Field(pattern=CUSTOMER_ID, examples=["c_0001"])
    display_name: str = Field(default="", max_length=60)
    cart_id: str | None = Field(default=None, description="Guest cart to merge into the customer's cart")


class CheckoutInput(BaseModel):
    cart_id: str
    customer_id: str | None = Field(default=None, pattern=CUSTOMER_ID,
                                    description="Required when there is no logged-in session")


class MongoQuery(BaseModel):
    collection: str
    filter: dict[str, Any] = Field(default_factory=dict)
    sort: list[tuple[str, int]] = Field(default_factory=list)
    limit: int = Field(default=20, ge=1, le=100)
    skip: int = Field(default=0, ge=0)


class CypherQuery(BaseModel):
    query: str = Field(min_length=1, max_length=4000)
    params: dict[str, Any] = Field(default_factory=dict)


class RedisCommand(BaseModel):
    command: str = Field(min_length=1, max_length=300)


# ---------------------------------------------------------------------- application
def _degraded(store: str, message: str):
    def handler(request: Request, exc: Exception):
        return JSONResponse(status_code=503, content={"detail": message, "store": store,
                                                      "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
    return handler


def create_app() -> FastAPI:
    stores = get_stores()
    stores.ensure_schema()
    service = Commerce(stores)
    explorer = Explorer(stores, service)

    api = FastAPI(title="Polyglot Commerce", version="3.0.0",
                  description="Redis sessions & carts · MongoDB catalog, orders & outbox · Neo4j purchase graph")
    api.state.stores = stores
    api.state.service = service
    api.mount("/static", StaticFiles(directory=STATIC), name="static")

    api.add_exception_handler(redis.exceptions.ConnectionError, _degraded(
        "redis", "Redis is unavailable: carts and sessions are temporarily degraded; browsing still works."))
    api.add_exception_handler(redis.exceptions.TimeoutError, _degraded("redis", "Redis timed out."))
    api.add_exception_handler(ServerSelectionTimeoutError, _degraded(
        "mongo", "MongoDB is unavailable: the catalog and checkout cannot be served."))
    api.add_exception_handler(ConnectionFailure, _degraded("mongo", "MongoDB connection failed."))
    api.add_exception_handler(ServiceUnavailable, _degraded(
        "neo4j", "Neo4j is unavailable: graph features are degraded; orders still commit to MongoDB."))
    api.add_exception_handler(SessionExpired, _degraded("neo4j", "Neo4j session expired."))

    @api.exception_handler(OperationFailure)
    def mongo_operation_failure(request: Request, exc: OperationFailure):
        if exc.code == 121:  # DocumentValidationFailure from the collection's $jsonSchema
            return JSONResponse(status_code=400, content={
                "detail": "MongoDB schema validation rejected the document", "store": "mongo",
                "validation": jsonable((exc.details or {}).get("errInfo"))})
        return JSONResponse(status_code=500, content={"detail": f"MongoDB error: {exc}", "store": "mongo"})

    @api.middleware("http")
    async def correlate_and_trace(request: Request, call_next):
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)
        incoming = request.headers.get("X-Correlation-ID", "")
        correlation_id = incoming if SAFE_CORRELATION.match(incoming) else uuid4().hex[:12]
        quiet = path.startswith(("/api/admin/", "/api/explorer/")) or path == "/api/health"
        state, token = trace.begin(correlation_id, record_stats=not quiet)
        start = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            trace.end(token)
        elapsed = (time.perf_counter() - start) * 1000
        trace.record_request(state, request.method, path, response.status_code, elapsed)
        response.headers["X-Correlation-ID"] = correlation_id
        response.headers["X-Response-Time-ms"] = f"{elapsed:.2f}"
        response.headers["X-DB-Trace"] = json.dumps(state.ops[:25], separators=(",", ":"))
        return response

    # ------------------------------------------------------------------ pages
    def page_handler(filename: str):
        def handler():
            return FileResponse(STATIC / filename, headers={"Cache-Control": "no-cache"})
        return handler

    for route, page in (("/", "index.html"), ("/admin", "admin.html"), ("/dashboard", "dashboard.html"),
                        ("/explorer", "explorer.html")):
        api.add_api_route(route, page_handler(page), methods=["GET"], include_in_schema=False)

    @api.get("/favicon.ico", include_in_schema=False)
    def favicon():
        return Response(status_code=204)

    # ------------------------------------------------------------------ health
    @api.get("/api/health", tags=["health"])
    def health():
        result = {}
        checks = {
            "redis": lambda: {"version": stores.redis.info("server").get("redis_version"), "ping": stores.redis.ping()},
            "mongo": lambda: {"version": stores.mongo_client.server_info().get("version"),
                              "replica_set": stores.mongo_client.admin.command("hello").get("setName")},
            "neo4j": lambda: (stores.graph.verify_connectivity(), {"version": stores.graph.get_server_info().agent})[1],
        }
        for name, check in checks.items():
            start = time.perf_counter()
            try:
                info = check()
                result[name] = {"ok": True, **info}
            except Exception as exc:
                result[name] = {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:160]}"}
            result[name]["ms"] = round((time.perf_counter() - start) * 1000, 2)
        status = "ok" if all(v["ok"] for v in result.values()) else "degraded"
        return {"status": status, "stores": result}

    # ------------------------------------------------------------------ catalog (MongoDB)
    @api.get("/api/categories", tags=["catalog"])
    def categories():
        return service.catalog.categories()

    @api.get("/api/products", tags=["catalog"])
    def list_products(q: str | None = Query(None, max_length=100), category: str | None = None,
                      min_price: int | None = Query(None, ge=0), max_price: int | None = Query(None, ge=0),
                      attr: list[str] = Query(default=[], description="Attribute filters such as waterproof:true"),
                      sort: Literal["name", "price_asc", "price_desc", "newest", "relevance"] = "name",
                      limit: int = Query(24, ge=1, le=100), offset: int = Query(0, ge=0),
                      include_inactive: bool = False):
        try:
            attributes = parse_attribute_filters(attr)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return service.catalog.list(q=q, category=category, min_price=min_price, max_price=max_price,
                                    attributes=attributes, sort=sort, limit=limit, offset=offset,
                                    include_inactive=include_inactive)

    @api.get("/api/products/{product_id}", tags=["catalog"])
    def get_product(product_id: str):
        product = service.catalog.get(product_id)
        if not product:
            raise HTTPException(404, "product not found")
        return product

    @api.post("/api/products", status_code=201, tags=["catalog"])
    def create_product(body: ProductInput):
        try:
            return service.catalog.create(body.model_dump())
        except DuplicateProduct as exc:
            raise HTTPException(409, str(exc)) from exc

    @api.patch("/api/products/{product_id}", tags=["catalog"])
    def update_product(product_id: str, body: ProductPatch):
        # Only fields the client sent, but nested models dumped whole (so price.currency keeps its default).
        full = body.model_dump()
        changes = {k: full[k] for k in body.model_fields_set if full[k] is not None}
        try:
            return service.update_product(product_id, changes)
        except DuplicateProduct as exc:
            raise HTTPException(409, str(exc)) from exc
        except NotFound as exc:
            raise HTTPException(404, "product not found") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @api.delete("/api/products/{product_id}", tags=["catalog"])
    def delete_product(product_id: str):
        """Soft delete: historical orders keep referencing the product."""
        try:
            product = service.update_product(product_id, {"status": "INACTIVE"})
        except NotFound as exc:
            raise HTTPException(404, "product not found") from exc
        return {"id": product["id"], "status": product["status"]}

    @api.get("/api/products/{product_id}/recommendations", tags=["recommendations"])
    def recommendations(product_id: str, limit: int = Query(5, ge=1, le=20),
                        min_support: int = Query(1, ge=1, le=1000),
                        strategy: Literal["support", "jaccard"] = "support",
                        same_category: bool = False, fallback: bool = True):
        try:
            return service.recommendations(product_id, limit=limit, min_support=min_support,
                                           strategy=strategy, same_category=same_category, fallback=fallback)
        except NotFound as exc:
            raise HTTPException(404, "product not found") from exc

    # ------------------------------------------------------------------ sessions (Redis)
    def _set_session_cookie(response: Response, session_id: str):
        response.set_cookie("sid", session_id, max_age=config.SESSION_TTL, httponly=True,
                            samesite="lax", secure=config.COOKIE_SECURE, path="/")

    @api.post("/api/sessions", status_code=201, tags=["sessions"])
    def login(body: SessionInput, response: Response):
        """Create a session (login). A guest cart passed in `cart_id` is merged into the customer's cart."""
        result = service.login(body.customer_id, body.display_name or body.customer_id, body.cart_id)
        _set_session_cookie(response, result["session"]["id"])
        return result

    @api.get("/api/sessions/current", tags=["sessions"])
    def current_session(response: Response, sid: str | None = Cookie(default=None)):
        session = service.carts.get_session(sid)
        if not session:
            raise HTTPException(404, "no active session")
        _set_session_cookie(response, session["id"])
        return session

    @api.delete("/api/sessions/current", tags=["sessions"])
    def logout(response: Response, sid: str | None = Cookie(default=None)):
        deleted = service.carts.delete_session(sid)
        response.delete_cookie("sid", path="/")
        return {"deleted": deleted}

    @api.get("/api/sessions/{session_id}", tags=["sessions"])
    def get_session(session_id: str):
        session = service.carts.get_session(session_id)
        if not session:
            raise HTTPException(404, "session not found")
        return session

    @api.delete("/api/sessions/{session_id}", tags=["sessions"])
    def delete_session(session_id: str):
        if not service.carts.delete_session(session_id):
            raise HTTPException(404, "session not found")
        return {"deleted": True}

    # ------------------------------------------------------------------ carts (Redis)
    def _cart(fn, *args):
        try:
            return fn(*args)
        except NotFound as exc:
            raise HTTPException(404, "cart not found") from exc
        except QuantityLimitExceeded as exc:
            raise HTTPException(400, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @api.post("/api/carts", status_code=201, tags=["carts"])
    def create_cart():
        return service.cart_view(service.carts.create_cart())

    @api.get("/api/carts/{cart_id}", tags=["carts"])
    def get_cart(cart_id: str):
        return _cart(service.cart_view, cart_id)

    @api.put("/api/carts/{cart_id}/items/{product_id}", tags=["carts"])
    def set_cart_item(cart_id: str, product_id: str, body: CartItemInput):
        return _cart(service.set_cart_item, cart_id, product_id, body.quantity)

    @api.post("/api/carts/{cart_id}/items/{product_id}/increment", tags=["carts"])
    def increment_cart_item(cart_id: str, product_id: str, body: CartDeltaInput = Body(default=CartDeltaInput())):
        """Atomic HINCRBY-style change (Lua script) with the 99-unit guard and TTL refresh."""
        return _cart(service.add_to_cart, cart_id, product_id, body.delta)

    @api.delete("/api/carts/{cart_id}/items/{product_id}", tags=["carts"])
    def remove_cart_item(cart_id: str, product_id: str):
        return _cart(service.remove_cart_item, cart_id, product_id)

    @api.delete("/api/carts/{cart_id}/items", tags=["carts"])
    def clear_cart(cart_id: str):
        _cart(service.cart_view, cart_id)
        service.carts.clear_items(cart_id)
        return service.cart_view(cart_id)

    # ------------------------------------------------------------------ orders (MongoDB)
    @api.post("/api/orders", tags=["orders"], responses={200: {"description": "Replay of an existing order"},
                                                         201: {"description": "Order created"}})
    def checkout(body: CheckoutInput, response: Response,
                 idempotency_key: str = Header(min_length=8, max_length=120),
                 sid: str | None = Cookie(default=None)):
        session = service.carts.get_session(sid) if sid else None
        customer_id = session["customerId"] if session else body.customer_id
        if not customer_id:
            raise HTTPException(400, "log in or provide customer_id")
        try:
            order, created = service.checkout(body.cart_id, customer_id, idempotency_key)
        except NotFound as exc:
            raise HTTPException(404, "cart not found") from exc
        except Conflict as exc:
            raise HTTPException(409, str(exc), headers={"Retry-After": "1"}) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        response.status_code = 201 if created else 200
        return {"created": created, "order": service.order_view(order)}

    @api.get("/api/orders", tags=["orders"])
    def list_orders(customer_id: str | None = Query(None, pattern=CUSTOMER_ID), limit: int = Query(20, ge=1, le=100),
                    sid: str | None = Cookie(default=None)):
        if not customer_id:
            session = service.carts.get_session(sid) if sid else None
            if not session:
                raise HTTPException(400, "log in or provide customer_id")
            customer_id = session["customerId"]
        return service.orders_for_customer(customer_id, limit)

    @api.get("/api/orders/{order_id}", tags=["orders"])
    def get_order(order_id: str):
        order = service.orders.get(order_id)
        if not order:
            raise HTTPException(404, "order not found")
        return service.order_view(order)

    # ------------------------------------------------------------------ admin / observability
    @api.get("/api/admin/outbox", tags=["admin"])
    def outbox(status: Literal["PENDING", "PROCESSING", "PROCESSED", "DEAD"] | None = None,
               limit: int = Query(50, ge=1, le=500)):
        return service.outbox.list(status, limit)

    @api.get("/api/admin/outbox/summary", tags=["admin"])
    def outbox_summary():
        return service.outbox.summary()

    @api.post("/api/admin/project", tags=["admin"])
    def project_now():
        """Run one projector batch immediately (the background worker also does this every second)."""
        return service.projector.run_once()

    @api.post("/api/admin/outbox/retry-dead", tags=["admin"])
    def retry_dead():
        return {"requeued": service.outbox.retry(None)}

    @api.post("/api/admin/outbox/{event_id}/retry", tags=["admin"])
    def retry_event(event_id: str):
        count = service.outbox.retry(event_id)
        if not count:
            raise HTTPException(404, "no retryable event with that id")
        return {"requeued": count}

    @api.get("/api/admin/metrics", tags=["admin"])
    def metrics():
        flows = trace.summarize(trace.FLOW_LATENCIES)
        for name, target in config.LATENCY_TARGETS.items():
            entry = flows.setdefault(name, {"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None})
            entry["target_p95_ms"] = target
            entry["meets_target"] = None if entry["p95_ms"] is None else entry["p95_ms"] <= target
        return {"stores": trace.summarize(trace.STORE_LATENCIES), "flows": flows,
                "note": "Server-side latencies since the API process started (admin/explorer calls excluded)."}

    @api.post("/api/admin/metrics/reset", tags=["admin"])
    def reset_metrics():
        trace.reset()
        return {"reset": True}

    @api.get("/api/admin/traces", tags=["admin"])
    def traces(limit: int = Query(50, ge=1, le=150)):
        return list(trace.RECENT_REQUESTS)[:limit]

    # ------------------------------------------------------------------ explorer (read-only)
    def _explore(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except (ValueError, ReadOnlyViolation) as exc:
            raise HTTPException(400, str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(404, f"unknown item {exc}") from exc
        except redis.exceptions.ResponseError as exc:
            raise HTTPException(400, f"Redis error: {exc}") from exc

    @api.get("/api/explorer/redis", tags=["explorer"])
    def explore_redis(pattern: str = Query("*", max_length=100), limit: int = Query(200, ge=1, le=1000)):
        return _explore(explorer.redis_overview, pattern, limit)

    @api.get("/api/explorer/redis/key", tags=["explorer"])
    def explore_redis_key(key: str = Query(min_length=1, max_length=200)):
        return _explore(explorer.redis_key, key)

    @api.post("/api/explorer/redis/command", tags=["explorer"])
    def explore_redis_command(body: RedisCommand):
        return {"command": body.command, "result": _explore(explorer.redis_command, body.command)}

    @api.get("/api/explorer/mongo", tags=["explorer"])
    def explore_mongo():
        return explorer.mongo_overview()

    @api.post("/api/explorer/mongo/find", tags=["explorer"])
    def explore_mongo_find(body: MongoQuery):
        from pymongo.errors import OperationFailure
        try:
            return _explore(explorer.mongo_find, body.collection, body.filter, [list(s) for s in body.sort],
                            body.limit, body.skip)
        except OperationFailure as exc:
            raise HTTPException(400, f"MongoDB error: {exc.details.get('errmsg', exc) if exc.details else exc}") from exc

    @api.get("/api/explorer/mongo/presets", tags=["explorer"])
    def explore_mongo_presets():
        return {name: {"title": p["title"], "collection": p["collection"], "filter": p["filter"],
                       "sort": p.get("sort", [])} for name, p in MONGO_PRESETS.items()}

    @api.get("/api/explorer/mongo/presets/{name}", tags=["explorer"])
    def explore_mongo_preset(name: str):
        return _explore(explorer.mongo_preset, name)

    @api.get("/api/explorer/mongo/co-purchase/{product_id}", tags=["explorer"])
    def explore_mongo_co_purchase(product_id: str):
        return explorer.mongo_co_purchase(product_id)

    @api.get("/api/explorer/neo4j", tags=["explorer"])
    def explore_neo4j():
        return explorer.neo4j_overview()

    @api.get("/api/explorer/neo4j/neighborhood/{product_id}", tags=["explorer"])
    def explore_neo4j_neighborhood(product_id: str, buyers: int = Query(15, ge=1, le=60)):
        return explorer.neo4j_neighborhood(product_id, buyers)

    @api.post("/api/explorer/neo4j/query", tags=["explorer"])
    def explore_neo4j_query(body: CypherQuery):
        from neo4j.exceptions import ClientError
        try:
            return _explore(service.graph.run_read_only, body.query, body.params)
        except ClientError as exc:
            raise HTTPException(400, f"Cypher error: {exc.message}") from exc

    return api


app = create_app()
