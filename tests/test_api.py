"""Live integration tests: they run against the real Redis, MongoDB and Neo4j containers.

    docker compose exec app pytest -q
"""
from __future__ import annotations

import threading
import time
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app import config
from app.main import create_app
from app.outbox import new_event
from app.serialize import utcnow


@pytest.fixture(scope="module")
def client():
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture(scope="module")
def service(client):
    return client.app.state.service


TEST_PRODUCTS = r"^p_(test|cold|x)"
TEST_CUSTOMERS = r"^c_(test|race|proj|merge)"
CREATED_CARTS: list[str] = []


@pytest.fixture(scope="module", autouse=True)
def cleanup(service):
    """Remove everything the tests created so the demo data stays clean."""
    yield
    db = service.stores.mongo
    orders = [o["_id"] for o in db.orders.find({"customer_id": {"$regex": TEST_CUSTOMERS}}, {"_id": 1})]
    db.outbox.delete_many({"$or": [{"order_id": {"$in": orders}}, {"aggregate_id": {"$regex": TEST_PRODUCTS}}]})
    db.orders.delete_many({"_id": {"$in": orders}})
    db.products.delete_many({"_id": {"$regex": TEST_PRODUCTS}})
    service.stores.graph.execute_query(
        "MATCH (n) WHERE n.customerId =~ $c OR n.productId =~ $p DETACH DELETE n",
        {"c": TEST_CUSTOMERS.replace("^", "") + ".*", "p": TEST_PRODUCTS.replace("^", "") + ".*"},
        database_=service.stores.graph_db)
    for cart in CREATED_CARTS:
        service.stores.redis.delete(f"cart:{cart}", f"cartmeta:{cart}")
    for pattern in ("usercart:c_merge_*", "checkout:idem:test-*", "checkout:idem:empty-*"):
        for key in service.stores.redis.scan_iter(match=pattern):
            service.stores.redis.delete(key)


def uid() -> str:
    return uuid4().hex[:8]


def wait_for_status(service, query: dict, statuses: set[str], timeout: float = 15.0) -> dict:
    """The background worker may lease an event before the test's own projector run does,
    so wait for the event to reach its final status instead of assuming who processed it."""
    deadline = time.time() + timeout
    while True:
        service.projector.drain()
        event = service.stores.mongo.outbox.find_one(query)
        if event["status"] in statuses or time.time() > deadline:
            return event
        time.sleep(0.25)


def new_cart(client) -> str:
    cart = client.post("/api/carts").json()["id"]
    CREATED_CARTS.append(cart)
    return cart


# ---------------------------------------------------------------------- health
def test_all_three_stores_are_live(client):
    body = client.get("/api/health").json()
    assert body["status"] == "ok"
    assert all(body["stores"][s]["ok"] for s in ("redis", "mongo", "neo4j"))
    assert body["stores"]["mongo"]["replica_set"] == "rs0"


# ---------------------------------------------------------------------- MongoDB catalog
def test_product_crud_validation_and_outbox(client, service):
    s = uid()
    pid = f"p_test_{s}"
    body = {"id": pid, "sku": f"TEST-{s}", "slug": f"test-{s}", "name": "Test Flask",
            "category": {"id": "accessories", "name": "Accessories"}, "price": {"amount": 700},
            "attributes": {"volumeMl": 500}, "variants": [{"variantId": "v1", "stock": 3, "color": "red"}]}
    created = client.post("/api/products", json=body)
    assert created.status_code == 201, created.text
    assert created.json()["variants"][0]["color"] == "red"  # extra variant fields are kept
    assert client.post("/api/products", json=body).status_code == 409           # unique sku/slug/_id
    assert client.post("/api/products", json={**body, "id": "bad"}).status_code == 422
    assert client.post("/api/products", json={**body, "id": f"p_x{s}", "sku": f"X{s}", "slug": f"x-{s}",
                                              "price": {"amount": -5}}).status_code == 422

    patched = client.patch(f"/api/products/{pid}", json={"price": {"amount": 650},
                                                          "attributes": {"insulated": True, "volumeMl": None}})
    assert patched.status_code == 200
    assert patched.json()["price"]["amount"] == 650
    assert patched.json()["attributes"] == {"insulated": True}                  # merged + $unset
    found = client.get("/api/products", params={"category": "accessories", "attr": "insulated:true", "limit": 100})
    assert pid in {p["id"] for p in found.json()["items"]}

    assert client.delete(f"/api/products/{pid}").json()["status"] == "INACTIVE"
    assert pid not in {p["id"] for p in client.get("/api/products", params={"limit": 100}).json()["items"]}
    assert client.get(f"/api/products/{pid}").json()["status"] == "INACTIVE"    # soft delete keeps the doc
    events = list(service.stores.mongo.outbox.find({"aggregate_id": pid, "type": "PRODUCT_UPSERTED"}))
    assert len(events) == 3                                                       # create, patch, delete


def test_text_search_and_price_filter(client):
    res = client.get("/api/products", params={"q": "waterproof", "max_price": 5000}).json()
    assert res["total"] >= 1
    assert all(p["price"]["amount"] <= 5000 for p in res["items"])
    assert client.get("/api/products", params={"attr": "nonsense"}).status_code == 400


def test_mongo_validator_rejects_bad_document_even_without_api(service):
    from pymongo.errors import WriteError
    with pytest.raises(WriteError):
        service.stores.mongo.products.insert_one({"_id": f"p_raw{uid()}", "name": "no price"})


# ---------------------------------------------------------------------- Redis carts and sessions
def test_cart_operations_ttl_and_guards(client, service):
    cart = new_cart(client)
    r = client.post(f"/api/carts/{cart}/items/p_104/increment", json={"delta": 2})
    assert r.status_code == 200 and r.json()["items"] == {"p_104": 2}
    assert r.json()["subtotal"] == 2 * 3499
    assert client.put(f"/api/carts/{cart}/items/p_105", json={"quantity": 98}).status_code == 200
    assert client.post(f"/api/carts/{cart}/items/p_105/increment", json={"delta": 5}).status_code == 400  # > 99
    assert client.put(f"/api/carts/{cart}/items/p_150", json={"quantity": 1}).status_code == 400  # inactive
    assert client.put(f"/api/carts/{cart}/items/p_nope", json={"quantity": 1}).status_code == 400
    removed = client.delete(f"/api/carts/{cart}/items/p_105").json()
    assert removed["items"] == {"p_104": 2}
    r = service.stores.redis
    assert config.CART_TTL - 5 <= r.ttl(f"cart:{cart}") <= config.CART_TTL
    assert config.CART_TTL - 5 <= r.ttl(f"cartmeta:{cart}") <= config.CART_TTL
    assert int(r.hget(f"cartmeta:{cart}", "version")) >= 4
    assert client.get("/api/carts/cart_0000000000000000").status_code == 404
    assert client.get("/api/carts/not-a-cart").status_code == 404


def test_login_session_ttl_and_guest_cart_merge(service):
    customer = f"c_merge_{uid()}"
    with TestClient(create_app()) as c:
        first = new_cart(c)
        c.put(f"/api/carts/{first}/items/p_105", json={"quantity": 1})
        login = c.post("/api/sessions", json={"customer_id": customer, "display_name": "A", "cart_id": first}).json()
        assert login["cart_id"] == first and login["merged_lines"] == 0     # guest cart adopted
        sid = login["session"]["id"]
        assert config.SESSION_TTL - 5 <= service.stores.redis.ttl(f"sess:{sid}") <= config.SESSION_TTL
        assert c.get("/api/sessions/current").json()["customerId"] == customer  # HttpOnly cookie works
        assert c.delete("/api/sessions/current").json()["deleted"] is True
        assert c.get("/api/sessions/current").status_code == 404

        guest = new_cart(c)
        c.put(f"/api/carts/{guest}/items/p_105", json={"quantity": 2})
        c.put(f"/api/carts/{guest}/items/p_104", json={"quantity": 1})
        again = c.post("/api/sessions", json={"customer_id": customer, "cart_id": guest}).json()
        assert again["cart_id"] == first and again["merged_lines"] == 2      # merged into saved cart
        assert c.get(f"/api/carts/{first}").json()["items"] == {"p_105": 3, "p_104": 1}
        assert c.get(f"/api/carts/{guest}").status_code == 404               # guest keys deleted
        assert service.stores.redis.get(f"usercart:{customer}") == first


# ---------------------------------------------------------------------- checkout and idempotency
def test_checkout_is_idempotent_and_clears_cart(client, service):
    cart = new_cart(client)
    client.put(f"/api/carts/{cart}/items/p_104", json={"quantity": 2})
    client.put(f"/api/carts/{cart}/items/p_105", json={"quantity": 1})
    key = f"test-checkout-{uid()}"
    body = {"cart_id": cart, "customer_id": f"c_test_{uid()}"}
    first = client.post("/api/orders", json=body, headers={"Idempotency-Key": key})
    second = client.post("/api/orders", json=body, headers={"Idempotency-Key": key})
    assert (first.status_code, second.status_code) == (201, 200)
    order = first.json()["order"]
    assert order["id"] == second.json()["order"]["id"]
    assert order["total_amount"] == 2 * 3499 + 399
    assert [l["name_snapshot"] for l in order["line_items"]] == ["Trail Runner", "Hiking Socks"]
    assert service.stores.mongo.orders.count_documents({"idempotency_key": key}) == 1
    assert service.stores.mongo.outbox.count_documents({"order_id": order["id"]}) == 1
    assert client.get(f"/api/carts/{cart}").json()["items"] == {}
    assert service.stores.redis.get(f"checkout:idem:{key}") == order["id"]
    assert client.get(f"/api/orders/{order['id']}").json()["customer_id"] == body["customer_id"]


def test_concurrent_checkouts_with_one_key_create_one_order(client, service):
    cart = new_cart(client)
    client.put(f"/api/carts/{cart}/items/p_106", json={"quantity": 1})
    key = f"test-race-{uid()}"
    outcomes, errors = [], []

    def attempt():
        try:
            outcomes.append(service.checkout(cart, "c_race", key)[1])
        except Exception as exc:  # Conflict while the first request holds the Redis reservation
            errors.append(type(exc).__name__)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert outcomes.count(True) == 1
    assert set(errors) <= {"Conflict"}
    assert service.stores.mongo.orders.count_documents({"idempotency_key": key}) == 1


def test_checkout_rejects_empty_cart_and_bad_key(client):
    cart = new_cart(client)
    assert client.post("/api/orders", json={"cart_id": cart, "customer_id": "c_x"},
                       headers={"Idempotency-Key": f"empty-{uid()}"}).status_code == 400
    assert client.post("/api/orders", json={"cart_id": cart, "customer_id": "c_x"},
                       headers={"Idempotency-Key": "short"}).status_code == 422


# ---------------------------------------------------------------------- outbox -> Neo4j
def test_projection_is_idempotent_and_feeds_recommendations(client, service):
    cart = new_cart(client)
    client.put(f"/api/carts/{cart}/items/p_104", json={"quantity": 1})
    client.put(f"/api/carts/{cart}/items/p_105", json={"quantity": 1})
    customer = f"c_proj_{uid()}"
    order = client.post("/api/orders", json={"cart_id": cart, "customer_id": customer},
                        headers={"Idempotency-Key": f"test-proj-{uid()}"}).json()["order"]
    event = wait_for_status(service, {"order_id": order["id"]}, {"PROCESSED"})
    assert event["status"] == "PROCESSED"
    assert service.graph.purchases_for_event(event["_id"]) == 2
    # replaying the same event must not create a second purchase fact
    service.graph.project_orders([{**event["payload"], "eventId": event["_id"]}])
    assert service.graph.purchases_for_event(event["_id"]) == 2
    assert client.get(f"/api/orders/{order['id']}").json()["graph_projection"]["status"] == "PROCESSED"

    recs = client.get("/api/products/p_104/recommendations", params={"fallback": False}).json()
    assert recs["items"][0]["product"]["id"] == "p_105"
    assert "distinct customer" in recs["items"][0]["reason"]
    oracle = service.orders.co_purchase_counts("p_104", limit=5)
    assert recs["items"][0]["shared_buyers"] == oracle[0]["shared_buyers"]  # graph agrees with MongoDB
    jac = client.get("/api/products/p_104/recommendations", params={"strategy": "jaccard"}).json()
    assert all(0 < i["jaccard"] <= 1 for i in jac["items"] if i["source"] == "graph")


def test_poison_event_is_dead_lettered_without_blocking_others(service):
    bad = new_event("ORDER_PLACED", "ord_poison", {"orderId": "ord_poison", "customerId": None,
                                                   "purchasedAt": utcnow().isoformat(),
                                                   "items": [{"productId": "p_104", "quantity": 1}]})
    bad["attempts"] = config.OUTBOX_MAX_ATTEMPTS - 1
    service.stores.mongo.outbox.insert_one(bad)
    try:
        assert wait_for_status(service, {"_id": bad["_id"]}, {"DEAD"})["status"] == "DEAD"
        assert service.stores.mongo.outbox.count_documents({"status": "PENDING", "next_attempt_at": {"$lte": utcnow()}}) == 0
    finally:
        service.stores.mongo.outbox.delete_one({"_id": bad["_id"]})


def test_cold_start_fallback_for_unpurchased_product(client):
    s = uid()
    pid = f"p_cold_{s}"
    client.post("/api/products", json={"id": pid, "sku": f"COLD-{s}", "slug": f"cold-{s}", "name": "Cold Start Kit",
                                       "category": {"id": "outdoor", "name": "Outdoor Gear"}, "price": {"amount": 100}})
    recs = client.get(f"/api/products/{pid}/recommendations").json()
    assert recs["items"] and all(i["source"].startswith("fallback") for i in recs["items"])
    client.delete(f"/api/products/{pid}")


# ---------------------------------------------------------------------- explorer is read-only
def test_explorer_rejects_writes(client):
    assert client.post("/api/explorer/neo4j/query", json={"query": "MATCH (n) DETACH DELETE n"}).status_code == 400
    assert client.post("/api/explorer/neo4j/query", json={"query": "CREATE (:X)"}).status_code == 400
    assert client.post("/api/explorer/mongo/find", json={"collection": "products",
                                                         "filter": {"$where": "1"}}).status_code == 400
    assert client.post("/api/explorer/mongo/find", json={"collection": "system.users"}).status_code == 400
    assert client.post("/api/explorer/redis/command", json={"command": "FLUSHALL"}).status_code == 400
    assert client.post("/api/explorer/redis/command", json={"command": "KEYS *"}).status_code == 400
    assert client.post("/api/explorer/redis/command", json={"command": "DBSIZE"}).status_code == 200


def test_explain_shows_index_vs_collection_scan(client):
    indexed = client.get("/api/explorer/mongo/presets/category_price").json()["plan"]
    scan = client.get("/api/explorer/mongo/presets/unindexed_attribute").json()["plan"]
    assert indexed["uses_index"] and "category_price" in indexed["indexes"]
    assert scan["collection_scan"]
