"""Evaluation from the Review 1 plan (section 13): latency, correctness, idempotency,
recommendation quality and schema evolution. Results go to artifacts/evaluation.json.

    docker compose exec app python scripts/evaluate.py
"""
from __future__ import annotations

import json
import random
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import config  # noqa: E402
from app.db import get_stores  # noqa: E402
from app.services import Commerce  # noqa: E402
from app.trace import percentile  # noqa: E402

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
rng = random.Random(7)
stores = get_stores()
service = Commerce(stores)
http = httpx.Client(base_url=BASE, timeout=20)
results: dict = {"base_url": BASE, "started_at": time.strftime("%Y-%m-%d %H:%M:%S")}


def timed(fn):
    start = time.perf_counter()
    response = fn()
    return (time.perf_counter() - start) * 1000, response


def run_load(name, calls, concurrency, target=None):
    latencies, errors = [], 0

    def one(call):
        ms, r = timed(call)
        return ms, r.status_code

    start = time.perf_counter()
    with ThreadPoolExecutor(concurrency) as pool:
        for ms, status in pool.map(one, calls):
            latencies.append(ms)
            errors += status >= 400
    seconds = time.perf_counter() - start
    out = {"requests": len(calls), "concurrency": concurrency, "errors": errors,
           "throughput_rps": round(len(calls) / seconds, 1),
           "p50_ms": percentile(latencies, 50), "p95_ms": percentile(latencies, 95)}
    if target:
        out["target_p95_ms"] = target
        out["meets_target"] = out["p95_ms"] <= target
    results[name] = out
    print(f"{name:<28} {out}")
    return out


active = [p["_id"] for p in stores.mongo.products.find({"status": "ACTIVE"}, {"_id": 1})]
categories = stores.mongo.products.distinct("category.id")
skus = [p["sku"] for p in stores.mongo.products.find({}, {"sku": 1}).limit(200)]

# ---------------------------------------------------------------- 1. cart workload (Redis)
carts = [http.post("/api/carts").json()["id"] for _ in range(20)]
for c in carts:
    http.put(f"/api/carts/{c}/items/{active[0]}", json={"quantity": 1})


def cart_call():
    c, p = rng.choice(carts), rng.choice(active)
    kind = rng.random()
    if kind < 0.4:
        return lambda: http.get(f"/api/carts/{c}")
    if kind < 0.8:
        return lambda: http.post(f"/api/carts/{c}/items/{p}/increment", json={"delta": 1})
    return lambda: http.put(f"/api/carts/{c}/items/{p}", json={"quantity": rng.randint(1, 5)})


for concurrency in (10, 50):
    run_load(f"cart_mixed_c{concurrency}", [cart_call() for _ in range(1000)], concurrency,
             config.LATENCY_TARGETS["cart/session"])
for c in carts:
    stores.redis.delete(f"cart:{c}", f"cartmeta:{c}")

# ---------------------------------------------------------------- 2. catalog workload (MongoDB)


def catalog_call():
    kind = rng.random()
    if kind < 0.3:
        return lambda: http.get(f"/api/products/{rng.choice(active)}")
    if kind < 0.7:
        lo = rng.randrange(0, 5000, 500)
        return lambda: http.get("/api/products", params={"category": rng.choice(categories), "min_price": lo,
                                                         "max_price": lo + 20000, "sort": "price_asc"})
    if kind < 0.85:
        return lambda: http.get("/api/products", params={"q": rng.choice(["waterproof", "laptop", "travel", "running"])})
    return lambda: http.get("/api/products", params={"attr": "waterproof:true"})


run_load("catalog_mixed_c10", [catalog_call() for _ in range(500)], 10, config.LATENCY_TARGETS["catalog"])

# ---------------------------------------------------------------- 3. graph workload (Neo4j + hydrate)
run_load("recommendation_c10",
         [(lambda p=rng.choice(active): http.get(f"/api/products/{p}/recommendations")) for _ in range(300)],
         10, config.LATENCY_TARGETS["recommendation"])

# ---------------------------------------------------------------- 4. checkout duplicates
cart = http.post("/api/carts").json()["id"]
http.put(f"/api/carts/{cart}/items/p_104", json={"quantity": 1})
http.put(f"/api/carts/{cart}/items/p_105", json={"quantity": 1})
key = f"eval-{int(time.time())}"
with ThreadPoolExecutor(10) as pool:
    statuses = list(pool.map(lambda _: http.post("/api/orders", json={"cart_id": cart, "customer_id": "c_eval"},
                                                  headers={"Idempotency-Key": key}).status_code, range(10)))
for _ in range(20):  # retries after the first commit replay the stored order
    replay = http.post("/api/orders", json={"cart_id": cart, "customer_id": "c_eval"}, headers={"Idempotency-Key": key})
    if replay.status_code == 200:
        break
    time.sleep(0.1)
order_id = replay.json()["order"]["id"]
for _ in range(60):  # the background worker or this drain projects it, whichever leases it first
    service.projector.drain()
    event = stores.mongo.outbox.find_one({"order_id": order_id})
    if event["status"] == "PROCESSED":
        break
    time.sleep(0.25)
results["checkout_idempotency"] = {
    "concurrent_requests": 10, "status_codes": {str(s): statuses.count(s) for s in sorted(set(statuses))},
    "orders_with_key": stores.mongo.orders.count_documents({"idempotency_key": key}),
    "outbox_events_for_order": stores.mongo.outbox.count_documents({"order_id": order_id}),
    "graph_purchase_facts": service.graph.purchases_for_event(event["_id"]), "expected_facts": 2,
}
results["checkout_idempotency"]["pass"] = (results["checkout_idempotency"]["orders_with_key"] == 1
                                           and results["checkout_idempotency"]["graph_purchase_facts"] == 2)
print("checkout_idempotency        ", results["checkout_idempotency"])

# ---------------------------------------------------------------- 5. recommendation correctness vs MongoDB oracle
checked, matches = 0, 0
for pid in rng.sample(active, min(20, len(active))):
    oracle = {r["product_id"]: r["shared_buyers"] for r in service.orders.co_purchase_counts(pid, limit=1000)}
    graph = {r["productId"]: r["sharedBuyers"] for r in service.graph.recommend(
        pid, limit=1000, min_support=1, strategy="support", same_category=False)}
    oracle = {k: v for k, v in oracle.items() if k in set(active)}
    checked += 1
    matches += oracle == graph
results["recommendation_correctness"] = {"products_checked": checked, "exact_matches": matches,
                                         "pass": matches == checked}
print("recommendation_correctness  ", results["recommendation_correctness"])

# ---------------------------------------------------------------- 6. coverage and temporal hold-out precision
covered = sum(1 for pid in active if service.graph.recommend(pid, limit=1, min_support=1, strategy="support",
                                                             same_category=False))
orders = list(stores.mongo.orders.find({"status": "PLACED"}, {"customer_id": 1, "line_items.product_id": 1,
                                                              "created_at": 1}).sort("created_at", 1))
cut = int(len(orders) * 0.8)
buyers_by_customer = defaultdict(set)
for o in orders[:cut]:
    buyers_by_customer[o["customer_id"]].update(l["product_id"] for l in o["line_items"])
co = defaultdict(lambda: defaultdict(int))
for items in buyers_by_customer.values():
    for a in items:
        for b in items:
            if a != b:
                co[a][b] += 1
hits = trials = 0
precisions = []
for o in orders[cut:]:
    items = {l["product_id"] for l in o["line_items"]}
    if len(items) < 2:
        continue
    for seed_item in items:
        top = [p for p, _ in sorted(co[seed_item].items(), key=lambda kv: (-kv[1], kv[0]))[:5]]
        if not top:
            continue
        relevant = items - {seed_item}
        found = len(relevant & set(top))
        trials += 1
        hits += found > 0
        precisions.append(found / 5)
results["recommendation_quality"] = {
    "coverage": round(covered / len(active), 3), "active_products": len(active),
    "holdout_split": "first 80% of orders by time train, last 20% test",
    "hit_rate_at_5": round(hits / trials, 3) if trials else None,
    "precision_at_5": round(sum(precisions) / len(precisions), 3) if precisions else None, "trials": trials,
}
print("recommendation_quality      ", results["recommendation_quality"])

# ---------------------------------------------------------------- 7. schema evolution
before = {d["_id"]: d["updated_at"] for d in stores.mongo.products.find({}, {"updated_at": 1})}
laptops = stores.mongo.products.update_many({"category.id": "laptops", "attributes.ramGb": {"$exists": False}},
                                            [{"$set": {"attributes.ramGb": 16, "updated_at": "$$NOW"}}])
shoes = stores.mongo.products.update_many({"category.id": "footwear", "attributes.waterproof": {"$exists": False}},
                                          [{"$set": {"attributes.waterproof": False, "updated_at": "$$NOW"}}])
after = {d["_id"]: (d["updated_at"], d["category"]["id"]) for d in stores.mongo.products.find({}, {"updated_at": 1, "category": 1})}
untouched_changed = sum(1 for pid, (ts, cat) in after.items() if cat not in ("laptops", "footwear") and ts != before.get(pid))
results["schema_evolution"] = {
    "change": "add laptops.attributes.ramGb and footwear.attributes.waterproof",
    "laptops_modified": laptops.modified_count, "footwear_modified": shoes.modified_count,
    "other_documents_modified": untouched_changed, "migration_needed": False,
    "graph_rewrite_needed": False, "pass": untouched_changed == 0,
}
print("schema_evolution            ", results["schema_evolution"])

# ---------------------------------------------------------------- 8. polyglot overhead (honest cost)
results["polyglot_overhead"] = {
    "containers": ["redis", "mongo", "mongo-init", "neo4j", "seed", "app", "worker", "mongo-express", "redis-commander"],
    "databases_to_operate": 3, "cross_store_calls_per_checkout": "Redis 4-6 + MongoDB 3-4 (1 transaction)",
    "async_boundary": "MongoDB outbox -> Neo4j projector (eventual, target <= 5 s)",
    "projection_lag": service.outbox.summary(),
}

out = Path(__file__).resolve().parents[1] / "artifacts" / "evaluation.json"
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(results, indent=2, default=str))
print(f"\nwritten {out}")
