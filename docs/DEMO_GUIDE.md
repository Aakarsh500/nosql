# Demo guide for the review (about 15 minutes)

## Before the class (5 minutes)

1. Start **Docker Desktop** and wait for "Engine running".
2. In a terminal in the project folder:
   ```bash
   docker compose up -d
   docker compose run --rm seed python -m app.seed --reset
   docker compose restart app
   ```
   The `--reset` gives clean demo data. Restarting `app` zeroes the latency counters.
3. Check `docker compose ps`: `redis`, `mongo`, `neo4j`, `app` and `worker` are running; `mongo-init` and `seed` show `Exited (0)` (they are one-shot jobs, so that is correct).
4. Open these browser tabs in order: Shop `localhost:8001`, Dashboard `/dashboard`, Explorer `/explorer`, Neo4j Browser `localhost:7474`, Mongo Express `localhost:8081`, Redis Commander `localhost:8082`.

## 1. The idea (30 seconds)

> "One store can hold all the data, but not every access pattern fits it equally well. Carts are expiring key lookups, so they go in **Redis**. Products have different attributes per category, so they are **MongoDB** documents. 'Customers who bought this also bought' is a relationship traversal, so it runs in **Neo4j**. MongoDB is the source of truth for orders, and Neo4j is updated from an outbox."

Show the dashboard's **"Who owns what"** table.

## 2. Redis: session and cart (3 minutes)

1. **Shop:** click *Add to cart* on two products. In the **Database trace** panel, each click shows `Redis EVALSHA guarded HINCRBY + EXPIRE`, the atomic Lua script.
2. Point at the cart panel: key `cart:cart_…`, TTL **604,800 s = 7 days**, version number.
3. **Explorer → Redis tab:** click the `cart:*` chip, then click the key. Show the hash `productId → quantity`, with no names or prices. *"Price is re-read from MongoDB at checkout; Redis only holds working state."*
4. **Log in** as `c_0001`. Explain the session key `sess:…` with a **1800 s sliding TTL**, and that the guest cart was merged into the customer's saved cart (Lua merge).
5. **Redis Commander (:8082)** shows the same keys in a separate tool, proof that they live in Redis itself.

## 3. MongoDB: catalog documents (3 minutes)

1. **Shop:** pick the category *Laptops*, then *Footwear*. The "Keys used in this category" chips differ: laptops have `cpu, storageGb, screenInches`, shoes have `material, waterproof, weightGrams`. *"No sparse table, no migration."*
2. Open **Trail Runner → Details**. Show the embedded attributes, the embedded **variants** array, and the *Raw MongoDB document*.
3. **Explorer → MongoDB tab:**
   * Collection cards: counts, **unique indexes** (`sku_unique`, `idempotency_key_unique` …) and the **$jsonSchema validator**.
   * Preset *Attribute filter without an index*: **COLLSCAN**, every document examined.
   * Preset *Category + price range*: **IXSCAN on `category_price`**, only matching keys examined. *"This is why we designed that compound index."*
4. **Mongo Express (:8081, admin/admin123):** open `polyglot_commerce → products` to show the real documents.
5. **Admin page:** create a product with your own attributes, then edit or soft-delete it. *"Soft delete keeps old orders valid."*

## 4. Checkout: consistency and idempotency (3 minutes)

1. **Shop → Place order.** The trace shows: Redis `SET NX` on `checkout:idem:<key>`, MongoDB `txn: orders.insertOne + outbox.insertOne`, then Redis `DEL cart`. The result shows **201 Created**.
2. Click **Retry same key**. The result is **200 OK (replay)** with the *same order id*. *"A network retry can never create a second order: Redis catches it quickly and MongoDB's unique index guarantees it."*
3. **My orders** shows Graph `PENDING` → `PROCESSED` within about a second. That is the outbox projector running.
4. **Dashboard:** outbox backlog 0, projection lag about 1 s, worker running.

## 5. Neo4j: recommendations (3 minutes)

1. **Shop → Trail Runner → Details:** *"11 distinct customers who bought Trail Runner also bought Hiking Socks"*, plus the Cypher that ran. Switch the score to **Jaccard** to show reduced popularity bias.
2. **Explorer → Neo4j tab:** the radial graph shows Trail Runner in the centre, its buyers on the inner ring, and what they also bought on the outer ring. *"That's the 2-hop traversal."*
3. Run *Duplicate-fact check*. It returns **no rows**: retries never duplicate a purchase.
4. **Explorer → MongoDB → "Same question, two databases":** the MongoDB aggregation and the Neo4j traversal give **identical counts**. The MongoDB version needs `$unwind → $group → $unwind → $group`, while Cypher states the relationship directly.
5. **Neo4j Browser (:7474, neo4j / review2password):** run
   ```cypher
   MATCH p=(:Product {productId:'p_104'})<-[:PURCHASED]-(:Customer)-[:PURCHASED]->(:Product) RETURN p LIMIT 40
   ```
   for the native graph visualisation.

## 6. Failure handling (2 minutes)

```bash
powershell -ExecutionPolicy Bypass -File scripts/outage_demo.ps1
```

The script stops Neo4j and places 3 orders, which **still succeed** in MongoDB with their events pending. Recommendations degrade to a catalog fallback instead of crashing. When Neo4j restarts, the worker catches up (about 20 s, including Neo4j boot time). **Zero orders are lost.** Keep the Dashboard open while it runs: the health card turns red, the backlog rises, then drains.

## 7. Evidence (1 minute)

```bash
docker compose exec app pytest -q
docker compose exec app python scripts/evaluate.py
```

14 tests pass. The evaluation writes `artifacts/evaluation.json`: latencies vs. targets, idempotency, graph-vs-MongoDB correctness, HitRate@5, and schema evolution. Be honest about the one miss: at **50 concurrent** cart clients p95 exceeds 100 ms. That is because requests queue in a single Python API process, while Redis itself answers in about 4 ms. Adding API workers or replicas is the fix.

## Likely questions

* **Why not one relational database?** It could work. Here, though, expiry needs cleanup jobs, variable attributes need sparse or EAV tables, and co-purchase needs self-joins. We accept running 3 databases because the measured fit is better, and the dashboard shows that cost.
* **What if Redis dies?** Browsing still works, and cart endpoints return a clear 503. No order lives only in Redis.
* **What if the worker crashes mid-batch?** Events are leased for 30 s. After the lease expires another worker claims them, and MERGE makes the replay harmless.
* **Why is the graph only eventually consistent?** Because a checkout should not fail just because the recommendation store is down. The order is authoritative in MongoDB, and the lag is measured (target 5 s).
