# The three databases: what is stored and how to show it

Three ways to show each database: the app's **Explorer** page (`/explorer`), the database's **own UI**, or its **CLI** from the terminal.

---

## Redis 7.4 — key-value (sessions, carts)

| Key | Type | TTL | Content |
|---|---|---|---|
| `sess:{sessionId}` | HASH | 1800 s sliding | customerId, displayName, role, createdAt, lastSeenAt |
| `cart:{cartId}` | HASH | 604800 s sliding | productId → quantity |
| `cartmeta:{cartId}` | HASH | 604800 s sliding | userId, currency, createdAt, updatedAt, version |
| `usercart:{customerId}` | STRING | 604800 s sliding | the customer's active cartId |
| `checkout:idem:{key}` | STRING | 86400 s | `PROCESSING` or the orderId |

* Persistence: AOF on (`--appendonly yes`), data in the `redis_data` volume.
* Atomicity: cart changes run as **Lua scripts** (check limit, HSET or HINCRBY, bump version, refresh both TTLs in one atomic step). A missing cart is an empty cart.
* **UI:** Redis Commander, http://localhost:8082
* **CLI:**
  ```bash
  docker compose exec redis redis-cli
  SCAN 0 MATCH cart:* COUNT 100
  HGETALL cart:cart_xxxxxxxxxxxxxxxx
  TTL cart:cart_xxxxxxxxxxxxxxxx
  HGETALL sess:sess_xxxxx
  INFO keyspace
  ```

---

## MongoDB 8.0 — documents (catalog, orders, outbox)

Database `polyglot_commerce` runs on a single-node **replica set `rs0`**, which multi-document transactions require.

**products** — one document per product. Attributes and variants are embedded because the product page reads them together.
```json
{ "_id": "p_104", "sku": "SHOE-TRAIL-42", "slug": "trail-runner", "name": "Trail Runner",
  "category": {"id": "footwear", "name": "Footwear"}, "price": {"amount": 3499, "currency": "INR"},
  "status": "ACTIVE", "attributes": {"material": "mesh", "waterproof": true, "weightGrams": 290},
  "variants": [{"variantId": "v_40_bla", "size": 40, "color": "black", "stock": 12}], "tags": ["hiking"], ... }
```
**orders** — durable checkout record with **snapshots** (name, SKU and unit price at purchase time); the customer is referenced by ID.
**outbox** — one event per order (`ORDER_PLACED`) or product change (`PRODUCT_UPSERTED`); `status` PENDING → PROCESSING → PROCESSED, or DEAD.
**projector_state** — the worker's heartbeat and totals.

| Collection | Index | Serves |
|---|---|---|
| products | `sku_unique`, `slug_unique` (unique) | no duplicate catalog identities |
| products | `category_price` {category.id, price.amount} | category filter + price range/sort |
| products | `product_text` (name ×10, tags ×5, description) | search box |
| orders | `idempotency_key_unique` (unique) | exactly one order per checkout key |
| orders | `customer_history` {customer_id, created_at −1} | "my orders" |
| outbox | `event_id_unique`, `status_next_attempt`, `status_created`, `order_lookup` | projector queue, order → graph status |

Every collection except `projector_state` has a **`$jsonSchema` validator** (required fields, non-negative integer prices, status enums), so even a raw insert that bypasses the API is rejected.

* **UI:** Mongo Express, http://localhost:8081 (admin / admin123), or MongoDB Compass at `mongodb://localhost:27017/?directConnection=true`
* **CLI:**
  ```bash
  docker compose exec mongo mongosh polyglot_commerce
  db.products.findOne({_id: "p_104"})
  db.products.find({"category.id": "laptops"}, {name: 1, attributes: 1})
  db.products.find({"category.id": "footwear", "price.amount": {$lte: 4000}}).explain("executionStats").queryPlanner.winningPlan
  db.orders.find().sort({created_at: -1}).limit(1).pretty()
  db.outbox.aggregate([{$group: {_id: "$status", n: {$sum: 1}}}])
  db.getCollectionInfos({name: "products"})[0].options.validator
  db.products.getIndexes()
  ```

---

## Neo4j 5.26 — property graph (co-purchase)

```
(:Customer {customerId})-[:PURCHASED {eventId, productId, orderId, quantity, purchasedAt}]->(:Product {productId, categoryId, active})
(:Product)-[:IN_CATEGORY]->(:Category {categoryId, name})
```

* The graph is a **projection**: only completed MongoDB orders create `PURCHASED` edges, and only the worker writes it.
* Constraints: unique `Customer.customerId`, `Product.productId` and `Category.categoryId`; a range index on `PURCHASED.eventId`.
* Idempotency: `MERGE (c)-[r:PURCHASED {eventId, productId}]->(p)`, so a replayed event matches the existing edge.
* The graph holds no names, prices or personal data. Display data is hydrated from MongoDB.
* **UI:** Neo4j Browser, http://localhost:7474 (neo4j / review2password)
* **Queries:**
  ```cypher
  // recommendation baseline
  MATCH (buyer:Customer)-[:PURCHASED]->(seed:Product {productId:'p_104'})
  MATCH (buyer)-[:PURCHASED]->(candidate:Product)
  WHERE candidate <> seed AND candidate.active = true
  RETURN candidate.productId, count(DISTINCT buyer) AS sharedBuyers ORDER BY sharedBuyers DESC LIMIT 5;

  // picture of the 2-hop neighbourhood
  MATCH p=(:Product {productId:'p_104'})<-[:PURCHASED]-(:Customer)-[:PURCHASED]->(:Product) RETURN p LIMIT 40;

  // schema
  SHOW CONSTRAINTS; SHOW INDEXES;
  CALL db.schema.visualization();
  ```
* **CLI:** `docker compose exec neo4j cypher-shell -u neo4j -p review2password`
