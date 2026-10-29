# Polyglot Persistence for an E-Commerce Application

**Redis** for sessions and carts · **MongoDB** for the catalog, orders and outbox · **Neo4j** for the co-purchase graph.

Team: Aakarsh Bhardwaj (23BCE0341) · Pratham Rajshree (23BCE0342) · Aditya Sinha (23BCE0263), BCSE406L NoSQL Databases.

One application, three databases, each chosen for the access pattern it serves:

| Workload | Store | Why |
|---|---|---|
| Session, cart | Redis | Known key, tiny atomic updates, automatic expiry (TTL) |
| Product catalog | MongoDB | Category-specific attributes and embedded variants in one document |
| Orders + outbox | MongoDB | Durable, self-contained snapshot; order + event written in one transaction |
| "Bought together" | Neo4j | 2-hop relationship traversal over customers and products |

## Run it (Docker)

Requirements: Docker Desktop running. From this folder:

```bash
docker compose up --build -d
```

The first start takes 1-2 minutes. Compose starts, in order: Redis, MongoDB (as replica set `rs0`), Neo4j, a one-shot **seed** job (27 products, 60 customers, 240 orders, projected into Neo4j), then the API, the projector worker, and two database UIs.

| What | URL | Login |
|---|---|---|
| Shop (storefront) | http://localhost:8001 | — |
| Admin (product CRUD) | http://localhost:8001/admin | — |
| Review dashboard | http://localhost:8001/dashboard | — |
| Database explorer | http://localhost:8001/explorer | — |
| Interactive API docs | http://localhost:8001/docs | — |
| Neo4j Browser | http://localhost:7474 | `neo4j` / `review2password` |
| Mongo Express | http://localhost:8081 | `admin` / `admin123` |
| Redis Commander | http://localhost:8082 | — |

You can also connect MongoDB Compass to `mongodb://localhost:27017/?directConnection=true`, and redis-cli to `localhost:6379`.

Useful commands:

```bash
docker compose ps                                            # everything should be running/healthy (seed + mongo-init exit 0)
docker compose exec app pytest -q                            # 14 live integration tests
docker compose exec app python scripts/evaluate.py           # evaluation -> artifacts/evaluation.json
powershell -ExecutionPolicy Bypass -File scripts/outage_demo.ps1   # stop Neo4j, order, restart, catch-up
docker compose run --rm seed python -m app.seed --reset      # wipe and re-seed the demo data
docker compose run --rm seed python -m app.seed --reset --large    # 500 products / 1,000 customers / 10,000 orders
docker compose down                                          # stop (data kept in volumes)
docker compose down -v                                       # stop and delete all data
```

## Architecture

```
Browser (Shop / Admin / Dashboard / Explorer)
        │  every response carries X-Correlation-ID and X-DB-Trace (which store did what, how long)
        ▼
FastAPI app ──► sessions_carts.py ──► Redis      sess:*, cart:*, cartmeta:*, usercart:*, checkout:idem:*
            ──► catalog.py / orders.py / outbox.py ──► MongoDB   products, orders, outbox
            ──► graph.py (reads only) ──► Neo4j
                                                  ▲
worker.py (projector) ── claims outbox events ────┘  MERGE Customer-[:PURCHASED]->Product (idempotent)
```

* **Checkout** re-reads prices from MongoDB, then inserts the order **and** its `ORDER_PLACED` outbox event in one MongoDB transaction. The `Idempotency-Key` header is reserved in Redis (`SET NX`) for fast duplicate suppression, and the unique index on `orders.idempotency_key` stays authoritative. The cart is cleared afterwards on a best-effort basis.
* **The API never writes to Neo4j.** The worker leases pending events, MERGEs them into the graph, and marks them `PROCESSED`. On failure it retries with exponential backoff (2, 4, 8 … 30 s) and moves the event to `DEAD` after 10 attempts. Replaying an event cannot duplicate a purchase, because the relationship key is `(eventId, productId)`.
* **Recommendations** run a bounded 2-hop Cypher query (shared buyers, optional Jaccard), hydrate names and prices from MongoDB, and explain every result. Cold-start products fall back to popular items in the same category. If Neo4j is down, the API returns a catalog fallback with `degraded` set.
* **Admin product changes** also go through the outbox (`PRODUCT_UPSERTED`), so the graph learns about category and active-flag changes. Renaming a product needs no graph rewrite, because the graph stores IDs only.

## Repository map

| Path | Contents |
|---|---|
| `app/main.py` | REST API, request tracing middleware, error mapping (503 when a store is down) |
| `app/services.py` | Business flows: login and cart merge, checkout, recommendations |
| `app/sessions_carts.py` | All Redis access (Lua scripts for atomic, guarded cart updates and merges) |
| `app/catalog.py`, `app/orders.py`, `app/outbox.py` | MongoDB access |
| `app/graph.py` | Neo4j projection and queries |
| `app/projector.py`, `app/worker.py` | Outbox → Neo4j projector with lease, backoff and dead letters |
| `app/db.py` | Connections, MongoDB `$jsonSchema` validators and indexes, Neo4j constraints |
| `app/explorer.py` | Read-only database explorer (SCAN, find + explain, read-only Cypher) |
| `app/seed.py` | Deterministic synthetic data (demo and `--large`) |
| `app/static/` | Shop, Admin, Dashboard and Explorer pages |
| `tests/test_api.py` | Live integration tests (they clean up after themselves) |
| `scripts/evaluate.py`, `scripts/outage_demo.ps1` | Evaluation and failure-injection demo |
| `docs/DEMO_GUIDE.md` | Step-by-step presentation script |
| `docs/DATABASES.md` | How to show and explain each database |

## Measured results (local Docker, demo dataset)

From `artifacts/evaluation.json` and `artifacts/outage_recovery.json`:

| Check | Result |
|---|---|
| Cart mixed ops, 10 concurrent clients | p95 64 ms (target 100) ✔ |
| Cart mixed ops, 50 concurrent clients | p95 449 ms ✘ — requests queue in the single API process; Redis itself answered in ~4 ms |
| Catalog queries, 10 concurrent | p95 47 ms (target 300) ✔ |
| Recommendations, 10 concurrent | p95 263 ms (target 500) ✔ |
| 10 concurrent checkouts with one key | 1 order, 1 outbox event, 2 graph facts (one × 201, nine × 409 "in progress") ✔ |
| Graph vs. MongoDB oracle co-purchase counts | 20 / 20 products identical ✔ |
| Recommendation coverage / HitRate@5 / Precision@5 (temporal 80/20) | 1.00 / 0.85 / 0.25 |
| Schema evolution (add `laptops.ramGb`) | 2 laptop docs changed, 0 other docs touched, no migration ✔ |
| Neo4j stopped during checkout | orders committed, events waited, all projected ~20 s after restart, 0 lost ✔ |

Out of scope, as in Review 1: payments, stock reservation, production authentication, sharding and multi-region deployment. All data is synthetic.
