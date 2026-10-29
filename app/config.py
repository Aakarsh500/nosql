"""Environment-based configuration (Review 1, NFR 'Portability')."""
import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "polyglot_commerce")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "review2password")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# Redis key lifetimes (Review 1, section 07)
SESSION_TTL = _int("SESSION_TTL", 1800)          # 30 min sliding
CART_TTL = _int("CART_TTL", 604800)              # 7 days sliding
IDEMPOTENCY_TTL = _int("IDEMPOTENCY_TTL", 86400)  # 24 hours
MAX_ITEM_QUANTITY = 99

# Outbox projector (Review 1, section 06)
OUTBOX_BATCH = _int("OUTBOX_BATCH", 200)
OUTBOX_MAX_ATTEMPTS = _int("OUTBOX_MAX_ATTEMPTS", 10)
OUTBOX_MAX_BACKOFF = _int("OUTBOX_MAX_BACKOFF", 30)
OUTBOX_LEASE_SECONDS = _int("OUTBOX_LEASE_SECONDS", 30)
WORKER_INTERVAL = float(os.getenv("WORKER_INTERVAL", "1.0"))
PROJECTION_LAG_TARGET = _int("PROJECTION_LAG_TARGET", 5)

# Latency targets from the non-functional requirements (milliseconds)
LATENCY_TARGETS = {"cart/session": 100, "catalog": 300, "recommendation": 500}

COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"
