"""Session and cart state in Redis. This is the only module that reads or writes Redis keys.

Key-space (Review 1, section 07):
    sess:{sessionId}        HASH    customerId, displayName, role, createdAt, lastSeenAt   TTL 30 min sliding
    cart:{cartId}           HASH    productId -> quantity                                  TTL 7 days sliding
    cartmeta:{cartId}       HASH    userId?, currency, createdAt, updatedAt, version       TTL 7 days sliding
    usercart:{customerId}   STRING  cartId                                                 TTL 7 days sliding
    checkout:idem:{key}     STRING  PROCESSING | orderId                                   TTL 24 hours
"""
from __future__ import annotations

import re
import secrets

from . import config
from .serialize import utcnow
from .trace import op

CART_ID = re.compile(r"^cart_[0-9a-f]{16}$")
SESSION_ID = re.compile(r"^sess_[A-Za-z0-9_-]{20,64}$")
IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9_.:-]{8,120}$")

# Atomic, guarded quantity change: no read-modify-write across separate calls.
# Returns the new quantity, -1 if the cart does not exist, -2 if the limit is exceeded.
_MUTATE_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
local current = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
local qty = tonumber(ARGV[3])
if ARGV[2] == 'incr' then qty = current + qty end
if qty > tonumber(ARGV[4]) then return -2 end
if qty < 0 then qty = 0 end
if qty == 0 then redis.call('HDEL', KEYS[1], ARGV[1]) else redis.call('HSET', KEYS[1], ARGV[1], qty) end
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('HSET', KEYS[2], 'updatedAt', ARGV[6])
redis.call('EXPIRE', KEYS[2], ARGV[5])
if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('EXPIRE', KEYS[1], ARGV[5]) end
return qty
"""

# Merge a guest cart into a customer's cart after login, capping each line at the max quantity.
_MERGE_LUA = """
local items = redis.call('HGETALL', KEYS[1])
for i = 1, #items, 2 do
  local q = redis.call('HINCRBY', KEYS[3], items[i], tonumber(items[i + 1]))
  if q > tonumber(ARGV[1]) then redis.call('HSET', KEYS[3], items[i], ARGV[1]) end
end
redis.call('DEL', KEYS[1], KEYS[2])
redis.call('HINCRBY', KEYS[4], 'version', 1)
redis.call('HSET', KEYS[4], 'updatedAt', ARGV[3])
redis.call('EXPIRE', KEYS[4], ARGV[2])
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('EXPIRE', KEYS[3], ARGV[2]) end
return #items / 2
"""


class QuantityLimitExceeded(ValueError):
    pass


class SessionCartStore:
    def __init__(self, client):
        self.r = client
        self._mutate = client.register_script(_MUTATE_LUA)
        self._merge = client.register_script(_MERGE_LUA)

    # ------------------------------------------------------------------ sessions
    def create_session(self, customer_id: str, display_name: str) -> dict:
        session_id = "sess_" + secrets.token_urlsafe(24)
        key = f"sess:{session_id}"
        now = utcnow().isoformat()
        with op("redis", "HSET + EXPIRE", key):
            pipe = self.r.pipeline(transaction=True)
            pipe.hset(key, mapping={"customerId": customer_id, "displayName": display_name,
                                    "role": "customer", "createdAt": now, "lastSeenAt": now})
            pipe.expire(key, config.SESSION_TTL)
            pipe.execute()
        return {"id": session_id, "customerId": customer_id, "displayName": display_name,
                "role": "customer", "createdAt": now, "lastSeenAt": now, "ttl_seconds": config.SESSION_TTL}

    def get_session(self, session_id: str | None, touch: bool = True) -> dict | None:
        if not session_id or not SESSION_ID.match(session_id):
            return None
        key = f"sess:{session_id}"
        with op("redis", "HGETALL", key):
            data = self.r.hgetall(key)
        if not data:
            return None
        if touch:
            now = utcnow().isoformat()
            with op("redis", "HSET lastSeenAt + EXPIRE (sliding)", key):
                pipe = self.r.pipeline(transaction=True)
                pipe.hset(key, "lastSeenAt", now)
                pipe.expire(key, config.SESSION_TTL)
                pipe.execute()
            data["lastSeenAt"] = now
        with op("redis", "TTL", key):
            ttl = self.r.ttl(key)
        return {"id": session_id, **data, "ttl_seconds": max(ttl, 0)}

    def delete_session(self, session_id: str | None) -> bool:
        if not session_id or not SESSION_ID.match(session_id):
            return False
        with op("redis", "DEL", f"sess:{session_id}"):
            return bool(self.r.delete(f"sess:{session_id}"))

    # ------------------------------------------------------------------ carts
    def create_cart(self, customer_id: str | None = None) -> str:
        cart_id = "cart_" + secrets.token_hex(8)
        now = utcnow().isoformat()
        meta = {"currency": "INR", "createdAt": now, "updatedAt": now, "version": 1}
        if customer_id:
            meta["userId"] = customer_id
        with op("redis", "HSET + EXPIRE", f"cartmeta:{cart_id}"):
            pipe = self.r.pipeline(transaction=True)
            pipe.hset(f"cartmeta:{cart_id}", mapping=meta)
            pipe.expire(f"cartmeta:{cart_id}", config.CART_TTL)
            pipe.execute()
        return cart_id

    def get_cart(self, cart_id: str) -> dict | None:
        if not cart_id or not CART_ID.match(cart_id):
            return None
        with op("redis", "HGETALL cart + cartmeta, TTL", f"cart:{cart_id}"):
            pipe = self.r.pipeline(transaction=False)
            pipe.hgetall(f"cartmeta:{cart_id}")
            pipe.hgetall(f"cart:{cart_id}")
            pipe.ttl(f"cartmeta:{cart_id}")
            pipe.ttl(f"cart:{cart_id}")
            meta, items, meta_ttl, items_ttl = pipe.execute()
        if not meta:
            return None  # a missing cart is an empty cart, not an error for the caller to infer anything from
        return {"id": cart_id, "items": {k: int(v) for k, v in items.items()}, "meta": meta,
                "ttl_seconds": max(meta_ttl, 0), "items_ttl_seconds": max(items_ttl, 0) if items else None}

    def change_item(self, cart_id: str, product_id: str, *, quantity: int | None = None,
                    delta: int | None = None) -> int | None:
        if not cart_id or not CART_ID.match(cart_id):
            return None
        mode, value = ("set", quantity) if quantity is not None else ("incr", delta)
        command = "HSET" if mode == "set" else "HINCRBY"
        with op("redis", f"EVALSHA guarded {command} + EXPIRE", f"cart:{cart_id} {product_id}"):
            result = self._mutate(keys=[f"cart:{cart_id}", f"cartmeta:{cart_id}"],
                                  args=[product_id, mode, value, config.MAX_ITEM_QUANTITY,
                                        config.CART_TTL, utcnow().isoformat()])
        if result == -1:
            return None
        if result == -2:
            raise QuantityLimitExceeded(f"quantity cannot exceed {config.MAX_ITEM_QUANTITY}")
        return int(result)

    def clear_items(self, cart_id: str) -> None:
        with op("redis", "DEL cart + HINCRBY version", f"cart:{cart_id}"):
            pipe = self.r.pipeline(transaction=True)
            pipe.delete(f"cart:{cart_id}")
            pipe.hincrby(f"cartmeta:{cart_id}", "version", 1)
            pipe.hset(f"cartmeta:{cart_id}", "updatedAt", utcnow().isoformat())
            pipe.execute()

    def attach_to_customer(self, guest_cart_id: str | None, customer_id: str) -> tuple[str, int]:
        """Return the customer's active cart id and the number of guest lines merged into it."""
        user_key = f"usercart:{customer_id}"
        with op("redis", "GET", user_key):
            user_cart = self.r.get(user_key)
        guest_ok = bool(guest_cart_id and CART_ID.match(guest_cart_id))
        with op("redis", "EXISTS cartmeta (user, guest)", user_key):
            user_exists = bool(user_cart and self.r.exists(f"cartmeta:{user_cart}"))
            guest_exists = bool(guest_ok and self.r.exists(f"cartmeta:{guest_cart_id}"))
        merged = 0
        if user_exists:
            target = user_cart
            if guest_exists and guest_cart_id != user_cart:
                with op("redis", "EVALSHA merge guest cart", f"cart:{guest_cart_id} -> cart:{user_cart}"):
                    merged = int(self._merge(
                        keys=[f"cart:{guest_cart_id}", f"cartmeta:{guest_cart_id}",
                              f"cart:{user_cart}", f"cartmeta:{user_cart}"],
                        args=[config.MAX_ITEM_QUANTITY, config.CART_TTL, utcnow().isoformat()]))
        elif guest_exists:
            target = guest_cart_id
            with op("redis", "HSET cartmeta userId", f"cartmeta:{target}"):
                self.r.hset(f"cartmeta:{target}", "userId", customer_id)
        else:
            target = self.create_cart(customer_id)
        with op("redis", "SET usercart EX", user_key):
            self.r.set(user_key, target, ex=config.CART_TTL)
        return target, merged

    # ------------------------------------------------------------------ checkout idempotency fast path
    def reserve_checkout(self, key: str) -> tuple[bool, str | None]:
        """SET NX the key. Returns (reserved, existing_value)."""
        redis_key = f"checkout:idem:{key}"
        with op("redis", "SET NX EX", redis_key):
            if self.r.set(redis_key, "PROCESSING", nx=True, ex=config.IDEMPOTENCY_TTL):
                return True, None
            return False, self.r.get(redis_key)

    def complete_checkout(self, key: str, order_id: str) -> None:
        with op("redis", "SET EX", f"checkout:idem:{key}"):
            self.r.set(f"checkout:idem:{key}", order_id, ex=config.IDEMPOTENCY_TTL)

    def release_checkout(self, key: str) -> None:
        with op("redis", "DEL", f"checkout:idem:{key}"):
            self.r.delete(f"checkout:idem:{key}")

    def touch_user_cart(self, customer_id: str) -> None:
        with op("redis", "EXPIRE", f"usercart:{customer_id}"):
            self.r.expire(f"usercart:{customer_id}", config.CART_TTL)
