"""Request tracing: which store served each operation, and how long it took.

Every repository call is wrapped in ``op(store, operation, detail)``. The HTTP middleware
collects the operations of one request under a correlation ID and returns them in the
``X-DB-Trace`` header, which the browser shows in its database trace panel (Review 1, F8).
"""
from __future__ import annotations

import contextvars
import re
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field

from .serialize import utcnow


@dataclass
class _RequestState:
    correlation_id: str
    record_stats: bool
    ops: list = field(default_factory=list)


_current: contextvars.ContextVar[_RequestState | None] = contextvars.ContextVar("db_trace", default=None)

STORE_LATENCIES: dict[str, deque] = defaultdict(lambda: deque(maxlen=5000))
FLOW_LATENCIES: dict[str, deque] = defaultdict(lambda: deque(maxlen=5000))
RECENT_REQUESTS: deque = deque(maxlen=150)


def begin(correlation_id: str, record_stats: bool = True):
    state = _RequestState(correlation_id, record_stats)
    return state, _current.set(state)


def end(token) -> None:
    _current.reset(token)


@contextmanager
def op(store: str, operation: str, detail: str = ""):
    start = time.perf_counter()
    error = None
    try:
        yield
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        ms = (time.perf_counter() - start) * 1000
        state = _current.get()
        if state is None or state.record_stats:
            STORE_LATENCIES[store].append(ms)
        if state is not None:
            entry = {"store": store, "op": operation, "detail": detail[:120], "ms": round(ms, 2)}
            if error:
                entry["error"] = error
            state.ops.append(entry)


_FLOWS = [
    ("recommendation", re.compile(r"^/api/products/[^/]+/recommendations$")),
    ("catalog", re.compile(r"^/api/(products|categories)(/[^/]+)?$")),
    ("cart/session", re.compile(r"^/api/(carts|sessions)(/.*)?$")),
    ("checkout", re.compile(r"^/api/orders$")),
    ("orders", re.compile(r"^/api/orders/.+$")),
]


def classify(method: str, path: str) -> str | None:
    for name, pattern in _FLOWS:
        if pattern.match(path):
            if name == "checkout" and method != "POST":
                return "orders"
            return name
    return None


def record_request(state: _RequestState, method: str, path: str, status: int, ms: float) -> None:
    flow = classify(method, path)
    if flow:
        FLOW_LATENCIES[flow].append(ms)
    if state.record_stats:
        RECENT_REQUESTS.appendleft({
            "correlation_id": state.correlation_id, "method": method, "path": path, "flow": flow,
            "status": status, "ms": round(ms, 2), "at": utcnow().isoformat(), "ops": state.ops[:40],
        })


def percentile(values, pct: float):
    data = sorted(values)
    if not data:
        return None
    k = (len(data) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(data) - 1)
    return round(data[lo] + (data[hi] - data[lo]) * (k - lo), 2)


def summarize(samples: dict[str, deque]) -> dict:
    return {name: {"count": len(v), "p50_ms": percentile(v, 50), "p95_ms": percentile(v, 95),
                   "max_ms": round(max(v), 2) if v else None}
            for name, v in sorted(samples.items())}


def reset() -> None:
    STORE_LATENCIES.clear()
    FLOW_LATENCIES.clear()
    RECENT_REQUESTS.clear()
