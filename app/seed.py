"""Deterministic synthetic data (no real customers, no payment data).

    python -m app.seed                 # curated demo: 27 products, 60 customers, ~240 orders (skips if seeded)
    python -m app.seed --reset         # wipe all three stores and re-seed the demo dataset
    python -m app.seed --reset --large # Review 1 scale: 500 products, 1,000 customers, 10,000 orders

Orders are written straight into MongoDB with their ORDER_PLACED outbox events, then the normal
projector drains the outbox into Neo4j - the same path a live checkout takes."""
from __future__ import annotations

import argparse
import random
import time
from datetime import timedelta

from .catalog import upsert_event
from .db import get_stores
from .outbox import new_event
from .serialize import utcnow
from .services import Commerce

CATEGORIES = {
    "footwear": "Footwear", "apparel": "Apparel", "bags": "Bags", "accessories": "Accessories",
    "laptops": "Laptops", "electronics": "Electronics", "outdoor": "Outdoor Gear",
}


def P(pid, sku, name, cat, price, attrs, theme, desc, tags, variants=None, status="ACTIVE"):
    return {"_id": pid, "sku": sku, "slug": name.lower().replace(" ", "-").replace('"', "").replace("/", "-"),
            "name": name, "description": desc, "category": {"id": cat, "name": CATEGORIES[cat]},
            "price": {"amount": price, "currency": "INR"}, "status": status, "attributes": attrs,
            "variants": variants or [], "media": [{"url": f"/static/img/{cat}.svg", "alt": name}],
            "tags": tags, "schema_version": 1, "_theme": theme}


def _sizes(prefix, sizes, colors, stock=12):
    return [{"variantId": f"{prefix}_{s}_{c[:3]}".lower(), "size": s, "color": c, "stock": stock}
            for s in sizes for c in colors]


# The first four products keep the IDs used in the Review 1 and Review 2 reports.
CURATED = [
    # hiking
    P("p_104", "SHOE-TRAIL-42", "Trail Runner", "footwear", 3499,
      {"material": "mesh", "waterproof": True, "weightGrams": 290, "soleType": "Vibram lug"}, "hiking",
      "Lightweight waterproof trail shoe with a grippy lugged sole.", ["hiking", "trail", "waterproof", "running"],
      _sizes("v", [40, 41, 42, 43, 44], ["black", "olive"])),
    P("p_105", "SOCK-HIKE-OS", "Hiking Socks", "apparel", 399,
      {"fabric": "merino wool", "size": "one size", "packOf": 2, "cushioned": True}, "hiking",
      "Cushioned merino socks that stay dry on long hikes.", ["hiking", "socks", "wool"]),
    P("p_106", "BAG-CITY-20", "City Backpack", "bags", 1899,
      {"capacityLiters": 20, "laptopSleeveInches": 15, "waterResistant": True}, "office",
      "Slim everyday backpack with a padded laptop sleeve.", ["office", "commute", "laptop"]),
    P("p_107", "MUG-TRAVEL-350", "Travel Mug", "accessories", 599,
      {"volumeMl": 350, "material": "stainless steel", "leakProof": True}, "office",
      "Double-wall insulated mug that fits car cup holders.", ["coffee", "commute", "travel"]),
    P("p_108", "JKT-RAIN-SHELL", "Rain Shell Jacket", "apparel", 4299,
      {"fabric": "nylon ripstop", "waterproofRatingMm": 10000, "hood": True, "packable": True}, "hiking",
      "Packable waterproof shell for sudden mountain showers.", ["hiking", "rain", "waterproof", "jacket"],
      _sizes("v_jkt", ["S", "M", "L", "XL"], ["red", "navy"], 8)),
    P("p_109", "POLE-TREK-AL", "Trekking Poles", "outdoor", 1599,
      {"material": "aluminium", "collapsible": True, "lengthCm": 135, "pair": True}, "hiking",
      "Collapsible aluminium poles with cork grips.", ["hiking", "trekking", "poles"]),
    P("p_110", "LAMP-HEAD-300", "Headlamp 300", "outdoor", 999,
      {"lumens": 300, "batteryType": "AAA", "ipRating": "IPX4", "redLight": True}, "hiking",
      "300-lumen headlamp with red night mode.", ["camping", "hiking", "light"]),
    P("p_111", "BTL-INSUL-1L", "Insulated Bottle 1L", "accessories", 899,
      {"volumeMl": 1000, "material": "stainless steel", "keepsColdHours": 24}, "hiking",
      "Keeps water cold for a full day on the trail.", ["hiking", "bottle", "hydration"]),
    P("p_112", "BAG-TREK-40", "Trekking Backpack 40L", "bags", 3999,
      {"capacityLiters": 40, "rainCover": True, "hipBelt": True}, "hiking",
      "Ventilated 40-litre pack with an integrated rain cover.", ["hiking", "backpack", "trekking"]),
    # office / work
    P("p_120", "LAP-ULTRA-14", "UltraBook 14", "laptops", 74999,
      {"cpu": "Ryzen 7 7840U", "storageGb": 512, "screenInches": 14, "weightKg": 1.3}, "office",
      "Thin and light 14-inch laptop for work and study.", ["laptop", "office", "study"]),
    P("p_121", "LAP-WORK-16", "WorkStation 16", "laptops", 124999,
      {"cpu": "Core i7-13700H", "storageGb": 1024, "screenInches": 16, "gpu": "RTX 4060"}, "office",
      "16-inch performance laptop with a dedicated GPU.", ["laptop", "creator", "gaming"]),
    P("p_122", "SLV-LAP-14", "Laptop Sleeve 14in", "bags", 799,
      {"fitsScreenInches": 14, "material": "neoprene", "waterResistant": True}, "office",
      "Snug neoprene sleeve for 13-14 inch laptops.", ["laptop", "sleeve", "office"]),
    P("p_123", "MSE-WL-4K", "Wireless Mouse", "electronics", 1299,
      {"connectivity": "Bluetooth", "dpi": 4000, "batteryLifeDays": 70}, "office",
      "Quiet multi-device Bluetooth mouse.", ["office", "mouse", "wireless"]),
    P("p_124", "HUB-USBC-7", "USB-C Hub 7-in-1", "electronics", 2499,
      {"ports": 7, "powerDeliveryW": 100, "hdmi4k": True}, "office",
      "HDMI 4K, SD card and 100 W pass-through charging.", ["office", "usb-c", "laptop"]),
    P("p_125", "HP-ANC-30", "Noise-Cancelling Headphones", "electronics", 8999,
      {"batteryLifeHours": 30, "anc": True, "connectivity": "Bluetooth 5.3"}, "travel",
      "Over-ear ANC headphones for flights and focus.", ["travel", "music", "office"]),
    P("p_126", "NB-DOT-A5", "Dotted Notebook A5", "accessories", 349,
      {"pages": 192, "paperGsm": 100, "binding": "lay-flat"}, "office",
      "Lay-flat dotted notebook with 100 gsm paper.", ["stationery", "office", "study"]),
    # running
    P("p_130", "SHOE-ROAD-RC", "Road Racer Shoes", "footwear", 5499,
      {"material": "engineered knit", "dropMm": 8, "waterproof": False, "weightGrams": 230}, "running",
      "Responsive road shoe for tempo runs and races.", ["running", "road", "race"],
      _sizes("v_rr", [40, 41, 42, 43, 44], ["white"])),
    P("p_131", "SHRT-RUN-5", "Running Shorts", "apparel", 999,
      {"fabric": "recycled polyester", "linerIncluded": True, "inseamInches": 5}, "running",
      "Light shorts with a built-in liner and zip pocket.", ["running", "shorts"],
      _sizes("v_sh", ["S", "M", "L"], ["black"])),
    P("p_132", "SOCK-SPORT-3", "Sports Socks 3-Pack", "apparel", 449,
      {"fabric": "cotton blend", "packOf": 3, "size": "UK 6-10"}, "running",
      "Breathable ankle socks for daily training.", ["running", "socks", "gym"]),
    P("p_133", "BAND-FIT-14", "Fitness Band", "electronics", 2999,
      {"batteryLifeDays": 14, "heartRate": True, "waterResistance": "5ATM", "gps": False}, "running",
      "Tracks steps, heart rate and sleep for two weeks per charge.", ["running", "fitness", "tracker"]),
    P("p_134", "BELT-RUN-RF", "Running Belt", "accessories", 699,
      {"capacityLiters": 0.5, "reflective": True, "fitsPhoneInches": 6.8}, "running",
      "Bounce-free belt for phone and keys.", ["running", "belt", "reflective"]),
    # travel
    P("p_140", "TRL-CABIN-55", "Cabin Trolley 55cm", "bags", 6999,
      {"capacityLiters": 38, "wheels": 4, "tsaLock": True, "shell": "polycarbonate"}, "travel",
      "Hard-shell cabin trolley with spinner wheels.", ["travel", "luggage", "cabin"]),
    P("p_141", "PIL-NECK-MF", "Memory Foam Neck Pillow", "accessories", 899,
      {"material": "memory foam", "washableCover": True}, "travel",
      "Ergonomic neck pillow for long journeys.", ["travel", "comfort", "flight"]),
    P("p_142", "ADP-UNIV-150", "Universal Travel Adapter", "electronics", 1499,
      {"countries": 150, "usbPorts": 3, "usbC": True}, "travel",
      "One adapter for 150 countries with fast USB-C.", ["travel", "adapter", "charging"]),
    P("p_143", "CUBE-PACK-6", "Packing Cubes Set", "bags", 1199,
      {"pieces": 6, "material": "nylon", "compression": True}, "travel",
      "Six compression cubes to organise a suitcase.", ["travel", "organiser", "packing"]),
    P("p_144", "PWR-BANK-20K", "Power Bank 20000mAh", "electronics", 1999,
      {"capacityMah": 20000, "fastCharge": True, "outputW": 22.5}, "travel",
      "Charges a phone four times; airline friendly.", ["travel", "charging", "power"]),
    # inactive product: present in MongoDB, hidden from the storefront and recommendations
    P("p_150", "PHN-FLIP-RETRO", "Retro Flip Phone", "electronics", 2499,
      {"batteryLifeDays": 20, "dualSim": True}, "office",
      "Discontinued feature phone kept for order history.", ["phone", "retro"], status="INACTIVE"),
]

# Products that customers of a theme usually buy together, most popular first.
THEMES = {
    "hiking": ["p_104", "p_105", "p_108", "p_111", "p_109", "p_110", "p_112"],
    "office": ["p_120", "p_106", "p_123", "p_124", "p_122", "p_107", "p_126", "p_121"],
    "running": ["p_130", "p_132", "p_131", "p_133", "p_134"],
    "travel": ["p_140", "p_143", "p_142", "p_141", "p_125", "p_144", "p_107"],
}

# Strong pairs that make the graph's explanation easy to verify by hand.
COMPANIONS = {"p_104": ("p_105", 0.75), "p_120": ("p_106", 0.6), "p_130": ("p_132", 0.6), "p_140": ("p_143", 0.6)}

LARGE_ATTRS = {
    "footwear": lambda r: {"material": r.choice(["mesh", "leather", "knit", "canvas"]), "waterproof": r.random() < .4,
                           "weightGrams": r.randrange(200, 450, 10)},
    "apparel": lambda r: {"fabric": r.choice(["cotton", "wool", "polyester", "linen"]), "size": r.choice("SMLX"),
                          "fit": r.choice(["slim", "regular", "relaxed"])},
    "bags": lambda r: {"capacityLiters": r.choice([10, 15, 20, 30, 40, 55]), "waterResistant": r.random() < .6},
    "accessories": lambda r: {"material": r.choice(["steel", "bamboo", "silicone", "leather"]),
                              "volumeMl": r.choice([250, 350, 500, 750])},
    "laptops": lambda r: {"cpu": r.choice(["Ryzen 5", "Ryzen 7", "Core i5", "Core i7", "M-series"]),
                          "storageGb": r.choice([256, 512, 1024]), "screenInches": r.choice([13, 14, 15.6, 16])},
    "electronics": lambda r: {"connectivity": r.choice(["Bluetooth", "Wi-Fi", "USB-C"]),
                              "batteryLifeHours": r.choice([8, 12, 20, 30, 40])},
    "outdoor": lambda r: {"material": r.choice(["aluminium", "carbon", "nylon"]), "weightGrams": r.randrange(80, 2000, 20)},
}
ADJECTIVES = ["Classic", "Pro", "Lite", "Urban", "Summit", "Nomad", "Aero", "Flex", "Prime", "Trail", "Metro", "Zen"]
NOUNS = {"footwear": "Sneaker", "apparel": "Tee", "bags": "Pack", "accessories": "Flask", "laptops": "Notebook PC",
         "electronics": "Gadget", "outdoor": "Kit"}


def build_dataset(large: bool):
    rng = random.Random(20260823)
    products = [dict(p) for p in CURATED]
    themes = {k: list(v) for k, v in THEMES.items()}
    if large:
        extra_themes = [f"theme_{i}" for i in range(12)]
        for t in extra_themes:
            themes[t] = []
        cats = list(CATEGORIES)
        for i in range(1, 500 - len(CURATED) + 1):
            cat = cats[i % len(cats)]
            theme = extra_themes[i % len(extra_themes)] if i % 5 else rng.choice(list(THEMES))
            name = f"{rng.choice(ADJECTIVES)} {NOUNS[cat]} {i:03d}"
            pid = f"p_g{i:04d}"
            products.append(P(pid, f"GEN-{cat[:3].upper()}-{i:04d}", name, cat, rng.randrange(199, 20000, 50),
                              LARGE_ATTRS[cat](rng), theme, f"Generated {CATEGORIES[cat].lower()} item {i}.",
                              [cat, theme]))
            themes[theme].append(pid)
        customers, orders_n = 1000, 10_000
    else:
        customers, orders_n = 60, 240
    theme_names = list(themes)
    customer_themes = {}
    for c in range(1, customers + 1):
        primary = theme_names[(c - 1) % len(theme_names)] if c <= len(theme_names) * 3 else rng.choice(theme_names)
        customer_themes[f"c_{c:04d}"] = (primary, rng.choice([t for t in theme_names if t != primary]))
    price = {p["_id"]: p for p in products}
    now = utcnow()
    orders = []
    customer_ids = list(customer_themes)
    for n in range(orders_n):
        cid = customer_ids[n % customers] if n < customers else rng.choice(customer_ids)
        primary, secondary = customer_themes[cid]
        pool = themes[primary]
        weights = [1 / (rank + 1) ** 0.8 for rank in range(len(pool))]  # Zipf-like popularity skew
        picked = set(rng.choices(pool, weights=weights, k=rng.choice([1, 2, 2, 3])))
        for pid in list(picked):
            if pid in COMPANIONS and rng.random() < COMPANIONS[pid][1]:
                picked.add(COMPANIONS[pid][0])
        if rng.random() < 0.15:
            picked.add(rng.choice(themes[secondary]))
        created = now - timedelta(days=120) + timedelta(minutes=int(n * (120 * 24 * 60) / orders_n))
        lines = []
        for pid in sorted(picked):
            prod = price[pid]
            qty = 2 if rng.random() < 0.1 else 1
            lines.append({"product_id": pid, "sku": prod["sku"], "name_snapshot": prod["name"],
                          "category_id": prod["category"]["id"], "unit_price": prod["price"]["amount"],
                          "quantity": qty, "line_total": prod["price"]["amount"] * qty})
        orders.append({"_id": f"ord_seed_{n + 1:05d}", "customer_id": cid, "idempotency_key": f"seed-order-{n + 1}",
                       "status": "PLACED", "line_items": lines, "item_count": sum(l["quantity"] for l in lines),
                       "total_amount": sum(l["line_total"] for l in lines), "currency": "INR",
                       "created_at": created, "source": "seed"})
    return products, orders


def reset(stores) -> None:
    stores.redis.flushdb()
    for name in ("products", "orders", "outbox", "projector_state"):
        stores.mongo.drop_collection(name)
    with stores.graph.session(database=stores.graph_db) as session:
        session.run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS").consume()
    stores.graph_schema_ready = False
    stores.ensure_mongo_schema()
    stores.ensure_graph_schema()


def seed(large: bool = False, do_reset: bool = False) -> dict:
    stores = get_stores()
    stores.ensure_mongo_schema()
    stores.ensure_graph_schema()
    if do_reset:
        reset(stores)
    elif stores.mongo.products.estimated_document_count() > 0:
        return {"skipped": True, "reason": "catalog already seeded (use --reset to rebuild)",
                "products": stores.mongo.products.count_documents({})}
    started = time.perf_counter()
    products, orders = build_dataset(large)
    now = utcnow()
    product_docs = []
    for p in products:
        doc = {k: v for k, v in p.items() if k != "_theme"}
        doc["created_at"] = doc["updated_at"] = now
        product_docs.append(doc)
    stores.mongo.products.insert_many(product_docs)
    stores.mongo.outbox.insert_many([upsert_event(d) for d in product_docs])
    for i in range(0, len(orders), 1000):
        chunk = orders[i:i + 1000]
        stores.mongo.orders.insert_many(chunk)
        events = []
        for o in chunk:
            payload = {"orderId": o["_id"], "customerId": o["customer_id"], "purchasedAt": o["created_at"].isoformat(),
                       "items": [{"productId": l["product_id"], "categoryId": l["category_id"],
                                  "quantity": l["quantity"]} for l in o["line_items"]]}
            events.append(new_event("ORDER_PLACED", o["_id"], payload, order_id=o["_id"]))
        stores.mongo.outbox.insert_many(events)
    projected = Commerce(stores).projector.drain(batch=1000)
    return {"products": len(product_docs), "orders": len(orders),
            "customers": len({o["customer_id"] for o in orders}), "projected_events": projected,
            "seconds": round(time.perf_counter() - started, 2)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--large", action="store_true", help="500 products, 1,000 customers, 10,000 orders")
    parser.add_argument("--reset", action="store_true", help="wipe Redis, MongoDB and Neo4j data first")
    args = parser.parse_args()
    print(seed(large=args.large, do_reset=args.reset), flush=True)
