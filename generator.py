import random
import uuid
import time
import json

# =====================================================================
# CONFIG
# =====================================================================
SEED = 42                       # reproducibility: same seed -> same output
NUM_USERS_PER_APP = 400         # KNOB: more users -> more events (emergent volume)

# Time window: users install on some day within it; events span the window.
DAYS = 30
DAY_MS = 24 * 3600 * 1000
START_TIME_MS = int(time.time() * 1000) - DAYS * DAY_MS   # window start (N days ago)

DUP_RATE = 0.01                 # ~1% of events are duplicated (same event_id)
MALFORMED_RATE = 0.01           # ~1% of events are corrupted (bad data)

# Region-based conversion: probability a user ever purchases, by region.
CONVERSION_RATE = {"US": 0.12, "EU": 0.10, "APAC": 0.06, "LATAM": 0.04}
# Region popularity (how users are distributed across regions).
REGION_WEIGHTS = {"US": 40, "EU": 30, "APAC": 20, "LATAM": 10}

# ---------- Per-app realistic configuration ----------
APPS = {
    "ledgerly": {  # finance tool (fictional)
        "versions": ["3.1.0", "3.2.0"],
        "products": [("basic_weekly", 2.99), ("premium_monthly", 8.99), ("premium_yearly", 79.99)],
        "features": ["add_budget", "link_bank", "export_report", "view_dashboard"],
        "endpoints": ["/sync/bank", "/dashboard"],
        "has_llm": False,
    },
    "promptly": {  # AI assistant (fictional) — the only app with llm_call events
        "versions": ["2.4.1", "2.5.0"],
        "products": [("credits_100", 9.99), ("pro_monthly", 21.99)],
        "features": ["new_chat", "regenerate", "export_pdf", "switch_model"],
        "endpoints": ["/generate", "/chat"],
        "has_llm": True,
    },
    "streamio": {  # content app (fictional)
        "versions": ["1.9.2", "2.0.0"],
        "products": [("basic_weekly", 4.99), ("standard_monthly", 9.99), ("premium_yearly", 99.99)],
        "features": ["play_video", "add_to_list", "search", "share"],
        "endpoints": ["/stream", "/search"],
        "has_llm": False,
    },
}

DEVICES = ["ios", "android"]
OS_VERSIONS = ["16.5", "17.2", "18.0", "14.0"]
MODELS = [("quasar-mini-2", "astral-ai"), ("quasar-pro", "astral-ai"), ("nova-1", "nebula")]

# Map each event to its category (top-level event_type column)
EVENT_TYPE = {
    "install": "engagement",
    "session_start": "engagement", "session_end": "engagement", "feature_used": "engagement",
    "purchase": "financial", "renewal": "financial", "refund": "financial",
    "api_error": "backend", "latency": "backend", "llm_call": "backend",
}


# ---------- Build the flexible `properties` per event type ----------
def build_properties(event_name, cfg):
    if event_name == "install":
        return {"device": random.choice(DEVICES), "os_version": random.choice(OS_VERSIONS)}
    if event_name == "feature_used":
        return {"feature": random.choice(cfg["features"]), "screen": "home"}
    if event_name == "purchase":
        product, price = random.choice(cfg["products"])
        return {"product_id": product, "price": price, "currency": "USD",
                "payment_method": random.choice(["card", "paypal"])}
    if event_name == "renewal":
        product, price = random.choice(cfg["products"])
        return {"product_id": product, "price": price, "currency": "USD"}
    if event_name == "refund":
        product, price = random.choice(cfg["products"])
        return {"product_id": product, "amount": price, "currency": "USD",
                "reason": random.choice(["user_request", "chargeback", "fraud"])}
    if event_name == "api_error":
        return {"error_code": random.choice(["500", "502", "429"]),
                "endpoint": random.choice(cfg["endpoints"]), "message": "internal error"}
    if event_name == "latency":
        return {"endpoint": random.choice(cfg["endpoints"]), "latency_ms": random.randint(50, 3000)}
    if event_name == "llm_call":
        model, provider = random.choice(MODELS)
        return {"model": model, "provider": provider,
                "input_tokens": random.randint(50, 2000), "output_tokens": random.randint(20, 800)}
    return {}


# ---------- Build a full event (envelope + properties) ----------
# region is fixed per user; event_time/properties can be overridden so paired
# events (a session's start and end) share a session_id and consistent times.
def build_event(app, event_name, user_id, cfg, region, event_time, version, properties=None):
    if properties is None:
        properties = build_properties(event_name, cfg)
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "app": app,
        "event_name": event_name,
        "event_type": EVENT_TYPE[event_name],
        "user_id": user_id,
        "event_time": event_time,
        "version": version,
        "region": region,
        "properties": properties,
    }


# ---------- Map a product_id to its billing period (for renewal timing) ----------
def product_period(product_id):
    if "weekly" in product_id:
        return "weekly"
    if "monthly" in product_id:
        return "monthly"
    if "yearly" in product_id:
        return "yearly"
    return "one_off"   # e.g. credits_100 — not a subscription


# ---------- Corrupt an event to test the pipeline's error handling ----------
# Corruptions target THREE failure layers:
#   1. PARSE layer   -> invalid JSON that cannot even be deserialized
#   2. SCHEMA layer  -> valid JSON but missing field / wrong type
#   3. VALUE layer   -> valid schema but null payload
def corrupt_event(event):
    kind = random.choice(["invalid_json", "missing_field", "bad_type", "missing_props"])
    if kind == "invalid_json":
        good = json.dumps(event)
        broken = random.choice([
            good[:-1],                    # truncated: missing final "}"
            good.replace(":", "=", 1),    # bad syntax: "=" instead of ":"
            good + " }}",                 # extra stray braces
            good.replace('"', "'", 2),    # single quotes -> invalid JSON
        ])
        return {"__raw__": broken}
    bad = dict(event)
    if kind == "missing_field":
        bad.pop(random.choice(["app", "event_name", "user_id"]), None)  # SCHEMA
    elif kind == "bad_type":
        bad["event_time"] = "not_a_timestamp"                            # SCHEMA
    elif kind == "missing_props":
        bad["properties"] = None                                         # VALUE
    return bad


# ---------- Draw a user's active-period length (casual/regular/loyal) ----------
def draw_active_period():
    tier = random.choices(["casual", "regular", "loyal"], weights=[60, 30, 10])[0]
    if tier == "casual":
        return random.randint(1, 3)
    if tier == "regular":
        return random.randint(4, 10)
    return random.randint(11, DAYS)   # loyal


# ---------- The main generator (per-user lifecycle) ----------
def generate_events():
    random.seed(SEED)
    events = []
    stats = {"base": 0, "duplicates": 0, "malformed": 0, "installs": 0, "converted": 0}

    def emit(event):
        """Apply malformed/duplicate injection to one event, then append it."""
        if random.random() < MALFORMED_RATE:
            event = corrupt_event(event)
            stats["malformed"] += 1
        events.append(event)
        stats["base"] += 1
        if random.random() < DUP_RATE:
            events.append(dict(event))
            stats["duplicates"] += 1

    regions = list(REGION_WEIGHTS.keys())
    region_w = list(REGION_WEIGHTS.values())

    # ---- One session: linked start/end + engagement/backend events ----
    # Defined inside generate_events() so it can use emit()/stats. A user's FIRST
    # purchase happens here (buying requires opening the app = being in a session).
    # It MUTATES `sub` in place (the caller carries `sub` across sessions/days).
    # Returns the session's (start_time, end_time) so day-level financial events
    # can be timestamped within the day.
    def run_session(app, user, cfg, region, day, floor_ms, day_end_ms, version,
                    converted, sub):
        session_id = f"s_{uuid.uuid4().hex[:8]}"
        device = random.choice(DEVICES)
        duration_sec = random.randint(5, 1800)
        # start no earlier than floor_ms, and end no later than the day's end,
        # so a session never spills into the next day (keeps global time order
        # consistent with the per-day version).
        latest_start = max(floor_ms, day_end_ms - duration_sec * 1000)
        start_time = random.randint(floor_ms, latest_start)
        end_time = min(start_time + duration_sec * 1000, day_end_ms)

        # linked session start/end (same session_id + same version)
        emit(build_event(app, "session_start", user, cfg, region, start_time, version,
                         properties={"session_id": session_id, "device": device}))
        emit(build_event(app, "session_end", user, cfg, region, end_time, version,
                         properties={"session_id": session_id, "duration_sec": duration_sec}))

        # a few feature_used events during the session
        for _ in range(random.randint(0, 4)):
            t = random.randint(start_time, end_time)
            emit(build_event(app, "feature_used", user, cfg, region, t, version))

        # occasional backend events
        if random.random() < 0.3:
            emit(build_event(app, "latency", user, cfg, region,
                             random.randint(start_time, end_time), version))
        if random.random() < 0.15:
            emit(build_event(app, "api_error", user, cfg, region,
                             random.randint(start_time, end_time), version))
        if cfg["has_llm"] and random.random() < 0.6:   # AI app: LLM calls
            for _ in range(random.randint(1, 3)):
                emit(build_event(app, "llm_call", user, cfg, region,
                                 random.randint(start_time, end_time), version))

        # ---- PURCHASE: user's first purchase, in-app (once per user, in-session) --
        # `ever_purchased` never resets, so a refunded user does NOT buy again.
        if converted and not sub["ever_purchased"]:
            product, price = random.choice(cfg["products"])
            sub["ever_purchased"] = True
            sub["active"] = True
            sub["day"] = day
            sub["type"] = product_period(product)      # weekly/monthly/yearly/one_off
            sub["product"] = product                   # remember WHAT they bought
            sub["price"] = price
            sub["last_renewal_day"] = day              # cycle anchor for renewals
            stats["converted"] += 1
            emit(build_event(app, "purchase", user, cfg, region,
                             random.randint(start_time, end_time), version,
                             properties={"product_id": product, "price": price,
                                         "currency": "USD",
                                         "payment_method": random.choice(["card", "paypal"])}))

        return start_time, end_time

    for app, cfg in APPS.items():
        for i in range(NUM_USERS_PER_APP):
            user = f"u_{app}_{i}"

            # ---- lifecycle decisions, precomputed per user ----
            region = random.choices(regions, weights=region_w)[0]     # fixed per user
            install_day = random.randint(0, DAYS - 1)                 # day index in window
            active_days = draw_active_period()
            last_day = min(install_day + active_days, DAYS)           # clamp to window
            converted = random.random() < CONVERSION_RATE[region]     # region-based

            # subscription state, carried across the user's sessions/days.
            #   ever_purchased  -> True once the user has EVER bought (never resets;
            #                      prevents re-purchase after a refund)
            #   active          -> True while a subscription is live (a refund sets
            #                      it False, stopping renewals)
            sub = {"ever_purchased": False, "active": False, "day": None,
                   "type": None, "product": None, "price": None,
                   "last_renewal_day": None}

            # version starts at the OLDEST and only ever increases over time
            version_index = 0
            version = cfg["versions"][version_index]

            # ---- INSTALL event: the user's FIRST event ----
            # install happens in the FIRST HALF of the install day, so there is
            # always room for that day's sessions to follow it within the day.
            install_ms = START_TIME_MS + install_day * DAY_MS + random.randint(0, DAY_MS // 2)
            emit(build_event(app, "install", user, cfg, region, install_ms, version))
            stats["installs"] += 1

            # ---- ACTIVE PERIOD: loop over the user's active days ----
            for day in range(install_day, last_day):
                day_start_ms = START_TIME_MS + day * DAY_MS
                day_end_ms = day_start_ms + DAY_MS

                # some days the user does not open the app (realistic gaps)
                if random.random() < 0.25:
                    continue

                # occasionally the app updates to a newer version (never older)
                if random.random() < 0.3:
                    version_index = min(version_index + 1, len(cfg["versions"]) - 1)
                    version = cfg["versions"][version_index]

                # ensure the install day's events start AFTER the install moment
                floor_ms = install_ms if day == install_day else day_start_ms

                # 1-3 SEQUENTIAL sessions this day (each = linked start/end +
                # in-session events). Sessions do NOT overlap: each one starts
                # after the previous ended (a user has one session at a time). We
                # move a `cursor` forward as the floor for the next session, and
                # stop early if the day runs out of room.
                # `sub` is a dict, mutated in place by run_session.
                cursor = floor_ms
                for _ in range(random.randint(1, 3)):
                    if cursor >= day_end_ms - 5000:      # no room left in the day
                        break
                    _, s_end = run_session(app, user, cfg, region, day, cursor,
                                           day_end_ms, version, converted, sub)
                    cursor = s_end                        # next session starts after this one
                day_last_end = cursor

                # ---- DAY-LEVEL FINANCIAL EVENTS (at most one each per day) ----
                # Renewal and refund are NOT tied to a session: a renewal is an
                # automatic billing charge, and a refund is requested via the app
                # store / support — neither requires opening the app. They get a
                # timestamp after the day's activity, within the day.
                fin_time = random.randint(day_last_end, day_end_ms)

                # RENEWAL: automatic, once per billing cycle (not per day/session),
                # only while the sub is active and the period elapsed, same product.
                if sub["active"] and sub["type"] != "one_off":
                    period = {"weekly": 7, "monthly": 30, "yearly": 365}[sub["type"]]
                    due = day >= sub["last_renewal_day"] + period
                    if due and random.random() < 0.5:
                        emit(build_event(app, "renewal", user, cfg, region, fin_time, version,
                                         properties={"product_id": sub["product"],
                                                     "price": sub["price"], "currency": "USD"}))
                        sub["last_renewal_day"] = day     # advance the cycle anchor

                # REFUND: once per day, within 3 days of purchase; cancels the sub.
                if sub["active"] and day <= sub["day"] + 3 and random.random() < 0.02:
                    emit(build_event(app, "refund", user, cfg, region, fin_time, version,
                                     properties={"product_id": sub["product"],
                                                 "amount": sub["price"], "currency": "USD",
                                                 "reason": random.choice(["user_request", "chargeback", "fraud"])}))
                    sub["active"] = False                 # refund cancels the subscription (stops future renewals)

            # ---- after last_day: user is silent = CHURNED (nothing to emit) ----

    random.shuffle(events)   # interleave apps/users/times like a real stream
    return events, stats


if __name__ == "__main__":
    events, stats = generate_events()
    print(f"Generated {len(events)} events total")
    print(f"  base events:      {stats['base']}")
    print(f"  installs (users): {stats['installs']}")
    print(f"  converted users:  {stats['converted']}")
    print(f"  duplicates added: {stats['duplicates']}")
    print(f"  malformed:        {stats['malformed']}")

    with open("events.jsonl", "w") as f:
        for e in events:
            if isinstance(e, dict) and "__raw__" in e:
                f.write(e["__raw__"] + "\n")     # invalid JSON: raw broken text
            else:
                f.write(json.dumps(e) + "\n")
    print("Wrote events.jsonl")

    print("\nSample (valid) events:")
    shown = 0
    for e in events:
        if isinstance(e, dict) and "__raw__" not in e and all(k in e for k in ("app", "event_name")):
            print(json.dumps(e, indent=2))
            shown += 1
            if shown == 3:
                break
