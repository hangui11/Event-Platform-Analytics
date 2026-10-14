# Event Analytics Platform
A small production-shaped event analytics platform on Google Cloud: a shared event
envelope, a seeded generator, an HTTP ingestion service into Pub/Sub → BigQuery,
and a fast Analytics API with a daily dashboard on top. The seeded generator
produces **3 fictional apps** (`ledgerly` finance, `promptly` AI assistant,
`streamio` video) over a **30-day window**, ~400 users per app.


```
generator (seed=42) → events.jsonl
     │  replayed through the LIVE endpoint
     ▼
ingestion service (Node.js, Cloud Run) → Pub/Sub topic → BigQuery Subscription
     ▼
analytics.events  (partitioned by day, clustered by app, event_name)
     │  dedup + roll-up (once/day)
     ▼
analytics.daily_metrics  (one row per app/day — pre-aggregation)
     ▼
Analytics API (FastAPI, Cloud Run) — dedup · pre-aggregation · in-memory cache
     ▼
daily dashboard (static HTML, consumes the API)
```

---

## 1. Live endpoints (deployed, testable)

**Ingestion** — POST an event, it lands in BigQuery:
```
https://ingestion-265843273395.europe-west1.run.app/events
```
Sample request body (send as-is, Content-Type: application/json → returns 202):
```json
{
  "app": "ledgerly", "event_name": "purchase", "event_type": "financial",
  "user_id": "u_sample_1", "event_time": 1789000000000,
  "version": "3.1.0", "region": "US",
  "properties": { "product_id": "premium_monthly", "price": 8.99, "currency": "USD" }
}
```
Health: `GET /`

**Analytics API** — metrics as JSON:
```
https://analytics-api-265843273395.europe-west1.run.app
```
Interactive docs (try every endpoint): `/docs`. Example calls:
```
GET /metrics/revenue?app=promptly            # daily net revenue
GET /metrics/active_users?app=ledgerly       # daily active users
GET /metrics/feature_uses?app=streamio       # daily feature usage
GET /metrics/llm_tokens?app=promptly         # daily LLM token cost
GET /metrics/compare                         # all apps, one row each
GET /metrics/retention?day_n=7               # Day-N retention (1..30)
POST /admin/refresh                          # rebuild the summary + flush cache
GET /cache/stats                             # inspect the cache
```
Every response includes `cached` and `bytes_processed`, so query cost is visible.

**Dashboard** — the daily PM dashboard, live on the API above. Open
`dashboard/index.html` in a browser; it fetches from the Analytics API and renders
the four daily panels (active users, revenue, feature usage, LLM cost) per app.

---

## 2. What's in this repo

| File / folder | What |
|---------------|------|
| `README.md` | This file — front door: URLs, examples, key questions |
| `WRITEUP.pdf` | **The short writeup** — event format, table layout, performance strategy, numbers |
| `AI_worklog.pdf` | The AI work-log |
| `generator.py` | Seeded event generator (seed=42) → `events.jsonl` (3 apps × 30 days) |
| `bigquery_table.sql` | The `CREATE TABLE` DDL (partition + cluster) for `analytics.events` |
| `ingestion/` | Node.js ingestion service (`server.js`) + load tool (`sender.js`) |
| `api/` | Analytics API (`main.py`, `requirements.txt`, `Procfile`). All metric SQL is embedded in `main.py`. |
| `dashboard/` | The live daily dashboard (`index.html`) |

---

## 3. Writeup (headline numbers)

The full short writeup — event format, table layout, and performance strategy — is
in **`WRITEUP.pdf`**. The headline numbers:

**Pipeline (generated → landed):**
```
events generated:        40,887   (events.jsonl, seed=42)
rejected by service (400):  209    (109 invalid JSON + 100 missing a required field)
accepted (202):          40,678
rows landed in BigQuery: 40,580    (419 duplicate event_ids present)
unique after dedup:      40,161
```

**API performance (before → after, "promptly revenue per day"):**

| Tier | Source | Bytes scanned | Latency (cold, incl. network) |
|------|--------|:---:|:---:|
| 1 raw events (dedup + JSON) | `analytics.events` | ~3 MB | ~0.9 s |
| 2 pre-aggregated summary | `analytics.daily_metrics` | ~2.4 KB | ~0.75 s |
| 3 in-memory cache hit | server RAM | 0 B | ~0.12 s |

Tier 1 → Tier 2 is ~1,270× fewer bytes; the cache is ~5–7× faster and scans 0
bytes. It won't slow as history grows: the summary is one row per (app, day), so a
month is ~30 rows no matter how many billions of raw events accumulate. Every metric
was independently recomputed in Python and diffed row-by-row against BigQuery —
money/identity metrics match exactly, raw counts differ by 0.03% (explained by
malformed-data rejection + NULL-guards). Full detail is in `WRITEUP.pdf`.

---

## 4. Key Questions (from the brief)

**Q1 — What did you design into ingestion to improve query performance, and how
would we see it in BigQuery?**

The table is partitioned by `DATE(event_time)` and clustered by `app, event_name`.
In BigQuery you'd see it in the table's DETAILS tab ("Partitioned by DAY",
"Clustered by app, event_name") and in each query's bytes-scanned: a one-app,
few-day question prunes partitions and reads only that app's block. On top of the
raw table, the `daily_metrics` summary makes the common questions ~1,270× cheaper.

**Q2 — The same purchase arrives twice. How is revenue not double-counted, and
where does the protection live?**

Every event carries an `event_id`. Duplicates are allowed to LAND (Pub/Sub is
at-least-once; BigQuery doesn't enforce uniqueness on insert), and are removed at
QUERY time with `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at)`,
keeping one row per id. The protection lives in the SERVING/query layer, keyed on
`event_id` — so a purchase that arrived twice is summed once. (Proven: 419 duplicate
event_ids present in the landed data; revenue still matches the deduped expectation.)

**Q3 — Monday 9am, hundreds of dashboards hit the API asking the same questions.
What happens, and what did you build for it?**

Three defenses: 
- the API reads the pre-aggregated `daily_metrics` summary
(~2.4 KB), not the raw table (~3 MB).
- an in-memory cache serves repeated questions from RAM (0 bytes, ~0.12 s);
- BigQuery's own result cache backs that up. So the same query fired hundreds of
times mostly costs one BigQuery read, then RAM.

**Q4 — An app team designs a new event for a new feature without asking the data
team. Can they, end to end?**

Yes. The envelope's `properties` is JSON, so a new event type is just new keys in
`properties`. The ingestion service validates only the fixed required fields and
publishes; the BigQuery Subscription maps the known columns; the new payload lands
in `properties` with no schema change and no redeploy. They can query it
immediately with `JSON_VALUE(properties, '$.their_new_field')`.

---

## 5. Setup

**Generator** (Python): `python generator.py` → writes `events.jsonl` (seed=42, so
it's reproducible).

**Ingestion** (Node ≥ 20): `cd ingestion && npm install`, then
`gcloud auth application-default login`, `npm start` (listens on :8080). Load the
data through the live endpoint: `ENDPOINT=<url>/events node sender.js`.

**Analytics API** (Python 3.11): `cd api && pip install -r requirements.txt`,
`gcloud auth application-default login`, `uvicorn main:app --port 8080`. Build the
`daily_metrics` summary once via `POST /admin/refresh` (the rebuild SQL is embedded
in `main.py`).

**Dashboard**: open `dashboard/index.html` (it calls the live API).

**Deploy** (both services, from source): `gcloud run deploy <name> --source . --region
europe-west1 --allow-unauthenticated --set-env-vars ...` — pass each env var as its
OWN `--set-env-vars` flag (on PowerShell, comma-separated values get merged into one
invalid variable). The API's service account needs `roles/bigquery.jobUser` +
`roles/bigquery.dataEditor`.

---

## 6. AI use

AI (Claude) was used throughout as a pair-programmer, as the case expects. The
key prompts, the places I corrected it, and one prompt that failed (with the wrong
output and my fix) are in `AI_worklog.pdf`.
