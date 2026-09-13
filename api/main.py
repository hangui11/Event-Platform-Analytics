"""
Run locally:  uvicorn main:app --reload --port 8080
Docs:         http://localhost:8080/docs   (FastAPI auto-generates them)
"""

import os
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

import uvicorn

# ---------------------------------------------------------------------------
# Config — project/dataset come from env so the same code runs locally and on
# Cloud Run. Defaults match this case's project.
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "project-bd5f1c88-1252-4a0a-a34")
DATASET = os.environ.get("DATASET", "analytics")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "300"))  # 5 min

EVENTS = f"`{PROJECT_ID}.{DATASET}.events`"
DAILY = f"`{PROJECT_ID}.{DATASET}.daily_metrics`"

VALID_APPS = {"ledgerly", "promptly", "streamio"}

app = FastAPI(
    title="Event Analytics API",
    description="Fast metrics over the event pipeline (pre-aggregated + cached).",
    version="1.0.0",
)

# CORS: the dashboard is a static HTML page that calls this API from the
# browser. Without CORS the browser blocks cross-origin fetches. This is a
# read-only analytics API, so allowing any origin for GET is fine for the demo;
# in production you'd restrict allow_origins to the dashboard's domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# One BigQuery client, reused across requests (created lazily on first use).
_bq: Optional[bigquery.Client] = None


def bq() -> bigquery.Client:
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=PROJECT_ID)
    return _bq


# ---------------------------------------------------------------------------
# In-memory cache. A dict keyed on the request, each entry stamped
# with the time it was stored. On a HIT within TTL we return the cached rows
# and never touch BigQuery. This is the 0-byte / sub-ms tier.
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, list[dict], int]] = {}  # key -> (stored_at, rows, bytes)


def run_query(sql: str, params: list, cache_key: str) -> dict:
    """Run a parameterized query with caching. Returns rows + perf metadata."""
    now = time.time()

    # cache HIT
    hit = _cache.get(cache_key)
    if hit and (now - hit[0]) < CACHE_TTL_SECONDS:
        return {
            "cached": True,
            "bytes_processed": 0,        # served from RAM, no BigQuery scan
            "rows": hit[1],
        }

    # cache MISS -> query BigQuery
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = bq().query(sql, job_config=job_config)
    rows = [dict(r) for r in job.result()]
    bytes_processed = job.total_bytes_processed or 0

    _cache[cache_key] = (now, rows, bytes_processed)
    return {
        "cached": False,
        "bytes_processed": bytes_processed,
        "rows": rows,
    }


def check_app(app_name: str) -> None:
    if app_name not in VALID_APPS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown app '{app_name}'. valid: {sorted(VALID_APPS)}",
        )


@app.get("/")
def health():
    return {"status": "ok", "service": "event-analytics-api"}


@app.get("/cache/stats")
def cache_stats():
    """How many entries are cached and how old (useful for the demo)."""
    now = time.time()
    return {
        "ttl_seconds": CACHE_TTL_SECONDS,
        "entries": [
            {"key": k, "age_seconds": round(now - v[0], 1), "rows": len(v[1])}
            for k, v in _cache.items()
        ],
    }


# ---------------------------------------------------------------------------
# Admin: rebuild the daily_metrics summary from the raw events table.
#
# This is the "refresh" that keeps the summary current. In production it runs
# on a schedule (e.g. Cloud Scheduler -> this endpoint, or a scheduled query),
# and this endpoint also lets you trigger it on demand (e.g. after a big load).
# ---------------------------------------------------------------------------
@app.post("/admin/refresh")
def refresh_daily_metrics():
    sql = f"""
        CREATE OR REPLACE TABLE {DAILY}
        PARTITION BY day
        CLUSTER BY app
        AS
        WITH dedup AS (
          SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at) AS rn
            FROM {EVENTS}
          ) WHERE rn = 1
        )
        SELECT
          app,
          DATE(event_time) AS day,
          COUNT(DISTINCT CASE WHEN event_name='install' THEN user_id END) AS installs,
          COUNT(DISTINCT user_id) AS active_users,
          COUNT(DISTINCT CASE WHEN event_name='purchase' THEN user_id END) AS paying_users,
          ROUND(
            SUM(CASE WHEN event_name IN ('purchase','renewal')
                     THEN CAST(JSON_VALUE(properties,'$.price') AS FLOAT64) ELSE 0 END)
            - SUM(CASE WHEN event_name='refund'
                     THEN CAST(JSON_VALUE(properties,'$.amount') AS FLOAT64) ELSE 0 END)
          , 2) AS revenue,
          COUNT(CASE WHEN event_name='refund' THEN 1 END) AS refunds,
          COUNT(CASE WHEN event_name='feature_used'
                     AND JSON_VALUE(properties,'$.feature') IS NOT NULL
                     THEN 1 END) AS feature_uses,
          SUM(CASE WHEN event_name='llm_call'
                      AND JSON_VALUE(properties,'$.input_tokens')  IS NOT NULL
                      AND JSON_VALUE(properties,'$.output_tokens') IS NOT NULL
                   THEN CAST(JSON_VALUE(properties,'$.input_tokens')  AS INT64)
                      + CAST(JSON_VALUE(properties,'$.output_tokens') AS INT64)
                   ELSE 0 END) AS llm_tokens,
          COUNT(*) AS total_events,
          CURRENT_TIMESTAMP() AS refreshed_at
        FROM dedup
        GROUP BY app, day
    """
    job = bq().query(sql)
    job.result()  # wait for the rebuild to finish

    _cache.clear()  # flush stale cached rows so reads see fresh data

    # how many summary rows now exist
    count_job = bq().query(f"SELECT COUNT(*) AS n FROM {DAILY}")
    n = list(count_job.result())[0]["n"]
    return {
        "status": "refreshed",
        "summary_rows": n,
        "bytes_processed": job.total_bytes_processed or 0,
        "cache_flushed": True,
    }


# ---------------------------------------------------------------------------
# Metric endpoints — additive metrics read from the SUMMARY table (fast).
# Each returns per-day rows for one app, optionally bounded by a date range.
# ---------------------------------------------------------------------------
def _daily_metric(app_name: str, column: str, start: Optional[str], end: Optional[str]):
    """Read one additive column per day from daily_metrics."""
    check_app(app_name)
    where = ["app = @app"]
    params = [bigquery.ScalarQueryParameter("app", "STRING", app_name)]
    if start:
        where.append("day >= @start")
        params.append(bigquery.ScalarQueryParameter("start", "DATE", start))
    if end:
        where.append("day <= @end")
        params.append(bigquery.ScalarQueryParameter("end", "DATE", end))

    sql = f"""
        SELECT day, {column} AS value
        FROM {DAILY}
        WHERE {' AND '.join(where)}
        ORDER BY day
    """
    key = f"daily:{column}:{app_name}:{start}:{end}"
    result = run_query(sql, params, key)
    return {
        "app": app_name,
        "metric": column,
        "cached": result["cached"],
        "bytes_processed": result["bytes_processed"],
        "series": result["rows"],
    }


@app.get("/metrics/revenue")
def revenue(app: str = Query(...), start: Optional[str] = None, end: Optional[str] = None):
    return _daily_metric(app, "revenue", start, end)


@app.get("/metrics/installs")
def installs(app: str = Query(...), start: Optional[str] = None, end: Optional[str] = None):
    return _daily_metric(app, "installs", start, end)


@app.get("/metrics/active_users")
def active_users(app: str = Query(...), start: Optional[str] = None, end: Optional[str] = None):
    # daily distinct — correct per day (not summable across days)
    return _daily_metric(app, "active_users", start, end)


@app.get("/metrics/feature_uses")
def feature_uses(app: str = Query(...), start: Optional[str] = None, end: Optional[str] = None):
    return _daily_metric(app, "feature_uses", start, end)


@app.get("/metrics/llm_tokens")
def llm_tokens(app: str = Query(...), start: Optional[str] = None, end: Optional[str] = None):
    return _daily_metric(app, "llm_tokens", start, end)


# ---------------------------------------------------------------------------
# Compare apps — leadership table. Additive columns SUM across days from the
# summary; active/paying users are left out here because they are NOT summable
# (see /metrics/active_users for the per-day distinct series).
# ---------------------------------------------------------------------------
@app.get("/metrics/compare")
def compare(start: Optional[str] = None, end: Optional[str] = None):
    where = []
    params = []
    if start:
        where.append("day >= @start")
        params.append(bigquery.ScalarQueryParameter("start", "DATE", start))
    if end:
        where.append("day <= @end")
        params.append(bigquery.ScalarQueryParameter("end", "DATE", end))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT
          app,
          SUM(installs)     AS installs,
          SUM(revenue)      AS revenue,
          SUM(refunds)      AS refunds,
          SUM(feature_uses) AS feature_uses,
          SUM(llm_tokens)   AS llm_tokens,
          SUM(total_events) AS total_events,
          ROUND(SAFE_DIVIDE(SUM(revenue), SUM(installs)), 2) AS revenue_per_install
        FROM {DAILY}
        {where_sql}
        GROUP BY app
        ORDER BY revenue DESC
    """
    key = f"compare:{start}:{end}"
    result = run_query(sql, params, key)
    return {
        "cached": result["cached"],
        "bytes_processed": result["bytes_processed"],
        "apps": result["rows"],
    }


# ---------------------------------------------------------------------------
# Retention — Day-N. NOT summarizable (needs per-user install day vs activity),
# so it runs against the RAW events table with inline dedup. This is the case
# where pre-aggregation does not apply; the cache still protects repeat calls.
# ---------------------------------------------------------------------------
@app.get("/metrics/retention")
def retention(day_n: int = Query(1, ge=1, le=30)):
    sql = f"""
        WITH dedup AS (
          SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at) AS rn
            FROM {EVENTS}
          ) WHERE rn = 1
        ),
        installs AS (
          SELECT app, user_id, DATE(MIN(event_time)) AS install_day
          FROM dedup WHERE event_name = 'install'
          GROUP BY app, user_id
        ),
        activity AS (
          SELECT DISTINCT app, user_id, DATE(event_time) AS active_day FROM dedup
        )
        SELECT
          i.app,
          COUNT(DISTINCT i.user_id) AS installed,
          COUNT(DISTINCT CASE
                WHEN a.active_day = DATE_ADD(i.install_day, INTERVAL @n DAY)
                THEN i.user_id END) AS retained,
          ROUND(SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                  WHEN a.active_day = DATE_ADD(i.install_day, INTERVAL @n DAY)
                  THEN i.user_id END),
            COUNT(DISTINCT i.user_id)) * 100, 1) AS retention_pct
        FROM installs i
        LEFT JOIN activity a ON i.app = a.app AND i.user_id = a.user_id
        GROUP BY i.app
        ORDER BY i.app
    """
    params = [bigquery.ScalarQueryParameter("n", "INT64", day_n)]
    key = f"retention:{day_n}"
    result = run_query(sql, params, key)
    return {
        "day_n": day_n,
        "cached": result["cached"],
        "bytes_processed": result["bytes_processed"],
        "apps": result["rows"],
    }



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
