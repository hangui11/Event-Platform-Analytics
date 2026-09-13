/**
 * An HTTP endpoint that receives events, validates and enriches them, and
 * publishes table-ready messages to a Pub/Sub topic. A managed BigQuery
 * Subscription then lands them in the `analytics.events` table.
 *
 *   App / sender  --HTTP POST /events-->  THIS SERVICE
 *      - parse JSON            (rejects invalid JSON = parse layer)
 *      - validate schema       (rejects missing/bad fields = schema layer)
 *      - enrich: event_id (if missing), ingested_at (now),
 *                event_time ms -> ISO, properties -> JSON string
 *      - publish to Pub/Sub topic "events"
 *   -->  Pub/Sub  -->  BigQuery Subscription  -->  analytics.events
 *
 * Deduplication is NOT done here: Pub/Sub is at-least-once, so duplicates may
 * land in BigQuery; they are removed at QUERY time via event_id (see Step 5).
 *
 * Run locally:  npm install  &&  npm start
 * Env:  PROJECT_ID, TOPIC_ID (defaults below), PORT
 */
import express from "express";
import { PubSub } from "@google-cloud/pubsub";
import { v4 as uuidv4 } from "uuid";

// ---------- config ----------
const PROJECT_ID = process.env.PROJECT_ID || "project-bd5f1c88-1252-4a0a-a34";
const TOPIC_ID = process.env.TOPIC_ID || "events";
const PORT = process.env.PORT || 8080;

const pubsub = new PubSub({ projectId: PROJECT_ID });
const topic = pubsub.topic(TOPIC_ID);

// The columns the BigQuery table expects (subscription uses the table schema).
const REQUIRED_FIELDS = ["app", "event_name", "user_id", "event_time"];

// ---------- validation ----------
// Returns an array of problems; empty array = valid.
function validate(event) {
  const problems = [];
  if (typeof event !== "object" || event === null || Array.isArray(event)) {
    return ["event is not a JSON object"];
  }
  for (const f of REQUIRED_FIELDS) {
    if (!(f in event)) problems.push(`missing required field: ${f}`);
  }
  if ("event_time" in event && typeof event.event_time !== "number") {
    problems.push("event_time must be a number (epoch milliseconds)");
  }
  if ("properties" in event && event.properties !== null &&
      typeof event.properties !== "object") {
    problems.push("properties must be an object");
  }
  return problems;
}

// ---------- enrichment: make the event table-ready ----------
function enrich(event) {
  const nowIso = new Date().toISOString();
  return {
    // add event_id if the client didn't send one (needed for dedup)
    event_id: event.event_id || `evt_${uuidv4().replace(/-/g, "").slice(0, 12)}`,
    app: event.app,
    event_name: event.event_name,
    event_type: event.event_type ?? null,
    user_id: event.user_id,
    // BigQuery accepts for a TIMESTAMP column
    event_time: new Date(event.event_time).toISOString(),
    ingested_at: nowIso, // stamped by THIS service on receipt (pipeline lag anchor)
    version: event.version ?? null,
    region: event.region ?? null,
    // JSON column: send the payload as a JSON string
    properties: JSON.stringify(event.properties ?? {}),
  };
}

// ---------- publish to Pub/Sub ----------
// The message data is the enriched event as JSON bytes; the BigQuery
// subscription (using the table schema) maps its keys to table columns.
async function publish(enriched) {
  const dataBuffer = Buffer.from(JSON.stringify(enriched));
  return topic.publishMessage({ data: dataBuffer });
}

// ---------- HTTP server ----------
const app = express();

// Capture the RAW body so we can detect invalid JSON ourselves (the parse
// layer). We do NOT let express throw on bad JSON — we handle it explicitly.
app.use(express.text({ type: "*/*", limit: "1mb" }));

// Simple counters, how much bad data was rejected.
const rejected = { invalid_json: 0, bad_schema: 0 };

// Log a rejected event (observability). When deployed to Cloud Run these lines
// appear in Cloud Logging, so every malformed event is recorded and countable,
// even though it is never published into the pipeline.
function logReject(kind, reason, rawBody) {
  if (kind === "invalid_json") rejected.invalid_json++;
  else rejected.bad_schema++;
  console.warn(JSON.stringify({
    level: "reject",
    kind,
    reason,
    totals: rejected,
    sample: String(rawBody).slice(0, 200), // first 200 chars, for debugging
  }));
}

// health check
app.get("/", (_req, res) => res.status(200).send("ingestion service: ok"));

// rejection stats (how many malformed events were rejected, by kind)
app.get("/stats", (_req, res) => res.status(200).json({ rejected }));

// main ingestion endpoint
app.post("/events", async (req, res) => {
  // 1) PARSE layer — is the body valid JSON at all?
  let event;
  try {
    event = JSON.parse(req.body);
  } catch {
    // invalid JSON: log + reject with 400 (this is where the invalid-JSON test
    // events are caught, before any validation). Never published.
    logReject("invalid_json", "body is not valid JSON", req.body);
    return res.status(400).json({ status: "rejected", reason: "invalid JSON" });
  }

  // 2) SCHEMA / VALUE layer — required fields present and well-typed?
  const problems = validate(event);
  if (problems.length > 0) {
    logReject("bad_schema", problems, req.body);
    return res.status(400).json({ status: "rejected", reason: problems });
  }

  // 3) ENRICH + 4) PUBLISH
  try {
    const enriched = enrich(event);
    await publish(enriched);
    return res.status(202).json({ status: "accepted", event_id: enriched.event_id });
  } catch (err) {
    // publishing failed (e.g. Pub/Sub unreachable) — 500 so the client retries
    console.error("publish error:", err.message);
    return res.status(500).json({ status: "error", reason: "publish failed" });
  }
});

app.listen(PORT, () => {
  console.log(`Ingestion service listening on :${PORT}`);
  console.log(`  project=${PROJECT_ID}  topic=${TOPIC_ID}`);
});
