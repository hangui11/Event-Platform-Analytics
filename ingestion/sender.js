/**
 * Reads events.jsonl (from the generator, Step 2) and POSTs each line to the
 * ingestion service's /events endpoint, simulating apps sending events. This
 * naturally includes the injected DUPLICATES and MALFORMED lines, so it also
 * exercises the pipeline's dedup (at query time) and error handling.
 *
 * Run (with the server already running):
 *   node sender.js                          # sends all events to localhost:8080
 *   ENDPOINT=https://.../events node sender.js   # against a deployed URL
 *   LIMIT=500 node sender.js                # send only the first 500 (quick test)
 */
import fs from "fs";
import readline from "readline";

const ENDPOINT = process.env.ENDPOINT || "http://localhost:8080/events";
const FILE = process.env.FILE || "../events.jsonl";
// How many lines to send (default = all).
const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : Infinity; 
// How many lines to send in parallel (default = 20).
const CONCURRENCY = process.env.CONCURRENCY ? parseInt(process.env.CONCURRENCY, 10) : 20; 

async function postLine(line) {
  try {
    const res = await fetch(ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: line, // send the raw line verbatim — invalid-JSON lines included
    });
    return res.status;
  } catch (err) {
    return `ERR:${err.message}`;
  }
}

async function main() {
  const stats = { sent: 0, accepted: 0, rejected: 0, error: 0 };
  const rl = readline.createInterface({
    input: fs.createReadStream(FILE),
    crlfDelay: Infinity,
  });

  let batch = [];
  const flush = async () => {
    const results = await Promise.all(batch.map(postLine));
    for (const st of results) {
      stats.sent++;
      if (st === 202) stats.accepted++;
      else if (st === 400) stats.rejected++;
      else stats.error++;
    }
    batch = [];
  };

  for await (const line of rl) {
    if (!line.trim()) continue;
    if (stats.sent + batch.length >= LIMIT) break;
    batch.push(line);
    if (batch.length >= CONCURRENCY) await flush();
  }
  if (batch.length) await flush();

  console.log("Done sending.");
  console.log(`  sent:     ${stats.sent}`);
  console.log(`  accepted: ${stats.accepted}  (202 -> published to Pub/Sub)`);
  console.log(`  rejected: ${stats.rejected}  (400 -> invalid JSON / bad schema)`);
  console.log(`  errors:   ${stats.error}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
