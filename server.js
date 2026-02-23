import express from "express";
import fetch from "node-fetch";
import cors from "cors";

const app = express();
app.use(cors());

let cache = { jobs: [], updatedAt: null };

// Hämta snapshot (alla jobb en gång)
async function fetchSnapshot() {
  try {
    const res = await fetch(
      "https://jobstream.api.jobtechdev.se/snapshot",
      { headers: { "accept": "application/json" } }
    );
    const data = await res.json();
    cache.jobs = data.hits?.hits?.map(h => h._source) ?? [];
    cache.updatedAt = new Date().toISOString();
    console.log(`Snapshot laddad: ${cache.jobs.length} jobb`);
  } catch (err) {
    console.error("Snapshot-fel:", err.message);
  }
}

// Uppdatera med stream var 60:e sek
async function fetchStream() {
  try {
    const since = cache.updatedAt ?? new Date(Date.now() - 60_000).toISOString();
    const res = await fetch(
      `https://jobstream.api.jobtechdev.se/stream?date=${since}`,
      { headers: { "accept": "application/json" } }
    );
    const updates = await res.json();
    const newJobs = updates.hits?.hits?.map(h => h._source) ?? [];
    const map = new Map(cache.jobs.map(j => [j.id, j]));
    newJobs.forEach(j => map.set(j.id, j));
    cache.jobs = [...map.values()];
    cache.updatedAt = new Date().toISOString();
    console.log(`Stream: +${newJobs.length} uppdateringar`);
  } catch (err) {
    console.error("Stream-fel:", err.message);
  }
}

// Starta och schemalägg
fetchSnapshot().then(() => {
  setInterval(fetchStream, 60_000);
});

// API-endpoint för B12
app.get("/api/jobs", (req, res) => {
  const { q, lan } = req.query;
  let jobs = cache.jobs;

  if (q) {
    const term = q.toLowerCase();
    jobs = jobs.filter(j =>
      j.headline?.toLowerCase().includes(term) ||
      j.description?.text?.toLowerCase().includes(term)
    );
  }
  if (lan) {
    jobs = jobs.filter(j =>
      j.workplace_address?.region?.toLowerCase().includes(lan.toLowerCase())
    );
  }

  res.json({ total: jobs.length, updatedAt: cache.updatedAt, jobs });
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`JobForGo backend kör på port ${port}`));
