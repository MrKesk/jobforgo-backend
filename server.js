import express from "express";
import fetch from "node-fetch";
import cors from "cors";

const app = express();
app.use(cors());

let cache = { jobs: [], updatedAt: null };

// Hämta snapshot paginerat (100 jobb åt gången)
async function fetchSnapshot() {
  try {
    let allJobs = [];
    let offset = 0;
    const limit = 100;
    let total = Infinity;

    while (allJobs.length < total) {
      const url = `https://jobstream.api.jobtechdev.se/snapshot?limit=${limit}&offset=${offset}`;
      const res = await fetch(url, { headers: { "accept": "application/json" } });
      if (!res.ok) {
        console.error(`Snapshot HTTP-fel: ${res.status}`);
        break;
      }
      const text = await res.text();
      let data;
      try {
        data = JSON.parse(text);
      } catch (e) {
        console.error("Snapshot JSON-fel:", e.message.slice(0, 100));
        break;
      }
      const hits = data?.hits?.hits ?? [];
      if (hits.length === 0) break;
      const jobs = hits.map(h => h._source);
      allJobs = allJobs.concat(jobs);
      if (total === Infinity) total = Math.min(data?.hits?.total?.value ?? 1000, 1000);
      offset += limit;
      console.log(`Snapshot: hämtat ${allJobs.length}/${total}`);
      if (allJobs.length >= 1000) break; // max 1000 jobb
    }

    cache.jobs = allJobs;
    cache.updatedAt = new Date().toISOString();
    console.log(`Snapshot klar: ${cache.jobs.length} jobb`);
  } catch (err) {
    console.error("Snapshot-fel:", err.message);
  }
}

// Uppdatera med stream var 60s
let lastSeen = null;
async function fetchStream() {
  try {
    const url = lastSeen
      ? `https://jobstream.api.jobtechdev.se/stream?date=${encodeURIComponent(lastSeen)}`
      : `https://jobstream.api.jobtechdev.se/stream`;
    const res = await fetch(url, { headers: { "accept": "application/json" } });
    if (!res.ok) { console.error(`Stream HTTP-fel: ${res.status}`); return; }
    const data = await res.json();
    const newJobs = data ?? [];
    if (newJobs.length > 0) {
      const existingIds = new Set(cache.jobs.map(j => j.id));
      const added = newJobs.filter(j => !existingIds.has(j.id) && !j.removed);
      const removedIds = new Set(newJobs.filter(j => j.removed).map(j => j.id));
      cache.jobs = cache.jobs.filter(j => !removedIds.has(j.id));
      cache.jobs = [...added, ...cache.jobs];
      cache.updatedAt = new Date().toISOString();
      lastSeen = new Date().toISOString();
      console.log(`Stream: +${added.length} tillagda, -${removedIds.size} borttagna`);
    } else {
      console.log(`Stream: +0 uppdateringar`);
    }
  } catch (err) {
    console.error("Stream-fel:", err.message);
  }
}

// API-endpoint
app.get("/api/jobs", (req, res) => {
  let jobs = cache.jobs;
  const q = req.query.q?.toLowerCase();
  const lan = req.query.lan?.toLowerCase();
  if (q) {
    jobs = jobs.filter(j =>
      j.headline?.toLowerCase().includes(q) ||
      j.employer?.name?.toLowerCase().includes(q) ||
      j.description?.text?.toLowerCase().includes(q)
    );
  }
  if (lan) {
    jobs = jobs.filter(j =>
      j.workplace_address?.municipality?.toLowerCase().includes(lan) ||
      j.workplace_address?.region?.toLowerCase().includes(lan)
    );
  }
  res.json({ total: jobs.length, updatedAt: cache.updatedAt, jobs });
});

app.get("/health", (req, res) => res.json({ status: "ok", jobs: cache.jobs.length }));

const PORT = process.env.PORT || 8080;
app.listen(PORT, async () => {
  console.log(`JobForGo backend kör på port ${PORT}`);
  await fetchSnapshot();
  lastSeen = new Date().toISOString();
  setInterval(fetchStream, 60000);
});
