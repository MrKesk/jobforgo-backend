import express from 'express';
import fetch from 'node-fetch';
import cors from 'cors';
import { pipeline } from 'stream/promises';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { pick } = require('stream-json/filters/Pick');

const app = express();
app.use(cors());

// Minnescache för alla jobb
let cache = { jobs: [], updatedAt: null };
let isLoadingSnapshot = false;

// Strömma snapshot (~300 MB) utan att ladda allt i minnet
async function fetchSnapshot() {
  if (isLoadingSnapshot) return;
  isLoadingSnapshot = true;
  console.log('Startar snapshot-strömning...');

  try {
    const res = await fetch('https://jobstream.api.jobtechdev.se/snapshot', {
      headers: { 'accept': 'application/json' }
    });

    if (!res.ok) {
      console.error(`Snapshot HTTP-fel: ${res.status}`);
      isLoadingSnapshot = false;
      return;
    }

    const jobs = [];
    let count = 0;

    await new Promise((resolve, reject) => {
      const stream = res.body
        .pipe(parser())
        .pipe(pick({ filter: 'hits.hits' }))
        .pipe(streamArray());

      stream.on('data', ({ value }) => {
        const job = value._source ?? value;
        jobs.push(job);
        count++;
        if (count % 10000 === 0) {
          console.log(`Snapshot: strömmat ${count} jobb...`);
        }
      });

      stream.on('end', () => {
        console.log(`Snapshot klar: ${count} jobb totalt`);
        resolve();
      });

      stream.on('error', (err) => {
        console.error('Snapshot stream-fel:', err.message);
        reject(err);
      });
    });

    cache.jobs = jobs;
    cache.updatedAt = new Date().toISOString();
    console.log(`Cache uppdaterad med ${cache.jobs.length} jobb`);
  } catch (err) {
    console.error('fetchSnapshot-fel:', err.message);
  } finally {
    isLoadingSnapshot = false;
  }
}

// Uppdatera med stream var 60s (lägger till/tar bort jobb)
let lastSeen = null;
async function fetchStream() {
  try {
    const now = new Date();
    const since = lastSeen ?? new Date(now.getTime() - 2 * 60 * 1000).toISOString().replace('T', 'T').slice(0, 19);
    const url = `https://jobstream.api.jobtechdev.se/stream?date=${encodeURIComponent(since)}`;
    const res = await fetch(url, { headers: { 'accept': 'application/json' } });

    if (!res.ok) { console.error(`Stream HTTP-fel: ${res.status}`); return; }

    const data = await res.json();
    if (!Array.isArray(data)) return;

    const removed = new Set(data.filter(j => j.removed).map(j => j.id));
    const added = data.filter(j => !j.removed && j.id);

    if (removed.size > 0) {
      cache.jobs = cache.jobs.filter(j => !removed.has(j.id));
    }
    if (added.length > 0) {
      const existingIds = new Set(cache.jobs.map(j => j.id));
      const newJobs = added.filter(j => !existingIds.has(j.id));
      cache.jobs = [...newJobs, ...cache.jobs];
    }

    lastSeen = now.toISOString().slice(0, 19);
    cache.updatedAt = now.toISOString();
    console.log(`Stream: +${added.length} nya, -${removed.size} borttagna. Totalt: ${cache.jobs.length}`);
  } catch (err) {
    console.error('fetchStream-fel:', err.message);
  }
}

// API: GET /api/jobs?q=keyword&lan=ort&limit=50&offset=0
app.get('/api/jobs', (req, res) => {
  const q = (req.query.q || '').toLowerCase().trim();
  const lan = (req.query.lan || '').toLowerCase().trim();
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  const offset = parseInt(req.query.offset) || 0;

  let jobs = cache.jobs;

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
      j.workplace_address?.region?.toLowerCase().includes(lan) ||
      j.workplace_address?.city?.toLowerCase().includes(lan)
    );
  }

  const total = jobs.length;
  const page = jobs.slice(offset, offset + limit);

  res.json({
    total,
    offset,
    limit,
    updatedAt: cache.updatedAt,
    isLoading: isLoadingSnapshot,
    jobs: page
  });
});

app.get('/health', (req, res) => res.json({
  status: 'ok',
  jobs: cache.jobs.length,
  isLoading: isLoadingSnapshot,
  updatedAt: cache.updatedAt
}));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`JobForGo backend kör på port ${PORT}`);
  // Starta snapshot-hämtning i bakgrunden
  fetchSnapshot().then(() => {
    lastSeen = new Date().toISOString().slice(0, 19);
    // Uppdatera med stream var 60s
    setInterval(fetchStream, 60 * 1000);
  });
});
