import express from 'express';
import fetch from 'node-fetch';
import cors from 'cors';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { pick } = require('stream-json/filters/Pick');
const Database = require('better-sqlite3');

const app = express();
app.use(cors());

// SQLite-databas på disk
const db = new Database('/tmp/jobs.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    headline TEXT,
    employer_name TEXT,
    municipality TEXT,
    region TEXT,
    employment_type TEXT,
    webpage_url TEXT,
    publication_date TEXT,
    description TEXT,
    raw JSON
  );
  CREATE INDEX IF NOT EXISTS idx_headline ON jobs(headline);
  CREATE INDEX IF NOT EXISTS idx_municipality ON jobs(municipality);
  CREATE INDEX IF NOT EXISTS idx_region ON jobs(region);
`);

let isLoadingSnapshot = false;
let snapshotDone = false;
let updatedAt = null;
let jobCount = 0;

// Strömma snapshot (~300 MB, ~150k jobb) direkt in i SQLite
async function fetchSnapshot() {
  if (isLoadingSnapshot) return;
  isLoadingSnapshot = true;
  console.log('Startar snapshot-strömning till SQLite...');

  try {
    const res = await fetch('https://jobstream.api.jobtechdev.se/snapshot', {
      headers: { 'accept': 'application/json' }
    });
    if (!res.ok) { console.error(`Snapshot HTTP ${res.status}`); return; }

    // Rensa gamla jobb
    db.exec('DELETE FROM jobs');

    const insertJob = db.prepare(`
      INSERT OR REPLACE INTO jobs (id, headline, employer_name, municipality, region, employment_type, webpage_url, publication_date, description, raw)
      VALUES (@id, @headline, @employer_name, @municipality, @region, @employment_type, @webpage_url, @publication_date, @description, @raw)
    `);
    const insertMany = db.transaction((jobs) => {
      for (const j of jobs) insertJob.run(j);
    });

    let batch = [];
    let count = 0;

    await new Promise((resolve, reject) => {
      const stream = res.body
        .pipe(parser())
        .pipe(pick({ filter: 'hits.hits' }))
        .pipe(streamArray());

      stream.on('data', ({ value }) => {
        const j = value._source ?? value;
        batch.push({
          id: j.id ?? '',
          headline: j.headline ?? '',
          employer_name: j.employer?.name ?? '',
          municipality: j.workplace_address?.municipality ?? '',
          region: j.workplace_address?.region ?? '',
          employment_type: j.employment_type?.label ?? '',
          webpage_url: j.webpage_url ?? '',
          publication_date: j.publication_date ?? '',
          description: (j.description?.text ?? '').slice(0, 500),
          raw: JSON.stringify({
            id: j.id, headline: j.headline,
            employer: j.employer,
            workplace_address: j.workplace_address,
            employment_type: j.employment_type,
            webpage_url: j.webpage_url,
            publication_date: j.publication_date,
            description: { text: (j.description?.text ?? '').slice(0, 300) }
          })
        });
        count++;
        if (batch.length >= 500) {
          insertMany(batch);
          batch = [];
          if (count % 10000 === 0) console.log(`Snapshot: ${count} jobb...`);
        }
      });

      stream.on('end', () => {
        if (batch.length > 0) insertMany(batch);
        console.log(`Snapshot klar: ${count} jobb i SQLite`);
        resolve();
      });

      stream.on('error', (err) => {
        console.error('Stream-fel:', err.message);
        reject(err);
      });
    });

    jobCount = db.prepare('SELECT COUNT(*) as c FROM jobs').get().c;
    updatedAt = new Date().toISOString();
    snapshotDone = true;
    console.log(`Klar! ${jobCount} jobb tillgängliga`);
  } catch (err) {
    console.error('fetchSnapshot-fel:', err.message);
  } finally {
    isLoadingSnapshot = false;
  }
}

// Stream-uppdatering var 60s
let lastSeen = null;
async function fetchStream() {
  if (!snapshotDone) return;
  try {
    const now = new Date();
    const since = lastSeen ?? new Date(now - 120000).toISOString().slice(0, 19);
    const res = await fetch(`https://jobstream.api.jobtechdev.se/stream?date=${encodeURIComponent(since)}`, {
      headers: { 'accept': 'application/json' }
    });
    if (!res.ok) return;
    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) return;

    const insertJob = db.prepare(`
      INSERT OR REPLACE INTO jobs (id, headline, employer_name, municipality, region, employment_type, webpage_url, publication_date, description, raw)
      VALUES (@id, @headline, @employer_name, @municipality, @region, @employment_type, @webpage_url, @publication_date, @description, @raw)
    `);
    const deleteJob = db.prepare('DELETE FROM jobs WHERE id = ?');

    const update = db.transaction(() => {
      let added = 0, removed = 0;
      for (const j of data) {
        if (j.removed) { deleteJob.run(j.id); removed++; }
        else {
          insertJob.run({
            id: j.id ?? '', headline: j.headline ?? '',
            employer_name: j.employer?.name ?? '',
            municipality: j.workplace_address?.municipality ?? '',
            region: j.workplace_address?.region ?? '',
            employment_type: j.employment_type?.label ?? '',
            webpage_url: j.webpage_url ?? '',
            publication_date: j.publication_date ?? '',
            description: (j.description?.text ?? '').slice(0, 500),
            raw: JSON.stringify({
              id: j.id, headline: j.headline, employer: j.employer,
              workplace_address: j.workplace_address,
              employment_type: j.employment_type,
              webpage_url: j.webpage_url, publication_date: j.publication_date,
              description: { text: (j.description?.text ?? '').slice(0, 300) }
            })
          });
          added++;
        }
      }
      return { added, removed };
    });

    const { added, removed } = update();
    jobCount = db.prepare('SELECT COUNT(*) as c FROM jobs').get().c;
    updatedAt = now.toISOString();
    lastSeen = now.toISOString().slice(0, 19);
    console.log(`Stream: +${added} -${removed}. Totalt: ${jobCount}`);
  } catch (err) {
    console.error('fetchStream-fel:', err.message);
  }
}

// API: GET /api/jobs?q=&lan=&limit=50&offset=0
app.get('/api/jobs', (req, res) => {
  const q = (req.query.q || '').trim();
  const lan = (req.query.lan || '').trim();
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  const offset = parseInt(req.query.offset) || 0;

  let whereClauses = [];
  let params = [];

  if (q) {
    whereClauses.push('(headline LIKE ? OR employer_name LIKE ? OR description LIKE ?)');
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }
  if (lan) {
    whereClauses.push('(municipality LIKE ? OR region LIKE ?)');
    params.push(`%${lan}%`, `%${lan}%`);
  }

  const where = whereClauses.length > 0 ? 'WHERE ' + whereClauses.join(' AND ') : '';

  const total = db.prepare(`SELECT COUNT(*) as c FROM jobs ${where}`).get(...params).c;
  const rows = db.prepare(`SELECT raw FROM jobs ${where} ORDER BY publication_date DESC LIMIT ? OFFSET ?`).all(...params, limit, offset);
  const jobs = rows.map(r => JSON.parse(r.raw));

  res.json({ total, offset, limit, updatedAt, isLoading: isLoadingSnapshot, jobs });
});

app.get('/health', (req, res) => res.json({
  status: 'ok', jobs: jobCount,
  isLoading: isLoadingSnapshot,
  snapshotDone, updatedAt
}));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`JobForGo backend kör på port ${PORT}`);
  fetchSnapshot().then(() => {
    lastSeen = new Date().toISOString().slice(0, 19);
    setInterval(fetchStream, 60000);
  });
});
