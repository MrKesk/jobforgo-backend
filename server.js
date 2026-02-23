'use strict';
const express = require('express');
const cors = require('cors');
const https = require('https');
const http = require('http');
const JSONStream = require('JSONStream');
const Database = require('better-sqlite3');

const app = express();
app.use(cors());
app.use(express.json());

// SQLite databas
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
    description TEXT,
    published_at TEXT
  )
`);

let isLoading = false;
let snapshotDone = false;
let updatedAt = null;

// Forbered INSERT
const insertStmt = db.prepare(`
  INSERT OR REPLACE INTO jobs
  (id, headline, employer_name, municipality, region, employment_type, webpage_url, description, published_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

function mapJob(job) {
  return [
    job.id,
    job.headline || '',
    job.employer ? job.employer.name : '',
    job.workplace_address ? job.workplace_address.municipality : '',
    job.workplace_address ? job.workplace_address.region : '',
    job.employment_type ? job.employment_type.label : '',
    job.webpage_url || '',
    job.description ? (job.description.text || '').substring(0, 500) : '',
    job.publication_date || ''
  ];
}

// Funktion for att hamta med omdirigering
function fetchWithRedirects(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, { headers: { 'Accept': 'application/json' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        return fetchWithRedirects(res.headers.location, maxRedirects - 1).then(resolve).catch(reject);
      }
      resolve(res);
    });
    req.on('error', reject);
  });
}

// Hamta snapshot med streaming JSON
async function loadSnapshot() {
  if (isLoading) return;
  isLoading = true;
  console.log('Startar snapshot-hamtning med JSONStream...');

  try {
    const res = await fetchWithRedirects('https://jobstream.api.jobtechdev.se/snapshot');
    console.log('Ansluten till snapshot, status:', res.statusCode);

    let jobCount = 0;
    let batchBuffer = [];
    const BATCH_SIZE = 500;

    const insertBatch = db.transaction((jobs) => {
      for (const job of jobs) {
        insertStmt.run(mapJob(job));
      }
    });

    const jsonStream = JSONStream.parse('*');

    jsonStream.on('data', (job) => {
      batchBuffer.push(job);
      if (batchBuffer.length >= BATCH_SIZE) {
        insertBatch(batchBuffer);
        jobCount += batchBuffer.length;
        batchBuffer = [];
        if (jobCount % 10000 === 0) {
          console.log(`Sparat ${jobCount} jobb...`);
        }
      }
    });

    jsonStream.on('end', () => {
      if (batchBuffer.length > 0) {
        insertBatch(batchBuffer);
        jobCount += batchBuffer.length;
        batchBuffer = [];
      }
      const count = db.prepare('SELECT COUNT(*) as c FROM jobs').get();
      snapshotDone = true;
      isLoading = false;
      updatedAt = new Date().toISOString();
      console.log(`Snapshot klar! Totalt ${count.c} jobb i SQLite.`);
    });

    jsonStream.on('error', (err) => {
      console.error('JSONStream fel:', err.message);
      isLoading = false;
    });

    res.pipe(jsonStream);

  } catch (err) {
    console.error('Fetch-fel:', err.message);
    isLoading = false;
  }
}

// Stream-uppdatering var 60 sek
async function pollStream() {
  if (!snapshotDone) return;

  try {
    const since = updatedAt || new Date(Date.now() - 120000).toISOString();
    const url = `https://jobstream.api.jobtechdev.se/stream?date=${encodeURIComponent(since)}`;
    const res = await fetchWithRedirects(url);

    let rawData = '';
    res.on('data', (chunk) => { rawData += chunk.toString(); });
    res.on('end', () => {
      try {
        const jobs = JSON.parse(rawData);
        if (!Array.isArray(jobs) || jobs.length === 0) return;

        const upsert = db.transaction((jobList) => {
          for (const job of jobList) {
            if (job.removed) {
              db.prepare('DELETE FROM jobs WHERE id = ?').run(job.id);
            } else {
              insertStmt.run(mapJob(job));
            }
          }
        });

        upsert(jobs);
        updatedAt = new Date().toISOString();
        console.log(`Stream: uppdaterade ${jobs.length} jobb`);
      } catch (e) {
        console.error('Stream parse-fel:', e.message);
      }
    });
  } catch (err) {
    console.error('Poll-fel:', err.message);
  }
}

// API: GET /api/jobs
app.get('/api/jobs', (req, res) => {
  const { q, lan, limit = 50, offset = 0 } = req.query;

  let query = 'SELECT id, headline, employer_name, municipality, region, employment_type, webpage_url, published_at FROM jobs WHERE 1=1';
  const params = [];

  if (q) {
    query += ' AND (headline LIKE ? OR employer_name LIKE ?)';
    const term = `%${q}%`;
    params.push(term, term);
  }

  if (lan) {
    query += ' AND (region LIKE ? OR municipality LIKE ?)';
    params.push(`%${lan}%`, `%${lan}%`);
  }

  query += ' ORDER BY published_at DESC LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));

  try {
    const jobs = db.prepare(query).all(...params);

    const countSql = 'SELECT COUNT(*) as total FROM jobs WHERE 1=1' +
      (q ? ' AND (headline LIKE ? OR employer_name LIKE ?)' : '') +
      (lan ? ' AND (region LIKE ? OR municipality LIKE ?)' : '');
    const countParams = params.slice(0, -2);
    const { total } = db.prepare(countSql).get(...countParams);

    res.json({ total, offset: parseInt(offset), limit: parseInt(limit), jobs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Halsocheck
app.get('/health', (req, res) => {
  const count = db.prepare('SELECT COUNT(*) as c FROM jobs').get();
  res.json({ status: 'ok', jobs: count.c, isLoading, snapshotDone, updatedAt });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`JobForGo backend koer paa port ${PORT}`);
  loadSnapshot();
  setInterval(pollStream, 60000);
});
