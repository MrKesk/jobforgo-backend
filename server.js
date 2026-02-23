'use strict';
const express = require('express');
const cors = require('cors');
const https = require('https');
const http = require('http');
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

// Funktion for att hamta med omdirigering
function fetchWithRedirects(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: { 'Accept': 'application/json' }
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        return fetchWithRedirects(res.headers.location, maxRedirects - 1).then(resolve).catch(reject);
      }
      resolve(res);
    });
    req.on('error', reject);
  });
}

// Hamta och spara snapshot
async function loadSnapshot() {
  if (isLoading) return;
  isLoading = true;
  console.log('Startar snapshot-hamtning...');
  
  try {
    const res = await fetchWithRedirects('https://jobstream.api.jobtechdev.se/snapshot');
    console.log('Ansluten till snapshot, status:', res.statusCode);
    
    let rawData = '';
    let totalBytes = 0;
    
    res.on('data', (chunk) => {
      rawData += chunk.toString();
      totalBytes += chunk.length;
      if (totalBytes % (5 * 1024 * 1024) < chunk.length) {
        console.log(`Hamtat ${Math.round(totalBytes / 1024 / 1024)} MB...`);
      }
    });
    
    res.on('end', () => {
      console.log(`Snapshot klar: ${Math.round(totalBytes / 1024 / 1024)} MB. Parsar JSON...`);
      try {
        const jobs = JSON.parse(rawData);
        rawData = ''; // frigör minne
        console.log(`Parsade ${jobs.length} jobb. Sparar till SQLite...`);
        
        const insert = db.prepare(`
          INSERT OR REPLACE INTO jobs
          (id, headline, employer_name, municipality, region, employment_type, webpage_url, description, published_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        
        const insertMany = db.transaction((jobList) => {
          for (const job of jobList) {
            insert.run(
              job.id,
              job.headline || '',
              job.employer ? job.employer.name : '',
              job.workplace_address ? job.workplace_address.municipality : '',
              job.workplace_address ? job.workplace_address.region : '',
              job.employment_type ? job.employment_type.label : '',
              job.webpage_url || '',
              job.description ? job.description.text : '',
              job.publication_date || ''
            );
          }
        });
        
        insertMany(jobs);
        snapshotDone = true;
        updatedAt = new Date().toISOString();
        const count = db.prepare('SELECT COUNT(*) as c FROM jobs').get();
        console.log(`Sparat ${count.c} jobb i SQLite!`);
      } catch (parseErr) {
        console.error('JSON parse-fel:', parseErr.message);
      } finally {
        isLoading = false;
      }
    });
    
    res.on('error', (err) => {
      console.error('Stream-fel:', err.message);
      isLoading = false;
    });
    
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
        
        const insert = db.prepare(`
          INSERT OR REPLACE INTO jobs
          (id, headline, employer_name, municipality, region, employment_type, webpage_url, description, published_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        
        const insertMany = db.transaction((jobList) => {
          for (const job of jobList) {
            if (job.removed) {
              db.prepare('DELETE FROM jobs WHERE id = ?').run(job.id);
            } else {
              insert.run(
                job.id,
                job.headline || '',
                job.employer ? job.employer.name : '',
                job.workplace_address ? job.workplace_address.municipality : '',
                job.workplace_address ? job.workplace_address.region : '',
                job.employment_type ? job.employment_type.label : '',
                job.webpage_url || '',
                job.description ? job.description.text : '',
                job.publication_date || ''
              );
            }
          }
        });
        
        insertMany(jobs);
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
  
  let query = 'SELECT * FROM jobs WHERE 1=1';
  const params = [];
  
  if (q) {
    query += ' AND (headline LIKE ? OR employer_name LIKE ? OR description LIKE ?)';
    const term = `%${q}%`;
    params.push(term, term, term);
  }
  
  if (lan) {
    query += ' AND (region LIKE ? OR municipality LIKE ?)';
    params.push(`%${lan}%`, `%${lan}%`);
  }
  
  query += ' ORDER BY published_at DESC LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  
  try {
    const jobs = db.prepare(query).all(...params);
    const countQuery = query.replace('SELECT *', 'SELECT COUNT(*) as total').replace(/ ORDER BY.*/, '');
    const countParams = params.slice(0, -2);
    const { total } = db.prepare(countQuery).get(...countParams);
    
    res.json({
      total,
      offset: parseInt(offset),
      limit: parseInt(limit),
      jobs
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Halsocheck
app.get('/health', (req, res) => {
  const count = db.prepare('SELECT COUNT(*) as c FROM jobs').get();
  res.json({
    status: 'ok',
    jobs: count.c,
    isLoading,
    snapshotDone,
    updatedAt
  });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`JobForGo backend koer paa port ${PORT}`);
  loadSnapshot();
  setInterval(pollStream, 60000);
});
