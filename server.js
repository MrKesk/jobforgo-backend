import express from "express";
import fetch from "node-fetch";
import cors from "cors";

const app = express();
app.use(cors());

// Cache med jobb
let cache = { jobs: [], updatedAt: null };

// Hämta jobb från JobSearch API (ingen API-nyckel krävs, stöder paginering)
async function fetchJobs(q = '', lan = '') {
  try {
    let allJobs = [];
    const limit = 100;
    let offset = 0;
    const maxJobs = 500;

    while (allJobs.length < maxJobs) {
      let url = `https://jobsearch.api.jobtechdev.se/search?limit=${limit}&offset=${offset}&sort=pubdate-desc`;
      if (q) url += `&q=${encodeURIComponent(q)}`;
      if (lan) url += `&municipality-concept-id=${encodeURIComponent(lan)}`;

      const res = await fetch(url, {
        headers: { 'accept': 'application/json' }
      });

      if (!res.ok) {
        console.error(`JobSearch HTTP-fel: ${res.status}`);
        break;
      }

      const data = await res.json();
      const hits = data?.hits ?? [];
      if (hits.length === 0) break;

      allJobs = allJobs.concat(hits);
      console.log(`Hämtat ${allJobs.length} jobb (total: ${data.total?.value ?? '?'})`);

      if (allJobs.length >= (data.total?.value ?? 0)) break;
      offset += limit;
    }

    return allJobs;
  } catch (err) {
    console.error('fetchJobs-fel:', err.message);
    return [];
  }
}

// Initial hämtning + uppdatera var 10 min
async function refreshCache() {
  console.log('Uppdaterar jobbcache...');
  const jobs = await fetchJobs();
  cache.jobs = jobs;
  cache.updatedAt = new Date().toISOString();
  console.log(`Cache uppdaterad: ${jobs.length} jobb`);
}

// API: GET /api/jobs?q=keyword&lan=stockholm
app.get('/api/jobs', async (req, res) => {
  const q = req.query.q || '';
  const lan = req.query.lan || '';

  // Om sökparametrar: hämta direkt från API
  if (q || lan) {
    try {
      let url = `https://jobsearch.api.jobtechdev.se/search?limit=100&sort=pubdate-desc`;
      if (q) url += `&q=${encodeURIComponent(q)}`;
      // För lan: sök i fritext
      if (lan) url += `&q=${encodeURIComponent(q + ' ' + lan).trim()}`;

      const apiRes = await fetch(url, { headers: { 'accept': 'application/json' } });
      const data = await apiRes.json();
      const jobs = data?.hits ?? [];
      return res.json({ total: jobs.length, updatedAt: new Date().toISOString(), jobs });
    } catch (err) {
      console.error('Sök-fel:', err.message);
      return res.json({ total: 0, updatedAt: null, jobs: [] });
    }
  }

  // Annars returnera cache
  res.json({ total: cache.jobs.length, updatedAt: cache.updatedAt, jobs: cache.jobs });
});

app.get('/health', (req, res) => res.json({ status: 'ok', jobs: cache.jobs.length, updatedAt: cache.updatedAt }));

const PORT = process.env.PORT || 8080;
app.listen(PORT, async () => {
  console.log(`JobForGo backend kör på port ${PORT}`);
  await refreshCache();
  setInterval(refreshCache, 10 * 60 * 1000); // Uppdatera var 10 min
});
