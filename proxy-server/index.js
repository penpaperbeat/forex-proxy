'use strict';

const express = require('express');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.TWELVEDATA_API_KEY;
const PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,AUD/USD')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const ratesCache = {};
const historyCache = {};
let ratesLastUpdated = null;
let historyLastUpdated = null;
const HISTORY_TTL_MS = 6 * 60 * 60 * 1000; // 6 hours
const startTime = Date.now();

async function fetchHistory() {
  console.log(`[${new Date().toISOString()}] Fetching historical OHLC data for ${PAIRS.length} pairs...`);
  let anyFetched = false;
  for (const pair of PAIRS) {
    try {
      const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(pair)}&interval=1h&outputsize=200&apikey=${API_KEY}`;
      const response = await axios.get(url, { timeout: 15000 });
      if (response.data && response.data.values) {
        historyCache[pair] = response.data.values;
        anyFetched = true;
        console.log(`[${new Date().toISOString()}] History fetched for ${pair}: ${response.data.values.length} candles`);
      } else {
        console.warn(`[${new Date().toISOString()}] No history data for ${pair}:`, JSON.stringify(response.data));
        if (!historyCache[pair]) historyCache[pair] = [];
      }
    } catch (err) {
      console.warn(`[${new Date().toISOString()}] Failed to fetch history for ${pair}:`, err.message);
      if (!historyCache[pair]) historyCache[pair] = [];
    }
  }
  if (anyFetched) {
    historyLastUpdated = new Date().toISOString();
  }
}

async function pollRates() {
  let updated = 0;
  for (const pair of PAIRS) {
    try {
      const url = `https://api.twelvedata.com/price?symbol=${encodeURIComponent(pair)}&apikey=${API_KEY}`;
      const response = await axios.get(url, { timeout: 10000 });
      if (response.data && response.data.price) {
        ratesCache[pair] = {
          price: response.data.price,
          timestamp: new Date().toISOString()
        };
        updated++;
      } else {
        console.warn(`[${new Date().toISOString()}] Unexpected price response for ${pair}:`, JSON.stringify(response.data));
      }
    } catch (err) {
      console.warn(`[${new Date().toISOString()}] Poll failed for ${pair}:`, err.message);
    }
  }
  if (updated > 0) {
    ratesLastUpdated = new Date().toISOString();
  }
  console.log(`[${new Date().toISOString()}] Poll cycle complete — ${updated} pairs updated (next poll in 15 minutes)`);
}

app.get('/rates', (req, res) => {
  res.json({ pairs: ratesCache });
});

app.get('/history', async (req, res) => {
  const now = Date.now();
  const cacheAge = historyLastUpdated ? now - new Date(historyLastUpdated).getTime() : Infinity;
  const cacheExpired = cacheAge >= HISTORY_TTL_MS;

  if (cacheExpired) {
    console.log(`[${new Date().toISOString()}] History cache expired (age: ${Math.round(cacheAge / 60000)}m) — re-fetching from Twelvedata...`);
    await fetchHistory();
  }

  res.json({ pairs: historyCache });
});

app.get('/health', (req, res) => {
  const historyNextRefresh = historyLastUpdated
    ? new Date(new Date(historyLastUpdated).getTime() + HISTORY_TTL_MS).toISOString()
    : null;
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    ratesLastUpdated,
    historyLastUpdated,
    historyNextRefresh
  });
});

async function start() {
  await pollRates();
  setInterval(pollRates, 900000); // 15 minutes
  app.listen(PORT, () => {
    console.log(`[${new Date().toISOString()}] Forex proxy server listening on port ${PORT} (rates poll: 15min, history TTL: 6h)`);
  });
}

start().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
