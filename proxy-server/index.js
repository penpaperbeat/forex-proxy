'use strict';

const express = require('express');
const axios = require('axios');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.TWELVEDATA_API_KEY;
const PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,AUD/USD')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const CANDLE_STORE_PATH = '/tmp/candle-store.json';
const CANDLE_PAIRS = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'AUD/USD', 'USD/CAD', 'NZD/USD'];
let candleStore;
try {
  const raw = fs.readFileSync(CANDLE_STORE_PATH, 'utf8');
  candleStore = JSON.parse(raw);
  console.log(`[${new Date().toISOString()}] Loaded candle store from ${CANDLE_STORE_PATH}`);
} catch (_) {
  candleStore = { pairs: {} };
}

const ratesCache = {};
const historyCache = {};
let ratesLastUpdated = null;
let historyLastUpdated = null;
const HISTORY_TTL_MS = 6 * 60 * 60 * 1000; // 6 hours
const startTime = Date.now();
function nextMidnightUTC() {
  const now = new Date();
  const midnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1));
  return midnight.getTime();
}

function incrementCallCount() {
  if (Date.now() >= callCountResetAt) {
    dailyCallCount = 0;
    callCountResetAt = nextMidnightUTC();
  }
  dailyCallCount++;
}

let dailyCallCount = 0;
let callCountResetAt = nextMidnightUTC();
let lastCollectionError = null;

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

app.get('/stored-history', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const limit = req.query.limit ? parseInt(req.query.limit) : null;

  if (!limit || isNaN(limit)) {
    return res.json(candleStore);
  }

  const limited = { pairs: {} };
  for (const pair of Object.keys(candleStore.pairs)) {
    const candles = candleStore.pairs[pair];
    limited.pairs[pair] = candles.slice(-limit);
  }

  return res.json(limited);
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

  setInterval(async () => {
    console.log(`[${new Date().toISOString()}] Candle store refresh: fetching history for ${CANDLE_PAIRS.length} pairs...`);
    for (const pair of CANDLE_PAIRS) {
      try {
        const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(pair)}&interval=1h&outputsize=200&apikey=${API_KEY}`;
        const response = await axios.get(url, { timeout: 15000 });
        if (!response.data || !response.data.values) {
          console.error(`[${new Date().toISOString()}] Candle store: no values for ${pair}`);
          continue;
        }
        const existing = candleStore.pairs[pair] || [];
        const combined = [...existing, ...response.data.values];
        const seen = new Map();
        for (const candle of combined) {
          seen.set(candle.datetime, candle);
        }
        let merged = Array.from(seen.values()).sort((a, b) => a.datetime < b.datetime ? -1 : a.datetime > b.datetime ? 1 : 0);
        if (merged.length > 5000) {
          merged = merged.slice(merged.length - 5000);
        }
        candleStore.pairs[pair] = merged;
        fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore));
        console.log(`[${new Date().toISOString()}] Candle store updated for ${pair}: ${merged.length} candles`);
      } catch (err) {
        console.error(`[${new Date().toISOString()}] Candle store fetch failed for ${pair}:`, err.message);
      }
    }
  }, 30 * 60 * 1000); // 30 minutes
}

start().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
