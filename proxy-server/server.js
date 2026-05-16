'use strict';

const express = require('express');
const axios = require('axios');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.TWELVEDATA_API_KEY;
const FOREX_PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,USD/CHF,AUD/USD,USD/CAD,NZD/USD')
  .split(',')
  .map(p => p.trim());

const CANDLE_STORE_PATH = '/tmp/candle-store.json';
const CANDLE_CAP = 5000;

// --- In-memory state ---
let ratesCache = { data: { pairs: {} }, fetchedAt: 0 };
let historyCache = { data: { pairs: {} }, fetchedAt: 0 };
let candleStore = { pairs: {} };
let dailyCallCount = 0;
let callCountResetAt = nextMidnightUTC();
let lastCollectionError = null;

function nextMidnightUTC() {
  const now = new Date();
  const midnight = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1
  ));
  return midnight.getTime();
}

function incrementCallCount() {
  if (Date.now() >= callCountResetAt) {
    dailyCallCount = 0;
    callCountResetAt = nextMidnightUTC();
  }
  dailyCallCount++;
}

// --- Load candle store from disk on startup ---
try {
  if (fs.existsSync(CANDLE_STORE_PATH)) {
    const raw = fs.readFileSync(CANDLE_STORE_PATH, 'utf8');
    candleStore = JSON.parse(raw);
    console.log('Candle store loaded from disk.');
  }
} catch (err) {
  console.error('Failed to load candle store from disk:', err.message);
  candleStore = { pairs: {} };
}

// --- Collection function: rates + history, staggered, async, per-pair error-safe ---
async function runCollection() {
  console.log(`[${new Date().toISOString()}] Collection cycle starting...`);

  // Fetch rates for each pair, staggered 2s apart
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 2000));
    try {
      const response = await axios.get('https://api.twelvedata.com/exchange_rate', {
        params: { symbol: pair, apikey: API_KEY }
      });
      if (response.data && response.data.rate) {
        ratesCache.data.pairs[pair] = {
          price: String(response.data.rate),
          fetchedAt: new Date().toISOString()
        };
        ratesCache.fetchedAt = Date.now();
      }
      incrementCallCount();
    } catch (err) {
      lastCollectionError = `${pair} rates: ${err.message}`;
      console.error(`[${new Date().toISOString()}] Rate fetch failed for ${pair}:`, err.message);
    }
  }

  // Pause before history fetches
  await new Promise(r => setTimeout(r, 2000));

  // Fetch history for each pair, staggered 2s apart
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 2000));
    try {
      const response = await axios.get('https://api.twelvedata.com/time_series', {
        params: { symbol: pair, interval: '1h', outputsize: 5000, apikey: API_KEY }
      });
      const newCandles = (response.data.values || [])
        .map(c => ({
          datetime: c.datetime,
          open: parseFloat(c.open),
          high: parseFloat(c.high),
          low: parseFloat(c.low),
          close: parseFloat(c.close)
        }))
        .filter(c => !isNaN(c.open) && !isNaN(c.high) && !isNaN(c.low) && !isNaN(c.close));

      if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
      const map = {};
      for (const c of candleStore.pairs[pair]) map[c.datetime] = c;
      for (const c of newCandles) map[c.datetime] = c;
      let merged = Object.values(map).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
      if (merged.length > CANDLE_CAP) merged = merged.slice(-CANDLE_CAP);
      candleStore.pairs[pair] = merged;
      historyCache.data.pairs[pair] = merged;
      historyCache.fetchedAt = Date.now();
      incrementCallCount();
    } catch (err) {
      lastCollectionError = `${pair} history: ${err.message}`;
      console.error(`[${new Date().toISOString()}] History fetch failed for ${pair}:`, err.message);
    }
  }

  // Persist to disk
  try {
    fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8');
    lastCollectionError = null;
    console.log(`[${new Date().toISOString()}] Collection complete. Daily calls: ${dailyCallCount}`);
  } catch (err) {
    lastCollectionError = `File write failed: ${err.message}`;
    console.error('Failed to write candle store to disk:', err.message);
  }
}

// --- Startup: run immediately, then every 15 minutes ---
runCollection();
setInterval(runCollection, 15 * 60 * 1000);

// --- Routes ---

app.get('/rates', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json(ratesCache.data);
});

app.get('/history', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json(historyCache.data);
});

app.get('/stored-history', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const limit = req.query.limit ? parseInt(req.query.limit) : null;
  if (!limit || isNaN(limit)) return res.json(candleStore);
  const limited = { pairs: {} };
  for (const pair of Object.keys(candleStore.pairs)) {
    limited.pairs[pair] = candleStore.pairs[pair].slice(-limit);
  }
  return res.json(limited);
});

app.get('/status', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json({
    callsToday: dailyCallCount,
    limit: 800,
    resetAt: new Date(callCountResetAt).toUTCString(),
    candlePairsStored: Object.keys(candleStore.pairs).length,
    lastCollectionError: lastCollectionError || null
  });
});

app.get('/health', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    ratesCachedAt: ratesCache.fetchedAt ? new Date(ratesCache.fetchedAt).toISOString() : null,
    historyCachedAt: historyCache.fetchedAt ? new Date(historyCache.fetchedAt).toISOString() : null
  });
});

// --- Start server ---
app.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] Forex proxy running on port ${PORT}`);
});
