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

const RATES_CACHE_TTL = 15 * 60 * 1000;
const HISTORY_CACHE_TTL = 6 * 60 * 60 * 1000;
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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

// --- Fetch rates (sequential with delay) ---
async function fetchRates() {
  const newPairs = {};
  for (const pair of FOREX_PAIRS) {
    try {
      incrementCallCount();
      const response = await axios.get('https://api.twelvedata.com/exchange_rate', {
        params: { symbol: pair, apikey: API_KEY }
      });
      newPairs[pair] = {
        price: response.data.rate,
        fetchedAt: new Date().toISOString()
      };
    } catch (err) {
      console.error(`[${new Date().toISOString()}] Rate fetch failed for ${pair}:`, err.message);
      if (ratesCache.data.pairs[pair]) {
        newPairs[pair] = ratesCache.data.pairs[pair];
      }
    }
    await sleep(500);
  }
  ratesCache = { data: { pairs: newPairs }, fetchedAt: Date.now() };
  console.log(`[${new Date().toISOString()}] Rates polled. Daily API calls so far: ${dailyCallCount}`);
}

// --- Fetch history + merge into candle store (sequential with delay) ---
// This single function replaces both fetchHistory() and collectCandles()
// so we never make duplicate calls to /time_series
async function fetchHistoryAndCollect() {
  const newPairs = {};
  console.log(`[${new Date().toISOString()}] Starting history + candle collection...`);
  for (const pair of FOREX_PAIRS) {
    try {
      incrementCallCount();
      const response = await axios.get('https://api.twelvedata.com/time_series', {
        params: {
          symbol: pair,
          interval: '1h',
          outputsize: 200,
          apikey: API_KEY
        }
      });
      const candles = response.data.values || [];
      newPairs[pair] = candles;
      mergeCandles(pair, candles);
      lastCollectionError = null;
    } catch (err) {
      lastCollectionError = err.message;
      console.error(`[${new Date().toISOString()}] History/candle fetch failed for ${pair}:`, err.message);
      if (historyCache.data.pairs[pair]) {
        newPairs[pair] = historyCache.data.pairs[pair];
      }
    }
    await sleep(500);
  }
  historyCache = { data: { pairs: newPairs }, fetchedAt: Date.now() };
  try {
    fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8');
  } catch (err) {
    console.error('Failed to write candle store to disk:', err.message);
  }
  console.log(`[${new Date().toISOString()}] History + candles updated. Daily API calls so far: ${dailyCallCount}`);
}

// --- Merge candles into store ---
function mergeCandles(pair, newCandles) {
  if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
  const existing = candleStore.pairs[pair];
  const existingDatetimes = new Set(existing.map(c => c.datetime));
  for (const candle of newCandles) {
    if (!existingDatetimes.has(candle.datetime)) {
      existing.push(candle);
    }
  }
  existing.sort((a, b) => new Date(a.datetime) - new Date(b.datetime));
  if (existing.length > CANDLE_CAP) {
    candleStore.pairs[pair] = existing.slice(existing.length - CANDLE_CAP);
  }
}

// --- Startup (staggered) ---
(async () => {
  // Fetch rates first
  await fetchRates();

  // Wait 10 seconds before firing history + candle collection
  // so startup doesn't blast all calls at once
  await sleep(10000);
  await fetchHistoryAndCollect();

  // Rates: check every minute, fetch only when TTL expired
  setInterval(async () => {
    if (Date.now() - ratesCache.fetchedAt >= RATES_CACHE_TTL) {
      await fetchRates();
    }
  }, 60 * 1000);

  // History + candles: check every minute, fetch only when TTL expired
  // TTL is the longer of HISTORY_CACHE_TTL (6h) and 30 minutes
  // We use 30 minutes so candles are refreshed at least every 30 min
  const COLLECTION_TTL = 30 * 60 * 1000;
  setInterval(async () => {
    if (Date.now() - historyCache.fetchedAt >= COLLECTION_TTL) {
      await fetchHistoryAndCollect();
    }
  }, 60 * 1000);
})();

// --- Routes ---

app.get('/rates', (req, res) => {
  res.json(ratesCache.data);
});

app.get('/history', (req, res) => {
  res.json(historyCache.data);
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    dailyCallCount,
    ratesCachedAt: ratesCache.fetchedAt ? new Date(ratesCache.fetchedAt).toISOString() : null,
    historyCachedAt: historyCache.fetchedAt ? new Date(historyCache.fetchedAt).toISOString() : null,
    lastCollectionError
  });
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

// --- Start server ---
app.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] Forex proxy running on port ${PORT}`);
});
