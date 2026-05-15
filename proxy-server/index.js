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
const startTime = Date.now();

async function fetchHistory() {
  console.log(`[${new Date().toISOString()}] Fetching historical OHLC data for ${PAIRS.length} pairs...`);
  for (const pair of PAIRS) {
    try {
      const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(pair)}&interval=1h&outputsize=200&apikey=${API_KEY}`;
      const response = await axios.get(url, { timeout: 15000 });
      if (response.data && response.data.values) {
        historyCache[pair] = response.data.values;
        console.log(`[${new Date().toISOString()}] History fetched for ${pair}: ${response.data.values.length} candles`);
      } else {
        console.warn(`[${new Date().toISOString()}] No history data for ${pair}:`, JSON.stringify(response.data));
        historyCache[pair] = [];
      }
    } catch (err) {
      console.warn(`[${new Date().toISOString()}] Failed to fetch history for ${pair}:`, err.message);
      historyCache[pair] = [];
    }
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
  console.log(`[${new Date().toISOString()}] Poll cycle complete — ${updated} pairs updated`);
}

app.get('/rates', (req, res) => {
  res.json({ pairs: ratesCache });
});

app.get('/history', (req, res) => {
  res.json({ pairs: historyCache });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

async function start() {
  await fetchHistory();
  await pollRates();
  setInterval(pollRates, 120000);
  app.listen(PORT, () => {
    console.log(`[${new Date().toISOString()}] Forex proxy server listening on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
