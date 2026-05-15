const express = require('express');
const axios = require('axios');
const cors = require('cors');

// --- Config ---
const TWELVEDATA_API_KEY = process.env.TWELVEDATA_API_KEY;
if (!TWELVEDATA_API_KEY) {
  console.error('[ERROR] TWELVEDATA_API_KEY environment variable is required but not set.');
  process.exit(1);
}

const FOREX_PAIRS_RAW = process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD';
const FOREX_PAIRS = FOREX_PAIRS_RAW.split(',').map(p => p.trim()).filter(Boolean);
const PORT = process.env.PORT || 3000;

// --- In-memory cache ---
const cache = {}; // { "EUR/USD": { price: 1.234, timestamp: "..." } }
let lastPoll = null;

// --- Poll function ---
async function pollForexRates() {
  const now = new Date().toISOString();
  console.log(`[${now}] Poll cycle started for pairs: ${FOREX_PAIRS.join(', ')}`);

  const symbol = FOREX_PAIRS.join(',');
  const url = `https://api.twelvedata.com/price?symbol=${encodeURIComponent(symbol)}&apikey=${TWELVEDATA_API_KEY}`;

  try {
    const response = await axios.get(url, { timeout: 10000 });

    if (response.status !== 200) {
      throw new Error(`Non-200 status: ${response.status}`);
    }

    const data = response.data;

    if (FOREX_PAIRS.length === 1) {
      // Single pair: { price: "1.234" }
      const pair = FOREX_PAIRS[0];
      if (data.price) {
        cache[pair] = {
          price: parseFloat(data.price),
          timestamp: new Date().toISOString(),
        };
        console.log(`[${new Date().toISOString()}] Poll success: ${pair} = ${cache[pair].price}`);
      } else {
        throw new Error(`Unexpected response for ${pair}: ${JSON.stringify(data)}`);
      }
    } else {
      // Multiple pairs: { "EUR/USD": { price: "1.234" }, ... }
      let updated = 0;
      for (const pair of FOREX_PAIRS) {
        const pairData = data[pair];
        if (pairData && pairData.price) {
          cache[pair] = {
            price: parseFloat(pairData.price),
            timestamp: new Date().toISOString(),
          };
          updated++;
        } else {
          console.warn(`[${new Date().toISOString()}] No price data for ${pair} in response`);
        }
      }
      console.log(`[${new Date().toISOString()}] Poll success: updated ${updated}/${FOREX_PAIRS.length} pairs`);
    }

    lastPoll = new Date().toISOString();
  } catch (err) {
    console.error(`[${new Date().toISOString()}] Poll failed: ${err.message}`);
    console.error('Serving last cached values (if any).');
  }
}

// --- Express app ---
const app = express();

app.use(cors());
app.use(express.json());

// GET /rates
app.get('/rates', (req, res) => {
  res.json({
    success: true,
    data: cache,
    cachedAt: lastPoll,
    pairs: FOREX_PAIRS,
  });
});

// GET /health
app.get('/health', (req, res) => {
  const cachedPairs = Object.keys(cache).length;
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    pairs: FOREX_PAIRS,
    cachedPairs,
    lastPoll,
  });
});

// --- Start ---
app.listen(PORT, '0.0.0.0', async () => {
  console.log(`Forex proxy server running on port ${PORT}`);
  // Immediate first poll on startup
  await pollForexRates();
  // Schedule subsequent polls every 2 minutes
  setInterval(pollForexRates, 120000);
});
