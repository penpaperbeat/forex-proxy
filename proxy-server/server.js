'use strict';

const express = require('express');
const axios   = require('axios');
const fs      = require('fs');
const crypto  = require('crypto');

const app  = express();
const PORT = process.env.PORT || 3000;
// SMC Engine — Smart Money Concepts detection layer
const smcEngine      = require('./smcEngine');
const ensembleScorer = require('./ensembleScorer');
const API_KEY        = process.env.TWELVEDATA_API_KEY;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const PROXY_SECRET   = process.env.PROXY_SECRET || '';

// ---------------------------------------------------------------------------
// Single source-of-truth for all Forex pairs
// ---------------------------------------------------------------------------
const FOREX_PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,USD/CHF,AUD/USD,USD/CAD,NZD/USD')
  .split(',')
  .map(p => p.trim());

// Bid-to-mid price adjustments (half-spread) per pair
const BID_TO_MID_ADJUSTMENT = {
  'EUR/USD': 0.00015,
  'GBP/USD': 0.00025,
  'USD/JPY': 0.015,
  'USD/CHF': 0.00020,
  'AUD/USD': 0.00020,
  'USD/CAD': 0.00025,
  'NZD/USD': 0.00025
};

// Dukascopy instrument name mapping (no slash, lowercase)
const DUKASCOPY_INSTRUMENTS = {
  'EUR/USD': 'eurusd',
  'GBP/USD': 'gbpusd',
  'USD/JPY': 'usdjpy',
  'USD/CHF': 'usdchf',
  'AUD/USD': 'audusd',
  'USD/CAD': 'usdcad',
  'NZD/USD': 'nzdusd'
};

// ---------------------------------------------------------------------------
// File paths — ALL use /data (Railway persistent volume)
// ---------------------------------------------------------------------------
const CANDLE_STORE_PATH         = '/data/candle-store.json';
const BACKFILL_PROGRESS_PATH    = '/data/backfill-progress.json';
const INTELLIGENCE_PROFILE_PATH = '/data/intelligence-profile.json';

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------
let ratesCache      = { data: { pairs: {} }, fetchedAt: 0 };
let historyCache    = { data: { pairs: {} }, fetchedAt: 0 };
let candleStore     = { pairs: {} };
let calendarCache   = { data: [], fetchedAt: 0 };
let intelligenceProfile = null;

let dailyCallCount    = 0;
let callCountResetAt  = nextMidnightUTC();
let lastCollectionError = null;

let backfillStatus        = 'pending';  // pending | running | complete | error
let backfillPairsComplete = 0;
let backfillTotalCandles  = 0;

let intelligenceStatus          = 'pending'; // pending | calculating | active | stale
let intelligenceLastCalculated  = null;

// ---------------------------------------------------------------------------
// SMC / DXY in-memory state
// ---------------------------------------------------------------------------
let currentDXYBias   = 'NEUTRAL'; // updated by background DXY refresh
let dxyLastFetchedAt = null;

// Paper signals buffer (up to 500 entries)
const PAPER_SIGNALS_MAX = 500;
let paperSignalsBuffer = [];

// Resolved signals buffer for ensemble retraining
// Each entry: { features: number[], outcome: bool }
let resolvedSignalsBuffer = [];
let newResolvedSinceRetrain = 0;

// Track last Holy Trinity signal time per pair (for 24h fallback)
// { 'EUR/USD': Date, ... }
const lastHolyTrinityAt = {};

// Candle push batching: only push candles to canister every N hours.
// Configurable via env var — default 2 hours.
let lastCandlePushTime = 0;
const CANDLE_PUSH_INTERVAL_MS = parseInt(process.env.CANDLE_PUSH_INTERVAL_MS, 10) || (2 * 60 * 60 * 1000);

// Concurrent collection cycle guard
let isCollectionRunning = false;

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

/**
 * Returns the UTC timestamp for the start of the next calendar day in UTC.
 * Uses explicit UTC month arithmetic to handle month boundaries correctly.
 */
function nextMidnightUTC() {
  const now   = new Date();
  const year  = now.getUTCFullYear();
  const month = now.getUTCMonth();
  const date  = now.getUTCDate();
  // Start of the next calendar day — safe across all month/year boundaries
  return Date.UTC(year, month, date + 1);
}

function incrementCallCount() {
  if (Date.now() >= callCountResetAt) {
    dailyCallCount   = 0;
    callCountResetAt = nextMidnightUTC();
  }
  dailyCallCount++;
}

/**
 * Timing-safe secret comparison to prevent timing attacks.
 */
function secretMatches(provided) {
  if (!PROXY_SECRET || !provided) return false;
  try {
    const a = Buffer.from(PROXY_SECRET, 'utf8');
    const b = Buffer.from(provided,     'utf8');
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch (_) {
    return false;
  }
}

/**
 * CORS — uses ALLOWED_ORIGIN env var only; no wildcard fallback.
 */
function cors(res) {
  const origin = process.env.ALLOWED_ORIGIN;
  if (origin) res.setHeader('Access-Control-Allow-Origin', origin);
}

/**
 * Atomic file write: write to .tmp then rename to prevent corruption on crash.
 */
function atomicWriteJSON(filePath, data) {
  const tmpPath = filePath + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(data), 'utf8');
  fs.renameSync(tmpPath, filePath);
}

// ---------------------------------------------------------------------------
// Load persisted data from disk on startup
// ---------------------------------------------------------------------------
try {
  if (fs.existsSync(CANDLE_STORE_PATH)) {
    const raw = fs.readFileSync(CANDLE_STORE_PATH, 'utf8');
    candleStore = JSON.parse(raw);
    // Populate historyCache from store so /history works immediately
    historyCache.data = { pairs: {} };
    for (const pair of Object.keys(candleStore.pairs)) {
      historyCache.data.pairs[pair] = candleStore.pairs[pair].slice(-5000);
    }
    console.log(`[startup] Candle store loaded from disk (${Object.keys(candleStore.pairs).length} pairs).`);
  }
} catch (err) {
  console.error('[startup] Failed to load candle store from disk:', err.message);
  candleStore = { pairs: {} };
}

try {
  if (fs.existsSync(INTELLIGENCE_PROFILE_PATH)) {
    const raw = fs.readFileSync(INTELLIGENCE_PROFILE_PATH, 'utf8');
    intelligenceProfile = JSON.parse(raw);
    intelligenceStatus  = 'active';
    intelligenceLastCalculated = intelligenceProfile._calculatedAt || null;
    console.log('[startup] Intelligence profile loaded from disk.');
  }
} catch (err) {
  console.error('[startup] Failed to load intelligence profile from disk:', err.message);
  intelligenceProfile = null;
}

// Warn clearly if MASSIVE_API_KEY is absent
if (!process.env.MASSIVE_API_KEY) {
  console.warn('[startup] WARNING: MASSIVE_API_KEY not set — backfill will fail with 403');
}

// ---------------------------------------------------------------------------
// Resolve signals that passed their 24h window since previous session
// ---------------------------------------------------------------------------
function resolveOldSignals() {
  const now = Date.now();
  let resolved = 0;
  for (const sig of paperSignalsBuffer) {
    if (sig.resolvedAt || !sig.generatedAt) continue;
    const age = now - new Date(sig.generatedAt).getTime();
    if (age >= 24 * 60 * 60 * 1000) {
      // Paper signals resolve as informational — no outcome tracking needed
      sig.resolvedAt = new Date().toISOString();
      resolved++;
    }
  }
  if (resolved > 0) console.log(`[resolveOldSignals] Resolved ${resolved} stale paper signals.`);
}

// ---------------------------------------------------------------------------
// Technical indicator helpers
// ---------------------------------------------------------------------------

/**
 * Calculate ATR (Average True Range) for an array of OHLC candles.
 * @param {Array} candles  — [{open, high, low, close}...] sorted ascending
 * @param {number} period
 * @returns {number[]} ATR values (length = candles.length - 1, aligned to index 1..n)
 */
function calculateATR(candles, period = 14) {
  if (candles.length < 2) return [];
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const curr = candles[i];
    const prev = candles[i - 1];
    const tr = Math.max(
      curr.high - curr.low,
      Math.abs(curr.high - prev.close),
      Math.abs(curr.low  - prev.close)
    );
    trs.push(tr);
  }
  const atrs = [];
  let sum = 0;
  for (let i = 0; i < trs.length; i++) {
    if (i < period) {
      sum += trs[i];
      if (i === period - 1) atrs.push(sum / period);
      else atrs.push(NaN);
    } else {
      const prev = atrs[atrs.length - 1];
      atrs.push((prev * (period - 1) + trs[i]) / period);
    }
  }
  return atrs;
}

/**
 * Calculate RSI for an array of candles.
 * @param {Array} candles — [{close}...] sorted ascending
 * @param {number} period
 * @returns {number[]} RSI values (NaN for initial period)
 */
function calculateRSI(candles, period = 14) {
  if (candles.length < period + 1) return candles.map(() => NaN);
  const rsi = new Array(candles.length).fill(NaN);
  let avgGain = 0;
  let avgLoss = 0;

  for (let i = 1; i <= period; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    if (diff >= 0) avgGain += diff / period;
    else           avgLoss += Math.abs(diff) / period;
  }

  const firstRS = avgLoss === 0 ? Infinity : avgGain / avgLoss;
  rsi[period] = 100 - 100 / (1 + firstRS);

  for (let i = period + 1; i < candles.length; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? Math.abs(diff) : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    const rs = avgLoss === 0 ? Infinity : avgGain / avgLoss;
    rsi[i] = 100 - 100 / (1 + rs);
  }
  return rsi;
}

// ---------------------------------------------------------------------------
// Twelve Data collection (rates + history), staggered 8s per pair
// ---------------------------------------------------------------------------
async function runCollection() {
  console.log(`[${new Date().toISOString()}] Collection cycle starting...`);

  // Clear any stale error at the start of each new cycle
  lastCollectionError = null;

  // Fetch rates — 8s stagger between pairs to stay under 8 calls/minute limit
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 8000));
    try {
      const response = await axios.get('https://api.twelvedata.com/exchange_rate', {
        params: { symbol: pair, apikey: API_KEY },
        timeout: 10000
      });
      if (response.data && response.data.rate) {
        ratesCache.data.pairs[pair] = {
          price:     String(response.data.rate),
          fetchedAt: new Date().toISOString()
        };
        ratesCache.fetchedAt = Date.now();
      }
      incrementCallCount();
    } catch (err) {
      lastCollectionError = `${pair} rates: ${err.message}`;
      console.error(`[collection] Rate fetch failed for ${pair}:`, err.message);
    }
  }

  await new Promise(r => setTimeout(r, 8000));

  // Fetch history — 8s stagger between pairs to stay under 8 calls/minute limit
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 8000));
    try {
      const response = await axios.get('https://api.twelvedata.com/time_series', {
        params: { symbol: pair, interval: '1h', outputsize: 5000, apikey: API_KEY },
        timeout: 30000
      });
      const newCandles = (response.data.values || [])
        .map(c => ({
          datetime: c.datetime,
          open:  parseFloat(c.open),
          high:  parseFloat(c.high),
          low:   parseFloat(c.low),
          close: parseFloat(c.close)
        }))
        .filter(c => !isNaN(c.open) && !isNaN(c.close));

      if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
      const map = {};
      for (const c of candleStore.pairs[pair]) map[c.datetime] = c;
      for (const c of newCandles)              map[c.datetime] = c;
      const merged = Object.values(map).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
      candleStore.pairs[pair]       = merged;
      historyCache.data.pairs[pair] = merged.slice(-5000);
      historyCache.fetchedAt        = Date.now();
      incrementCallCount();
    } catch (err) {
      lastCollectionError = `${pair} history: ${err.message}`;
      console.error(`[collection] History fetch failed for ${pair}:`, err.message);
    }
  }

  // Atomic persist candles to disk (every cycle)
  try {
    atomicWriteJSON(CANDLE_STORE_PATH, candleStore);
    console.log(`[${new Date().toISOString()}] Collection complete. Daily calls: ${dailyCallCount}`);
  } catch (err) {
    lastCollectionError = `File write failed: ${err.message}`;
    console.error('[collection] Failed to write candle store to disk:', err.message);
  }

  // Candle push to canister — batched every N hours to minimise cycle usage.
  // Paper signals are NEVER included here; they live only in paperSignalsBuffer.
  const now = Date.now();
  if (now - lastCandlePushTime >= CANDLE_PUSH_INTERVAL_MS) {
    lastCandlePushTime = now;
    // Wire canister push here when ready. Example:
    // pushCandlesToCanister(candleStore).catch(err => console.error('[canister] Candle push failed:', err.message));
    console.log('[collection] Candle push window reached. Ready for canister sync.');
  }
}

// ---------------------------------------------------------------------------
// Economic calendar — Finnhub primary, ForexFactory RSS fallback
// ---------------------------------------------------------------------------
const CALENDAR_CACHE_TTL   = 60 * 60 * 1000; // 1 hour
const RELEVANT_CURRENCIES  = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD'];

// Map Finnhub country codes to currency codes
const COUNTRY_TO_CURRENCY = {
  US: 'USD', EU: 'EUR', GB: 'GBP', JP: 'JPY',
  CH: 'CHF', AU: 'AUD', CA: 'CAD', NZ: 'NZD',
  DE: 'EUR', FR: 'EUR', IT: 'EUR', ES: 'EUR'
};

async function fetchCalendar() {
  const now  = new Date();
  const from = now.toISOString().split('T')[0];
  const to   = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  if (FINNHUB_API_KEY) {
    try {
      const response = await axios.get('https://finnhub.io/api/v1/calendar/economic', {
        params: { from, to, token: FINNHUB_API_KEY },
        timeout: 10000
      });
      const events = (response.data.economicCalendar || [])
        .filter(e => {
          const impact   = (e.impact   || '').toLowerCase();
          // Map country code to currency ONCE here
          const currency = COUNTRY_TO_CURRENCY[(e.country || '').toUpperCase()] || (e.country || '').toUpperCase();
          return impact === 'high' && RELEVANT_CURRENCIES.includes(currency);
        })
        .map(e => {
          const currency = COUNTRY_TO_CURRENCY[(e.country || '').toUpperCase()] || (e.country || '').toUpperCase();
          return {
            time:     e.time,
            currency, // correctly mapped currency code
            event:    e.event,
            impact:   e.impact,
            forecast: e.estimate || null,
            previous: e.prev    || null
          };
        });
      calendarCache = { data: events, fetchedAt: Date.now() };
      console.log(`[calendar] Finnhub: ${events.length} high-impact events.`);
      return;
    } catch (err) {
      console.error('[calendar] Finnhub fetch failed:', err.message);
    }
  }

  // ForexFactory RSS fallback
  try {
    const rssResponse = await axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.xml', {
      timeout: 10000
    });
    const xml    = rssResponse.data;
    const events = [];
    const itemRe = /<item>([\s\S]*?)<\/item>/g;
    let match;
    while ((match = itemRe.exec(xml)) !== null) {
      const item     = match[1];
      const title    = (item.match(/<title>(.*?)<\/title>/)    || [])[1] || '';
      const pubDate  = (item.match(/<pubDate>(.*?)<\/pubDate>/) || [])[1] || '';
      const impact   = (item.match(/<impact>(.*?)<\/impact>/)   || [])[1] || '';
      const currency = (item.match(/<country>(.*?)<\/country>/) || [])[1] || '';
      if (impact.toLowerCase() === 'high' && RELEVANT_CURRENCIES.includes(currency.toUpperCase())) {
        events.push({ time: pubDate, currency: currency.toUpperCase(), event: title, impact: 'high', forecast: null, previous: null });
      }
    }
    calendarCache = { data: events, fetchedAt: Date.now() };
    console.log(`[calendar] ForexFactory RSS: ${events.length} high-impact events.`);
  } catch (err) {
    console.error('[calendar] ForexFactory RSS fallback failed:', err.message);
  }
}

// ---------------------------------------------------------------------------
// Dukascopy 10-year historical backfill
// ---------------------------------------------------------------------------
async function runDukascopyBackfill() {
  backfillStatus = 'running';
  console.log('[backfill] Starting Dukascopy 10-year backfill...');

  let getHistoricalRates;
  try {
    ({ getHistoricalRates } = require('dukascopy-node'));
  } catch (err) {
    backfillStatus = 'error';
    console.error('[backfill] dukascopy-node not available:', err.message);
    return;
  }

  // Load progress checkpoint
  let progress = {};
  try {
    if (fs.existsSync(BACKFILL_PROGRESS_PATH)) {
      progress = JSON.parse(fs.readFileSync(BACKFILL_PROGRESS_PATH, 'utf8'));
    }
  } catch (err) {
    console.error('[backfill] Failed to load progress checkpoint:', err.message);
    progress = {};
  }

  const now       = new Date();
  const startYear = now.getUTCFullYear() - 10;

  for (const pair of FOREX_PAIRS) {
    if (progress[pair] === 'complete') {
      console.log(`[backfill] ${pair}: already complete, skipping.`);
      backfillPairsComplete++;
      continue;
    }

    const instrument = DUKASCOPY_INSTRUMENTS[pair];
    if (!instrument) {
      console.warn(`[backfill] ${pair}: no Dukascopy instrument mapping, skipping.`);
      continue;
    }

    const adjustment = BID_TO_MID_ADJUSTMENT[pair] || 0;
    let pairCandles  = [];
    let yearsFailed  = 0;

    for (let year = startYear; year <= now.getUTCFullYear(); year++) {
      const yearStart = new Date(Date.UTC(year, 0, 1)).toISOString();
      const yearEnd   = year === now.getUTCFullYear()
        ? now.toISOString()
        : new Date(Date.UTC(year + 1, 0, 1)).toISOString();

      // Yield to event loop between each yearly fetch
      await new Promise(r => setImmediate(r));

      try {
        console.log(`[backfill] ${pair} / ${year}: fetching...`);
        const result = await getHistoricalRates({
          instrument: instrument,
          dates:      { from: new Date(yearStart), to: new Date(yearEnd) },
          timeframe:  'h1',
          format:     'object',
          flushDownloadProgress: false
        });

        const raw = Array.isArray(result) ? result : [];
        let prev  = null;
        let kept  = 0;

        for (const row of raw) {
          // dukascopy-node 'object' format: { timestamp, open, high, low, close, volume }
          const open  = (row.open  || row.askOpen  || 0) + adjustment;
          const high  = (row.high  || row.askHigh  || 0) + adjustment;
          const low   = (row.low   || row.askLow   || 0) + adjustment;
          const close = (row.close || row.askClose || 0) + adjustment;

          if (!open || !close) continue;

          // Data quality filter: reject candles >20% away from previous close
          if (prev !== null) {
            const change = Math.abs(close - prev) / prev;
            if (change > 0.20) {
              console.warn(`[backfill] ${pair} outlier rejected: close=${close}, prev=${prev}`);
              continue;
            }
          }

          const ts = row.timestamp
            ? new Date(row.timestamp).toISOString().slice(0, 19).replace('T', ' ')
            : null;
          if (!ts) continue;

          pairCandles.push({ datetime: ts, open, high, low, close });
          prev = close;
          kept++;
        }

        console.log(`[backfill] ${pair} / ${year}: ${kept} candles kept.`);
      } catch (err) {
        yearsFailed++;
        console.error(`[backfill] ${pair} / ${year}: fetch error: ${err.message}`);
        if (yearsFailed >= 3) {
          console.error(`[backfill] ${pair}: too many year failures, skipping pair.`);
          break;
        }
      }

      // Small pause between yearly batches to avoid hammering Dukascopy
      await new Promise(r => setTimeout(r, 500));
    }

    if (pairCandles.length === 0) {
      progress[pair] = 'incomplete';
      try { atomicWriteJSON(BACKFILL_PROGRESS_PATH, progress); } catch (_) {}
      continue;
    }

    // Merge with existing candleStore (no cap — deep history store)
    if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
    const mapByDt = {};
    for (const c of candleStore.pairs[pair]) mapByDt[c.datetime] = c;
    for (const c of pairCandles)             mapByDt[c.datetime] = c;
    const merged = Object.values(mapByDt).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
    candleStore.pairs[pair] = merged;
    backfillTotalCandles   += pairCandles.length;

    // Update historyCache working window
    historyCache.data.pairs[pair] = merged.slice(-5000);

    // Atomic persist candle store and checkpoint after each pair
    try {
      atomicWriteJSON(CANDLE_STORE_PATH, candleStore);
    } catch (err) {
      console.error('[backfill] Failed to persist candle store:', err.message);
    }

    progress[pair] = 'complete';
    backfillPairsComplete++;
    try { atomicWriteJSON(BACKFILL_PROGRESS_PATH, progress); } catch (_) {}

    console.log(`[backfill] ${pair}: complete (${pairCandles.length} candles). Total so far: ${backfillTotalCandles}.`);
  }

  const allComplete = FOREX_PAIRS.every(p => progress[p] === 'complete');
  backfillStatus = allComplete ? 'complete' : 'error';
  console.log(`[backfill] Done. Status: ${backfillStatus}. Total candles: ${backfillTotalCandles}.`);
}

// ---------------------------------------------------------------------------
// Intelligence profile calculation
// ---------------------------------------------------------------------------

/** Yield to event loop */
function yieldLoop() {
  return new Promise(r => setImmediate(r));
}

/** Percentile from sorted numeric array */
function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.floor((p / 100) * (sorted.length - 1));
  return sorted[idx];
}

/** Session for a given UTC datetime string (YYYY-MM-DD HH:MM:SS) */
function classifySession(datetime) {
  const hour = parseInt(datetime.slice(11, 13), 10);
  const londonOpen   = hour >= 7  && hour < 12;
  const nyOpen       = hour >= 12 && hour < 17;
  const londonNY     = hour >= 12 && hour < 16;
  const tokyoOpen    = (hour >= 23 || hour < 7);
  const sydneyOpen   = hour >= 21 && hour < 23;

  if (londonNY)  return 'londonNY';
  if (nyOpen)    return 'newYork';
  if (londonOpen)return 'london';
  if (tokyoOpen) return 'tokyo';
  if (sydneyOpen)return 'sydney';
  return null;
}

async function calculateIntelligenceProfile() {
  const totalPairs = FOREX_PAIRS.length;
  let calculated   = 0;

  intelligenceStatus = 'calculating';
  console.log('[intelligence] Starting profile calculation...');

  const profile = { _calculatedAt: new Date().toISOString() };

  for (const pair of FOREX_PAIRS) {
    await yieldLoop();

    const candles = candleStore.pairs[pair];
    if (!candles || candles.length < 200) {
      console.warn(`[intelligence] ${pair}: insufficient candles (${(candles || []).length}), skipping.`);
      profile[pair] = { status: 'insufficient_data' };
      calculated++;
      continue;
    }

    // --- ATR percentiles ---
    await yieldLoop();
    const atrs    = calculateATR(candles, 14).filter(v => !isNaN(v));
    const atrSort = [...atrs].sort((a, b) => a - b);
    const atrPercentiles = {
      p25: percentile(atrSort, 25),
      p50: percentile(atrSort, 50),
      p75: percentile(atrSort, 75),
      p95: percentile(atrSort, 95)
    };

    // Current volatility regime (last 14 candles)
    const recentCandles = candles.slice(-15);
    const recentATR     = calculateATR(recentCandles, 14).filter(v => !isNaN(v));
    const currentATR    = recentATR.length ? recentATR[recentATR.length - 1] : atrPercentiles.p50;
    let volatilityRegime;
    if      (currentATR < atrPercentiles.p25) volatilityRegime = 'low';
    else if (currentATR < atrPercentiles.p75) volatilityRegime = 'normal';
    else if (currentATR < atrPercentiles.p95) volatilityRegime = 'high';
    else                                       volatilityRegime = 'extreme';

    // --- Adaptive RSI thresholds (rolling walk-forward, train 70% / test 30%) ---
    await yieldLoop();
    const splitIdx    = Math.floor(candles.length * 0.7);
    const testCandles = candles.slice(splitIdx);
    const rsiValues   = calculateRSI(testCandles, 14);

    let bestOversold   = 30;
    let bestOverbought = 70;
    let bestScore      = -Infinity;

    for (let os = 25; os <= 45; os += 2.5) {
      for (let ob = 55; ob <= 75; ob += 2.5) {
        let wins = 0, total = 0;
        for (let i = 1; i < testCandles.length - 4; i++) {
          const rsi = rsiValues[i];
          if (isNaN(rsi)) continue;
          const isBuy  = rsi < os;
          const isSell = rsi > ob;
          if (!isBuy && !isSell) continue;
          const atr1 = atrs.length ? atrs[Math.min(splitIdx + i, atrs.length - 1)] : 0.001;
          const future4 = testCandles.slice(i + 1, i + 5).map(c => c.close);
          if (future4.length < 4) continue;
          const maxMove = isBuy
            ? Math.max(...future4) - testCandles[i].close
            : testCandles[i].close - Math.min(...future4);
          if (maxMove >= atr1) wins++;
          total++;
        }
        if (total < 20) continue;
        const score = wins / total;
        if (score > bestScore) {
          bestScore      = score;
          bestOversold   = os;
          bestOverbought = ob;
        }
      }
    }
    const adaptiveRSI = { oversold: bestOversold, overbought: bestOverbought };

    // --- Session win rate matrix ---
    await yieldLoop();
    const sessions = ['london', 'newYork', 'londonNY', 'tokyo', 'sydney'];
    const sessionMatrix = {};
    for (const sess of sessions) sessionMatrix[sess] = { wins: 0, total: 0 };

    // Exponential decay weight per year (most recent = 1.0, each prior = *0.85)
    const nowYear = new Date().getUTCFullYear();

    for (let i = 14; i < candles.length - 4; i++) {
      const c   = candles[i];
      const sess = classifySession(c.datetime);
      if (!sess) continue;

      const candleYear   = parseInt(c.datetime.slice(0, 4), 10);
      const yearsAgo     = nowYear - candleYear;
      const weight       = Math.pow(0.85, yearsAgo);

      // Win: price moves >= 1×ATR in next 4 candles
      const atrIdx  = Math.max(0, i - 1);  // offset: ATR array starts at index 1
      const atrVal  = atrs[atrIdx] || atrPercentiles.p50;
      const future4 = candles.slice(i + 1, i + 5);
      if (future4.length < 4) continue;

      const upMove   = future4.length ? Math.max(...future4.map(fc => fc.close)) - c.close : 0;
      const downMove = future4.length ? c.close - Math.min(...future4.map(fc => fc.close)) : 0;
      const moved    = Math.max(upMove, downMove) >= atrVal;

      sessionMatrix[sess].total += weight;
      if (moved) sessionMatrix[sess].wins += weight;
    }

    const finalSessionMatrix = {};
    for (const sess of sessions) {
      const { wins, total } = sessionMatrix[sess];
      const winRate    = total >= 5 ? wins / total : 0.55;
      const sampleSize = Math.round(total);
      const confidence = sampleSize >= 500 ? 'high' :
                         sampleSize >= 200 ? 'medium' :
                         sampleSize >= 100 ? 'low' : 'insufficient';
      finalSessionMatrix[sess] = { winRate: Math.round(winRate * 1000) / 1000, sampleSize, confidence };
    }

    // --- Seasonal bias ---
    await yieldLoop();
    const monthBuckets = {};
    for (let m = 1; m <= 12; m++) monthBuckets[m] = { totalWeight: 0, moveWeight: 0 };

    for (let i = 14; i < candles.length - 4; i++) {
      const c = candles[i];
      const month      = parseInt(c.datetime.slice(5, 7), 10);
      const candleYear = parseInt(c.datetime.slice(0, 4), 10);
      const yearsAgo   = nowYear - candleYear;
      const weight     = Math.pow(0.85, yearsAgo);

      const atrVal  = atrs[Math.max(0, i - 1)] || atrPercentiles.p50;
      const future4 = candles.slice(i + 1, i + 5);
      if (future4.length < 4) continue;
      const upMove   = future4.length ? Math.max(...future4.map(fc => fc.close)) - c.close : 0;
      const downMove = future4.length ? c.close - Math.min(...future4.map(fc => fc.close)) : 0;
      const relMove  = Math.max(upMove, downMove) / atrVal;

      monthBuckets[month].totalWeight += weight;
      monthBuckets[month].moveWeight  += weight * relMove;
    }

    const seasonalBias = {};
    const avgMove = Object.values(monthBuckets)
      .filter(b => b.totalWeight > 0)
      .reduce((s, b) => s + b.moveWeight / b.totalWeight, 0) / 12 || 1;
    for (let m = 1; m <= 12; m++) {
      const b = monthBuckets[m];
      seasonalBias[String(m)] = b.totalWeight > 0
        ? Math.round((b.moveWeight / b.totalWeight / avgMove) * 100) / 100
        : 1.00;
    }

    profile[pair] = {
      adaptiveRSI,
      atrPercentiles,
      sessionMatrix: finalSessionMatrix,
      seasonalBias,
      volatilityRegime,
      profileVersion: 1,
      lastCalculated: new Date().toISOString(),
      status: 'active'
    };

    calculated++;
    console.log(`[intelligence] ${pair}: done (${calculated}/${totalPairs}).`);
  }

  intelligenceProfile        = profile;
  intelligenceStatus         = 'active';
  intelligenceLastCalculated = profile._calculatedAt;

  try {
    atomicWriteJSON(INTELLIGENCE_PROFILE_PATH, profile);
    console.log('[intelligence] Profile saved to disk.');
  } catch (err) {
    console.error('[intelligence] Failed to persist profile:', err.message);
  }
}

// ---------------------------------------------------------------------------
// SMC signal evaluation — called after each collection cycle
// ---------------------------------------------------------------------------
async function runSMCEvaluation() {
  try {
    // Refresh DXY (fire-and-forget with cached result — never blocks)
    const dxyData = await smcEngine.fetchDXYData();
    currentDXYBias   = smcEngine.calculateDXYTrend(dxyData);
    dxyLastFetchedAt = new Date().toISOString();
  } catch (err) {
    console.error('[SMC] DXY refresh failed:', err.message);
  }

  const killzone = smcEngine.isInsideKillzone();

  for (const pair of FOREX_PAIRS) {
    try {
      const candles = candleStore.pairs[pair];
      if (!candles || candles.length < 50) continue;

      // ---- Derive a candidate direction from simple RSI bias ----
      const rsiVals   = calculateRSI(candles, 14);
      const lastRSI   = rsiVals[rsiVals.length - 1];
      if (isNaN(lastRSI)) continue;

      // Adaptive thresholds from intelligence profile if available
      const pairProfile  = intelligenceProfile && intelligenceProfile[pair];
      const oversold     = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.oversold  : 35;
      const overbought   = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.overbought : 65;

      let candidateDirection = null;
      if (lastRSI < oversold)   candidateDirection = 'BUY';
      if (lastRSI > overbought) candidateDirection = 'SELL';
      if (!candidateDirection) continue;

      const indicatorPassed = true; // RSI bias = indicator confirmation for this pass

      // ---- SMC detection ----
      const obList     = smcEngine.detectOrderBlocks(candles, candidateDirection);
      const fvgList    = smcEngine.detectFairValueGaps(candles, candidateDirection);
      const sweepResult = smcEngine.detectLiquiditySweep(candles);
      const pdZone      = smcEngine.getPremiumDiscountZone(candles);
      const trinity     = smcEngine.evaluateHolyTrinity(
        candles, candidateDirection, obList, fvgList, sweepResult, pdZone
      );

      // ---- DXY penalty ----
      const dxyPenalty = smcEngine.getDXYPenalty(currentDXYBias, pair, candidateDirection);

      // ---- Base confidence (RSI distance from threshold, 60–100) ----
      const rawConfidence = candidateDirection === 'BUY'
        ? Math.min(100, Math.round(60 + (oversold - lastRSI) * 2))
        : Math.min(100, Math.round(60 + (lastRSI - overbought) * 2));
      const adjustedConfidence = Math.max(0, rawConfidence - dxyPenalty);

      // ---- Signal type key ----
      const signalTypeKey = smcEngine.computeSignalTypeKey({
        orderBlockPresent:    trinity.orderBlockPresent,
        fvgPresent:           trinity.fvgPresent,
        liquiditySweepPresent: trinity.liquiditySweepPresent
      });

      // ---- Classify ----
      const classification = smcEngine.classifySignal(trinity, indicatorPassed, killzone);

      const now        = new Date();
      // isPaper is set immutably at creation time based on classification
      const isPaperSignal = (classification !== 'LIVE' && classification !== 'STANDARD');

      const signalBase = {
        pair,
        direction:              candidateDirection,
        confidence:             adjustedConfidence,
        orderBlockPresent:      trinity.orderBlockPresent,
        fvgPresent:             trinity.fvgPresent,
        liquiditySweepPresent:  trinity.liquiditySweepPresent,
        killzoneActive:         killzone.active,
        killzoneName:           killzone.killzoneName || null,
        dxyBias:                currentDXYBias,
        obZone:                 trinity.obZone,
        fvgZone:                trinity.fvgZone,
        ensembleScore:          null, // populated by XGBoost layer
        signalTypeKey,
        generatedAt:            now.toISOString(),
        // isPaper is frozen at creation — cannot change even if isModelTrained flips mid-cycle
        isPaper:                isPaperSignal
      };

      // ---- Ensemble scoring (async — await inside the async function) ----
      let ensembleScore  = null;
      let highConviction = false;
      if (classification === 'LIVE' || classification === 'STANDARD') {
        try {
          const features = ensembleScorer.extractFeatures(signalBase, null);
          const score    = await ensembleScorer.scoreSignal(features);
          if (score !== null) {
            ensembleScore  = score;
            highConviction = score >= 75;
          }
        } catch (err) {
          console.warn(`[Ensemble] Scoring failed for ${pair}:`, err.message);
        }
      }

      if (classification === 'LIVE') {
        // Demote to paper if ensembleScore < 40 (only when model is trained)
        const demoteToPaper = ensembleScore !== null && ensembleScore < 40;

        if (demoteToPaper) {
          const paperSignal = {
            ...signalBase,
            ensembleScore,
            isPaper:     true, // explicit: demoted paper
            paperReason: 'ENSEMBLE_LOW_SCORE'
          };
          if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift();
          paperSignalsBuffer.push(paperSignal);
          console.log(`[SMC] LIVE demoted to PAPER (ensembleScore=${ensembleScore}): ${pair} ${candidateDirection}`);
        } else {
          // Track last Holy Trinity signal time for 24h fallback
          lastHolyTrinityAt[pair] = now;
          const liveSignal = {
            ...signalBase,
            ensembleScore,
            highConviction,
            isPaper: false // explicit: not paper
          };
          console.log(`[SMC] LIVE signal: ${pair} ${candidateDirection} (confidence ${adjustedConfidence}, typeKey ${signalTypeKey}, ensemble ${ensembleScore !== null ? ensembleScore : 'N/A'})`);
          // TODO: push liveSignal to canister when pushSignal endpoint is wired
          // NOTE: Only LIVE and STANDARD signals go to canister. Paper signals NEVER go to canister.
        }

      } else if (classification === 'PAPER_OUTSIDE_KILLZONE' || classification === 'PAPER_INDICATOR_FAILED') {
        const paperSignal = { ...signalBase, ensembleScore: null, isPaper: true, paperReason: classification };
        // Buffer paper signals (ring buffer, drop oldest)
        if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift();
        paperSignalsBuffer.push(paperSignal);
        console.log(`[SMC] PAPER signal: ${pair} ${candidateDirection} (${classification}) — stored in memory buffer only, NOT pushed to canister`);

      } else if (classification === 'STANDARD') {
        // Standard indicator signal — confidence capped at 70
        const demoteToPaper = ensembleScore !== null && ensembleScore < 40;
        if (demoteToPaper) {
          const paperSignal = {
            ...signalBase,
            confidence:  Math.min(70, adjustedConfidence),
            ensembleScore,
            isPaper:     true,
            paperReason: 'ENSEMBLE_LOW_SCORE'
          };
          if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift();
          paperSignalsBuffer.push(paperSignal);
          console.log(`[SMC] STANDARD demoted to PAPER (ensembleScore=${ensembleScore}): ${pair} ${candidateDirection}`);
        } else {
          const standardSignal = {
            ...signalBase,
            confidence:    Math.min(70, adjustedConfidence),
            ensembleScore,
            highConviction,
            isPaper:       false
          };
          console.log(`[SMC] STANDARD signal: ${pair} ${candidateDirection} (confidence ${standardSignal.confidence}, ensemble ${ensembleScore !== null ? ensembleScore : 'N/A'})`);
          // TODO: push standardSignal to canister
        }
      }

      // ---- 24h fallback: if no Holy Trinity signal for this pair in 24h, surface best indicator signal ----
      const holyTrinityAge = lastHolyTrinityAt[pair]
        ? (now - lastHolyTrinityAt[pair]) / 1000 / 3600
        : Infinity;
      if (holyTrinityAge > 24 && classification !== 'LIVE' && adjustedConfidence >= 50) {
        let fallbackEnsembleScore  = null;
        let fallbackHighConviction = false;
        try {
          const fallbackBase = {
            ...signalBase,
            confidence:            Math.min(70, adjustedConfidence),
            isPaper:               false,
            signalTypeKey:         0,
            orderBlockPresent:     false,
            fvgPresent:            false,
            liquiditySweepPresent: false,
            obZone:                null,
            fvgZone:               null,
            isFallback:            true
          };
          const fbFeatures = ensembleScorer.extractFeatures(fallbackBase, null);
          fallbackEnsembleScore  = await ensembleScorer.scoreSignal(fbFeatures);
          fallbackHighConviction = fallbackEnsembleScore !== null && fallbackEnsembleScore >= 75;
        } catch (_) {}

        const fallbackSignal = {
          ...signalBase,
          confidence:            Math.min(70, adjustedConfidence),
          ensembleScore:         fallbackEnsembleScore,
          highConviction:        fallbackHighConviction,
          isPaper:               false,
          signalTypeKey:         0,
          orderBlockPresent:     false,
          fvgPresent:            false,
          liquiditySweepPresent: false,
          obZone:                null,
          fvgZone:               null,
          isFallback:            true
        };
        console.log(`[SMC] FALLBACK signal: ${pair} ${candidateDirection} (24h without Holy Trinity, ensemble ${fallbackEnsembleScore !== null ? fallbackEnsembleScore : 'N/A'})`);
        // TODO: push fallbackSignal to canister
      }

    } catch (err) {
      console.error(`[SMC] Evaluation error for ${pair}:`, err.message);
    }
  }
}

// ---------------------------------------------------------------------------
// Auth middleware — timing-safe secret check
// ---------------------------------------------------------------------------
function requireSecret(req, res, next) {
  const provided = req.headers['x-forexmind-key'] || '';
  if (!secretMatches(provided)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Ensemble status endpoint
// ---------------------------------------------------------------------------
app.get('/ensemble-status', (req, res) => {
  cors(res);
  res.json(ensembleScorer.getEnsembleStatus());
});

// ---------------------------------------------------------------------------
// Paper signals endpoint
// ---------------------------------------------------------------------------
app.get('/paper-signals', (req, res) => {
  cors(res);
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : paperSignalsBuffer.length;
  const safe  = isNaN(limit) ? paperSignalsBuffer.length : Math.min(limit, paperSignalsBuffer.length);
  res.json({
    count:   paperSignalsBuffer.length,
    signals: paperSignalsBuffer.slice(-safe)
  });
});

// ---------------------------------------------------------------------------
// SMC status endpoint
// ---------------------------------------------------------------------------
app.get('/smc-status', (req, res) => {
  cors(res);
  try {
    const killzone  = smcEngine.isInsideKillzone();
    const dxyCache  = smcEngine.getDXYCache();
    const activePairs = {};

    for (const pair of FOREX_PAIRS) {
      const candles = candleStore.pairs[pair];
      if (!candles || candles.length < 50) {
        activePairs[pair] = { activeOBCount: 0, activeFVGCount: 0, lastSweepAge: null, premiumDiscount: 'UNKNOWN' };
        continue;
      }

      // Use BUY + SELL OBs for count
      const obsBuy  = smcEngine.detectOrderBlocks(candles, 'BUY');
      const obsSell = smcEngine.detectOrderBlocks(candles, 'SELL');
      const fvgsBuy  = smcEngine.detectFairValueGaps(candles, 'BUY');
      const fvgsSell = smcEngine.detectFairValueGaps(candles, 'SELL');
      const sweep    = smcEngine.detectLiquiditySweep(candles);
      const pdZone   = smcEngine.getPremiumDiscountZone(candles);

      let lastSweepAge = null;
      if (sweep.sweepCandleIndex !== null) {
        lastSweepAge = candles.length - 1 - sweep.sweepCandleIndex;
      }

      const pdLabel = pdZone.isPremium ? 'PREMIUM' : pdZone.isDiscount ? 'DISCOUNT' : 'NEUTRAL';

      activePairs[pair] = {
        activeOBCount:  obsBuy.length + obsSell.length,
        activeFVGCount: fvgsBuy.length + fvgsSell.length,
        lastSweepAge,
        premiumDiscount: pdLabel
      };
    }

    res.json({
      dxyBias:         currentDXYBias,
      dxyLastFetched:  dxyLastFetchedAt || (dxyCache.fetchedAt ? new Date(dxyCache.fetchedAt).toISOString() : null),
      killzoneActive:  killzone.active,
      killzoneName:    killzone.killzoneName,
      activePairs
    });
  } catch (err) {
    console.error('[SMC] /smc-status error:', err.message);
    res.status(500).json({ error: 'SMC status unavailable' });
  }
});

app.get('/rates', (req, res) => {
  cors(res);
  res.json(ratesCache.data);
});

app.get('/history', (req, res) => {
  cors(res);
  res.json(historyCache.data);
});

// /stored-history requires authentication
app.get('/stored-history', requireSecret, (req, res) => {
  cors(res);
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : null;
  if (!limit || isNaN(limit)) return res.json(candleStore);
  const limited = { pairs: {} };
  for (const pair of Object.keys(candleStore.pairs)) {
    limited.pairs[pair] = candleStore.pairs[pair].slice(-limit);
  }
  return res.json(limited);
});

app.get('/calendar', (req, res) => {
  cors(res);
  res.json({
    events:    calendarCache.data,
    fetchedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    count:     calendarCache.data.length
  });
});

app.get('/intelligence', (req, res) => {
  cors(res);
  if (!intelligenceProfile || intelligenceStatus === 'calculating' || intelligenceStatus === 'pending') {
    return res.json({
      status:      'calculating',
      pairsComplete: backfillPairsComplete,
      pairsTotal:    FOREX_PAIRS.length
    });
  }
  res.json({
    status:         'active',
    profile:        intelligenceProfile,
    fetchedAt:      intelligenceLastCalculated,
    profileVersion: 1
  });
});

app.get('/status', (req, res) => {
  cors(res);
  const storeCounts = {};
  for (const pair of FOREX_PAIRS) {
    storeCounts[pair] = (candleStore.pairs[pair] || []).length;
  }
  res.json({
    callsToday:               dailyCallCount,
    limit:                    800,
    resetAt:                  new Date(callCountResetAt).toUTCString(),
    candlePairsStored:        Object.keys(candleStore.pairs).length,
    candleCountPerPair:       storeCounts,
    calendarEvents:           calendarCache.data.length,
    calendarFetchedAt:        calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    backfillStatus,
    backfillPairsComplete,
    backfillPairsTotal:       FOREX_PAIRS.length,
    backfillTotalCandles,
    intelligenceStatus,
    intelligenceLastCalculated,
    lastCollectionError:      lastCollectionError || null
  });
});

app.get('/health', (req, res) => {
  cors(res);
  res.json({
    status:            'ok',
    uptime:            process.uptime(),
    ratesCachedAt:     ratesCache.fetchedAt   ? new Date(ratesCache.fetchedAt).toISOString()   : null,
    historyCachedAt:   historyCache.fetchedAt ? new Date(historyCache.fetchedAt).toISOString() : null,
    calendarCachedAt:  calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    intelligenceStatus
  });
});

// ---------------------------------------------------------------------------
// Start server — all background jobs launched AFTER server is listening
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] ForexMind proxy running on port ${PORT}`);

  // Twelve Data collection — start immediately, then every 15 minutes.
  // isCollectionRunning guards against concurrent cycles.
  const runCollectionWithSMC = async () => {
    if (isCollectionRunning) {
      console.warn('[collection] Cycle already running — skipping this tick.');
      return;
    }
    isCollectionRunning = true;
    try {
      await runCollection();
      await runSMCEvaluation().catch(err => console.error('[SMC] Post-collection evaluation error:', err.message));
    } finally {
      isCollectionRunning = false;
    }
  };
  runCollectionWithSMC();
  setInterval(runCollectionWithSMC, 15 * 60 * 1000);

  // DXY initial fetch (background, non-blocking)
  smcEngine.fetchDXYData()
    .then(data => {
      currentDXYBias   = smcEngine.calculateDXYTrend(data);
      dxyLastFetchedAt = new Date().toISOString();
      console.log(`[DXY] Initial trend: ${currentDXYBias}`);
    })
    .catch(err => console.warn('[DXY] Initial fetch failed:', err.message));

  // Economic calendar — start immediately, then every hour
  fetchCalendar();
  setInterval(fetchCalendar, CALENDAR_CACHE_TTL);

  // Dukascopy backfill — non-blocking background job
  setImmediate(() => runDukascopyBackfill());

  // Intelligence profile — first calculation after 2 minutes (allow backfill to run),
  // then recalculate every 6 hours (was 30 days — fresher volatility regime)
  setTimeout(() => calculateIntelligenceProfile(), 2 * 60 * 1000);
  setInterval(() => calculateIntelligenceProfile(), 6 * 60 * 60 * 1000);

  // Resolve any paper signals that passed their 24h window during the previous session
  setTimeout(() => resolveOldSignals(), 5 * 1000);

  // Ensemble scorer — attempt to initialise from canister resolved signals after 30s
  setTimeout(async () => {
    try {
      const CANISTER_HOST = process.env.CANISTER_HOST || null;
      const CANISTER_ID   = process.env.CANISTER_ID   || null;

      if (!CANISTER_HOST || !CANISTER_ID) {
        console.log('[Ensemble] CANISTER_HOST or CANISTER_ID not set. Skipping initial training fetch.');
        return;
      }

      // Use configurable base URL for canister queries
      const PROXY_BASE_URL = process.env.PROXY_BASE_URL || `http://localhost:${PORT}`;

      // Fetch resolved signals from canister via paginated getSignalPage
      const allSignals = [];
      const PAGE_SIZE  = 100;
      let   page       = 0;
      let   done       = false;

      while (!done) {
        try {
          const url = `${CANISTER_HOST}/api/v1/query`;
          const resp = await axios.post(url, {
            canisterId: CANISTER_ID,
            method:     'getSignalPage',
            args:       ['ALL', page, PAGE_SIZE]
          }, { timeout: 15000 });

          const signals = (resp.data && Array.isArray(resp.data.result)) ? resp.data.result : [];
          if (signals.length === 0) { done = true; break; }

          for (const s of signals) {
            if (s.outcome && s.outcome !== 'Pending') {
              const outcome  = s.outcome === 'Win';
              const features = ensembleScorer.extractFeatures(s, null);
              allSignals.push({ features, outcome });
            }
          }
          if (signals.length < PAGE_SIZE) { done = true; break; }
          page++;
        } catch (pageErr) {
          console.warn(`[Ensemble] Canister page fetch error (page ${page}):`, pageErr.message);
          done = true;
        }
      }

      console.log(`[Ensemble] Fetched ${allSignals.length} resolved signals from canister.`);
      ensembleScorer.initEnsembleScorer(allSignals);
    } catch (err) {
      console.warn('[Ensemble] Startup training fetch failed:', err.message);
    }
  }, 30 * 1000);
});
