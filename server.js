'use strict';

const express   = require('express');
const axios     = require('axios');
const fs        = require('fs');
const crypto    = require('crypto');
const { HttpAgent, Actor } = require('@dfinity/agent');
const { IDL }   = require('@dfinity/candid');
const rateLimit = require('express-rate-limit');

const app = express();
app.set('trust proxy', 1);
app.use(express.json()); // body parsing for future POST endpoints
const PORT = process.env.PORT || 3000;

// SMC Engine — Smart Money Concepts detection layer
const smcEngine      = require('./smcEngine');
const ensembleScorer = require('./ensembleScorer');

const API_KEY         = process.env.TWELVEDATA_API_KEY;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const PROXY_SECRET    = process.env.PROXY_SECRET || '';

// SEC-H3 — warn immediately if PROXY_SECRET is missing; block all authenticated calls
let _proxySecretMissing = false;
if (!process.env.PROXY_SECRET) {
  _proxySecretMissing = true;
  console.error('CRITICAL WARNING: PROXY_SECRET is not set — all authenticated endpoints will reject all requests until PROXY_SECRET is configured!');
}

// ---------------------------------------------------------------------------
// Single source-of-truth for all Forex pairs
// ---------------------------------------------------------------------------
const FOREX_PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,USD/CHF,AUD/USD,USD/CAD,NZD/USD')
  .split(',')
  .map(p => p.trim());

// ---------------------------------------------------------------------------
// File paths — ALL use /data (Render persistent volume)
// ---------------------------------------------------------------------------
const DATA_DIR                  = process.env.DATA_DIR || '/data';
const CANDLE_STORE_PATH         = `${DATA_DIR}/candle-store.json`;
const INTELLIGENCE_PROFILE_PATH = `${DATA_DIR}/intelligence-profile.json`;
const PAPER_SIGNALS_PATH        = `${DATA_DIR}/paper-signals.json`;
const RESOLVED_SIGNALS_PATH     = `${DATA_DIR}/resolved-signals.json`;

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------
let ratesCache      = { data: { pairs: {} }, fetchedAt: 0 };
let historyCache    = { data: { pairs: {} }, fetchedAt: 0 };
let candleStore     = { pairs: {} };
let calendarCache   = { data: [], fetchedAt: 0 };
let intelligenceProfile = null;

let dailyCallCount      = 0;
let callCountResetAt    = nextMidnightUTC();
let lastCollectionError = null;

let intelligenceStatus         = 'pending'; // pending | calculating | active | stale
let intelligenceLastCalculated = null;

// ---------------------------------------------------------------------------
// SMC / DXY in-memory state
// ---------------------------------------------------------------------------
let currentDXYBias   = 'NEUTRAL';
let dxyLastFetchedAt = null;

// Paper signals buffer (ring buffer, max 500 entries)
const PAPER_SIGNALS_MAX = 500;
let paperSignalsBuffer = [];

// Resolved signals buffer for ensemble retraining
// Each entry: { features: number[], outcome: 0|1 }
let resolvedSignalsBuffer  = [];
let newResolvedSinceRetrain = 0; // incremented on each new resolved outcome

// Track last Holy Trinity signal time per pair (for 24h fallback)
const lastHolyTrinityAt = {};

// Candle push batching — push to canister every N hours to minimise cycle usage
let lastCandlePushTime = 0;
const CANDLE_PUSH_INTERVAL_MS = parseInt(process.env.CANDLE_PUSH_INTERVAL_MS, 10) || (2 * 60 * 60 * 1000);

// Concurrent collection cycle guard
let isCollectionRunning = false;
let collectionStartTime = 0;

// ---------------------------------------------------------------------------
// @dfinity/agent canister actor setup
// ---------------------------------------------------------------------------
const backendIDL = ({ IDL }) => {
  const SignalDirection = IDL.Variant({ 'Buy': IDL.Null, 'Sell': IDL.Null });
  const PushSignalInput = IDL.Record({
    pair:                  IDL.Text,
    direction:             SignalDirection,
    confidence:            IDL.Nat,
    signalTypeKey:         IDL.Nat,
    entryPrice:            IDL.Float64,
    stopLoss:              IDL.Float64,
    takeProfit1:           IDL.Float64,
    takeProfit2:           IDL.Float64,
    timestamp:             IDL.Int,
    sessionAtGeneration:   IDL.Text,
    dxyBias:               IDL.Text,
    dxyStale:              IDL.Bool,
    fvgPresent:            IDL.Bool,
    fvgZoneHigh:           IDL.Float64,
    fvgZoneLow:            IDL.Float64,
    orderBlockPresent:     IDL.Bool,
    obZoneHigh:            IDL.Float64,
    obZoneLow:             IDL.Float64,
    liquiditySweepPresent: IDL.Bool,
    killzoneActive:        IDL.Bool,
    isPaper:               IDL.Bool,
    plainReason:           IDL.Text,
    modelVersion:          IDL.Text,
  });
  const CandleInput = IDL.Record({
    datetime: IDL.Text,
    open:     IDL.Float64,
    high:     IDL.Float64,
    low:      IDL.Float64,
    close:    IDL.Float64,
    volume:   IDL.Opt(IDL.Float64),
  });
  const Result = IDL.Variant({ 'ok': IDL.Nat, 'err': IDL.Text });
  return IDL.Service({
    pushSignalInput: IDL.Func([PushSignalInput], [Result], []),
    pushCandles:     IDL.Func([IDL.Text, IDL.Vec(CandleInput)], [IDL.Bool], []),
    getCandlePage:   IDL.Func([IDL.Text, IDL.Nat, IDL.Nat], [IDL.Vec(CandleInput)], ['query']),
  });
};

let canisterActor = null;

async function initCanisterActor() {
  try {
    const host = process.env.CANISTER_HOST || 'https://ic0.app';
    const { Ed25519KeyIdentity } = require('@dfinity/identity');
    const hexKey = process.env.PROXY_IDENTITY_KEY;
    let identity = null;
    if (hexKey) {
      try {
        const derBuffer = Buffer.from(hexKey, 'hex');
        const seed      = derBuffer.slice(16, 48);
        identity        = Ed25519KeyIdentity.fromSecretKey(seed);
        console.log('[canister] Proxy identity loaded. Principal:', identity.getPrincipal().toText());
      } catch (identityErr) {
        console.error('[canister] Failed to load proxy identity from PROXY_IDENTITY_KEY:', identityErr.message);
        identity = null;
      }
    } else {
      console.error('CRITICAL: PROXY_IDENTITY_KEY not set — canister calls will be anonymous and rejected');
    }
    const agent = new HttpAgent(identity ? { host, identity } : { host });
    canisterActor = Actor.createActor(backendIDL, {
      agent,
      // FIX: correct fallback canister ID
      canisterId: process.env.CANISTER_ID || 'n4eej-giaaa-aaaae-aanwa-cai',
    });
    console.log('[canister] Actor initialized for canister:', process.env.CANISTER_ID || 'n4eej-giaaa-aaaae-aanwa-cai');
  } catch (e) {
    console.error('[canister] Failed to initialize actor:', e.message);
  }
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function nextMidnightUTC() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);
}

function incrementCallCount() {
  if (Date.now() >= callCountResetAt) {
    dailyCallCount   = 0;
    callCountResetAt = nextMidnightUTC();
  }
  dailyCallCount++;
}

/**
 * Timing-safe secret comparison — SEC-H3.
 * Returns false immediately if PROXY_SECRET is not configured.
 */
function secretMatches(provided) {
  if (_proxySecretMissing) return false;
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
 * CORS — SEC-H2: uses ALLOWED_ORIGIN env var only.
 * If unset, no header is emitted and browsers block all cross-origin requests.
 */
if (!process.env.ALLOWED_ORIGIN) {
  console.warn('WARNING: ALLOWED_ORIGIN is not set — all browser cross-origin requests will be blocked by CORS.');
}
function cors(res) {
  const origin = process.env.ALLOWED_ORIGIN;
  if (origin) res.setHeader('Access-Control-Allow-Origin', origin);
}

// CORS preflight — must come before all routes
app.options('*', (req, res) => {
  const origin = process.env.ALLOWED_ORIGIN;
  if (origin) res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST');
  res.setHeader('Access-Control-Allow-Headers', 'X-ForexMind-Key,Content-Type');
  res.sendStatus(204);
});

// Persist paper signals to disk (atomic write)
function savePaperSignals() {
  try {
    const tmp = PAPER_SIGNALS_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(paperSignalsBuffer), 'utf8');
    fs.renameSync(tmp, PAPER_SIGNALS_PATH);
  } catch (e) { console.error('[persist] paper signals save failed:', e.message); }
}

// Persist resolved signals to disk (atomic write)
function saveResolvedSignals() {
  try {
    const tmp = RESOLVED_SIGNALS_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(resolvedSignalsBuffer), 'utf8');
    fs.renameSync(tmp, RESOLVED_SIGNALS_PATH);
  } catch (e) { console.error('[persist] resolved signals save failed:', e.message); }
}

/** Atomic JSON write: write to .tmp then rename to prevent corruption on crash. */
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
    candleStore = JSON.parse(fs.readFileSync(CANDLE_STORE_PATH, 'utf8'));
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
    intelligenceProfile        = JSON.parse(fs.readFileSync(INTELLIGENCE_PROFILE_PATH, 'utf8'));
    intelligenceStatus         = 'active';
    intelligenceLastCalculated = intelligenceProfile._calculatedAt ?? null;
    console.log('[startup] Intelligence profile loaded from disk.');
  }
} catch (err) {
  console.error('[startup] Failed to load intelligence profile from disk:', err.message);
  intelligenceProfile = null;
}

try {
  if (fs.existsSync(PAPER_SIGNALS_PATH)) {
    paperSignalsBuffer = JSON.parse(fs.readFileSync(PAPER_SIGNALS_PATH, 'utf8'));
    console.log(`[startup] Paper signals restored from disk (${paperSignalsBuffer.length} signals).`);
  }
} catch (err) {
  console.error('[startup] Failed to restore paper signals:', err.message);
  paperSignalsBuffer = [];
}

try {
  if (fs.existsSync(RESOLVED_SIGNALS_PATH)) {
    resolvedSignalsBuffer = JSON.parse(fs.readFileSync(RESOLVED_SIGNALS_PATH, 'utf8'));
    console.log(`[startup] Resolved signals restored from disk (${resolvedSignalsBuffer.length} entries).`);
  }
} catch (err) {
  console.error('[startup] Failed to restore resolved signals:', err.message);
  resolvedSignalsBuffer = [];
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
      sig.resolvedAt = new Date().toISOString();
      resolved++;
      const pairIntel = intelligenceProfile && intelligenceProfile[sig.pair];
      resolvedSignalsBuffer.push({
        features: ensembleScorer.extractFeatures(sig, pairIntel, undefined),
        outcome:  sig.outcome === 'WIN' ? 1 : 0
      });
      // FIX: increment counter so retraining logic can use it
      newResolvedSinceRetrain++;
    }
  }
  if (resolved > 0) {
    console.log(`[resolveOldSignals] Resolved ${resolved} stale paper signals.`);
    saveResolvedSignals();
    savePaperSignals();
  }
}

// ---------------------------------------------------------------------------
// Technical indicator helpers
// ---------------------------------------------------------------------------

function calculateATR(candles, period = 14) {
  if (candles.length < 2) return [];
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const curr = candles[i];
    const prev = candles[i - 1];
    trs.push(Math.max(
      curr.high - curr.low,
      Math.abs(curr.high - prev.close),
      Math.abs(curr.low  - prev.close)
    ));
  }
  const atrs = [];
  let sum = 0;
  for (let i = 0; i < trs.length; i++) {
    if (i < period) {
      sum += trs[i];
      atrs.push(i === period - 1 ? sum / period : NaN);
    } else {
      atrs.push((atrs[atrs.length - 1] * (period - 1) + trs[i]) / period);
    }
  }
  return atrs;
}

function calculateRSI(candles, period = 14) {
  if (candles.length < period + 1) return candles.map(() => NaN);
  const rsi = new Array(candles.length).fill(NaN);
  let avgGain = 0, avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    if (diff >= 0) avgGain += diff / period;
    else           avgLoss += Math.abs(diff) / period;
  }
  rsi[period] = 100 - 100 / (1 + (avgLoss === 0 ? Infinity : avgGain / avgLoss));
  for (let i = period + 1; i < candles.length; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (diff < 0 ? Math.abs(diff) : 0)) / period;
    rsi[i]  = 100 - 100 / (1 + (avgLoss === 0 ? Infinity : avgGain / avgLoss));
  }
  return rsi;
}

// ---------------------------------------------------------------------------
// Twelve Data collection (rates + history), staggered 8s per pair
// ---------------------------------------------------------------------------
async function runCollection() {
  console.log(`[${new Date().toISOString()}] Collection cycle starting...`);
  lastCollectionError = null;

  // Record oldest recent candle per pair to protect historical data
  const historicalCutoff = {};
  for (const pair of FOREX_PAIRS) {
    const existing = candleStore.pairs[pair];
    if (existing && existing.length >= 100) {
      historicalCutoff[pair] = existing[existing.length - 100].datetime;
    }
  }

  // Fetch rates — 8s stagger
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 8000));
    try {
      const response = await axios.get('https://api.twelvedata.com/exchange_rate', {
        params:  { symbol: pair, apikey: API_KEY },
        timeout: 10000
      });
      if (response.data && response.data.rate) {
        ratesCache.data.pairs[pair] = { price: String(response.data.rate), fetchedAt: new Date().toISOString() };
        ratesCache.fetchedAt = Date.now();
      }
      incrementCallCount();
    } catch (err) {
      lastCollectionError = `${pair} rates: ${err.message}`;
      console.error(`[collection] Rate fetch failed for ${pair}:`, err.message);
    }
  }

  await new Promise(r => setTimeout(r, 8000));

  // Fetch history — 8s stagger
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 8000));
    try {
      const response = await axios.get('https://api.twelvedata.com/time_series', {
        params:  { symbol: pair, interval: '1h', outputsize: 5000, apikey: API_KEY },
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
      for (const c of newCandles) {
        if (historicalCutoff[pair] && c.datetime < historicalCutoff[pair]) continue;
        map[c.datetime] = c;
      }
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

  try {
    atomicWriteJSON(CANDLE_STORE_PATH, candleStore);
    console.log(`[${new Date().toISOString()}] Collection complete. Daily calls: ${dailyCallCount}`);
  } catch (err) {
    lastCollectionError = `File write failed: ${err.message}`;
    console.error('[collection] Failed to write candle store to disk:', err.message);
  }

  // Candle push to canister — batched every N hours
  const now = Date.now();
  if (now - lastCandlePushTime >= CANDLE_PUSH_INTERVAL_MS) {
    lastCandlePushTime = now;
    console.log('[collection] Candle push window reached. Starting canister sync.');
    pushCandlesToCanister(JSON.parse(JSON.stringify(candleStore)))
      .catch(err => console.error('[canister] Candle push failed:', err.message));
  }
}

// ---------------------------------------------------------------------------
// Economic calendar — Finnhub primary, ForexFactory RSS fallback
// ---------------------------------------------------------------------------
const CALENDAR_CACHE_TTL  = 60 * 60 * 1000; // 1 hour
const CALENDAR_STALE_MS   = 12 * 60 * 60 * 1000; // 12 hours
const RELEVANT_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD'];

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
        params:  { from, to, token: FINNHUB_API_KEY },
        timeout: 10000
      });
      const events = (response.data.economicCalendar || [])
        .filter(e => {
          const impact   = (e.impact || '').toLowerCase();
          const currency = COUNTRY_TO_CURRENCY[(e.country || '').toUpperCase()] || (e.country || '').toUpperCase();
          return impact === 'high' && RELEVANT_CURRENCIES.includes(currency);
        })
        .map(e => {
          const currency = COUNTRY_TO_CURRENCY[(e.country || '').toUpperCase()] || (e.country || '').toUpperCase();
          return { time: e.time, currency, event: e.event, impact: e.impact, forecast: e.estimate || null, previous: e.prev || null };
        });
      calendarCache = { data: events, fetchedAt: Date.now() };
      console.log(`[calendar] Finnhub: ${events.length} high-impact events.`);
      return;
    } catch (err) {
      console.error('[calendar] Finnhub fetch failed:', err.message);
    }
  }

  // ForexFactory RSS fallback
  const cacheAge = calendarCache.fetchedAt ? Date.now() - calendarCache.fetchedAt : Infinity;
  try {
    const rssResponse = await axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.xml', { timeout: 10000 });
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
    if (cacheAge < CALENDAR_STALE_MS) {
      console.warn(`[calendar] Returning stale cache (${Math.round(cacheAge / 60000)} min old).`);
    } else {
      console.warn('[calendar] Cache is older than 12 hours — returning empty event list.');
      calendarCache = { data: [], fetchedAt: calendarCache.fetchedAt };
    }
  }
}

// ---------------------------------------------------------------------------
// Intelligence profile calculation
// ---------------------------------------------------------------------------

function yieldLoop() { return new Promise(r => setImmediate(r)); }

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  return sorted[Math.floor((p / 100) * (sorted.length - 1))];
}

function classifySession(datetime) {
  const hour       = parseInt(datetime.slice(11, 13), 10);
  const sydneyOpen = hour >= 21 || hour < 5;
  const tokyoOpen  = hour >= 0  && hour < 8;
  const londonOpen = hour >= 8  && hour < 12;
  const overlap    = hour >= 12 && hour < 17;
  const nyOpen     = hour >= 13 && hour < 21;
  if (overlap)    return 'londonNY';
  if (nyOpen)     return 'newYork';
  if (londonOpen) return 'london';
  if (tokyoOpen)  return 'tokyo';
  if (sydneyOpen) return 'sydney';
  return null;
}

async function calculateIntelligenceProfile() {
  intelligenceStatus = 'calculating';
  console.log('[intelligence] Starting profile calculation...');
  const profile  = { _calculatedAt: new Date().toISOString() };
  const nowYear  = new Date().getUTCFullYear();
  let calculated = 0;

  for (const pair of FOREX_PAIRS) {
    await yieldLoop();
    const candles = candleStore.pairs[pair];
    if (!candles || candles.length < 200) {
      console.warn(`[intelligence] ${pair}: insufficient candles (${(candles || []).length}), skipping.`);
      profile[pair] = { status: 'insufficient_data' };
      calculated++;
      continue;
    }

    // ATR percentiles
    await yieldLoop();
    const atrs    = calculateATR(candles, 14).filter(v => !isNaN(v));
    const atrSort = [...atrs].sort((a, b) => a - b);
    const atrPercentiles = {
      p25: percentile(atrSort, 25),
      p50: percentile(atrSort, 50),
      p75: percentile(atrSort, 75),
      p95: percentile(atrSort, 95)
    };

    // Volatility regime
    const recentATR    = calculateATR(candles.slice(-15), 14).filter(v => !isNaN(v));
    const currentATR   = recentATR.length ? recentATR[recentATR.length - 1] : atrPercentiles.p50;
    const volatilityRegime =
      currentATR < atrPercentiles.p25 ? 'low'     :
      currentATR < atrPercentiles.p75 ? 'normal'  :
      currentATR < atrPercentiles.p95 ? 'high'    : 'extreme';

    // Adaptive RSI thresholds (walk-forward, train 70% / test 30%)
    await yieldLoop();
    const splitIdx    = Math.floor(candles.length * 0.7);
    const testCandles = candles.slice(splitIdx);
    const rsiValues   = calculateRSI(testCandles, 14);
    let bestOversold = 30, bestOverbought = 70, bestScore = -Infinity;

    for (let os = 25; os <= 45; os += 2.5) {
      for (let ob = 55; ob <= 75; ob += 2.5) {
        let wins = 0, total = 0;
        for (let i = 1; i < testCandles.length - 4; i++) {
          const rsi = rsiValues[i];
          if (isNaN(rsi)) continue;
          const isBuy = rsi < os, isSell = rsi > ob;
          if (!isBuy && !isSell) continue;
          const atr1    = atrs.length ? atrs[Math.min(splitIdx + i, atrs.length - 1)] : 0.001;
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
        if (score > bestScore) { bestScore = score; bestOversold = os; bestOverbought = ob; }
      }
    }

    // Session win rate matrix
    await yieldLoop();
    const sessions      = ['london', 'newYork', 'londonNY', 'tokyo', 'sydney'];
    const sessionMatrix = {};
    for (const sess of sessions) sessionMatrix[sess] = { wins: 0, total: 0 };

    for (let i = 14; i < candles.length - 4; i++) {
      const c    = candles[i];
      const sess = classifySession(c.datetime);
      if (!sess) continue;
      const weight  = Math.pow(0.85, nowYear - parseInt(c.datetime.slice(0, 4), 10));
      const atrVal  = atrs[Math.max(0, i - 1)] || atrPercentiles.p50;
      const future4 = candles.slice(i + 1, i + 5);
      if (future4.length < 4) continue;
      const upMove   = Math.max(...future4.map(fc => fc.close)) - c.close;
      const downMove = c.close - Math.min(...future4.map(fc => fc.close));
      sessionMatrix[sess].total += weight;
      if (Math.max(upMove, downMove) >= atrVal) sessionMatrix[sess].wins += weight;
    }

    const finalSessionMatrix = {};
    for (const sess of sessions) {
      const { wins, total } = sessionMatrix[sess];
      const winRate    = total >= 5 ? wins / total : 0.55;
      const sampleSize = Math.round(total);
      finalSessionMatrix[sess] = {
        winRate:    Math.round(winRate * 1000) / 1000,
        sampleSize,
        confidence: sampleSize >= 500 ? 'high' : sampleSize >= 200 ? 'medium' : sampleSize >= 100 ? 'low' : 'insufficient'
      };
    }

    // Seasonal bias
    await yieldLoop();
    const monthBuckets = {};
    for (let m = 1; m <= 12; m++) monthBuckets[m] = { totalWeight: 0, moveWeight: 0 };

    for (let i = 14; i < candles.length - 4; i++) {
      const c      = candles[i];
      const month  = parseInt(c.datetime.slice(5, 7), 10);
      const weight = Math.pow(0.85, nowYear - parseInt(c.datetime.slice(0, 4), 10));
      const atrVal  = atrs[Math.max(0, i - 1)] || atrPercentiles.p50;
      const future4 = candles.slice(i + 1, i + 5);
      if (future4.length < 4) continue;
      const upMove   = Math.max(...future4.map(fc => fc.close)) - c.close;
      const downMove = c.close - Math.min(...future4.map(fc => fc.close));
      monthBuckets[month].totalWeight += weight;
      monthBuckets[month].moveWeight  += weight * (Math.max(upMove, downMove) / atrVal);
    }

    const avgMove = Object.values(monthBuckets)
      .filter(b => b.totalWeight > 0)
      .reduce((s, b) => s + b.moveWeight / b.totalWeight, 0) / 12 || 1;
    const seasonalBias = {};
    for (let m = 1; m <= 12; m++) {
      const b = monthBuckets[m];
      seasonalBias[String(m)] = b.totalWeight > 0
        ? Math.round((b.moveWeight / b.totalWeight / avgMove) * 100) / 100
        : 1.00;
    }

    profile[pair] = {
      adaptiveRSI:    { oversold: bestOversold, overbought: bestOverbought },
      atrPercentiles,
      sessionMatrix:  finalSessionMatrix,
      seasonalBias,
      volatilityRegime,
      profileVersion: 1,
      lastCalculated: new Date().toISOString(),
      status:         'active'
    };

    calculated++;
    console.log(`[intelligence] ${pair}: done (${calculated}/${FOREX_PAIRS.length}).`);
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
    const dxyData    = await smcEngine.fetchDXYData();
    currentDXYBias   = smcEngine.calculateDXYTrend(dxyData);
    dxyLastFetchedAt = new Date().toISOString();
  } catch (err) {
    const staleSecs = dxyLastFetchedAt
      ? Math.round((Date.now() - new Date(dxyLastFetchedAt).getTime()) / 1000)
      : null;
    console.warn('[SMC] DXY refresh failed:', err.message + '.' +
      (staleSecs !== null
        ? ` Current DXY bias (${currentDXYBias}) is ${staleSecs}s old.`
        : ' No DXY data available yet.'));
  }

  const killzone = smcEngine.isInsideKillzone();

  for (const pair of FOREX_PAIRS) {
    try {
      const candles = candleStore.pairs[pair];
      if (!candles || candles.length < 50) continue;

      const rsiVals = calculateRSI(candles, 14);
      const lastRSI = rsiVals[rsiVals.length - 1];
      if (isNaN(lastRSI)) continue;

      const pairProfile  = intelligenceProfile ? intelligenceProfile[pair] : null;
      const oversold     = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.oversold  : 35;
      const overbought   = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.overbought : 65;

      let candidateDirection = null;
      if (lastRSI < oversold)   candidateDirection = 'BUY';
      if (lastRSI > overbought) candidateDirection = 'SELL';
      if (!candidateDirection) continue;

      // FIX: indicatorPassed uses real RSI only — pairIntel never contains rsi/macd/adx fields
      const isBuyDir       = candidateDirection === 'BUY';
      const rsiOk          = isBuyDir ? lastRSI < 70 : lastRSI > 30;
      const indicatorPassed = rsiOk;

      // SMC detection
      const obList      = smcEngine.detectOrderBlocks(candles, candidateDirection);
      const fvgList     = smcEngine.detectFairValueGaps(candles, candidateDirection);
      const sweepResult = smcEngine.detectLiquiditySweep(candles);
      const pdZone      = smcEngine.getPremiumDiscountZone(candles);
      const trinity     = smcEngine.evaluateHolyTrinity(candles, candidateDirection, obList, fvgList, sweepResult, pdZone);

      // DXY penalty
      const dxyPenalty = smcEngine.getDXYPenalty(currentDXYBias, pair, candidateDirection);

      // Base confidence
      const rawConfidence = isBuyDir
        ? Math.min(100, Math.round(60 + (oversold  - lastRSI) * 2))
        : Math.min(100, Math.round(60 + (lastRSI - overbought) * 2));
      const adjustedConfidence = Math.max(0, rawConfidence - dxyPenalty);
      if (adjustedConfidence < 10) continue;

      const signalTypeKey = smcEngine.computeSignalTypeKey({
        orderBlockPresent:     trinity.orderBlockPresent,
        fvgPresent:            trinity.fvgPresent,
        liquiditySweepPresent: trinity.liquiditySweepPresent
      });

      const classification = smcEngine.classifySignal(trinity, indicatorPassed, killzone);
      const isPaperSignal  = (classification !== 'LIVE' && classification !== 'STANDARD');
      const now            = new Date();

      const signalBase = {
        pair,
        direction:             candidateDirection,
        confidence:            adjustedConfidence,
        orderBlockPresent:     trinity.orderBlockPresent,
        fvgPresent:            trinity.fvgPresent,
        liquiditySweepPresent: trinity.liquiditySweepPresent,
        killzoneActive:        killzone.active,
        killzoneName:          killzone.killzoneName || null,
        dxyBias:               currentDXYBias,
        obZone:                trinity.obZone,
        fvgZone:               trinity.fvgZone,
        ensembleScore:         null,
        signalTypeKey,
        generatedAt:           now.toISOString(),
        isPaper:               isPaperSignal
      };

      // Ensemble scoring
      let ensembleScore = null, highConviction = false;
      if (classification === 'LIVE' || classification === 'STANDARD') {
        try {
          const sweepAge = (sweepResult && sweepResult.sweepCandleIndex != null)
            ? candles.length - 1 - sweepResult.sweepCandleIndex
            : undefined;
          const score = await ensembleScorer.scoreSignal(ensembleScorer.extractFeatures(signalBase, null, sweepAge));
          if (score !== null) { ensembleScore = score; highConviction = score >= 75; }
        } catch (err) {
          console.warn(`[Ensemble] Scoring failed for ${pair}:`, err.message);
        }
      }

      const demoteToPaper = ensembleScore !== null && ensembleScore < 40;

      if (classification === 'LIVE') {
        if (demoteToPaper) {
          pushToPaperBuffer({ ...signalBase, ensembleScore, isPaper: true, paperReason: 'ENSEMBLE_LOW_SCORE' });
          console.log(`[SMC] LIVE demoted to PAPER (ensembleScore=${ensembleScore}): ${pair} ${candidateDirection}`);
        } else {
          lastHolyTrinityAt[pair] = now;
          console.log(`[SMC] LIVE signal: ${pair} ${candidateDirection} (confidence ${adjustedConfidence}, typeKey ${signalTypeKey}, ensemble ${ensembleScore ?? 'N/A'})`);
          pushSignalToCanister({ ...signalBase, ensembleScore, highConviction, isPaper: false });
        }

      } else if (classification === 'PAPER_OUTSIDE_KILLZONE' || classification === 'PAPER_INDICATOR_FAILED') {
        pushToPaperBuffer({ ...signalBase, ensembleScore: null, isPaper: true, paperReason: classification });
        console.log(`[SMC] PAPER signal: ${pair} ${candidateDirection} (${classification})`);

      } else if (classification === 'STANDARD') {
        if (demoteToPaper) {
          pushToPaperBuffer({ ...signalBase, confidence: Math.min(70, adjustedConfidence), ensembleScore, isPaper: true, paperReason: 'ENSEMBLE_LOW_SCORE' });
          console.log(`[SMC] STANDARD demoted to PAPER (ensembleScore=${ensembleScore}): ${pair} ${candidateDirection}`);
        } else {
          const conf = Math.min(70, adjustedConfidence);
          console.log(`[SMC] STANDARD signal: ${pair} ${candidateDirection} (confidence ${conf}, ensemble ${ensembleScore ?? 'N/A'})`);
          pushSignalToCanister({ ...signalBase, confidence: conf, ensembleScore, highConviction, isPaper: false });
        }
      }

      // 24h fallback: surface best indicator signal if no Holy Trinity in 24h
      const holyTrinityAge = lastHolyTrinityAt[pair]
        ? (now - lastHolyTrinityAt[pair]) / 3600000
        : Infinity;
      if (holyTrinityAge > 24 && classification !== 'LIVE' && adjustedConfidence >= 50) {
        let fbScore = null, fbHighConviction = false;
        try {
          const fbBase = { ...signalBase, confidence: Math.min(70, adjustedConfidence), isPaper: false,
            signalTypeKey: 0, orderBlockPresent: false, fvgPresent: false, liquiditySweepPresent: false,
            obZone: null, fvgZone: null, isFallback: true };
          const fbSweepAge = sweepResult && sweepResult.sweepCandleIndex != null
            ? candles.length - 1 - sweepResult.sweepCandleIndex : undefined;
          fbScore         = await ensembleScorer.scoreSignal(ensembleScorer.extractFeatures(fbBase, null, fbSweepAge));
          fbHighConviction = fbScore !== null && fbScore >= 75;
        } catch (_) {}
        console.log(`[SMC] FALLBACK signal: ${pair} ${candidateDirection} (24h without Holy Trinity, ensemble ${fbScore ?? 'N/A'})`);
        pushSignalToCanister({
          ...signalBase, confidence: Math.min(70, adjustedConfidence),
          ensembleScore: fbScore, highConviction: fbHighConviction, isPaper: false,
          signalTypeKey: 0, orderBlockPresent: false, fvgPresent: false, liquiditySweepPresent: false,
          obZone: null, fvgZone: null, isFallback: true
        });
      }

    } catch (err) {
      console.error(`[SMC] Evaluation error for ${pair}:`, err.message);
    }
  }
}

/** Push to paper buffer — ring buffer, drops oldest when full */
function pushToPaperBuffer(signal) {
  if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) {
    const dropped = paperSignalsBuffer.shift();
    console.log('[paper] Buffer full, dropping oldest signal:', dropped && dropped.id);
  }
  paperSignalsBuffer.push(signal);
  savePaperSignals();
}

// ---------------------------------------------------------------------------
// Canister push helpers
// ---------------------------------------------------------------------------

async function pushSignalToCanister(signal) {
  if (!canisterActor) { console.warn('[canister] Actor not initialized'); return; }
  if (signal.isPaper) return;
  try {
    const obZone  = signal.obZone  || {};
    const fvgZone = signal.fvgZone || {};
    const _candles = candleStore.pairs[signal.pair];
    if (!_candles || _candles.length < 15) {
      console.warn(`[canister] Skipping signal push for ${signal.pair}: insufficient candles for ATR`);
      return;
    }
    const _atrs = calculateATR(_candles, 14);
    const _atr  = _atrs.slice().reverse().find(v => v != null && !isNaN(v)) || 0;
    if (_atr === 0) {
      console.warn(`[canister] Skipping signal push for ${signal.pair}: ATR is zero`);
      return;
    }
    const _entry    = _candles[_candles.length - 1].close;
    const _decimals = (_entry.toString().split('.')[1] || '').length || 5;
    const _round    = v => parseFloat(v.toFixed(_decimals));
    const _sl  = signal.direction === 'BUY' ? _round(_entry - 1.5 * _atr) : _round(_entry + 1.5 * _atr);
    const _tp1 = signal.direction === 'BUY' ? _round(_entry + 2.0 * _atr) : _round(_entry - 2.0 * _atr);

    const input = {
      pair:                  signal.pair || '',
      direction:             signal.direction.toUpperCase() === 'SELL' ? { 'Sell': null } : { 'Buy': null },
      confidence:            Math.min(100, Math.max(0, Math.round(signal.confidence || 0))),
      signalTypeKey:         signal.signalTypeKey || 0,
      entryPrice:            _entry,
      stopLoss:              _sl,
      takeProfit1:           _tp1,
      takeProfit2:           _tp1,
      // FIX: use generatedAt (ISO string) instead of undefined signal.timestamp
      timestamp:             BigInt(Math.floor(new Date(signal.generatedAt).getTime() * 1_000_000)),
      sessionAtGeneration:   signal.killzoneName || signal.session || signal.sessionAtGeneration || '',
      dxyBias:               signal.dxyBias || 'UNKNOWN',
      dxyStale:              signal.dxyStale === true,
      fvgPresent:            signal.fvgPresent === true,
      fvgZoneHigh:           Number(fvgZone.high || 0),
      fvgZoneLow:            Number(fvgZone.low  || 0),
      orderBlockPresent:     signal.orderBlockPresent === true,
      obZoneHigh:            Number(obZone.high  || 0),
      obZoneLow:             Number(obZone.low   || 0),
      liquiditySweepPresent: signal.liquiditySweepPresent === true,
      killzoneActive:        signal.killzoneActive === true,
      isPaper:               false,
      plainReason:           signal.reason || signal.plainReason || '',
      modelVersion:          signal.modelVersion != null ? String(signal.modelVersion) : '',
    };
    const result = await canisterActor.pushSignalInput(input);
    if ('ok' in result) {
      console.log('[canister] Signal pushed:', signal.pair, signal.direction, '— total:', result.ok);
    } else {
      console.error('[canister] Signal push rejected:', result.err);
    }
  } catch (e) {
    console.error('[canister] Signal push failed:', e.message);
  }
}

async function pushCandlesToCanister(candleSnapshot) {
  if (!canisterActor) { console.warn('[canister] Actor not initialized'); return; }
  let totalPushed = 0;
  for (const pair of Object.keys(candleSnapshot.pairs || {})) {
    const candles = candleSnapshot.pairs[pair] || [];
    if (candles.length === 0) continue;
    try {
      // Send last 500 candles per pair
      const candleRecords = candles.slice(-500).map(c => ({
        datetime: c.datetime || new Date(c.timestamp || 0).toISOString(),
        open:     Number(c.open  || 0),
        high:     Number(c.high  || 0),
        low:      Number(c.low   || 0),
        close:    Number(c.close || 0),
        volume:   (c.volume != null && c.volume !== 0) ? [Number(c.volume)] : [],
      }));
      const ok = await canisterActor.pushCandles(pair, candleRecords);
      if (ok === true) {
        totalPushed += candleRecords.length;
      } else {
        console.error('[canister] Candles push rejected for pair:', pair);
      }
    } catch (e) {
      console.error('[canister] Candles push failed:', pair, e.message);
    }
  }
  if (totalPushed > 0) console.log('[canister] Candles pushed — total records:', totalPushed);
}

// ---------------------------------------------------------------------------
// Rate limiter & auth middleware
// ---------------------------------------------------------------------------
const publicLimiter = rateLimit({
  windowMs: 60 * 1000, max: 60,
  standardHeaders: true, legacyHeaders: false,
  message: { error: 'Too many requests, please try again later.' }
});

function requireSecret(req, res, next) {
  const provided = req.headers['x-forexmind-key'] || '';
  if (!secretMatches(provided)) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// FIX: /smc-status now authenticated
app.get('/smc-status', requireSecret, (req, res) => {
  cors(res);
  try {
    const killzone    = smcEngine.isInsideKillzone();
    const dxyCache    = smcEngine.getDXYCache();
    const activePairs = {};
    for (const pair of FOREX_PAIRS) {
      const candles = candleStore.pairs[pair];
      if (!candles || candles.length < 50) {
        activePairs[pair] = { activeOBCount: 0, activeFVGCount: 0, lastSweepAge: null, premiumDiscount: 'UNKNOWN' };
        continue;
      }
      const obsBuy   = smcEngine.detectOrderBlocks(candles, 'BUY');
      const obsSell  = smcEngine.detectOrderBlocks(candles, 'SELL');
      const fvgsBuy  = smcEngine.detectFairValueGaps(candles, 'BUY');
      const fvgsSell = smcEngine.detectFairValueGaps(candles, 'SELL');
      const sweep    = smcEngine.detectLiquiditySweep(candles);
      const pdZone   = smcEngine.getPremiumDiscountZone(candles);
      activePairs[pair] = {
        activeOBCount:   obsBuy.length + obsSell.length,
        activeFVGCount:  fvgsBuy.length + fvgsSell.length,
        lastSweepAge:    sweep.sweepCandleIndex !== null ? candles.length - 1 - sweep.sweepCandleIndex : null,
        premiumDiscount: pdZone.isPremium ? 'PREMIUM' : pdZone.isDiscount ? 'DISCOUNT' : 'NEUTRAL'
      };
    }
    res.json({
      dxyBias:        currentDXYBias,
      dxyLastFetched: dxyLastFetchedAt || (dxyCache.fetchedAt ? new Date(dxyCache.fetchedAt).toISOString() : null),
      killzoneActive: killzone.active,
      killzoneName:   killzone.killzoneName,
      activePairs
    });
  } catch (err) {
    console.error('[SMC] /smc-status error:', err.message);
    res.status(500).json({ error: 'SMC status unavailable' });
  }
});

app.get('/rates', publicLimiter, (req, res) => { cors(res); res.json(ratesCache.data); });

app.get('/history', publicLimiter, (req, res) => { cors(res); res.json(historyCache.data); });

app.get('/stored-history', requireSecret, publicLimiter, (req, res) => {
  cors(res);
  const rawLimit = parseInt(req.query.limit, 10);
  if (req.query.limit !== undefined && (isNaN(rawLimit) || rawLimit <= 0)) {
    return res.status(400).json({ error: 'Limit must be a positive integer' });
  }
  const limit   = rawLimit > 0 ? Math.min(rawLimit, 5000) : 5000;
  const limited = { pairs: {} };
  for (const pair of Object.keys(candleStore.pairs)) {
    limited.pairs[pair] = candleStore.pairs[pair].slice(-limit);
  }
  return res.json(limited);
});

app.get('/calendar', publicLimiter, (req, res) => {
  cors(res);
  res.json({ events: calendarCache.data, fetchedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null, count: calendarCache.data.length });
});

// FIX: /intelligence now authenticated
app.get('/intelligence', requireSecret, (req, res) => {
  cors(res);
  if (!intelligenceProfile || intelligenceStatus === 'calculating' || intelligenceStatus === 'pending') {
    return res.json({ status: 'calculating', pairsTotal: FOREX_PAIRS.length });
  }
  res.json({ status: 'active', profile: intelligenceProfile, fetchedAt: intelligenceLastCalculated, profileVersion: 1 });
});

app.get('/status', publicLimiter, (req, res) => {
  cors(res);
  const storeCounts = {};
  for (const pair of FOREX_PAIRS) storeCounts[pair] = (candleStore.pairs[pair] || []).length;
  res.json({
    callsToday:            dailyCallCount,
    limit:                 800,
    resetAt:               new Date(callCountResetAt).toUTCString(),
    candlePairsStored:     Object.keys(candleStore.pairs).length,
    candleCountPerPair:    storeCounts,
    calendarEvents:        calendarCache.data.length,
    calendarFetchedAt:     calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    intelligenceStatus,
    intelligenceLastCalculated,
    lastCollectionError:   lastCollectionError || null
  });
});

app.get('/health', publicLimiter, (req, res) => {
  cors(res);
  res.json({
    status:           'ok',
    uptime:           process.uptime(),
    ratesCachedAt:    ratesCache.fetchedAt    ? new Date(ratesCache.fetchedAt).toISOString()    : null,
    historyCachedAt:  historyCache.fetchedAt  ? new Date(historyCache.fetchedAt).toISOString()  : null,
    calendarCachedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    intelligenceStatus
  });
});

app.get('/ensemble-status', requireSecret, (req, res) => {
  cors(res);
  res.json(ensembleScorer.getEnsembleStatus());
});

app.get('/paper-signals', requireSecret, (req, res) => {
  cors(res);
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : paperSignalsBuffer.length;
  const safe  = isNaN(limit) ? paperSignalsBuffer.length : Math.min(limit, paperSignalsBuffer.length);
  res.json({ count: paperSignalsBuffer.length, signals: paperSignalsBuffer.slice(-safe) });
});

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] ForexMind proxy running on port ${PORT}`);

  const runCollectionWithSMC = async () => {
    if (isCollectionRunning) {
      const elapsed = collectionStartTime ? Date.now() - collectionStartTime : 0;
      if (elapsed >= 10 * 60 * 1000) {
        console.warn('[collection] Safety timeout: force-resetting after', Math.round(elapsed / 1000), 's');
        isCollectionRunning = false;
      } else {
        console.warn('[collection] Cycle already running — skipping this tick.');
        return;
      }
    }
    collectionStartTime = Date.now();
    isCollectionRunning = true;
    try {
      await runCollection();
      await runSMCEvaluation().catch(err => console.error('[SMC] Post-collection evaluation error:', err.message));
    } finally {
      isCollectionRunning = false;
      collectionStartTime = null;
    }
  };
  runCollectionWithSMC();
  setInterval(runCollectionWithSMC, 15 * 60 * 1000);

  smcEngine.fetchDXYData()
    .then(data => { currentDXYBias = smcEngine.calculateDXYTrend(data); dxyLastFetchedAt = new Date().toISOString(); console.log(`[DXY] Initial trend: ${currentDXYBias}`); })
    .catch(err  => console.warn('[DXY] Initial fetch failed:', err.message));

  fetchCalendar();
  setInterval(fetchCalendar, CALENDAR_CACHE_TTL);

  setTimeout(() => calculateIntelligenceProfile(), 2 * 60 * 1000);
  setInterval(() => calculateIntelligenceProfile(), 6 * 60 * 60 * 1000);

  setTimeout(() => resolveOldSignals(), 5 * 1000);

  initCanisterActor().then(() => {
    if (resolvedSignalsBuffer.length >= 100) {
      console.log(`[Ensemble] Seeding from ${resolvedSignalsBuffer.length} restored resolved signals...`);
      ensembleScorer.initEnsembleScorer(resolvedSignalsBuffer);
    } else {
      console.log(`[Ensemble] Insufficient resolved signals on startup (${resolvedSignalsBuffer.length}). Model will activate once 100+ outcomes are available.`);
    }
  }).catch(err => console.error('[canister] Actor init error:', err.message));

  setInterval(() => {
    try { ensembleScorer.retrainIfNeeded(resolvedSignalsBuffer); }
    catch (e) { console.error('[ensemble] Retrain interval error:', e.message); }
  }, 24 * 60 * 60 * 1000);
});
