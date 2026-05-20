'use strict';

const express = require('express');
const axios   = require('axios');
const fs      = require('fs');
const crypto  = require('crypto');
const { HttpAgent, Actor } = require('@dfinity/agent');
const { IDL } = require('@dfinity/candid');
const rateLimit = require('express-rate-limit'); // L8

const app  = express();
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;
// SMC Engine — Smart Money Concepts detection layer
const { getHistoricalRates } = require('dukascopy-node');
const smcEngine      = require('./smcEngine');
const ensembleScorer = require('./ensembleScorer');
const API_KEY        = process.env.TWELVEDATA_API_KEY;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const PROXY_SECRET   = process.env.PROXY_SECRET || '';

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
const DATA_DIR                  = process.env.DATA_DIR || '/data';
const CANDLE_STORE_PATH         = `${DATA_DIR}/candle-store.json`;
const BACKFILL_PROGRESS_PATH    = `${DATA_DIR}/backfill-progress.json`;
const INTELLIGENCE_PROFILE_PATH = `${DATA_DIR}/intelligence-profile.json`;
const PAPER_SIGNALS_PATH        = `${DATA_DIR}/paper-signals.json`;   // M1
const RESOLVED_SIGNALS_PATH     = `${DATA_DIR}/resolved-signals.json`; // M2

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
let dxyCache         = null;

// Paper signals buffer (up to 500 entries)
const PAPER_SIGNALS_MAX = 500;
let paperSignalsBuffer = [];
let paperSignalsPushCounter = 0;

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
let collectionStartTime = 0; // M5: safety timeout tracking

// ---------------------------------------------------------------------------
// @dfinity/agent canister actor setup (C1)
// ---------------------------------------------------------------------------
const backendIDL = ({ IDL }) => {
  const SignalDirection = IDL.Variant({ 'Buy': IDL.Null, 'Sell': IDL.Null });
  // Lean input type — matches SignalRecordTypes.SignalInput on the backend
  const PushSignalInput = IDL.Record({
    pair:                   IDL.Text,
    direction:              SignalDirection,
    confidence:             IDL.Nat,
    signalTypeKey:          IDL.Nat,
    entryPrice:             IDL.Float64,
    stopLoss:               IDL.Float64,
    takeProfit1:            IDL.Float64,
    takeProfit2:            IDL.Float64,
    timestamp:              IDL.Int,
    sessionAtGeneration:    IDL.Text,
    dxyBias:                IDL.Text,
    dxyStale:               IDL.Bool,
    fvgPresent:             IDL.Bool,
    fvgZoneHigh:            IDL.Float64,
    fvgZoneLow:             IDL.Float64,
    orderBlockPresent:      IDL.Bool,
    obZoneHigh:             IDL.Float64,
    obZoneLow:              IDL.Float64,
    liquiditySweepPresent:  IDL.Bool,
    killzoneActive:         IDL.Bool,
    isPaper:                IDL.Bool,
    plainReason:            IDL.Text,
    modelVersion:           IDL.Text,
  });
  // CandleInput matches CandlePushTypes.CandleInput on the backend
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
    // pushSignalInput: lean input, returns Result<Nat, Text>
    pushSignalInput: IDL.Func([PushSignalInput], [Result], []),
    // pushCandles: positional (pair, candles), returns Bool
    pushCandles:     IDL.Func([IDL.Text, IDL.Vec(CandleInput)], [IDL.Bool], []),
    // getCandlePage: paginated candle read for proxy restore-from-canister
    getCandlePage:   IDL.Func([IDL.Text, IDL.Nat, IDL.Nat], [IDL.Vec(CandleInput)], ['query']),
    pushResolvedOutcome: IDL.Func(
      [IDL.Record({
        pair: IDL.Text,
        direction: IDL.Text,
        entryPrice: IDL.Float64,
        stopLoss: IDL.Float64,
        takeProfit: IDL.Float64,
        result: IDL.Text,
        confidence: IDL.Float64,
        timestamp: IDL.Int,
      })],
      [IDL.Variant({ ok: IDL.Null, err: IDL.Text })],
      []
    ),
    getResolvedOutcomes: IDL.Func(
      [],
      [IDL.Variant({ ok: IDL.Vec(IDL.Record({
        pair: IDL.Text,
        direction: IDL.Text,
        entryPrice: IDL.Float64,
        stopLoss: IDL.Float64,
        takeProfit: IDL.Float64,
        result: IDL.Text,
        confidence: IDL.Float64,
        timestamp: IDL.Int,
      })), err: IDL.Text })],
      []
    ),
    pushModelState: IDL.Func([IDL.Record({
      weights: IDL.Vec(IDL.Float64),
      bias: IDL.Float64,
      means: IDL.Vec(IDL.Float64),
      stds: IDL.Vec(IDL.Float64),
      oosAccuracy: IDL.Float64,
      trainedAt: IDL.Int
    })], [IDL.Variant({ ok: IDL.Null, err: IDL.Text })], []),
    getModelState: IDL.Func([], [IDL.Variant({
      ok: IDL.Opt(IDL.Record({
        weights: IDL.Vec(IDL.Float64),
        bias: IDL.Float64,
        means: IDL.Vec(IDL.Float64),
        stds: IDL.Vec(IDL.Float64),
        oosAccuracy: IDL.Float64,
        trainedAt: IDL.Int
      })),
      err: IDL.Text
    })], []),
    pushIntelligenceProfiles: IDL.Func([IDL.Vec(IDL.Record({
      pair: IDL.Text,
      volatilityRegime: IDL.Text,
      adxClass: IDL.Text,
      sessionQuality: IDL.Float64,
      updatedAt: IDL.Int
    }))], [IDL.Variant({ ok: IDL.Null, err: IDL.Text })], []),
    getIntelligenceProfiles: IDL.Func([], [IDL.Variant({
      ok: IDL.Vec(IDL.Record({
        pair: IDL.Text,
        volatilityRegime: IDL.Text,
        adxClass: IDL.Text,
        sessionQuality: IDL.Float64,
        updatedAt: IDL.Int
      })),
      err: IDL.Text
    })], []),
    pushDxyState: IDL.Func([IDL.Record({
      trend: IDL.Text,
      dataPoints: IDL.Vec(IDL.Float64),
      fetchedAt: IDL.Int
    })], [IDL.Variant({ ok: IDL.Null, err: IDL.Text })], []),
    getDxyState: IDL.Func([], [IDL.Variant({
      ok: IDL.Opt(IDL.Record({
        trend: IDL.Text,
        dataPoints: IDL.Vec(IDL.Float64),
        fetchedAt: IDL.Int
      })),
      err: IDL.Text
    })], []),
    pushPaperSignals: IDL.Func([IDL.Vec(IDL.Record({
      pair: IDL.Text,
      direction: IDL.Text,
      entryPrice: IDL.Float64,
      sl: IDL.Float64,
      tp: IDL.Float64,
      confidence: IDL.Float64,
      timestamp: IDL.Int,
      smcPatterns: IDL.Vec(IDL.Text)
    }))], [IDL.Variant({ ok: IDL.Null, err: IDL.Text })], []),
    getPaperSignals: IDL.Func([], [IDL.Variant({
      ok: IDL.Vec(IDL.Record({
        pair: IDL.Text,
        direction: IDL.Text,
        entryPrice: IDL.Float64,
        sl: IDL.Float64,
        tp: IDL.Float64,
        confidence: IDL.Float64,
        timestamp: IDL.Int,
        smcPatterns: IDL.Vec(IDL.Text)
      })),
      err: IDL.Text
    })], []),
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
        const seed = derBuffer.slice(16, 48);
        identity = Ed25519KeyIdentity.fromSecretKey(seed);
        const principal = identity.getPrincipal().toText();
        console.log('[canister] Proxy identity loaded. Principal:', principal);
      } catch (identityErr) {
        console.error('[canister] Failed to load proxy identity from PROXY_IDENTITY_KEY:', identityErr.message);
        identity = null;
      }
    } else {
      console.error('CRITICAL: PROXY_IDENTITY_KEY not set — canister calls will be anonymous and rejected');
    }
    const agentOpts = identity ? { host, identity } : { host };
    const agent = new HttpAgent(agentOpts);
    canisterActor = Actor.createActor(backendIDL, {
      agent,
      canisterId: process.env.CANISTER_ID || 'i5pap-wiaaa-aaaad-agq6q-cai',
    });
    console.log('[canister] Actor initialized for canister:', process.env.CANISTER_ID);
  } catch (e) {
    console.error('[canister] Failed to initialize actor:', e.message);
  }
}

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
 * SEC-H3: returns false immediately if PROXY_SECRET is not configured.
 */
function secretMatches(provided) {
  if (_proxySecretMissing) return false; // SEC-H3: no secret configured — block all
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
 * CORS — uses ALLOWED_ORIGIN env var only.
 * SEC-H2: if ALLOWED_ORIGIN is not set, no Access-Control-Allow-Origin header is set,
 * which means browsers will block all cross-origin requests (same as blocking all origins).
 * A startup warning is logged so the operator is aware.
 */
if (!process.env.ALLOWED_ORIGIN) {
  console.warn('WARNING: ALLOWED_ORIGIN is not set — all browser cross-origin requests will be blocked by CORS.');
}
function cors(res) {
  const origin = process.env.ALLOWED_ORIGIN;
  if (origin) res.setHeader('Access-Control-Allow-Origin', origin);
  // If no origin is set, no CORS header is emitted — browsers block cross-origin calls (SEC-H2)
}
// C4 — CORS OPTIONS preflight handler (must come before all routes)
app.options('*', (req, res) => {
  const origin = process.env.ALLOWED_ORIGIN;
  if (origin) res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST');
  res.setHeader('Access-Control-Allow-Headers', 'X-ForexMind-Key,Content-Type');
  res.sendStatus(204);
});

// M1 — Persist paper signals to /data
function savePaperSignals() {
  try {
    const tmp = PAPER_SIGNALS_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(paperSignalsBuffer), 'utf8');
    fs.renameSync(tmp, PAPER_SIGNALS_PATH);
  } catch (e) { console.error('[persist] paper signals save failed:', e.message); }
}

// M2 — Persist resolved signals to /data
function saveResolvedSignals() {
  try {
    const tmp = RESOLVED_SIGNALS_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(resolvedSignalsBuffer), 'utf8');
    fs.renameSync(tmp, RESOLVED_SIGNALS_PATH);
  } catch (e) { console.error('[persist] resolved signals save failed:', e.message); }
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
    // NEW-M2: _calculatedAt may be missing in older profile versions — guard with optional chaining
    intelligenceLastCalculated = intelligenceProfile._calculatedAt ?? null;
    console.log('[startup] Intelligence profile loaded from disk.');
  }
} catch (err) {
  console.error('[startup] Failed to load intelligence profile from disk:', err.message);
  intelligenceProfile = null;
}

// M1 — Restore paper signals buffer from disk
try {
  if (fs.existsSync(PAPER_SIGNALS_PATH)) {
    paperSignalsBuffer = JSON.parse(fs.readFileSync(PAPER_SIGNALS_PATH, 'utf8'));
    console.log(`[startup] Paper signals restored from disk (${paperSignalsBuffer.length} signals).`);
  }
} catch (err) {
  console.error('[startup] Failed to restore paper signals:', err.message);
  paperSignalsBuffer = [];
}

// M2 — Restore resolved signals buffer from disk
try {
  if (fs.existsSync(RESOLVED_SIGNALS_PATH)) {
    resolvedSignalsBuffer = JSON.parse(fs.readFileSync(RESOLVED_SIGNALS_PATH, 'utf8'));
    console.log(`[startup] Resolved signals restored from disk (${resolvedSignalsBuffer.length} entries).`);
  }
} catch (err) {
  console.error('[startup] Failed to restore resolved signals:', err.message);
  resolvedSignalsBuffer = [];
}

// Warn clearly if MASSIVE_API_KEY is absent
if (!process.env.MASSIVE_API_KEY) {
  console.warn('[startup] WARNING: MASSIVE_API_KEY not set — backfill will fail with 403');
}

// Dukascopy is primary candle source (no key required).
// Twelve Data is the fallback — warn if key is absent so operator knows fallback is unavailable.
if (!process.env.TWELVEDATA_API_KEY) {
  console.warn('[config] TWELVEDATA_API_KEY not set — live rates and candle fallback will be unavailable');
}

// ---------------------------------------------------------------------------
// Dukascopy normalizer — converts dukascopy-node candle to internal format
// dukascopy-node returns: { timestamp: <Unix ms>, open, high, low, close, volume }
// Target format:          { datetime: "YYYY-MM-DD HH:MM:SS", open, high, low, close }
// ---------------------------------------------------------------------------
function normalizeDukascopyCandle(entry) {
  const d = new Date(entry.timestamp);
  const pad = n => String(n).padStart(2, '0');
  const datetime = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  return {
    datetime,
    open:   parseFloat(entry.open),
    high:   parseFloat(entry.high),
    low:    parseFloat(entry.low),
    close:  parseFloat(entry.close)
  };
}

/**
 * fetchCandlesDukascopy(pair, fromDate, toDate)
 * Fetches 15-minute candles from dukascopy-node for a given pair and date range.
 * Returns normalized candle array (oldest-first) or throws on failure.
 */
async function fetchCandlesDukascopy(pair, fromDate, toDate) {
  const instrument = DUKASCOPY_INSTRUMENTS[pair];
  if (!instrument) throw new Error(`No Dukascopy instrument mapping for pair: ${pair}`);
  const raw = await getHistoricalRates({
    instrument,
    dates:     { from: fromDate, to: toDate },
    timeframe: 'm15',
    format:    'json'
  });
  if (!raw || !Array.isArray(raw) || raw.length === 0) {
    throw new Error(`dukascopy-node returned empty data for ${pair}`);
  }
  return raw.map(normalizeDukascopyCandle);
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
      // H2: feed resolved paper signal outcome into resolvedSignalsBuffer for model training
      const pairIntel = intelligenceProfile && intelligenceProfile[sig.pair];
      resolvedSignalsBuffer.push({
        features: ensembleScorer.extractFeatures(sig, pairIntel, undefined),
        outcome:  sig.outcome === 'WIN' ? 1 : 0
      });
      pushResolvedOutcomeToCanister({
        pair: sig.pair,
        direction: sig.direction,
        entryPrice: sig.entryPrice,
        stopLoss: sig.stopLoss,
        takeProfit: sig.takeProfit,
        result: sig.outcome === 'WIN' ? 'TP_HIT' : sig.outcome === 'LOSS' ? 'SL_HIT' : 'EXPIRED',
        confidence: sig.confidence || 0,
        timestamp: sig.timestamp ? Math.round(sig.timestamp * 1_000_000) : Date.now() * 1_000_000,
      }).catch(() => {});
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

  // M3: Record the oldest recent candle timestamp per pair to protect historical data
  if (!candleStore.pairs) candleStore.pairs = {};
  const historicalCutoff = {};
  for (const pair of FOREX_PAIRS) {
    const existing = candleStore.pairs[pair];
    if (existing && existing.length >= 100) {
      historicalCutoff[pair] = existing[existing.length - 100].datetime;
    }
  }

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

  // Fetch candles — Dukascopy primary (free, no limits), Twelve Data fallback
  // 8s stagger between pairs to be respectful of upstream servers
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 8000));
    try {
      // --- Dukascopy primary ---
      let newCandles = null;
      try {
        const toDate   = new Date();
        const fromDate = new Date(toDate.getTime() - 7 * 24 * 60 * 60 * 1000); // last 7 days
        newCandles = await fetchCandlesDukascopy(pair, fromDate, toDate);
        console.log(`[dukascopy] Fetched ${newCandles.length} candles for ${pair}`);
      } catch (dukErr) {
        console.warn(`[dukascopy] Failed for ${pair}, falling back to Twelve Data:`, dukErr.message);
        // --- Twelve Data fallback ---
        if (process.env.TWELVEDATA_API_KEY) {
          try {
            const tdResp = await axios.get('https://api.twelvedata.com/time_series', {
              params: { symbol: pair, interval: '15min', outputsize: 500, apikey: API_KEY },
              timeout: 30000
            });
            if (tdResp.data && Array.isArray(tdResp.data.values)) {
              // Twelve Data returns newest-first — reverse to oldest-first
              newCandles = tdResp.data.values
                .map(c => ({
                  datetime: c.datetime,
                  open:  parseFloat(c.open),
                  high:  parseFloat(c.high),
                  low:   parseFloat(c.low),
                  close: parseFloat(c.close)
                }))
                .filter(c => !isNaN(c.open) && !isNaN(c.close))
                .reverse();
              console.log(`[twelve-data-fallback] Fetched ${newCandles.length} candles for ${pair}`);
              incrementCallCount();
            } else {
              console.warn(`[twelve-data-fallback] Bad response for ${pair}:`, JSON.stringify(tdResp.data).slice(0, 200));
            }
          } catch (tdErr) {
            console.error(`[twelve-data-fallback] Also failed for ${pair}:`, tdErr.message);
          }
        } else {
          console.warn(`[twelve-data-fallback] TWELVEDATA_API_KEY not set — candle fallback unavailable for ${pair}`);
        }
      }

      if (!newCandles || newCandles.length === 0) {
        console.warn(`[candles] No candles available for ${pair} this cycle, keeping existing data`);
        continue;
      }

      if (!candleStore.pairs) candleStore.pairs = {};
      if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
      const map = {};
      for (const c of candleStore.pairs[pair]) map[c.datetime] = c;
      for (const c of newCandles) {
        // M3: skip candles that would overwrite historical data older than the cutoff
        if (historicalCutoff[pair] && c.datetime < historicalCutoff[pair]) continue;
        map[c.datetime] = c;
      }
      const merged = Object.values(map).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
      candleStore.pairs[pair]       = merged;
      historyCache.data.pairs[pair] = merged.slice(-5000);
      historyCache.fetchedAt        = Date.now();
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
    console.log('[collection] Candle push window reached. Starting canister sync.');
    // C3: pass a deep copy snapshot to prevent race condition on mutable candleStore
    pushCandlesToCanister(JSON.parse(JSON.stringify(candleStore))).catch(err => console.error('[canister] Candle push failed:', err.message));
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
  // NEW-M6: check cache age before returning stale data
  const CALENDAR_STALE_MS = 12 * 60 * 60 * 1000; // 12 hours
  const cacheAge = calendarCache.fetchedAt ? Date.now() - calendarCache.fetchedAt : Infinity;
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
    // NEW-M6: return stale cache only if it is < 12 hours old
    if (cacheAge < CALENDAR_STALE_MS) {
      console.warn(`[calendar] Returning stale cache (${Math.round(cacheAge / 60000)} min old).`);
    } else {
      console.warn('[calendar] Cache is older than 12 hours — returning empty event list to avoid stale news filter.');
      calendarCache = { data: [], fetchedAt: calendarCache.fetchedAt };
    }
  }
}

// ---------------------------------------------------------------------------
// Dukascopy 10-year historical backfill
// ---------------------------------------------------------------------------
async function runDukascopyBackfill() {
  backfillStatus = 'running';
  console.log('[backfill] Starting Dukascopy 10-year backfill...');

  // getHistoricalRates imported at module level (top of file)

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

    if (!candleStore.pairs) candleStore.pairs = {};
    const existingCount = (candleStore.pairs[pair] || []).length;
    if (existingCount > 10000) {
      console.log('[backfill]', pair, 'already has', existingCount, 'candles — skipping');
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

      let yearAttempts = 0;
      let yearSuccess = false;
      let result = null;
      while (yearAttempts < 3 && !yearSuccess) {
        yearAttempts++;
        try {
          console.log(`[backfill] ${pair} / ${year}: fetching... (attempt ${yearAttempts})`);
          result = await getHistoricalRates({
            instrument: instrument,
            dates:      { from: new Date(yearStart), to: new Date(yearEnd) },
            timeframe:  'h1',
            format:     'object',
            flushDownloadProgress: false
          });
          yearSuccess = true;
        } catch (fetchErr) {
          console.error(`[backfill] ${pair} / ${year}: attempt ${yearAttempts} failed: ${fetchErr.message}`);
          if (yearAttempts < 3) {
            console.log(`[backfill] ${pair} / ${year}: retrying in 12s...`);
            await new Promise(r => setTimeout(r, 12000));
          }
        }
      }

      if (!yearSuccess) {
        yearsFailed++;
        console.error(`[backfill] ${pair} / ${year}: all ${yearAttempts} attempts failed.`);
        if (yearsFailed >= 3) {
          console.error(`[backfill] ${pair}: too many year failures, skipping pair.`);
          break;
        }
        continue;
      }

      try {
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
        console.error(`[backfill] ${pair} / ${year}: processing error: ${err.message}`);
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
    if (!candleStore.pairs) candleStore.pairs = {};
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

/**
 * Session for a given UTC datetime string (YYYY-MM-DD HH:MM:SS)
 * NEW-C2: aligned with backend signal_engine.mo session logic:
 *   Tokyo:    0–8 UTC
 *   London:   8–12 UTC
 *   Overlap:  12–17 UTC (London+NY)
 *   New York: 13–21 UTC
 *   Sydney:   21–23 UTC and 0–5 UTC
 */
function classifySession(datetime) {
  const hour = parseInt(datetime.slice(11, 13), 10);
  // Sydney spans midnight: hours 21-23 and 0-4
  const sydneyOpen = hour >= 21 || hour < 5;
  const tokyoOpen  = hour >= 0  && hour < 8;
  const londonOpen = hour >= 8  && hour < 12;
  const overlap    = hour >= 12 && hour < 17;
  const nyOpen     = hour >= 13 && hour < 21;

  // Priority order: overlap > NY > london > tokyo > sydney
  if (overlap)    return 'londonNY';
  if (nyOpen)     return 'newYork';
  if (londonOpen) return 'london';
  if (tokyoOpen)  return 'tokyo';
  if (sydneyOpen) return 'sydney';
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

  pushIntelligenceProfilesToCanister(intelligenceProfile).catch(() => {});
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
    dxyCache = {
      trend: currentDXYBias,
      dataPoints: dxyData && Array.isArray(dxyData) ? dxyData.map(d => d.close || d.value || 0).slice(-50) : [],
      fetchedAt: Date.now()
    };
    pushDxyStateToCanister(dxyCache).catch(() => {});
  } catch (err) {
    // BUG-M13: log DXY staleness when fetch fails
    const staleSecs = dxyLastFetchedAt ? Math.round((Date.now() - new Date(dxyLastFetchedAt).getTime()) / 1000) : null;
    const staleMsg  = staleSecs !== null ? ` Current DXY bias (${currentDXYBias}) is ${staleSecs}s old.` : ' No DXY data available yet.';
    console.warn('[SMC] DXY refresh failed:', err.message + '.' + staleMsg);
  }

  const killzone = smcEngine.isInsideKillzone();

  if (!candleStore.pairs) candleStore.pairs = {};
  for (const pair of FOREX_PAIRS) {
    try {
      const candles = candleStore.pairs[pair];
      if (!candles || candles.length < 50) continue;

      // ---- Derive a candidate direction from simple RSI bias ----
      const rsiVals   = calculateRSI(candles, 14);
      const lastRSI   = rsiVals[rsiVals.length - 1];
      if (isNaN(lastRSI)) continue;

      // BUG-M12: null-check intelligenceProfile before ANY use (was checked too late)
      const pairProfile  = intelligenceProfile ? intelligenceProfile[pair] : null;
      const oversold     = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.oversold  : 35;
      const overbought   = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.overbought : 65;

      let candidateDirection = null;
      if (lastRSI < oversold)   candidateDirection = 'BUY';
      if (lastRSI > overbought) candidateDirection = 'SELL';
      if (!candidateDirection) continue;

      // H3 — real indicator check: RSI + MACD + ADX from intelligence profile
      const pairIntel = pairProfile || {};
      const rsiForCheck = pairIntel.rsi || lastRSI || 50;
      const macdHist = pairIntel.macdHistogram || 0;
      const adx = pairIntel.adx || 0;
      const isBuyDir = candidateDirection === 'BUY';
      const rsiOk   = isBuyDir ? rsiForCheck < 70 : rsiForCheck > 30;
      const macdOk  = isBuyDir ? macdHist > 0 : macdHist < 0;
      const adxOk   = adx > 20;
      const indicatorPassed = rsiOk && macdOk && adxOk;

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

      // NEW-M5: suppress signals with confidence below minimum threshold after DXY penalty
      const MIN_CONFIDENCE_THRESHOLD = 10;
      if (adjustedConfidence < MIN_CONFIDENCE_THRESHOLD) continue;

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
          const sweepAge = (sweepResult && sweepResult.sweepCandleIndex != null)
            ? candles.length - 1 - sweepResult.sweepCandleIndex
            : undefined;
          const features = ensembleScorer.extractFeatures(signalBase, null, sweepAge);
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
          if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) {
            const dropped = paperSignalsBuffer.shift();
            console.log('[paper] Buffer full (500), dropping oldest signal:', dropped && dropped.id);
          }
          paperSignalsBuffer.push(paperSignal);
          paperSignalsPushCounter++;
          if (paperSignalsPushCounter % 10 === 0) {
            pushPaperSignalsToCanister(paperSignalsBuffer).catch(() => {});
          }
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
          // NOTE: Only LIVE and STANDARD signals go to canister. Paper signals NEVER go to canister.
          pushSignalToCanister(liveSignal);
        }

      } else if (classification === 'PAPER_OUTSIDE_KILLZONE' || classification === 'PAPER_INDICATOR_FAILED') {
        const paperSignal = { ...signalBase, ensembleScore: null, isPaper: true, paperReason: classification };
        // Buffer paper signals (ring buffer, drop oldest)
        if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) {
          const dropped = paperSignalsBuffer.shift();
          console.log('[paper] Buffer full (500), dropping oldest signal:', dropped && dropped.id);
        }
        paperSignalsBuffer.push(paperSignal);
        paperSignalsPushCounter++;
        if (paperSignalsPushCounter % 10 === 0) {
          pushPaperSignalsToCanister(paperSignalsBuffer).catch(() => {});
        }
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
          if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) {
            const dropped = paperSignalsBuffer.shift();
            console.log('[paper] Buffer full (500), dropping oldest signal:', dropped && dropped.id);
          }
          paperSignalsBuffer.push(paperSignal);
          paperSignalsPushCounter++;
          if (paperSignalsPushCounter % 10 === 0) {
            pushPaperSignalsToCanister(paperSignalsBuffer).catch(() => {});
          }
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
          pushSignalToCanister(standardSignal);
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
          const fbSweepAge = (sweepResult && sweepResult.sweepCandleIndex != null)
              ? candles.length - 1 - sweepResult.sweepCandleIndex
              : undefined;
            const fbFeatures = ensembleScorer.extractFeatures(fallbackBase, null, fbSweepAge);
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
        pushSignalToCanister(fallbackSignal);
      }

    } catch (err) {
      console.error(`[SMC] Evaluation error for ${pair}:`, err.message);
    }
  }
}

// ---------------------------------------------------------------------------
// Canister push helpers — signal and candle push to IC backend
// ---------------------------------------------------------------------------
const CANISTER_HOST = process.env.CANISTER_HOST || null;
const CANISTER_ID   = process.env.CANISTER_ID   || null;

/**
 * Push a single live signal to the canister via IC HTTP gateway.
 * Only called for non-paper signals (isPaper === false).
 * Silently skips if CANISTER_HOST / CANISTER_ID are not configured.
 */
async function pushSignalToCanister(signal) {
  if (!canisterActor) { console.warn('[canister] Actor not initialized'); return; }
  if (signal.isPaper) return; // safety guard — paper signals never go on-chain
  try {
    const direction = (signal.direction || '').toUpperCase() === 'SELL'
      ? { 'Sell': null } : { 'Buy': null };
    const smcZones = signal.smcZones || {};
    const obZone  = signal.obZone  || smcZones.orderBlock  || {};
    const fvgZone = signal.fvgZone || smcZones.fvg || {};
    if (!candleStore.pairs) candleStore.pairs = {};
    const _candles = candleStore.pairs[signal.pair];
    if (!_candles || _candles.length < 15) {
      console.warn(`[canister] Skipping signal push for ${signal.pair}: insufficient candles for ATR`);
      return;
    }
    const _atrs = calculateATR(_candles, 14);
    const _atr = _atrs.slice().reverse().find(v => v != null && !isNaN(v)) || 0;
    if (_atr === 0) {
      console.warn(`[canister] Skipping signal push for ${signal.pair}: ATR is zero`);
      return;
    }
    const _entry = _candles[_candles.length - 1].close;
    const _decimals = (_entry.toString().split('.')[1] || '').length || 5;
    const _round = (v) => parseFloat(v.toFixed(_decimals));
    const _slDist = 1.5 * _atr;
    const _tp1Dist = 2.0 * _atr;
    const _sl = signal.direction === 'BUY' ? _round(_entry - _slDist) : _round(_entry + _slDist);
    const _tp1 = signal.direction === 'BUY' ? _round(_entry + _tp1Dist) : _round(_entry - _tp1Dist);

    const input = {
      pair:                   signal.pair         || '',
      direction,
      confidence:             Math.min(100, Math.max(0, Math.round(signal.confidence || 0))),
      signalTypeKey:          signal.signalTypeKey || 0,
      entryPrice:             _entry,
      stopLoss:               _sl,
      takeProfit1:            _tp1,
      takeProfit2:            _tp1,
      timestamp:              BigInt(Math.floor((signal.timestamp || Date.now()) * 1_000_000)),
      sessionAtGeneration:    signal.killzoneName || signal.session || signal.sessionAtGeneration || '',
      dxyBias:                signal.dxyBias     || 'UNKNOWN',
      dxyStale:               signal.dxyStale    === true,
      fvgPresent:             signal.fvgPresent   === true,
      fvgZoneHigh:            Number(fvgZone.high || 0),
      fvgZoneLow:             Number(fvgZone.low  || 0),
      orderBlockPresent:      signal.orderBlockPresent === true,
      obZoneHigh:             Number(obZone.high  || 0),
      obZoneLow:              Number(obZone.low   || 0),
      liquiditySweepPresent:  signal.liquiditySweepPresent === true,
      killzoneActive:         signal.killzoneActive === true,
      isPaper:                false,
      plainReason:            signal.reason || signal.plainReason || '',
      modelVersion:           signal.modelVersion != null ? String(signal.modelVersion) : '',
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

/**
 * Push candle data to the canister, batched per pair (max 500 candles/pair).
 * Silently skips if CANISTER_HOST / CANISTER_ID are not configured.
 */
async function pushCandlesToCanister(candleSnapshot) {
  if (!canisterActor) { console.warn('[canister] Actor not initialized'); return; }
  let totalPushed = 0;
  for (const pair of Object.keys(candleSnapshot.pairs || {})) {
    const candles = candleSnapshot.pairs[pair] || [];
    if (candles.length === 0) continue;
    try {
      // FIX 2: CandleInput uses datetime (ISO text), no pair field, volume as opt ([v] or [])
      const candleRecords = candles.slice(-200).map(c => {
        const dt = c.datetime
          ? c.datetime
          : new Date(c.timestamp || 0).toISOString();
        const vol = (c.volume != null && c.volume !== 0)
          ? [Number(c.volume)]
          : [];
        return {
          datetime: dt,
          open:     Number(c.open  || 0),
          high:     Number(c.high  || 0),
          low:      Number(c.low   || 0),
          close:    Number(c.close || 0),
          volume:   vol,
        };
      });
      // FIX 1: positional args (pair, candles), not a record object
      // FIX 4: returns Bool, not Result variant
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
  if (totalPushed > 0) {
    console.log('[canister] Candles pushed — total records:', totalPushed);
  }
}

async function pushResolvedOutcomeToCanister(outcome) {
  if (!canisterActor) return;
  try {
    const result = await canisterActor.pushResolvedOutcome({
      pair: outcome.pair || '',
      direction: outcome.direction || '',
      entryPrice: outcome.entryPrice || 0,
      stopLoss: outcome.stopLoss || 0,
      takeProfit: outcome.takeProfit || 0,
      result: outcome.result || 'EXPIRED',
      confidence: outcome.confidence || 0,
      timestamp: typeof outcome.timestamp === 'bigint' ? outcome.timestamp : BigInt(Math.round(outcome.timestamp || Date.now() * 1_000_000)),
    });
    if (result && result.err) {
      console.warn('[canister] pushResolvedOutcome rejected:', result.err);
    } else {
      console.log('[canister] Pushed resolved outcome:', outcome.pair, outcome.result);
    }
  } catch (err) {
    console.warn('[canister] pushResolvedOutcome failed (non-blocking):', err.message || err);
  }
}

async function restoreResolvedOutcomesFromCanister() {
  if (!canisterActor) return;
  if (resolvedSignalsBuffer.length >= 100) return;
  try {
    const result = await canisterActor.getResolvedOutcomes();
    if (!result || result.err) {
      console.warn('[canister] getResolvedOutcomes returned error:', result?.err);
      return;
    }
    const canisterOutcomes = Array.isArray(result.ok) ? result.ok : [];
    let merged = 0;
    for (const co of canisterOutcomes) {
      const key = `${co.pair}:${String(co.timestamp)}`;
      const alreadyHave = resolvedSignalsBuffer.some(
        r => r._canisterKey === key
      );
      if (!alreadyHave) {
        resolvedSignalsBuffer.push({
          features: [0],
          outcome: co.result === 'TP_HIT',
          _canisterKey: key,
        });
        merged++;
      }
    }
    if (merged > 0) {
      console.log(`[canister] Restored ${merged} resolved outcomes from canister`);
      saveResolvedSignals();
    }
  } catch (err) {
    console.warn('[canister] restoreResolvedOutcomesFromCanister failed (non-blocking):', err.message || err);
  }
}

async function pushModelStateToCanister(modelState) {
  if (!canisterActor) return;
  try {
    const payload = {
      weights: modelState.weights || [],
      bias: modelState.bias || 0,
      means: modelState.means || [],
      stds: modelState.stds || [],
      oosAccuracy: modelState.oosAccuracy || 0,
      trainedAt: BigInt(modelState.trainedAt || Date.now())
    };
    const result = await canisterActor.pushModelState(payload);
    if ('ok' in result) {
      console.log('[canister] Model state pushed');
    } else {
      console.warn('[canister] pushModelState err:', result.err);
    }
  } catch (e) {
    console.warn('[canister] pushModelState failed:', e.message);
  }
}

async function restoreModelStateFromCanister() {
  if (!canisterActor) return null;
  try {
    const result = await canisterActor.getModelState();
    if ('ok' in result && result.ok.length > 0) {
      const state = result.ok[0];
      console.log('[canister] Model state restored from canister');
      return {
        weights: Array.from(state.weights),
        bias: state.bias,
        means: Array.from(state.means),
        stds: Array.from(state.stds),
        oosAccuracy: state.oosAccuracy,
        trainedAt: Number(state.trainedAt)
      };
    }
  } catch (e) {
    console.warn('[canister] restoreModelState failed:', e.message);
  }
  return null;
}

async function pushIntelligenceProfilesToCanister(profiles) {
  if (!canisterActor || !profiles || !Object.keys(profiles).length) return;
  try {
    const payload = Object.entries(profiles).map(([pair, p]) => ({
      pair,
      volatilityRegime: p.volatilityRegime || 'normal',
      adxClass: p.adxClass || 'ranging',
      sessionQuality: p.sessionQuality || 0.5,
      updatedAt: BigInt(p.updatedAt || Date.now())
    }));
    const result = await canisterActor.pushIntelligenceProfiles(payload);
    if ('ok' in result) {
      console.log('[canister] Intelligence profiles pushed:', payload.length, 'pairs');
    } else {
      console.warn('[canister] pushIntelligenceProfiles err:', result.err);
    }
  } catch (e) {
    console.warn('[canister] pushIntelligenceProfiles failed:', e.message);
  }
}

async function restoreIntelligenceProfilesFromCanister() {
  if (!canisterActor) return null;
  try {
    const result = await canisterActor.getIntelligenceProfiles();
    if ('ok' in result && result.ok.length > 0) {
      const profiles = {};
      for (const p of result.ok) {
        profiles[p.pair] = {
          volatilityRegime: p.volatilityRegime,
          adxClass: p.adxClass,
          sessionQuality: p.sessionQuality,
          updatedAt: Number(p.updatedAt)
        };
      }
      console.log('[canister] Intelligence profiles restored:', Object.keys(profiles).length, 'pairs');
      return profiles;
    }
  } catch (e) {
    console.warn('[canister] restoreIntelligenceProfiles failed:', e.message);
  }
  return null;
}

async function pushDxyStateToCanister(dxyState) {
  if (!canisterActor || !dxyState) return;
  try {
    const payload = {
      trend: dxyState.trend || 'neutral',
      dataPoints: (dxyState.dataPoints || []).map(Number),
      fetchedAt: BigInt(dxyState.fetchedAt || Date.now())
    };
    const result = await canisterActor.pushDxyState(payload);
    if ('ok' in result) {
      console.log('[canister] DXY state pushed, trend:', dxyState.trend);
    } else {
      console.warn('[canister] pushDxyState err:', result.err);
    }
  } catch (e) {
    console.warn('[canister] pushDxyState failed:', e.message);
  }
}

async function restoreDxyStateFromCanister() {
  if (!canisterActor) return null;
  try {
    const result = await canisterActor.getDxyState();
    if ('ok' in result && result.ok.length > 0) {
      const state = result.ok[0];
      console.log('[canister] DXY state restored from canister, trend:', state.trend);
      return {
        trend: state.trend,
        dataPoints: Array.from(state.dataPoints).map(Number),
        fetchedAt: Number(state.fetchedAt)
      };
    }
  } catch (e) {
    console.warn('[canister] restoreDxyState failed:', e.message);
  }
  return null;
}

async function pushPaperSignalsToCanister(paperSignals) {
  if (!canisterActor || !paperSignals || !paperSignals.length) return;
  try {
    const payload = paperSignals.slice(-200).map(s => ({
      pair: s.pair || '',
      direction: s.direction || 'Buy',
      entryPrice: s.entryPrice || 0,
      sl: s.sl || 0,
      tp: s.tp || 0,
      confidence: s.confidence || 0,
      timestamp: BigInt(s.timestamp || Date.now()),
      smcPatterns: s.smcPatterns || []
    }));
    const result = await canisterActor.pushPaperSignals(payload);
    if ('ok' in result) {
      console.log('[canister] Paper signals pushed:', payload.length);
    } else {
      console.warn('[canister] pushPaperSignals err:', result.err);
    }
  } catch (e) {
    console.warn('[canister] pushPaperSignals failed:', e.message);
  }
}

async function restorePaperSignalsFromCanister() {
  if (!canisterActor) return null;
  try {
    const result = await canisterActor.getPaperSignals();
    if ('ok' in result && result.ok.length > 0) {
      const signals = result.ok.map(s => ({
        pair: s.pair,
        direction: s.direction,
        entryPrice: s.entryPrice,
        sl: s.sl,
        tp: s.tp,
        confidence: s.confidence,
        timestamp: Number(s.timestamp),
        smcPatterns: Array.from(s.smcPatterns)
      }));
      console.log('[canister] Paper signals restored from canister:', signals.length);
      return signals;
    }
  } catch (e) {
    console.warn('[canister] restorePaperSignals failed:', e.message);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Rate limiters — L8
// ---------------------------------------------------------------------------
const publicLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later.' }
});

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
// H5 — secured with requireSecret
app.get('/ensemble-status', requireSecret, (req, res) => {
  cors(res);
  res.json(ensembleScorer.getEnsembleStatus());
});

// ---------------------------------------------------------------------------
// Paper signals endpoint
// ---------------------------------------------------------------------------
// H4 — secured with requireSecret
app.get('/paper-signals', requireSecret, (req, res) => {
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

    if (!candleStore.pairs) candleStore.pairs = {};
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

app.get('/rates', publicLimiter, (req, res) => {
  cors(res);
  res.json(ratesCache.data);
});

app.get('/history', publicLimiter, (req, res) => {
  cors(res);
  res.json(historyCache.data);
});

// /stored-history — authenticated + M4 hard limit
app.get('/stored-history', requireSecret, publicLimiter, (req, res) => {
  cors(res);
  const rawLimit = parseInt(req.query.limit, 10);
  if (req.query.limit !== undefined && (isNaN(rawLimit) || rawLimit <= 0)) {
    return res.status(400).json({ error: 'Limit must be a positive integer' });
  }
  const limit = (rawLimit > 0) ? Math.min(rawLimit, 5000) : 5000;
  if (!candleStore.pairs) candleStore.pairs = {};
  const limited = { pairs: {} };
  for (const pair of Object.keys(candleStore.pairs)) {
    limited.pairs[pair] = candleStore.pairs[pair].slice(-limit);
  }
  return res.json(limited);
});

app.get('/calendar', publicLimiter, (req, res) => {
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

app.get('/status', publicLimiter, (req, res) => {
  cors(res);
  if (!candleStore.pairs) candleStore.pairs = {};
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

app.get('/health', publicLimiter, (req, res) => {
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
      // M5: safety timeout — force reset if stuck >= 10 minutes (was >, off-by-one fixed)
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
      collectionStartTime = null; // BUG-H7/NEW-M4: always reset so elapsed never grows monotonically
    }
  };
  runCollectionWithSMC();
  setInterval(runCollectionWithSMC, 15 * 60 * 1000);

  // DXY initial fetch (background, non-blocking)
  smcEngine.fetchDXYData()
    .then(data => {
      currentDXYBias   = smcEngine.calculateDXYTrend(data);
      dxyLastFetchedAt = new Date().toISOString();
      dxyCache = {
        trend: currentDXYBias,
        dataPoints: data && Array.isArray(data) ? data.map(d => d.close || d.value || 0).slice(-50) : [],
        fetchedAt: Date.now()
      };
      pushDxyStateToCanister(dxyCache).catch(() => {});
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

  // C1 — initialise canister actor on startup, then seed ensemble scorer from canister
  initCanisterActor().then(async () => {
    await restoreResolvedOutcomesFromCanister();
    restoreModelStateFromCanister().then(state => { if (state && ensembleScorer) ensembleScorer.loadRemoteState(state); });
    restoreIntelligenceProfilesFromCanister().then(profiles => { if (profiles) { if (!intelligenceProfile) intelligenceProfile = {}; Object.assign(intelligenceProfile, profiles); } });
    restoreDxyStateFromCanister().then(state => { if (state) { dxyCache = state; } });
    restorePaperSignalsFromCanister().then(signals => { if (signals && signals.length) { paperSignalsBuffer.push(...signals.filter(s => !paperSignalsBuffer.find(e => e.timestamp === s.timestamp))); } });
    // NEW-H6: seed ensemble scorer from resolved signals already in the buffer
    // If resolved signals were restored from disk, init immediately — no cold start
    if (resolvedSignalsBuffer.length >= 100) {
      console.log(`[Ensemble] Seeding from ${resolvedSignalsBuffer.length} restored resolved signals...`);
      ensembleScorer.initEnsembleScorer(resolvedSignalsBuffer);
      const modelState = ensembleScorer.getModelState ? ensembleScorer.getModelState() : { weights: [], bias: 0, means: [], stds: [], oosAccuracy: 0, trainedAt: Date.now() };
      pushModelStateToCanister(modelState).catch(() => {});
    } else {
      console.log('[Ensemble] Insufficient resolved signals on startup (' + resolvedSignalsBuffer.length + '). Model will activate once 100+ outcomes are available.');
    }
  }).catch(err => console.error('[canister] Actor init error:', err.message));

  // H1 — retrain ensemble model daily
  setInterval(() => {
    try {
      ensembleScorer.retrainIfNeeded(resolvedSignalsBuffer);
      const modelState = ensembleScorer.getModelState ? ensembleScorer.getModelState() : { weights: [], bias: 0, means: [], stds: [], oosAccuracy: 0, trainedAt: Date.now() };
      pushModelStateToCanister(modelState).catch(() => {});
    }
    catch (e) { console.error('[ensemble] Retrain interval error:', e.message); }
  }, 24 * 60 * 60 * 1000);
});
