'use strict';

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
const smcEngine = require('./smcEngine');
const ensembleScorer = require('./ensembleScorer');

const API_KEY = process.env.TWELVEDATA_API_KEY;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const MASSIVE_API_KEY = process.env.MASSIVE_API_KEY || '';
const PROXY_SECRET = process.env.PROXY_SECRET || '';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const PROXY_IDENTITY_KEY = process.env.PROXY_IDENTITY_KEY || null;
const CANISTER_HOST = process.env.CANISTER_HOST || null;
const CANISTER_ID = process.env.CANISTER_ID || null;

const FOREX_PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,USD/CHF,AUD/USD,USD/CAD,NZD/USD')
  .split(',').map(p => p.trim());

const MASSIVE_TICKER_MAP = {
  'EUR/USD': 'C:EURUSD', 'GBP/USD': 'C:GBPUSD', 'USD/JPY': 'C:USDJPY',
  'USD/CHF': 'C:USDCHF', 'AUD/USD': 'C:AUDUSD', 'USD/CAD': 'C:USDCAD', 'NZD/USD': 'C:NZDUSD'
};

const COUNTRY_TO_CURRENCY = {
  'US': 'USD', 'EU': 'EUR', 'DE': 'EUR', 'FR': 'EUR', 'IT': 'EUR', 'ES': 'EUR',
  'GB': 'GBP', 'JP': 'JPY', 'CH': 'CHF', 'AU': 'AUD', 'CA': 'CAD', 'NZ': 'NZD'
};

const CANDLE_STORE_PATH = '/data/candle-store.json';
const BACKFILL_PROGRESS_PATH = '/data/backfill-progress.json';
const INTELLIGENCE_PROFILE_PATH = '/data/intelligence-profile.json';
const RESOLVED_SIGNALS_PATH = '/data/resolved-signals.json';

let ratesCache = { data: { pairs: {} }, fetchedAt: 0 };
let historyCache = { data: { pairs: {} }, fetchedAt: 0 };
let candleStore = { pairs: {} };
let calendarCache = { data: [], fetchedAt: 0 };
let intelligenceProfile = null;
let dailyCallCount = 0;
let callCountResetAt = nextMidnightUTC();
let lastCollectionError = null;
let lastHistoryFetchAt = 0;
let backfillStatus = 'pending';
let backfillPairsComplete = 0;
let backfillTotalCandles = 0;
let intelligenceStatus = 'pending';
let intelligenceLastCalculated = null;
let currentDXYBias = 'NEUTRAL';
let dxyLastFetchedAt = null;
const PAPER_SIGNALS_MAX = 500;
let paperSignalsBuffer = [];
let resolvedSignalsBuffer = [];
let newResolvedSinceRetrain = 0;
const lastHolyTrinityAt = {};

function nextMidnightUTC() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);
}

function incrementCallCount() {
  if (Date.now() >= callCountResetAt) { dailyCallCount = 0; callCountResetAt = nextMidnightUTC(); }
  dailyCallCount++;
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Vary', 'Origin');
}

function requireAuth(req, res) {
  if (!PROXY_SECRET) return true;
  const key = req.headers['x-forexmind-key'];
  if (key !== PROXY_SECRET) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`TIMEOUT after ${ms}ms: ${label}`)), ms);
    promise.then(val => { clearTimeout(timer); resolve(val); })
           .catch(err => { clearTimeout(timer); reject(err); });
  });
}

// --- Canister identity signing ---
function signRequest(body) {
  if (!PROXY_IDENTITY_KEY) return null;
  try {
    const privDer = Buffer.from(PROXY_IDENTITY_KEY, 'hex');
    const privateKey = crypto.createPrivateKey({ key: privDer, format: 'der', type: 'pkcs8' });
    const payload = JSON.stringify(body);
    const signature = crypto.sign(null, Buffer.from(payload), privateKey);
    return signature.toString('hex');
  } catch (err) {
    console.warn('[canister] Failed to sign request:', err.message);
    return null;
  }
}

// --- Push signal to canister ---
async function pushSignalToCanister(signal) {
  if (!CANISTER_HOST || !CANISTER_ID || !PROXY_IDENTITY_KEY) return;
  try {
    const body = {
      canisterId: CANISTER_ID,
      method: 'pushPaperSignal',
      args: [{
        pair: signal.pair,
        direction: signal.direction,
        confidence: signal.confidence,
        entry: signal.entry || 0,
        stopLoss: signal.stopLoss || 0,
        takeProfit: signal.takeProfit || 0,
        atr: signal.atr || 0,
        orderBlockPresent: signal.orderBlockPresent,
        fvgPresent: signal.fvgPresent,
        liquiditySweepPresent: signal.liquiditySweepPresent,
        killzoneActive: signal.killzoneActive,
        killzoneName: signal.killzoneName || '',
        dxyBias: signal.dxyBias || 'NEUTRAL',
        ensembleScore: signal.ensembleScore || 0,
        signalTypeKey: signal.signalTypeKey || 0,
        generatedAt: signal.generatedAt,
        isPaper: signal.isPaper || false,
        paperReason: signal.paperReason || ''
      }]
    };
    const signature = signRequest(body);
    await withTimeout(
      axios.post(`${CANISTER_HOST}/api/v1/update`, body, {
        headers: signature ? { 'X-Proxy-Signature': signature } : {},
        timeout: 15000
      }),
      20000, `pushSignalToCanister ${signal.pair}`
    );
    console.log(`[canister] Signal pushed: ${signal.pair} ${signal.direction}`);
  } catch (err) {
    console.warn(`[canister] pushSignalToCanister failed for ${signal.pair}:`, err.message);
  }
}

// --- Push candles to canister ---
async function pushCandlesToCanister(pair, candles) {
  if (!CANISTER_HOST || !CANISTER_ID || !PROXY_IDENTITY_KEY) return;
  if (!candles || candles.length === 0) return;
  try {
    const BATCH_SIZE = 500;
    for (let i = 0; i < candles.length; i += BATCH_SIZE) {
      const batch = candles.slice(i, i + BATCH_SIZE);
      const body = {
        canisterId: CANISTER_ID,
        method: 'pushCandles',
        args: [pair, batch.map(c => ({
          datetime: c.datetime,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close
        }))]
      };
      const signature = signRequest(body);
      await withTimeout(
        axios.post(`${CANISTER_HOST}/api/v1/update`, body, {
          headers: signature ? { 'X-Proxy-Signature': signature } : {},
          timeout: 20000
        }),
        25000, `pushCandlesToCanister ${pair} batch ${i}`
      );
      if (i + BATCH_SIZE < candles.length) await new Promise(r => setTimeout(r, 500));
    }
    console.log(`[canister] Candles pushed for ${pair}: ${candles.length} candles`);
  } catch (err) {
    console.warn(`[canister] pushCandlesToCanister failed for ${pair}:`, err.message);
  }
}

// --- Push resolved signal outcome to canister ---
async function pushOutcomeToCanister(signalId, isWin) {
  if (!CANISTER_HOST || !CANISTER_ID || !PROXY_IDENTITY_KEY) return;
  try {
    const body = {
      canisterId: CANISTER_ID,
      method: 'recordPaperSignalOutcome',
      args: [signalId, isWin]
    };
    const signature = signRequest(body);
    await withTimeout(
      axios.post(`${CANISTER_HOST}/api/v1/update`, body, {
        headers: signature ? { 'X-Proxy-Signature': signature } : {},
        timeout: 15000
      }),
      20000, `pushOutcomeToCanister ${signalId}`
    );
  } catch (err) {
    console.warn(`[canister] pushOutcomeToCanister failed for ${signalId}:`, err.message);
  }
}

// --- Load candle store from disk on startup ---
try {
  if (fs.existsSync(CANDLE_STORE_PATH)) {
    const raw = fs.readFileSync(CANDLE_STORE_PATH, 'utf8');
    candleStore = JSON.parse(raw);
    for (const pair of Object.keys(candleStore.pairs)) {
      historyCache.data.pairs[pair] = candleStore.pairs[pair].slice(-5000);
    }
    console.log(`[startup] Candle store loaded (${Object.keys(candleStore.pairs).length} pairs).`);
  }
} catch (err) { console.error('[startup] Failed to load candle store:', err.message); candleStore = { pairs: {} }; }

// --- Load intelligence profile from disk on startup ---
try {
  if (fs.existsSync(INTELLIGENCE_PROFILE_PATH)) {
    const raw = fs.readFileSync(INTELLIGENCE_PROFILE_PATH, 'utf8');
    intelligenceProfile = JSON.parse(raw);
    intelligenceStatus = 'active';
    intelligenceLastCalculated = intelligenceProfile._calculatedAt || null;
    console.log('[startup] Intelligence profile loaded.');
  }
} catch (err) { console.error('[startup] Failed to load intelligence profile:', err.message); }

// --- Load resolved signals from disk on startup ---
try {
  if (fs.existsSync(RESOLVED_SIGNALS_PATH)) {
    const raw = fs.readFileSync(RESOLVED_SIGNALS_PATH, 'utf8');
    resolvedSignalsBuffer = JSON.parse(raw);
    console.log(`[startup] Loaded ${resolvedSignalsBuffer.length} resolved signals.`);
  }
} catch (err) { console.error('[startup] Failed to load resolved signals:', err.message); }

// --- Restore candle store from canister on startup ---
async function restoreFromCanister() {
  if (!CANISTER_HOST || !CANISTER_ID) {
    console.log('[restore] CANISTER_HOST or CANISTER_ID not set. Skipping.');
    return;
  }
  console.log('[restore] Restoring from canister...');
  for (const pair of FOREX_PAIRS) {
    try {
      const resp = await withTimeout(
        axios.post(`${CANISTER_HOST}/api/v1/query`, {
          canisterId: CANISTER_ID, method: 'getStoredCandles', args: [pair]
        }, { timeout: 15000 }),
        20000, `restoreFromCanister ${pair}`
      );
      const candles = (resp.data && Array.isArray(resp.data.result)) ? resp.data.result : [];
      if (candles.length > 0) {
        if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
        const map = {};
        for (const c of candleStore.pairs[pair]) map[c.datetime] = c;
        for (const c of candles) map[c.datetime] = c;
        candleStore.pairs[pair] = Object.values(map).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
        historyCache.data.pairs[pair] = candleStore.pairs[pair].slice(-5000);
      }
    } catch (err) { console.warn(`[restore] ${pair}: failed:`, err.message); }
  }
  try { fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8'); } catch (_) {}
  console.log('[restore] Complete.');
}

// --- ATR ---
function calculateATR(candles, period = 14) {
  if (candles.length < 2) return [];
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const curr = candles[i], prev = candles[i - 1];
    trs.push(Math.max(curr.high - curr.low, Math.abs(curr.high - prev.close), Math.abs(curr.low - prev.close)));
  }
  const atrs = [];
  let sum = 0;
  for (let i = 0; i < trs.length; i++) {
    if (i < period) { sum += trs[i]; if (i === period - 1) atrs.push(sum / period); else atrs.push(NaN); }
    else { atrs.push((atrs[atrs.length - 1] * (period - 1) + trs[i]) / period); }
  }
  return atrs;
}

// --- RSI ---
function calculateRSI(candles, period = 14) {
  if (candles.length < period + 1) return candles.map(() => NaN);
  const rsi = new Array(candles.length).fill(NaN);
  let avgGain = 0, avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    if (diff >= 0) avgGain += diff / period; else avgLoss += Math.abs(diff) / period;
  }
  rsi[period] = 100 - 100 / (1 + (avgLoss === 0 ? Infinity : avgGain / avgLoss));
  for (let i = period + 1; i < candles.length; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (diff < 0 ? Math.abs(diff) : 0)) / period;
    rsi[i] = 100 - 100 / (1 + (avgLoss === 0 ? Infinity : avgGain / avgLoss));
  }
  return rsi;
}

// --- Resolve old signals and record outcomes ---
function resolveOldSignals() {
  const now = Date.now();
  const RESOLUTION_WINDOW_MS = 24 * 60 * 60 * 1000;
  const stillPending = [];

  for (const signal of paperSignalsBuffer) {
    if (!signal.generatedAt) { stillPending.push(signal); continue; }
    const age = now - new Date(signal.generatedAt).getTime();
    if (age < RESOLUTION_WINDOW_MS) { stillPending.push(signal); continue; }

    const candles = candleStore.pairs[signal.pair];
    if (!candles || candles.length < 2) { stillPending.push(signal); continue; }

    const signalTime = new Date(signal.generatedAt).getTime();
    const entryIdx = candles.findIndex(c => new Date(c.datetime).getTime() >= signalTime);
    if (entryIdx < 0 || entryIdx + 24 >= candles.length) { stillPending.push(signal); continue; }

    const entry = candles[entryIdx].close;
    const future = candles.slice(entryIdx + 1, entryIdx + 25);
    const atrVals = calculateATR(candles.slice(Math.max(0, entryIdx - 14), entryIdx + 1), 14);
    const atr = atrVals.length ? atrVals[atrVals.length - 1] : entry * 0.001;

    let outcome = 'Loss';
    if (signal.direction === 'BUY' && future.some(c => c.high >= entry + atr)) outcome = 'Win';
    if (signal.direction === 'SELL' && future.some(c => c.low <= entry - atr)) outcome = 'Win';

    const resolved = { ...signal, outcome, resolvedAt: new Date().toISOString() };
    resolvedSignalsBuffer.push(resolved);
    newResolvedSinceRetrain++;

    if (signal.id) {
      pushOutcomeToCanister(signal.id, outcome === 'Win').catch(() => {});
    }
  }

  paperSignalsBuffer = stillPending;

  if (newResolvedSinceRetrain > 0) {
    try { fs.writeFileSync(RESOLVED_SIGNALS_PATH, JSON.stringify(resolvedSignalsBuffer.slice(-2000)), 'utf8'); } catch (_) {}
  }

  if (newResolvedSinceRetrain >= 50 && resolvedSignalsBuffer.length >= 100) {
    console.log(`[ensemble] Retraining on ${resolvedSignalsBuffer.length} resolved signals...`);
    const trainingData = resolvedSignalsBuffer.map(s => ({
      features: ensembleScorer.extractFeatures(s, null),
      outcome: s.outcome === 'Win'
    }));
    ensembleScorer.initEnsembleScorer(trainingData);
    newResolvedSinceRetrain = 0;
  }
}

// --- Live collection (Twelve Data) ---
async function runCollection() {
  console.log(`[${new Date().toISOString()}] Collection cycle starting...`);
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 8000));
    try {
      const response = await withTimeout(
        axios.get('https://api.twelvedata.com/exchange_rate', {
          params: { symbol: pair, apikey: API_KEY }, timeout: 10000
        }),
        15000, `rates ${pair}`
      );
      if (response.data && response.data.rate) {
        ratesCache.data.pairs[pair] = { price: String(response.data.rate), fetchedAt: new Date().toISOString() };
        ratesCache.fetchedAt = Date.now();
      }
      incrementCallCount();
    } catch (err) { lastCollectionError = `${pair} rates: ${err.message}`; console.error(`[collection] Rate fetch failed for ${pair}:`, err.message); }
  }

  const now = Date.now();
  if (now - lastHistoryFetchAt >= 2 * 60 * 60 * 1000) {
    lastHistoryFetchAt = now;
    await new Promise(r => setTimeout(r, 2000));
    for (let i = 0; i < FOREX_PAIRS.length; i++) {
      const pair = FOREX_PAIRS[i];
      if (i > 0) await new Promise(r => setTimeout(r, 8000));
      try {
        const response = await withTimeout(
          axios.get('https://api.twelvedata.com/time_series', {
            params: { symbol: pair, interval: '1h', outputsize: 5000, apikey: API_KEY }, timeout: 30000
          }),
          40000, `history ${pair}`
        );
        const newCandles = (response.data.values || []).map(c => ({
          datetime: c.datetime, open: parseFloat(c.open), high: parseFloat(c.high),
          low: parseFloat(c.low), close: parseFloat(c.close)
        })).filter(c => !isNaN(c.open) && !isNaN(c.close));
        if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
        const map = {};
        for (const c of candleStore.pairs[pair]) map[c.datetime] = c;
        for (const c of newCandles) map[c.datetime] = c;
        candleStore.pairs[pair] = Object.values(map).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
        historyCache.data.pairs[pair] = candleStore.pairs[pair].slice(-5000);
        historyCache.fetchedAt = Date.now();
        incrementCallCount();

        const latest100 = candleStore.pairs[pair].slice(-100);
        pushCandlesToCanister(pair, latest100).catch(() => {});

      } catch (err) { lastCollectionError = `${pair} history: ${err.message}`; console.error(`[collection] History fetch failed for ${pair}:`, err.message); }
    }
    try {
      fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8');
      lastCollectionError = null;
    } catch (err) { lastCollectionError = `File write failed: ${err.message}`; }
  }

  resolveOldSignals();
}

// --- Calendar ---
const CALENDAR_CACHE_TTL = 60 * 60 * 1000;
const RELEVANT_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD'];

async function fetchCalendar() {
  const now = new Date();
  const from = now.toISOString().split('T')[0];
  const to = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  if (FINNHUB_API_KEY) {
    try {
      const response = await withTimeout(
        axios.get('https://finnhub.io/api/v1/calendar/economic', {
          params: { from, to, token: FINNHUB_API_KEY }, timeout: 10000
        }),
        15000, 'fetchCalendar finnhub'
      );
      const events = (response.data.economicCalendar || [])
        .filter(e => {
          const impact = (e.impact || '').toLowerCase();
          const currency = COUNTRY_TO_CURRENCY[e.country] || null;
          return impact === 'high' && currency && RELEVANT_CURRENCIES.includes(currency);
        })
        .map(e => ({
          time: e.time,
          currency: COUNTRY_TO_CURRENCY[e.country],
          event: e.event,
          impact: e.impact,
          forecast: e.estimate || null,
          previous: e.prev || null
        }));
      calendarCache = { data: events, fetchedAt: Date.now() };
      console.log(`[calendar] Finnhub: ${events.length} high-impact events.`);
      return;
    } catch (err) { console.error('[calendar] Finnhub failed:', err.message); }
  }

  // Fallback: ForexFactory RSS
  try {
    const rssResponse = await withTimeout(
      axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.xml', { timeout: 10000 }),
      15000, 'fetchCalendar forexfactory'
    );
    const xml = rssResponse.data;
    const events = [];
    const itemRe = /<event>([\s\S]*?)<\/event>/g;
    let match;
    while ((match = itemRe.exec(xml)) !== null) {
      const item = match[1];
      const titleMatch = item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/) || item.match(/<title>(.*?)<\/title>/);
      const dateMatch = item.match(/<date>(.*?)<\/date>/);
      const timeMatch = item.match(/<time>(.*?)<\/time>/);
      const impactMatch = item.match(/<impact>(.*?)<\/impact>/);
      const currencyMatch = item.match(/<country>(.*?)<\/country>/);
      const title = titleMatch ? titleMatch[1].trim() : '';
      const impact = impactMatch ? impactMatch[1].trim() : '';
      const currency = currencyMatch ? currencyMatch[1].trim().toUpperCase() : '';
      const date = dateMatch ? dateMatch[1].trim() : '';
      const time = timeMatch ? timeMatch[1].trim() : '';
      if (impact === 'High' && RELEVANT_CURRENCIES.includes(currency)) {
        events.push({ time: `${date} ${time}`.trim(), currency, event: title, impact: 'high', forecast: null, previous: null });
      }
    }
    calendarCache = { data: events, fetchedAt: Date.now() };
    console.log(`[calendar] ForexFactory RSS: ${events.length} high-impact events.`);
  } catch (err) { console.error('[calendar] ForexFactory RSS fallback failed:', err.message); }
}

// --- Massive.com backfill ---
async function runMassiveBackfill() {
  let remainingPairs = [...FOREX_PAIRS];
  let pass = 0;
  const MAX_PASSES = 3;

  while (remainingPairs.length > 0 && pass < MAX_PASSES) {
    pass++;
    console.log(`[backfill] Pass ${pass}. Pairs remaining: ${remainingPairs.join(', ')}`);
    backfillStatus = 'running';

    let progress = {};
    try {
      if (fs.existsSync(BACKFILL_PROGRESS_PATH)) {
        progress = JSON.parse(fs.readFileSync(BACKFILL_PROGRESS_PATH, 'utf8'));
      }
    } catch (_) {}

    const now = new Date();
    const twoYearsAgo = new Date(now);
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    function buildChunks(fromDate, toDate) {
      const chunks = [];
      let cursor = new Date(fromDate);
      while (cursor < toDate) {
        const chunkEnd = new Date(cursor);
        chunkEnd.setMonth(chunkEnd.getMonth() + 1);
        if (chunkEnd > toDate) chunkEnd.setTime(toDate.getTime());
        chunks.push({ from: cursor.toISOString().split('T')[0], to: chunkEnd.toISOString().split('T')[0] });
        cursor = new Date(chunkEnd);
        cursor.setDate(cursor.getDate() + 1);
      }
      return chunks;
    }

    const chunks = buildChunks(twoYearsAgo, now);
    const incompleteThisPass = [];

    for (const pair of remainingPairs) {
      if (progress[pair] === 'complete') {
        backfillPairsComplete++;
        backfillTotalCandles += (candleStore.pairs[pair] || []).length;
        continue;
      }

      const ticker = MASSIVE_TICKER_MAP[pair];
      if (!ticker) { console.warn(`[backfill] ${pair}: no ticker, skipping.`); continue; }

      console.log(`[backfill] Starting ${pair}...`);
      let pairCandles = [];
      let chunksFailed = 0;
      let pairAborted = false;

      for (const chunk of chunks) {
        if (pairAborted) break;
        await new Promise(r => setImmediate(r));
        try {
          let nextUrl = `https://api.massive.com/v2/aggs/ticker/${ticker}/range/1/hour/${chunk.from}/${chunk.to}`;
          let params = { adjusted: false, sort: 'asc', limit: 50000, apiKey: MASSIVE_API_KEY };
          let prev = null, pages = 0;

          while (nextUrl && pages < 10) {
            const response = await withTimeout(
              axios.get(nextUrl, { params, timeout: 25000 }),
              30000, `backfill ${pair} ${chunk.from}→${chunk.to} page ${pages}`
            );
            params = {};
            const results = (response.data && Array.isArray(response.data.results)) ? response.data.results : [];
            for (const bar of results) {
              const { o: open, h: high, l: low, c: close } = bar;
              if (!open || !close || isNaN(open) || isNaN(close)) continue;
              if (prev !== null && Math.abs(close - prev) / prev > 0.20) { prev = close; continue; }
              const dt = new Date(bar.t).toISOString().slice(0, 19).replace('T', ' ');
              pairCandles.push({ datetime: dt, open, high, low, close });
              prev = close;
            }
            nextUrl = response.data.next_url ? response.data.next_url : null;
            pages++;
            if (nextUrl) await new Promise(r => setTimeout(r, 200));
          }

          chunksFailed = 0;
          await new Promise(r => setTimeout(r, 12000));

        } catch (err) {
          chunksFailed++;
          const isRateLimit = err.response && err.response.status === 429;
          const msg = err.response ? `HTTP ${err.response.status}: ${JSON.stringify(err.response.data)}` : err.message;
          console.error(`[backfill] ${pair} ${chunk.from}→${chunk.to} failed (${chunksFailed}):`, msg);
          if (chunksFailed >= 3) {
            console.error(`[backfill] ${pair}: 3 consecutive failures, skipping pair.`);
            pairAborted = true;
            break;
          }
          const retryWait = isRateLimit ? 60000 : 30000;
          await new Promise(r => setTimeout(r, retryWait));
        }
      }

      if (pairCandles.length === 0 || pairAborted) {
        progress[pair] = 'incomplete';
        incompleteThisPass.push(pair);
        try { fs.writeFileSync(BACKFILL_PROGRESS_PATH, JSON.stringify(progress), 'utf8'); } catch (_) {}
        continue;
      }

      if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
      const mapByDt = {};
      for (const c of candleStore.pairs[pair]) mapByDt[c.datetime] = c;
      for (const c of pairCandles) mapByDt[c.datetime] = c;
      candleStore.pairs[pair] = Object.values(mapByDt).sort((a, b) => a.datetime < b.datetime ? -1 : 1);

      backfillTotalCandles += pairCandles.length;
      historyCache.data.pairs[pair] = candleStore.pairs[pair].slice(-5000);

      try { fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8'); } catch (_) {}

      progress[pair] = 'complete';
      backfillPairsComplete++;
      console.log(`[backfill] ${pair}: ${pairCandles.length} candles. Total: ${backfillTotalCandles}`);
      try { fs.writeFileSync(BACKFILL_PROGRESS_PATH, JSON.stringify(progress), 'utf8'); } catch (_) {}

      await new Promise(r => setTimeout(r, 15000));
    }

    remainingPairs = incompleteThisPass;
    if (remainingPairs.length > 0 && pass < MAX_PASSES) {
      console.log(`[backfill] ${remainingPairs.length} pairs incomplete. Retrying in 60s...`);
      await new Promise(r => setTimeout(r, 60000));
    }
  }

  backfillStatus = (() => {
    let prog = {};
    try { prog = JSON.parse(fs.readFileSync(BACKFILL_PROGRESS_PATH, 'utf8')); } catch (_) {}
    return FOREX_PAIRS.every(p => prog[p] === 'complete') ? 'complete' : 'partial';
  })();

  console.log(`[backfill] Done. Status: ${backfillStatus}. Total candles: ${backfillTotalCandles}`);
  scheduleIntelligenceIfReady();
}

function scheduleIntelligenceIfReady() {
  const totalCandles = FOREX_PAIRS.reduce((sum, p) => sum + (candleStore.pairs[p] || []).length, 0);
  if (totalCandles < 200 * FOREX_PAIRS.length) {
    console.log(`[intelligence] Not enough candles (${totalCandles}). Retrying in 1h.`);
    setTimeout(scheduleIntelligenceIfReady, 60 * 60 * 1000);
    return;
  }
  calculateIntelligenceProfile();
}

// --- Intelligence profile ---
function yieldLoop() { return new Promise(r => setImmediate(r)); }
function percentile(sorted, p) { if (!sorted.length) return 0; return sorted[Math.floor((p / 100) * (sorted.length - 1))]; }

async function calculateIntelligenceProfile() {
  intelligenceStatus = 'calculating';
  const profile = { _calculatedAt: new Date().toISOString() };
  for (const pair of FOREX_PAIRS) {
    await yieldLoop();
    const candles = candleStore.pairs[pair];
    if (!candles || candles.length < 200) { profile[pair] = { status: 'insufficient_data' }; continue; }
    const atrs = calculateATR(candles, 14).filter(v => !isNaN(v));
    const atrSort = [...atrs].sort((a, b) => a - b);
    const atrPercentiles = { p25: percentile(atrSort, 25), p50: percentile(atrSort, 50), p75: percentile(atrSort, 75), p95: percentile(atrSort, 95) };
    const recentATR = calculateATR(candles.slice(-15), 14).filter(v => !isNaN(v));
    const currentATR = recentATR.length ? recentATR[recentATR.length - 1] : atrPercentiles.p50;
    const volatilityRegime = currentATR < atrPercentiles.p25 ? 'low' : currentATR < atrPercentiles.p75 ? 'normal' : currentATR < atrPercentiles.p95 ? 'high' : 'extreme';
    const splitIdx = Math.floor(candles.length * 0.7);
    const testCandles = candles.slice(splitIdx);
    const rsiValues = calculateRSI(testCandles, 14);
    let bestOversold = 30, bestOverbought = 70, bestScore = -Infinity;
    for (let os = 25; os <= 45; os += 2.5) {
      for (let ob = 55; ob <= 75; ob += 2.5) {
        let wins = 0, total = 0;
        for (let i = 1; i < testCandles.length - 4; i++) {
          const rsi = rsiValues[i]; if (isNaN(rsi)) continue;
          const isBuy = rsi < os, isSell = rsi > ob; if (!isBuy && !isSell) continue;
          const atr1 = atrs.length ? atrs[Math.min(splitIdx + i, atrs.length - 1)] : 0.001;
          const future4 = testCandles.slice(i + 1, i + 5).map(c => c.close); if (future4.length < 4) continue;
          const maxMove = isBuy ? Math.max(...future4) - testCandles[i].close : testCandles[i].close - Math.min(...future4);
          if (maxMove >= atr1) wins++; total++;
        }
        if (total < 20) continue;
        const score = wins / total; if (score > bestScore) { bestScore = score; bestOversold = os; bestOverbought = ob; }
      }
    }
    profile[pair] = { adaptiveRSI: { oversold: bestOversold, overbought: bestOverbought }, atrPercentiles, volatilityRegime, profileVersion: 1, lastCalculated: new Date().toISOString(), status: 'active' };
  }
  intelligenceProfile = profile;
  intelligenceStatus = 'active';
  intelligenceLastCalculated = profile._calculatedAt;
  try { fs.writeFileSync(INTELLIGENCE_PROFILE_PATH, JSON.stringify(profile), 'utf8'); } catch (err) { console.error('[intelligence] Failed to persist:', err.message); }
  console.log('[intelligence] Profile calculated and saved.');
}

// --- SMC evaluation ---
async function runSMCEvaluation() {
  try {
    const dxyData = await smcEngine.fetchDXYData();
    currentDXYBias = smcEngine.calculateDXYTrend(dxyData);
    dxyLastFetchedAt = new Date().toISOString();
  } catch (err) { console.error('[SMC] DXY refresh failed:', err.message); }

  const killzone = smcEngine.isInsideKillzone();
  for (const pair of FOREX_PAIRS) {
    try {
      // FIX #7: per-pair volatility suppression — only skip the affected pair, not all pairs
      const { suppressed, reason } = smcEngine.isVolatilitySuppressed(pair, intelligenceProfile);
      if (suppressed) {
        console.log(`[SMC] ${reason}`);
        continue;
      }

      const candles = candleStore.pairs[pair]; if (!candles || candles.length < 50) continue;
      const rsiVals = calculateRSI(candles, 14), lastRSI = rsiVals[rsiVals.length - 1]; if (isNaN(lastRSI)) continue;
      const pairProfile = intelligenceProfile && intelligenceProfile[pair];
      const oversold = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.oversold : 35;
      const overbought = (pairProfile && pairProfile.adaptiveRSI) ? pairProfile.adaptiveRSI.overbought : 65;
      let candidateDirection = null;
      if (lastRSI < oversold) candidateDirection = 'BUY';
      if (lastRSI > overbought) candidateDirection = 'SELL';
      if (!candidateDirection) continue;
      const obList = smcEngine.detectOrderBlocks(candles, candidateDirection);
      const fvgList = smcEngine.detectFairValueGaps(candles, candidateDirection);
      const sweepResult = smcEngine.detectLiquiditySweep(candles);
      const pdZone = smcEngine.getPremiumDiscountZone(candles);
      const trinity = smcEngine.evaluateHolyTrinity(candles, candidateDirection, obList, fvgList, sweepResult, pdZone);
      const dxyPenalty = smcEngine.getDXYPenalty(currentDXYBias, pair, candidateDirection);
      const rawConfidence = candidateDirection === 'BUY'
        ? Math.min(100, Math.round(60 + (oversold - lastRSI) * 2))
        : Math.min(100, Math.round(60 + (lastRSI - overbought) * 2));
      const adjustedConfidence = Math.max(0, rawConfidence - dxyPenalty);

      const signalTypeKey = smcEngine.computeSignalTypeKey({
        orderBlockPresent: trinity.orderBlockPresent === true,
        fvgPresent: trinity.fvgPresent === true,
        liquiditySweepPresent: trinity.liquiditySweepPresent === true
      });

      const classification = smcEngine.classifySignal(trinity, true, killzone);
      const now = new Date();
      const signalId = `${pair}-${now.getTime()}`;
      const signalBase = {
        id: signalId,
        pair, direction: candidateDirection, confidence: adjustedConfidence,
        orderBlockPresent: trinity.orderBlockPresent === true,
        fvgPresent: trinity.fvgPresent === true,
        liquiditySweepPresent: trinity.liquiditySweepPresent === true,
        killzoneActive: killzone.active,
        killzoneName: killzone.killzoneName || null,
        dxyBias: currentDXYBias,
        obZone: trinity.obZone || null,
        fvgZone: trinity.fvgZone || null,
        entry:      trinity.entry      || null,
        stopLoss:   trinity.stopLoss   || null,
        takeProfit: trinity.takeProfit || null,
        atr:        trinity.atr        || null,
        ensembleScore: null, signalTypeKey, generatedAt: now.toISOString()
      };

      let ensembleScore = null;
      if (classification === 'LIVE' || classification === 'STANDARD') {
        try {
          const features = ensembleScorer.extractFeatures(signalBase, null);
          const score = await ensembleScorer.scoreSignal(features);
          if (score !== null) ensembleScore = score;
        } catch (ensErr) {
          console.warn(`[ensemble] Scoring failed for ${pair}:`, ensErr.message);
        }
      }

      if (classification === 'LIVE') {
        if (ensembleScore !== null && ensembleScore < 40) {
          const ps = { ...signalBase, ensembleScore, isPaper: true, paperReason: 'ENSEMBLE_LOW_SCORE' };
          if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift();
          paperSignalsBuffer.push(ps);
          pushSignalToCanister(ps).catch(() => {});
        } else {
          lastHolyTrinityAt[pair] = now;
          console.log(`[SMC] LIVE signal: ${pair} ${candidateDirection} conf=${adjustedConfidence} ensemble=${ensembleScore ?? 'N/A'}`);
          pushSignalToCanister({ ...signalBase, ensembleScore, isPaper: false }).catch(() => {});
        }
      } else if (classification === 'PAPER_OUTSIDE_KILLZONE' || classification === 'PAPER_INDICATOR_FAILED') {
        const ps = { ...signalBase, ensembleScore: null, isPaper: true, paperReason: classification };
        if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift();
        paperSignalsBuffer.push(ps);
        pushSignalToCanister(ps).catch(() => {});
      } else if (classification === 'STANDARD') {
        if (ensembleScore !== null && ensembleScore < 40) {
          const ps = { ...signalBase, confidence: Math.min(70, adjustedConfidence), ensembleScore, isPaper: true, paperReason: 'ENSEMBLE_LOW_SCORE' };
          if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift();
          paperSignalsBuffer.push(ps);
          pushSignalToCanister(ps).catch(() => {});
        }
      }
    } catch (err) { console.error(`[SMC] Error for ${pair}:`, err.message); }
  }
}

// --- Routes ---

app.get('/rates', (req, res) => { cors(res); res.json(ratesCache.data); });
app.get('/history', (req, res) => { cors(res); res.json(historyCache.data); });
app.get('/calendar', (req, res) => {
  cors(res);
  res.json({ events: calendarCache.data, fetchedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null, count: calendarCache.data.length });
});
app.get('/intelligence', (req, res) => {
  cors(res);
  if (!intelligenceProfile || intelligenceStatus !== 'active')
    return res.json({ status: intelligenceStatus, pairsComplete: backfillPairsComplete, pairsTotal: FOREX_PAIRS.length });
  res.json({ status: 'active', profile: intelligenceProfile, fetchedAt: intelligenceLastCalculated, profileVersion: 1 });
});
app.get('/status', (req, res) => {
  cors(res);
  const storeCounts = {};
  for (const pair of FOREX_PAIRS) storeCounts[pair] = (candleStore.pairs[pair] || []).length;
  res.json({
    callsToday: dailyCallCount, limit: 800, resetAt: new Date(callCountResetAt).toUTCString(),
    candlePairsStored: Object.keys(candleStore.pairs).length, candleCountPerPair: storeCounts,
    calendarEvents: calendarCache.data.length,
    backfillStatus, backfillPairsComplete, backfillPairsTotal: FOREX_PAIRS.length, backfillTotalCandles,
    intelligenceStatus, intelligenceLastCalculated,
    lastCollectionError: lastCollectionError || null,
    canisterConnected: !!(CANISTER_HOST && CANISTER_ID && PROXY_IDENTITY_KEY)
  });
});
app.get('/health', (req, res) => {
  cors(res);
  res.json({ status: 'ok', uptime: process.uptime(), ratesCachedAt: ratesCache.fetchedAt ? new Date(ratesCache.fetchedAt).toISOString() : null, historyCachedAt: historyCache.fetchedAt ? new Date(historyCache.fetchedAt).toISOString() : null, intelligenceStatus });
});
app.get('/stored-history', (req, res) => {
  cors(res);
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : null;
  if (!limit || isNaN(limit)) return res.json(candleStore);
  const limited = { pairs: {} };
  for (const pair of Object.keys(candleStore.pairs)) limited.pairs[pair] = candleStore.pairs[pair].slice(-limit);
  return res.json(limited);
});
app.get('/paper-signals', (req, res) => {
  cors(res);
  if (!requireAuth(req, res)) return;
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : paperSignalsBuffer.length;
  const safe = isNaN(limit) ? paperSignalsBuffer.length : Math.min(limit, paperSignalsBuffer.length);
  res.json({ count: paperSignalsBuffer.length, signals: paperSignalsBuffer.slice(-safe) });
});
app.get('/resolved-signals', (req, res) => {
  cors(res);
  if (!requireAuth(req, res)) return;
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : 200;
  res.json({ count: resolvedSignalsBuffer.length, signals: resolvedSignalsBuffer.slice(-limit) });
});
app.get('/smc-status', (req, res) => {
  cors(res);
  if (!requireAuth(req, res)) return;
  try {
    const killzone = smcEngine.isInsideKillzone();
    const activePairs = {};
    for (const pair of FOREX_PAIRS) {
      const candles = candleStore.pairs[pair];
      if (!candles || candles.length < 50) { activePairs[pair] = { activeOBCount: 0, activeFVGCount: 0, lastSweepAge: null, premiumDiscount: 'UNKNOWN' }; continue; }
      const obs = smcEngine.detectOrderBlocks(candles, 'BUY').length + smcEngine.detectOrderBlocks(candles, 'SELL').length;
      const fvgs = smcEngine.detectFairValueGaps(candles, 'BUY').length + smcEngine.detectFairValueGaps(candles, 'SELL').length;
      const sweep = smcEngine.detectLiquiditySweep(candles);
      const pdZone = smcEngine.getPremiumDiscountZone(candles);
      activePairs[pair] = { activeOBCount: obs, activeFVGCount: fvgs, lastSweepAge: sweep.sweepCandleIndex !== null ? candles.length - 1 - sweep.sweepCandleIndex : null, premiumDiscount: pdZone.isPremium ? 'PREMIUM' : pdZone.isDiscount ? 'DISCOUNT' : 'NEUTRAL' };
    }
    res.json({ dxyBias: currentDXYBias, dxyLastFetched: dxyLastFetchedAt, killzoneActive: killzone.active, killzoneName: killzone.killzoneName, activePairs });
  } catch (err) { res.status(500).json({ error: 'SMC status unavailable' }); }
});
app.get('/ensemble-status', (req, res) => {
  cors(res);
  if (!requireAuth(req, res)) return;
  res.json(ensembleScorer.getEnsembleStatus());
});

// --- Start ---
app.listen(PORT, async () => {
  console.log(`[${new Date().toISOString()}] ForexMind proxy on port ${PORT}`);
  console.log(`[startup] Canister connected: ${!!(CANISTER_HOST && CANISTER_ID && PROXY_IDENTITY_KEY)}`);
  try { fs.mkdirSync('/data', { recursive: true }); } catch (_) {}

  await restoreFromCanister();

  const runCollectionWithSMC = async () => {
    await runCollection();
    runSMCEvaluation().catch(err => console.error('[SMC] Post-collection error:', err.message));
  };
  runCollectionWithSMC();
  setInterval(runCollectionWithSMC, 15 * 60 * 1000);

  smcEngine.fetchDXYData()
    .then(data => { currentDXYBias = smcEngine.calculateDXYTrend(data); dxyLastFetchedAt = new Date().toISOString(); console.log(`[DXY] Initial: ${currentDXYBias}`); })
    .catch(err => console.warn('[DXY] Initial fetch failed:', err.message));

  fetchCalendar();
  setInterval(fetchCalendar, CALENDAR_CACHE_TTL);

  setImmediate(() => runMassiveBackfill());

  const existingCandles = FOREX_PAIRS.reduce((sum, p) => sum + (candleStore.pairs[p] || []).length, 0);
  if (existingCandles >= 200 * FOREX_PAIRS.length && !intelligenceProfile) {
    setTimeout(() => calculateIntelligenceProfile(), 2 * 60 * 1000);
  }
  // FIX #7: use INTELLIGENCE_RECALC_INTERVAL_MS from smcEngine (6 hours) instead of hardcoded 7 days
  setInterval(() => calculateIntelligenceProfile(), smcEngine.INTELLIGENCE_RECALC_INTERVAL_MS);

  setTimeout(async () => {
    let allSignals = [];

    if (resolvedSignalsBuffer.length >= 100) {
      allSignals = resolvedSignalsBuffer.map(s => ({
        features: ensembleScorer.extractFeatures(s, null), outcome: s.outcome === 'Win'
      }));
    }

    if (CANISTER_HOST && CANISTER_ID) {
      let page = 0, done = false;
      while (!done) {
        try {
          const resp = await withTimeout(
            axios.post(`${CANISTER_HOST}/api/v1/query`, { canisterId: CANISTER_ID, method: 'getSignalPage', args: ['ALL', page, 100] }, { timeout: 15000 }),
            20000, `ensemble training page ${page}`
          );
          const signals = (resp.data && Array.isArray(resp.data.result)) ? resp.data.result : [];
          if (signals.length === 0) { done = true; break; }
          for (const s of signals) {
            if (s.outcome && s.outcome !== 'Pending') allSignals.push({ features: ensembleScorer.extractFeatures(s, null), outcome: s.outcome === 'Win' });
          }
          if (signals.length < 100) { done = true; break; }
          page++;
        } catch (_) { done = true; }
      }
    }

    console.log(`[Ensemble] ${allSignals.length} resolved signals for training.`);
    if (allSignals.length >= 100) ensembleScorer.initEnsembleScorer(allSignals);
  }, 30 * 1000);
});
