'use strict';

const express = require('express');
const axios = require('axios');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const smcEngine = require('./smcEngine');
const ensembleScorer = require('./ensembleScorer');
const API_KEY = process.env.TWELVEDATA_API_KEY;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

const FOREX_PAIRS = (process.env.FOREX_PAIRS || 'EUR/USD,GBP/USD,USD/JPY,USD/CHF,AUD/USD,USD/CAD,NZD/USD')
  .split(',').map(p => p.trim());

const BID_TO_MID_ADJUSTMENT = {
  'EUR/USD': 0.00015, 'GBP/USD': 0.00025, 'USD/JPY': 0.015,
  'USD/CHF': 0.00020, 'AUD/USD': 0.00020, 'USD/CAD': 0.00025, 'NZD/USD': 0.00025
};

const DUKASCOPY_INSTRUMENTS = {
  'EUR/USD': 'eurusd', 'GBP/USD': 'gbpusd', 'USD/JPY': 'usdjpy',
  'USD/CHF': 'usdchf', 'AUD/USD': 'audusd', 'USD/CAD': 'usdcad', 'NZD/USD': 'nzdusd'
};

const CANDLE_STORE_PATH = '/tmp/candle-store.json';
const BACKFILL_PROGRESS_PATH = '/tmp/backfill-progress.json';
const INTELLIGENCE_PROFILE_PATH = '/tmp/intelligence-profile.json';

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

function cors(res) { res.setHeader('Access-Control-Allow-Origin', '*'); }

try {
  if (fs.existsSync(CANDLE_STORE_PATH)) {
    const raw = fs.readFileSync(CANDLE_STORE_PATH, 'utf8');
    candleStore = JSON.parse(raw);
    historyCache.data = { pairs: {} };
    for (const pair of Object.keys(candleStore.pairs)) {
      historyCache.data.pairs[pair] = candleStore.pairs[pair].slice(-5000);
    }
    console.log(`[startup] Candle store loaded from disk (${Object.keys(candleStore.pairs).length} pairs).`);
  }
} catch (err) { console.error('[startup] Failed to load candle store:', err.message); candleStore = { pairs: {} }; }

try {
  if (fs.existsSync(INTELLIGENCE_PROFILE_PATH)) {
    const raw = fs.readFileSync(INTELLIGENCE_PROFILE_PATH, 'utf8');
    intelligenceProfile = JSON.parse(raw);
    intelligenceStatus = 'active';
    intelligenceLastCalculated = intelligenceProfile._calculatedAt || null;
    console.log('[startup] Intelligence profile loaded from disk.');
  }
} catch (err) { console.error('[startup] Failed to load intelligence profile:', err.message); intelligenceProfile = null; }

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

async function runCollection() {
  console.log(`[${new Date().toISOString()}] Collection cycle starting...`);

  // --- Rates: every 15 minutes (7 calls per cycle) ---
  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await new Promise(r => setTimeout(r, 2000));
    try {
      const response = await axios.get('https://api.twelvedata.com/exchange_rate', { params: { symbol: pair, apikey: API_KEY }, timeout: 10000 });
      if (response.data && response.data.rate) { ratesCache.data.pairs[pair] = { price: String(response.data.rate), fetchedAt: new Date().toISOString() }; ratesCache.fetchedAt = Date.now(); }
      incrementCallCount();
    } catch (err) { lastCollectionError = `${pair} rates: ${err.message}`; console.error(`[collection] Rate fetch failed for ${pair}:`, err.message); }
  }
  console.log(`[${new Date().toISOString()}] Rates collection complete. Daily calls: ${dailyCallCount}`);

  // --- History: every 2 hours (7 calls per cycle) ---
  const now = Date.now();
  if (now - lastHistoryFetchAt >= 2 * 60 * 60 * 1000) {
    lastHistoryFetchAt = now;
    await new Promise(r => setTimeout(r, 2000));
    for (let i = 0; i < FOREX_PAIRS.length; i++) {
      const pair = FOREX_PAIRS[i];
      if (i > 0) await new Promise(r => setTimeout(r, 2000));
      try {
        const response = await axios.get('https://api.twelvedata.com/time_series', { params: { symbol: pair, interval: '1h', outputsize: 5000, apikey: API_KEY }, timeout: 30000 });
        const newCandles = (response.data.values || []).map(c => ({ datetime: c.datetime, open: parseFloat(c.open), high: parseFloat(c.high), low: parseFloat(c.low), close: parseFloat(c.close) })).filter(c => !isNaN(c.open) && !isNaN(c.close));
        if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
        const map = {};
        for (const c of candleStore.pairs[pair]) map[c.datetime] = c;
        for (const c of newCandles) map[c.datetime] = c;
        const merged = Object.values(map).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
        candleStore.pairs[pair] = merged;
        historyCache.data.pairs[pair] = merged.slice(-5000);
        historyCache.fetchedAt = Date.now();
        incrementCallCount();
      } catch (err) { lastCollectionError = `${pair} history: ${err.message}`; console.error(`[collection] History fetch failed for ${pair}:`, err.message); }
    }
    try { fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8'); lastCollectionError = null; console.log(`[${new Date().toISOString()}] History collection complete. Daily calls: ${dailyCallCount}`); }
    catch (err) { lastCollectionError = `File write failed: ${err.message}`; console.error('[collection] Failed to write candle store:', err.message); }
  }
}

const CALENDAR_CACHE_TTL = 60 * 60 * 1000;
const RELEVANT_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD'];

async function fetchCalendar() {
  const now = new Date();
  const from = now.toISOString().split('T')[0];
  const to = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  if (FINNHUB_API_KEY) {
    try {
      const response = await axios.get('https://finnhub.io/api/v1/calendar/economic', { params: { from, to, token: FINNHUB_API_KEY }, timeout: 10000 });
      const events = (response.data.economicCalendar || []).filter(e => (e.impact || '').toLowerCase() === 'high' && RELEVANT_CURRENCIES.includes((e.country || '').toUpperCase())).map(e => ({ time: e.time, currency: e.country, event: e.event, impact: e.impact, forecast: e.estimate || null, previous: e.prev || null }));
      calendarCache = { data: events, fetchedAt: Date.now() };
      console.log(`[calendar] Finnhub: ${events.length} high-impact events.`); return;
    } catch (err) { console.error('[calendar] Finnhub fetch failed:', err.message); }
  }
  try {
    const rssResponse = await axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.xml', { timeout: 10000 });
    const xml = rssResponse.data, events = [];
    const itemRe = /<item>([\s\S]*?)<\/item>/g; let match;
    while ((match = itemRe.exec(xml)) !== null) {
      const item = match[1];
      const title = (item.match(/<title>(.*?)<\/title>/) || [])[1] || '';
      const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/) || [])[1] || '';
      const impact = (item.match(/<impact>(.*?)<\/impact>/) || [])[1] || '';
      const currency = (item.match(/<country>(.*?)<\/country>/) || [])[1] || '';
      if (impact.toLowerCase() === 'high' && RELEVANT_CURRENCIES.includes(currency.toUpperCase())) events.push({ time: pubDate, currency: currency.toUpperCase(), event: title, impact: 'high', forecast: null, previous: null });
    }
    calendarCache = { data: events, fetchedAt: Date.now() };
    console.log(`[calendar] ForexFactory RSS: ${events.length} high-impact events.`);
  } catch (err) { console.error('[calendar] ForexFactory RSS fallback failed:', err.message); }
}

async function runDukascopyBackfill() {
  backfillStatus = 'running';
  let getHistoricalRates;
  try { ({ getHistoricalRates } = require('dukascopy-node')); }
  catch (err) { backfillStatus = 'error'; console.error('[backfill] dukascopy-node not available:', err.message); return; }
  let progress = {};
  try { if (fs.existsSync(BACKFILL_PROGRESS_PATH)) progress = JSON.parse(fs.readFileSync(BACKFILL_PROGRESS_PATH, 'utf8')); } catch (_) {}
  const now = new Date(), startYear = now.getUTCFullYear() - 10;
  for (const pair of FOREX_PAIRS) {
    if (progress[pair] === 'complete') { backfillPairsComplete++; continue; }
    const instrument = DUKASCOPY_INSTRUMENTS[pair];
    if (!instrument) continue;
    const adjustment = BID_TO_MID_ADJUSTMENT[pair] || 0;
    let pairCandles = [], yearsFailed = 0;
    for (let year = startYear; year <= now.getUTCFullYear(); year++) {
      await new Promise(r => setImmediate(r));
      try {
        const result = await getHistoricalRates({ instrument, dates: { from: new Date(Date.UTC(year, 0, 1)), to: year === now.getUTCFullYear() ? now : new Date(Date.UTC(year + 1, 0, 1)) }, timeframe: 'h1', format: 'object', flushDownloadProgress: false });
        const raw = Array.isArray(result) ? result : [];
        let prev = null, kept = 0;
        for (const row of raw) {
          const open = (row.open || row.askOpen || 0) + adjustment, high = (row.high || row.askHigh || 0) + adjustment, low = (row.low || row.askLow || 0) + adjustment, close = (row.close || row.askClose || 0) + adjustment;
          if (!open || !close) continue;
          if (prev !== null && Math.abs(close - prev) / prev > 0.20) continue;
          const ts = row.timestamp ? new Date(row.timestamp).toISOString().slice(0, 19).replace('T', ' ') : null;
          if (!ts) continue;
          pairCandles.push({ datetime: ts, open, high, low, close }); prev = close; kept++;
        }
        console.log(`[backfill] ${pair} / ${year}: ${kept} candles kept.`);
      } catch (err) { yearsFailed++; if (yearsFailed >= 3) break; }
      await new Promise(r => setTimeout(r, 500));
    }
    if (pairCandles.length === 0) { progress[pair] = 'incomplete'; try { fs.writeFileSync(BACKFILL_PROGRESS_PATH, JSON.stringify(progress), 'utf8'); } catch (_) {} continue; }
    if (!candleStore.pairs[pair]) candleStore.pairs[pair] = [];
    const mapByDt = {};
    for (const c of candleStore.pairs[pair]) mapByDt[c.datetime] = c;
    for (const c of pairCandles) mapByDt[c.datetime] = c;
    const merged = Object.values(mapByDt).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
    candleStore.pairs[pair] = merged; backfillTotalCandles += pairCandles.length;
    historyCache.data.pairs[pair] = merged.slice(-5000);
    try { fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8'); } catch (_) {}
    progress[pair] = 'complete'; backfillPairsComplete++;
    try { fs.writeFileSync(BACKFILL_PROGRESS_PATH, JSON.stringify(progress), 'utf8'); } catch (_) {}
  }
  backfillStatus = FOREX_PAIRS.every(p => progress[p] === 'complete') ? 'complete' : 'error';
}

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
  intelligenceProfile = profile; intelligenceStatus = 'active'; intelligenceLastCalculated = profile._calculatedAt;
  try { fs.writeFileSync(INTELLIGENCE_PROFILE_PATH, JSON.stringify(profile), 'utf8'); } catch (err) { console.error('[intelligence] Failed to persist profile:', err.message); }
}

async function runSMCEvaluation() {
  try { const dxyData = await smcEngine.fetchDXYData(); currentDXYBias = smcEngine.calculateDXYTrend(dxyData); dxyLastFetchedAt = new Date().toISOString(); } catch (err) { console.error('[SMC] DXY refresh failed:', err.message); }
  const killzone = smcEngine.isInsideKillzone();
  for (const pair of FOREX_PAIRS) {
    try {
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
      const rawConfidence = candidateDirection === 'BUY' ? Math.min(100, Math.round(60 + (oversold - lastRSI) * 2)) : Math.min(100, Math.round(60 + (lastRSI - overbought) * 2));
      const adjustedConfidence = Math.max(0, rawConfidence - dxyPenalty);
      const signalTypeKey = smcEngine.computeSignalTypeKey({ orderBlockPresent: trinity.orderBlockPresent, fvgPresent: trinity.fvgPresent, liquiditySweepPresent: trinity.liquiditySweepPresent });
      const classification = smcEngine.classifySignal(trinity, true, killzone);
      const now = new Date();
      const signalBase = { pair, direction: candidateDirection, confidence: adjustedConfidence, orderBlockPresent: trinity.orderBlockPresent, fvgPresent: trinity.fvgPresent, liquiditySweepPresent: trinity.liquiditySweepPresent, killzoneActive: killzone.active, killzoneName: killzone.killzoneName, dxyBias: currentDXYBias, obZone: trinity.obZone, fvgZone: trinity.fvgZone, ensembleScore: null, signalTypeKey, generatedAt: now.toISOString() };
      let ensembleScore = null, highConviction = false;
      if (classification === 'LIVE' || classification === 'STANDARD') {
        try { const features = ensembleScorer.extractFeatures(signalBase, null); const score = await ensembleScorer.scoreSignal(features); if (score !== null) { ensembleScore = score; highConviction = score >= 75; } } catch (_) {}
      }
      if (classification === 'LIVE') {
        if (ensembleScore !== null && ensembleScore < 40) { const ps = { ...signalBase, ensembleScore, isPaper: true, paperReason: 'ENSEMBLE_LOW_SCORE' }; if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift(); paperSignalsBuffer.push(ps); }
        else { lastHolyTrinityAt[pair] = now; console.log(`[SMC] LIVE signal: ${pair} ${candidateDirection} (confidence ${adjustedConfidence}, ensemble ${ensembleScore !== null ? ensembleScore : 'N/A'})`); }
      } else if (classification === 'PAPER_OUTSIDE_KILLZONE' || classification === 'PAPER_INDICATOR_FAILED') {
        const ps = { ...signalBase, ensembleScore: null, isPaper: true, paperReason: classification }; if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift(); paperSignalsBuffer.push(ps);
      } else if (classification === 'STANDARD') {
        if (ensembleScore !== null && ensembleScore < 40) { const ps = { ...signalBase, confidence: Math.min(70, adjustedConfidence), ensembleScore, isPaper: true, paperReason: 'ENSEMBLE_LOW_SCORE' }; if (paperSignalsBuffer.length >= PAPER_SIGNALS_MAX) paperSignalsBuffer.shift(); paperSignalsBuffer.push(ps); }
        else { console.log(`[SMC] STANDARD signal: ${pair} ${candidateDirection} (confidence ${Math.min(70, adjustedConfidence)})`); }
      }
      const holyTrinityAge = lastHolyTrinityAt[pair] ? (now - lastHolyTrinityAt[pair]) / 1000 / 3600 : Infinity;
      if (holyTrinityAge > 24 && classification !== 'LIVE' && adjustedConfidence >= 50) {
        console.log(`[SMC] FALLBACK signal: ${pair} ${candidateDirection} (24h without Holy Trinity)`);
      }
    } catch (err) { console.error(`[SMC] Evaluation error for ${pair}:`, err.message); }
  }
}

app.get('/ensemble-status', (req, res) => { cors(res); res.json(ensembleScorer.getEnsembleStatus()); });
app.get('/paper-signals', (req, res) => { cors(res); const limit = req.query.limit ? parseInt(req.query.limit, 10) : paperSignalsBuffer.length; const safe = isNaN(limit) ? paperSignalsBuffer.length : Math.min(limit, paperSignalsBuffer.length); res.json({ count: paperSignalsBuffer.length, signals: paperSignalsBuffer.slice(-safe) }); });
app.get('/smc-status', (req, res) => { cors(res); try { const killzone = smcEngine.isInsideKillzone(); const activePairs = {}; for (const pair of FOREX_PAIRS) { const candles = candleStore.pairs[pair]; if (!candles || candles.length < 50) { activePairs[pair] = { activeOBCount: 0, activeFVGCount: 0, lastSweepAge: null, premiumDiscount: 'UNKNOWN' }; continue; } const obs = smcEngine.detectOrderBlocks(candles, 'BUY').length + smcEngine.detectOrderBlocks(candles, 'SELL').length; const fvgs = smcEngine.detectFairValueGaps(candles, 'BUY').length + smcEngine.detectFairValueGaps(candles, 'SELL').length; const sweep = smcEngine.detectLiquiditySweep(candles); const pdZone = smcEngine.getPremiumDiscountZone(candles); activePairs[pair] = { activeOBCount: obs, activeFVGCount: fvgs, lastSweepAge: sweep.sweepCandleIndex !== null ? candles.length - 1 - sweep.sweepCandleIndex : null, premiumDiscount: pdZone.isPremium ? 'PREMIUM' : pdZone.isDiscount ? 'DISCOUNT' : 'NEUTRAL' }; } res.json({ dxyBias: currentDXYBias, dxyLastFetched: dxyLastFetchedAt, killzoneActive: killzone.active, killzoneName: killzone.killzoneName, activePairs }); } catch (err) { res.status(500).json({ error: 'SMC status unavailable' }); } });
app.get('/rates', (req, res) => { cors(res); res.json(ratesCache.data); });
app.get('/history', (req, res) => { cors(res); res.json(historyCache.data); });
app.get('/stored-history', (req, res) => { cors(res); const limit = req.query.limit ? parseInt(req.query.limit, 10) : null; if (!limit || isNaN(limit)) return res.json(candleStore); const limited = { pairs: {} }; for (const pair of Object.keys(candleStore.pairs)) limited.pairs[pair] = candleStore.pairs[pair].slice(-limit); return res.json(limited); });
app.get('/calendar', (req, res) => { cors(res); res.json({ events: calendarCache.data, fetchedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null, count: calendarCache.data.length }); });
app.get('/intelligence', (req, res) => { cors(res); if (!intelligenceProfile || intelligenceStatus === 'calculating' || intelligenceStatus === 'pending') return res.json({ status: 'calculating', pairsComplete: backfillPairsComplete, pairsTotal: FOREX_PAIRS.length }); res.json({ status: 'active', profile: intelligenceProfile, fetchedAt: intelligenceLastCalculated, profileVersion: 1 }); });
app.get('/status', (req, res) => { cors(res); const storeCounts = {}; for (const pair of FOREX_PAIRS) storeCounts[pair] = (candleStore.pairs[pair] || []).length; res.json({ callsToday: dailyCallCount, limit: 800, resetAt: new Date(callCountResetAt).toUTCString(), candlePairsStored: Object.keys(candleStore.pairs).length, candleCountPerPair: storeCounts, calendarEvents: calendarCache.data.length, backfillStatus, backfillPairsComplete, backfillPairsTotal: FOREX_PAIRS.length, backfillTotalCandles, intelligenceStatus, intelligenceLastCalculated, lastCollectionError: lastCollectionError || null }); });
app.get('/health', (req, res) => { cors(res); res.json({ status: 'ok', uptime: process.uptime(), ratesCachedAt: ratesCache.fetchedAt ? new Date(ratesCache.fetchedAt).toISOString() : null, historyCachedAt: historyCache.fetchedAt ? new Date(historyCache.fetchedAt).toISOString() : null, intelligenceStatus }); });

app.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] ForexMind proxy running on port ${PORT}`);
  const runCollectionWithSMC = async () => { await runCollection(); runSMCEvaluation().catch(err => console.error('[SMC] Post-collection error:', err.message)); };
  runCollectionWithSMC(); setInterval(runCollectionWithSMC, 15 * 60 * 1000);
  smcEngine.fetchDXYData().then(data => { currentDXYBias = smcEngine.calculateDXYTrend(data); dxyLastFetchedAt = new Date().toISOString(); console.log(`[DXY] Initial trend: ${currentDXYBias}`); }).catch(err => console.warn('[DXY] Initial fetch failed:', err.message));
  fetchCalendar(); setInterval(fetchCalendar, CALENDAR_CACHE_TTL);
  setImmediate(() => runDukascopyBackfill());
  setTimeout(() => calculateIntelligenceProfile(), 2 * 60 * 1000); setInterval(() => calculateIntelligenceProfile(), 7 * 24 * 60 * 60 * 1000);
  setTimeout(async () => {
    const CANISTER_HOST = process.env.CANISTER_HOST || null, CANISTER_ID = process.env.CANISTER_ID || null;
    if (!CANISTER_HOST || !CANISTER_ID) { console.log('[Ensemble] CANISTER_HOST or CANISTER_ID not set. Skipping training fetch.'); return; }
    const allSignals = []; let page = 0, done = false;
    while (!done) { try { const resp = await axios.post(`${CANISTER_HOST}/api/v1/query`, { canisterId: CANISTER_ID, method: 'getSignalPage', args: ['ALL', page, 100] }, { timeout: 15000 }); const signals = (resp.data && Array.isArray(resp.data.result)) ? resp.data.result : []; if (signals.length === 0) { done = true; break; } for (const s of signals) { if (s.outcome && s.outcome !== 'Pending') allSignals.push({ features: ensembleScorer.extractFeatures(s, null), outcome: s.outcome === 'Win' }); } if (signals.length < 100) { done = true; break; } page++; } catch (err) { done = true; } }
    console.log(`[Ensemble] Fetched ${allSignals.length} resolved signals.`); ensembleScorer.initEnsembleScorer(allSignals);
  }, 30 * 1000);
});
