'use strict';

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.TWELVEDATA_API_KEY;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

const FOREX_PAIRS = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'AUD/USD', 'USD/CAD', 'NZD/USD'];

const BID_TO_MID = {
  'EUR/USD': 0.00015,
  'GBP/USD': 0.00025,
  'USD/JPY': 0.015,
  'USD/CHF': 0.00020,
  'AUD/USD': 0.00020,
  'USD/CAD': 0.00025,
  'NZD/USD': 0.00025
};

const CANDLE_STORE_PATH = '/tmp/candle-store.json';
const BACKFILL_PROGRESS_PATH = '/tmp/backfill-progress.json';
const INTELLIGENCE_PROFILE_PATH = '/tmp/intelligence-profile.json';
const CALENDAR_CACHE_TTL = 60 * 60 * 1000; // 1 hour
const INTELLIGENCE_RECALC_INTERVAL = 30 * 24 * 60 * 60 * 1000; // 30 days

// --- In-memory state ---
let ratesCache = { data: { pairs: {} }, fetchedAt: 0 };
let historyCache = { data: { pairs: {} }, fetchedAt: 0 };
let candleStore = { pairs: {} };
let calendarCache = { data: [], fetchedAt: 0 };
let intelligenceProfile = null;
let intelligenceStatus = { status: 'pending', pairsComplete: 0, pairsTotal: FOREX_PAIRS.length };
let backfillStatus = { status: 'pending', pairsComplete: 0, totalCandles: 0 };
let backfillProgress = {};
let dailyCallCount = 0;
let callCountResetAt = nextMidnightUTC();
let lastCollectionError = null;

function nextMidnightUTC() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);
}

function incrementCallCount() {
  if (Date.now() >= callCountResetAt) {
    dailyCallCount = 0;
    callCountResetAt = nextMidnightUTC();
  }
  dailyCallCount++;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function yieldToEventLoop() {
  return new Promise(r => setImmediate(r));
}

// --- Load persisted state on startup ---
try {
  if (fs.existsSync(CANDLE_STORE_PATH)) {
    candleStore = JSON.parse(fs.readFileSync(CANDLE_STORE_PATH, 'utf8'));
    console.log('[startup] Candle store loaded from disk.');
  }
} catch (err) {
  console.error('[startup] Failed to load candle store:', err.message);
  candleStore = { pairs: {} };
}

try {
  if (fs.existsSync(BACKFILL_PROGRESS_PATH)) {
    backfillProgress = JSON.parse(fs.readFileSync(BACKFILL_PROGRESS_PATH, 'utf8'));
    const completed = Object.keys(backfillProgress).filter(p => backfillProgress[p].complete).length;
    backfillStatus.pairsComplete = completed;
    if (completed === FOREX_PAIRS.length) backfillStatus.status = 'complete';
    console.log(`[startup] Backfill progress loaded: ${completed}/${FOREX_PAIRS.length} pairs complete.`);
  }
} catch (err) {
  console.error('[startup] Failed to load backfill progress:', err.message);
  backfillProgress = {};
}

try {
  if (fs.existsSync(INTELLIGENCE_PROFILE_PATH)) {
    intelligenceProfile = JSON.parse(fs.readFileSync(INTELLIGENCE_PROFILE_PATH, 'utf8'));
    intelligenceStatus = { status: 'active', pairsComplete: FOREX_PAIRS.length, pairsTotal: FOREX_PAIRS.length };
    console.log('[startup] Intelligence profile loaded from disk.');
  }
} catch (err) {
  console.error('[startup] Failed to load intelligence profile:', err.message);
  intelligenceProfile = null;
}

// --- Twelve Data collection ---
async function runCollection() {
  console.log(`[${new Date().toISOString()}] Collection cycle starting...`);

  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await sleep(2000);
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
      console.error(`[collection] Rate fetch failed for ${pair}:`, err.message);
    }
  }

  await sleep(2000);

  for (let i = 0; i < FOREX_PAIRS.length; i++) {
    const pair = FOREX_PAIRS[i];
    if (i > 0) await sleep(2000);
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
      candleStore.pairs[pair] = merged;
      historyCache.data.pairs[pair] = merged.slice(-5000);
      historyCache.fetchedAt = Date.now();
      incrementCallCount();
    } catch (err) {
      lastCollectionError = `${pair} history: ${err.message}`;
      console.error(`[collection] History fetch failed for ${pair}:`, err.message);
    }
  }

  try {
    fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8');
    lastCollectionError = null;
    console.log(`[${new Date().toISOString()}] Collection complete. Daily calls: ${dailyCallCount}`);
  } catch (err) {
    lastCollectionError = `File write failed: ${err.message}`;
    console.error('[collection] Failed to write candle store:', err.message);
  }
}

// --- Economic calendar ---
async function fetchCalendar() {
  const now = new Date();
  const from = now.toISOString().split('T')[0];
  const to = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  const RELEVANT_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD'];

  if (FINNHUB_API_KEY) {
    try {
      const response = await axios.get('https://finnhub.io/api/v1/calendar/economic', {
        params: { from, to, token: FINNHUB_API_KEY }
      });
      const events = (response.data.economicCalendar || [])
        .filter(e => {
          const impact = (e.impact || '').toLowerCase();
          const currency = (e.country || '').toUpperCase();
          return impact === 'high' && RELEVANT_CURRENCIES.includes(currency);
        })
        .map(e => ({
          time: e.time,
          currency: e.country,
          event: e.event,
          impact: e.impact,
          forecast: e.estimate || null,
          previous: e.prev || null
        }));
      calendarCache = { data: events, fetchedAt: Date.now() };
      console.log(`[calendar] Finnhub: ${events.length} high-impact events.`);
      return;
    } catch (err) {
      console.error('[calendar] Finnhub failed:', err.message);
    }
  }

  try {
    const rssResponse = await axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.xml', { timeout: 10000 });
    const xml = rssResponse.data;
    const events = [];
    const itemRegex = /<item>([\s\S]*?)<\/item>/g;
    let match;
    while ((match = itemRegex.exec(xml)) !== null) {
      const item = match[1];
      const title = (item.match(/<title>(.*?)<\/title>/) || [])[1] || '';
      const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/) || [])[1] || '';
      const impact = (item.match(/<impact>(.*?)<\/impact>/) || [])[1] || '';
      const currency = (item.match(/<country>(.*?)<\/country>/) || [])[1] || '';
      if (impact.toLowerCase() === 'high' && RELEVANT_CURRENCIES.includes(currency.toUpperCase())) {
        events.push({ time: pubDate, currency: currency.toUpperCase(), event: title, impact: 'high', forecast: null, previous: null });
      }
    }
    calendarCache = { data: events, fetchedAt: Date.now() };
    console.log(`[calendar] ForexFactory RSS fallback: ${events.length} high-impact events.`);
  } catch (err) {
    console.error('[calendar] ForexFactory RSS fallback failed:', err.message);
  }
}

// --- Dukascopy backfill ---
async function runDukascopyBackfill() {
  let dukascopy;
  try {
    dukascopy = require('dukascopy-node');
  } catch (err) {
    console.error('[backfill] dukascopy-node not available:', err.message);
    return;
  }

  const { getHistoricalRates } = dukascopy;
  backfillStatus.status = 'running';
  const now = new Date();

  for (const pair of FOREX_PAIRS) {
    if (backfillProgress[pair] && backfillProgress[pair].complete) {
      console.log(`[backfill] ${pair} already complete, skipping.`);
      backfillStatus.pairsComplete++;
      continue;
    }

    console.log(`[backfill] Starting ${pair}...`);
    const instrument = pair.replace('/', '').toLowerCase();
    let pairCandles = candleStore.pairs[pair] ? [...candleStore.pairs[pair]] : [];
    const existingMap = {};
    for (const c of pairCandles) existingMap[c.datetime] = c;

    for (let yearsBack = 10; yearsBack >= 1; yearsBack--) {
      const yearStart = new Date(Date.UTC(now.getUTCFullYear() - yearsBack, 0, 1));
      const yearEnd = new Date(Date.UTC(now.getUTCFullYear() - yearsBack + 1, 0, 1));
      if (yearEnd > now) yearEnd.setTime(now.getTime());

      try {
        console.log(`[backfill] ${pair} fetching year ${now.getUTCFullYear() - yearsBack}...`);
        const data = await getHistoricalRates({
          instrument,
          dates: { from: yearStart, to: yearEnd },
          timeframe: '1h',
          format: 'object',
          retryCount: 3,
          retryOnEmpty: true,
          failAfterRetryCount: false
        });

        const offset = BID_TO_MID[pair] || 0;
        let prevClose = null;
        let yearCount = 0;

        for (const row of (data || [])) {
          const dt = new Date(row.timestamp);
          const datetime = dt.toISOString().replace('T', ' ').substring(0, 16);
          const open = parseFloat(row.open) + offset;
          const high = parseFloat(row.high) + offset;
          const low = parseFloat(row.low) + offset;
          const close = parseFloat(row.close) + offset;

          if (isNaN(open) || isNaN(high) || isNaN(low) || isNaN(close)) continue;
          if (prevClose !== null && Math.abs(close - prevClose) / prevClose > 0.20) {
            console.warn(`[backfill] ${pair} outlier rejected at ${datetime}: close=${close}, prev=${prevClose}`);
            prevClose = close;
            continue;
          }

          existingMap[datetime] = { datetime, open, high, low, close };
          prevClose = close;
          yearCount++;
        }

        console.log(`[backfill] ${pair} year ${now.getUTCFullYear() - yearsBack}: ${yearCount} candles added.`);
        backfillStatus.totalCandles += yearCount;
        await yieldToEventLoop();
      } catch (err) {
        console.error(`[backfill] ${pair} year ${now.getUTCFullYear() - yearsBack} failed:`, err.message);
      }

      await sleep(500);
    }

    const merged = Object.values(existingMap).sort((a, b) => a.datetime < b.datetime ? -1 : 1);
    candleStore.pairs[pair] = merged;

    try {
      fs.writeFileSync(CANDLE_STORE_PATH, JSON.stringify(candleStore), 'utf8');
    } catch (err) {
      console.error('[backfill] Failed to write candle store after pair:', err.message);
    }

    backfillProgress[pair] = { complete: true, candles: merged.length, completedAt: new Date().toISOString() };
    try {
      fs.writeFileSync(BACKFILL_PROGRESS_PATH, JSON.stringify(backfillProgress), 'utf8');
    } catch (err) {
      console.error('[backfill] Failed to write progress checkpoint:', err.message);
    }

    backfillStatus.pairsComplete++;
    console.log(`[backfill] ${pair} complete: ${merged.length} total candles. (${backfillStatus.pairsComplete}/${FOREX_PAIRS.length})`);
    await sleep(2000);
  }

  backfillStatus.status = 'complete';
  console.log(`[backfill] All pairs complete. Total candles: ${backfillStatus.totalCandles}`);

  // Trigger intelligence profile calculation after backfill
  setImmediate(() => calculateIntelligenceProfile());
}

// --- Intelligence profile calculation (non-blocking, chunked) ---
async function calculateIntelligenceProfile() {
  console.log('[intelligence] Starting profile calculation...');
  intelligenceStatus = { status: 'calculating', pairsComplete: 0, pairsTotal: FOREX_PAIRS.length };

  const profile = {};

  for (const pair of FOREX_PAIRS) {
    const candles = candleStore.pairs[pair] || [];
    if (candles.length < 500) {
      console.warn(`[intelligence] ${pair}: insufficient candles (${candles.length}), skipping.`);
      intelligenceStatus.pairsComplete++;
      continue;
    }

    await yieldToEventLoop();
    console.log(`[intelligence] Calculating ${pair} (${candles.length} candles)...`);

    // --- ATR percentile table ---
    const atrs = [];
    for (let i = 14; i < candles.length; i++) {
      await (i % 500 === 0 ? yieldToEventLoop() : Promise.resolve());
      const slice = candles.slice(i - 14, i + 1);
      let atrSum = 0;
      for (let j = 1; j < slice.length; j++) {
        const tr = Math.max(
          slice[j].high - slice[j].low,
          Math.abs(slice[j].high - slice[j - 1].close),
          Math.abs(slice[j].low - slice[j - 1].close)
        );
        atrSum += tr;
      }
      atrs.push(atrSum / 14);
    }
    atrs.sort((a, b) => a - b);
    const pct = (arr, p) => arr[Math.floor(arr.length * p / 100)];
    const atrPercentiles = {
      p25: pct(atrs, 25),
      p50: pct(atrs, 50),
      p75: pct(atrs, 75),
      p95: pct(atrs, 95)
    };

    // --- Current volatility regime ---
    const recentCandles = candles.slice(-15);
    let recentATR = 0;
    for (let j = 1; j < recentCandles.length; j++) {
      recentATR += Math.max(
        recentCandles[j].high - recentCandles[j].low,
        Math.abs(recentCandles[j].high - recentCandles[j - 1].close),
        Math.abs(recentCandles[j].low - recentCandles[j - 1].close)
      );
    }
    recentATR = recentATR / 14;
    let volatilityRegime = 'normal';
    if (recentATR < atrPercentiles.p25) volatilityRegime = 'low';
    else if (recentATR > atrPercentiles.p95) volatilityRegime = 'extreme';
    else if (recentATR > atrPercentiles.p75) volatilityRegime = 'high';

    await yieldToEventLoop();

    // --- Adaptive RSI thresholds (rolling walk-forward) ---
    const WINDOW_TRAIN = 3 * 365 * 24;
    const WINDOW_TEST = 365 * 24;
    const thresholdCandidates = [];
    for (let os = 25; os <= 45; os += 2.5) {
      for (let ob = 55; ob <= 75; ob += 2.5) {
        thresholdCandidates.push({ oversold: os, overbought: ob });
      }
    }

    let bestOversold = 30, bestOverbought = 70;
    let bestWFER = 0;

    for (const candidate of thresholdCandidates) {
      await yieldToEventLoop();
      const wferScores = [];
      let windowStart = 0;
      while (windowStart + WINDOW_TRAIN + WINDOW_TEST <= candles.length) {
        const trainSlice = candles.slice(windowStart, windowStart + WINDOW_TRAIN);
        const testSlice = candles.slice(windowStart + WINDOW_TRAIN, windowStart + WINDOW_TRAIN + WINDOW_TEST);
        const inSampleEdge = evaluateRSIThreshold(trainSlice, candidate.oversold, candidate.overbought);
        const outSampleEdge = evaluateRSIThreshold(testSlice, candidate.oversold, candidate.overbought);
        if (inSampleEdge > 0) wferScores.push(outSampleEdge / inSampleEdge);
        windowStart += WINDOW_TEST;
      }
      if (wferScores.length > 0) {
        const avgWFER = wferScores.reduce((a, b) => a + b, 0) / wferScores.length;
        if (avgWFER >= 0.5 && avgWFER > bestWFER) {
          bestWFER = avgWFER;
          bestOversold = candidate.oversold;
          bestOverbought = candidate.overbought;
        }
      }
    }

    await yieldToEventLoop();

    // --- Session win rate matrix ---
    const sessions = {
      tokyo:    { hours: [0, 1, 2, 3, 4, 5, 6, 7], wins: [], weightedWins: 0, weightedTotal: 0 },
      london:   { hours: [7, 8, 9, 10, 11, 12, 13, 14, 15], wins: [], weightedWins: 0, weightedTotal: 0 },
      newYork:  { hours: [13, 14, 15, 16, 17, 18, 19, 20], wins: [], weightedWins: 0, weightedTotal: 0 },
      londonNY: { hours: [13, 14, 15, 16], wins: [], weightedWins: 0, weightedTotal: 0 },
      sydney:   { hours: [21, 22, 23, 0], wins: [], weightedWins: 0, weightedTotal: 0 }
    };

    const now = new Date();
    for (let i = 20; i < candles.length - 4; i++) {
      await (i % 1000 === 0 ? yieldToEventLoop() : Promise.resolve());
      const c = candles[i];
      const dt = new Date(c.datetime.replace(' ', 'T') + ':00Z');
      const hour = dt.getUTCHours();
      const monthsAgo = (now.getFullYear() - dt.getFullYear()) * 12 + (now.getMonth() - dt.getMonth());
      const weight = Math.pow(0.85, monthsAgo);
      const atr = atrs[i - 14] || atrPercentiles.p50;
      const direction = c.close > c.open ? 1 : -1;
      let win = false;
      for (let k = 1; k <= 4; k++) {
        const future = candles[i + k];
        if (direction === 1 && future.close - c.close >= atr) { win = true; break; }
        if (direction === -1 && c.close - future.close >= atr) { win = true; break; }
      }
      for (const [sessionName, session] of Object.entries(sessions)) {
        if (session.hours.includes(hour)) {
          session.weightedWins += win ? weight : 0;
          session.weightedTotal += weight;
        }
      }
    }

    const sessionMatrix = {};
    const crossPairAvg = 0.58;
    for (const [sessionName, session] of Object.entries(sessions)) {
      const sampleSize = Math.round(session.weightedTotal);
      if (sampleSize < 100) {
        sessionMatrix[sessionName] = { winRate: crossPairAvg, sampleSize, confidence: 'insufficient' };
      } else {
        const winRate = session.weightedTotal > 0 ? session.weightedWins / session.weightedTotal : crossPairAvg;
        const confidence = sampleSize >= 500 ? 'high' : sampleSize >= 200 ? 'medium' : 'low';
        sessionMatrix[sessionName] = { winRate: Math.round(winRate * 1000) / 1000, sampleSize, confidence };
      }
    }

    await yieldToEventLoop();

    // --- Seasonal bias ---
    const monthBuckets = {};
    for (let m = 1; m <= 12; m++) monthBuckets[m] = { weightedSum: 0, weightedCount: 0 };

    for (let i = 1; i < candles.length; i++) {
      await (i % 2000 === 0 ? yieldToEventLoop() : Promise.resolve());
      const dt = new Date(candles[i].datetime.replace(' ', 'T') + ':00Z');
      const month = dt.getUTCMonth() + 1;
      const monthsAgo = (now.getFullYear() - dt.getFullYear()) * 12 + (now.getMonth() - dt.getMonth());
      const weight = Math.pow(0.85, monthsAgo);
      const followThrough = candles[i].close > candles[i - 1].close ? 1 : 0;
      monthBuckets[month].weightedSum += followThrough * weight;
      monthBuckets[month].weightedCount += weight;
    }

    const baselineAvg = Object.values(monthBuckets).reduce((sum, b) => sum + (b.weightedCount > 0 ? b.weightedSum / b.weightedCount : 0.5), 0) / 12;
    const seasonalBias = {};
    for (let m = 1; m <= 12; m++) {
      const avg = monthBuckets[m].weightedCount > 0 ? monthBuckets[m].weightedSum / monthBuckets[m].weightedCount : 0.5;
      seasonalBias[m] = Math.round((avg / baselineAvg) * 100) / 100;
    }

    profile[pair] = {
      adaptiveRSI: { oversold: bestOversold, overbought: bestOverbought },
      atrPercentiles,
      sessionMatrix,
      seasonalBias,
      volatilityRegime,
      profileVersion: 1,
      intelligenceProfileVersion: 1,
      lastCalculated: new Date().toISOString(),
      status: 'active'
    };

    intelligenceStatus.pairsComplete++;
    console.log(`[intelligence] ${pair} complete. RSI ${bestOversold}/${bestOverbought}, regime: ${volatilityRegime}`);
    await sleep(100);
  }

  intelligenceProfile = profile;
  intelligenceStatus = { status: 'active', pairsComplete: FOREX_PAIRS.length, pairsTotal: FOREX_PAIRS.length };

  try {
    fs.writeFileSync(INTELLIGENCE_PROFILE_PATH, JSON.stringify(profile), 'utf8');
    console.log('[intelligence] Profile saved to disk.');
  } catch (err) {
    console.error('[intelligence] Failed to save profile:', err.message);
  }
}

function evaluateRSIThreshold(candles, oversold, overbought) {
  let wins = 0, total = 0;
  const period = 14;
  for (let i = period + 1; i < candles.length - 4; i++) {
    let gains = 0, losses = 0;
    for (let j = i - period; j < i; j++) {
      const diff = candles[j + 1].close - candles[j].close;
      if (diff > 0) gains += diff; else losses -= diff;
    }
    const rs = losses === 0 ? 100 : gains / losses;
    const rsi = 100 - (100 / (1 + rs));
    if (rsi <= oversold || rsi >= overbought) {
      const direction = rsi <= oversold ? 1 : -1;
      let win = false;
      for (let k = 1; k <= 4; k++) {
        const diff = candles[i + k].close - candles[i].close;
        if (direction === 1 && diff > 0) { win = true; break; }
        if (direction === -1 && diff < 0) { win = true; break; }
      }
      if (win) wins++;
      total++;
    }
  }
  return total > 0 ? wins / total : 0;
}

// --- Startup sequence ---
app.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] Forex proxy running on port ${PORT}`);

  // Start Twelve Data collection immediately, then every 15 minutes
  runCollection();
  setInterval(runCollection, 15 * 60 * 1000);

  // Start economic calendar fetch, then every hour
  fetchCalendar();
  setInterval(fetchCalendar, CALENDAR_CACHE_TTL);

  // Start Dukascopy backfill in background after server is live
  setImmediate(() => {
    if (backfillStatus.status !== 'complete') {
      runDukascopyBackfill();
    } else {
      console.log('[startup] Backfill already complete.');
      // Check if intelligence profile needs recalculation
      if (!intelligenceProfile) {
        setImmediate(() => calculateIntelligenceProfile());
      } else {
        const lastCalc = new Date(intelligenceProfile[FOREX_PAIRS[0]]?.lastCalculated || 0);
        if (Date.now() - lastCalc.getTime() > INTELLIGENCE_RECALC_INTERVAL) {
          console.log('[startup] Intelligence profile is stale, recalculating...');
          setImmediate(() => calculateIntelligenceProfile());
        }
      }
    }
  });

  // Recalculate intelligence profile every 30 days
  setInterval(() => {
    if (backfillStatus.status === 'complete') {
      console.log('[scheduler] Triggering scheduled intelligence profile recalculation.');
      setImmediate(() => calculateIntelligenceProfile());
    }
  }, INTELLIGENCE_RECALC_INTERVAL);
});

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
  for (const pair of FOREX_PAIRS) {
    if (candleStore.pairs[pair]) {
      limited.pairs[pair] = candleStore.pairs[pair].slice(-limit);
    }
  }
  return res.json(limited);
});

app.get('/calendar', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json({
    events: calendarCache.data,
    fetchedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    count: calendarCache.data.length
  });
});

app.get('/intelligence', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (intelligenceProfile && intelligenceStatus.status === 'active') {
    res.json({ status: 'active', profile: intelligenceProfile });
  } else {
    res.json({
      status: intelligenceStatus.status,
      pairsComplete: intelligenceStatus.pairsComplete,
      pairsTotal: intelligenceStatus.pairsTotal
    });
  }
});

app.get('/status', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const totalStored = Object.values(candleStore.pairs).reduce((sum, arr) => sum + arr.length, 0);
  res.json({
    callsToday: dailyCallCount,
    limit: 800,
    resetAt: new Date(callCountResetAt).toUTCString(),
    candlePairsStored: Object.keys(candleStore.pairs).length,
    totalCandlesStored: totalStored,
    calendarEvents: calendarCache.data.length,
    calendarFetchedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    backfillStatus: backfillStatus.status,
    backfillPairsComplete: backfillStatus.pairsComplete,
    backfillTotalCandles: backfillStatus.totalCandles,
    intelligenceProfileStatus: intelligenceStatus.status,
    intelligenceProfileLastCalculated: intelligenceProfile
      ? intelligenceProfile[FOREX_PAIRS[0]]?.lastCalculated || null
      : null,
    lastCollectionError: lastCollectionError || null
  });
});

app.get('/health', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    ratesCachedAt: ratesCache.fetchedAt ? new Date(ratesCache.fetchedAt).toISOString() : null,
    historyCachedAt: historyCache.fetchedAt ? new Date(historyCache.fetchedAt).toISOString() : null,
    calendarCachedAt: calendarCache.fetchedAt ? new Date(calendarCache.fetchedAt).toISOString() : null,
    backfillStatus: backfillStatus.status,
    intelligenceStatus: intelligenceStatus.status
  });
});
