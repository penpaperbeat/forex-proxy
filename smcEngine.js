'use strict';

// ---------------------------------------------------------------------------
// SMC Engine — Smart Money Concepts detection for ForexMind proxy
// Functions work on candle arrays sorted oldest-first:
//   [{ datetime, open, high, low, close }, ...]
// All functions handle edge cases gracefully (too few candles → safe defaults).
// ---------------------------------------------------------------------------

const axios = require('axios');

const MIN_CANDLES = 50;

// ---------------------------------------------------------------------------
// DST helpers for accurate killzone windows
// ---------------------------------------------------------------------------

function getNthSundayOfMonth(year, month, n) {
  let count = 0;
  for (let d = 1; d <= 31; d++) {
    const date = new Date(Date.UTC(year, month, d));
    if (date.getMonth() !== month) break; // overflowed into next month
    if (date.getDay() === 0) { count++; if (count === n) return date; }
  }
  return null;
}

function getLastSundayOfMonth(year, month) {
  for (let d = 31; d >= 1; d--) {
    const date = new Date(Date.UTC(year, month, d));
    if (date.getMonth() !== month) continue;
    if (date.getDay() === 0) return date;
  }
  return null;
}

function isUsDst(date) {
  const y     = date.getUTCFullYear();
  const start = getNthSundayOfMonth(y, 2, 2);  // 2nd Sunday of March
  const end   = getNthSundayOfMonth(y, 10, 1); // 1st Sunday of November
  return date >= start && date < end;
}

function isEuDst(date) {
  const y     = date.getUTCFullYear();
  const start = getLastSundayOfMonth(y, 2); // last Sunday of March
  const end   = getLastSundayOfMonth(y, 9); // last Sunday of October
  return date >= start && date < end;
}

// ---------------------------------------------------------------------------
// Shared sort helper — candles use datetime (YYYY-MM-DD HH:MM:SS string)
// ---------------------------------------------------------------------------

/**
 * ensureAscending(candles)
 * Returns a copy sorted oldest-first by datetime if out of order.
 * Uses string comparison — safe for fixed-format datetime strings.
 */
function ensureAscending(candles) {
  if (candles.length > 1 && candles[candles.length - 1].datetime < candles[0].datetime) {
    return candles.slice().sort((a, b) => a.datetime < b.datetime ? -1 : 1);
  }
  return candles;
}

// ---------------------------------------------------------------------------
// PART 1 — SMC DETECTION FUNCTIONS
// ---------------------------------------------------------------------------

/**
 * detectOrderBlocks(candles, direction)
 * Returns up to 10 active (unmitigated) Order Block objects for the given direction.
 * direction: 'BUY' | 'SELL'
 * Each OB: { low, high, midpoint, index, direction }
 */
function detectOrderBlocks(candles, direction) {
  try {
    if (!candles || candles.length < MIN_CANDLES) return [];

    // Limit to recent 500 candles for performance, then ensure ascending order
    // FIX Bug 1 & 2: work exclusively on workCandles; use datetime (not timestamp) for sort
    let workCandles = ensureAscending(candles.slice(-500));

    const obs = [];
    const len = workCandles.length;

    for (let i = 2; i < len; i++) {
      // FIX Bug 1: was candles[i] / candles[i-2] — must be workCandles
      const c     = workCandles[i];
      const prev2 = workCandles[i - 2];

      if (direction === 'BUY') {
        // Bullish impulsive move: leaves a bullish FVG (c.low > prev2.high)
        if (c.low <= prev2.high) continue;

        // Break of structure: close above highest high in the 10 candles before i
        const slice = workCandles.slice(Math.max(0, i - 10), i);
        if (slice.length === 0) continue;
        if (c.close <= Math.max(...slice.map(x => x.high))) continue;

        // Find the last bearish candle (close < open) in the 5 candles before i
        let obIdx = -1;
        for (let j = i - 1; j >= Math.max(0, i - 5); j--) {
          if (workCandles[j].close < workCandles[j].open) { obIdx = j; break; }
        }
        if (obIdx < 0) continue;

        obs.push({
          low:       workCandles[obIdx].low,
          high:      workCandles[obIdx].high,
          midpoint:  (workCandles[obIdx].low + workCandles[obIdx].high) / 2,
          index:     obIdx,
          direction: 'BUY'
        });

      } else {
        // Bearish impulsive move: leaves a bearish FVG (c.high < prev2.low)
        if (c.high >= prev2.low) continue;

        const slice = workCandles.slice(Math.max(0, i - 10), i);
        if (slice.length === 0) continue;
        if (c.close >= Math.min(...slice.map(x => x.low))) continue;

        // Find the last bullish candle (close > open) in the 5 candles before i
        let obIdx = -1;
        for (let j = i - 1; j >= Math.max(0, i - 5); j--) {
          if (workCandles[j].close > workCandles[j].open) { obIdx = j; break; }
        }
        if (obIdx < 0) continue;

        obs.push({
          low:       workCandles[obIdx].low,
          high:      workCandles[obIdx].high,
          midpoint:  (workCandles[obIdx].low + workCandles[obIdx].high) / 2,
          index:     obIdx,
          direction: 'SELL'
        });
      }
    }

    // Filter out mitigated OBs — O(n) via Set, not O(n^2)
    const mitigatedIndices = new Set();
    for (const ob of obs) {
      for (let k = ob.index + 1; k < len; k++) {
        if (direction === 'BUY'  && workCandles[k].close < ob.midpoint) { mitigatedIndices.add(ob.index); break; }
        if (direction === 'SELL' && workCandles[k].close > ob.midpoint) { mitigatedIndices.add(ob.index); break; }
      }
    }
    const active = obs.filter(ob => !mitigatedIndices.has(ob.index));

    // Return most recent 10, deduplicated by index
    const seen   = new Set();
    const deduped = [];
    for (let i = active.length - 1; i >= 0 && deduped.length < 10; i--) {
      const ob = active[i];
      if (!seen.has(ob.index)) { seen.add(ob.index); deduped.unshift(ob); }
    }
    return deduped;

  } catch (err) {
    console.error('[SMC] detectOrderBlocks error:', err.message);
    return [];
  }
}

/**
 * detectFairValueGaps(candles, direction)
 * Returns up to 20 active (unfilled) FVG objects for the given direction.
 * direction: 'BUY' | 'SELL'
 * Each FVG: { low, high, direction, index }
 */
function detectFairValueGaps(candles, direction) {
  try {
    if (!candles || candles.length < MIN_CANDLES) return [];

    // FIX Bug 2: use datetime for sort guard via ensureAscending
    let workCandles = ensureAscending(candles.slice(-500));
    const fvgs = [];
    const len  = workCandles.length;

    for (let i = 2; i < len; i++) {
      const c     = workCandles[i];
      const prev2 = workCandles[i - 2];

      if (direction === 'BUY') {
        // Bullish FVG: current low > two-candles-ago high
        if (c.low > prev2.high) {
          fvgs.push({ low: prev2.high, high: c.low, direction: 'BUY', index: i });
        }
      } else {
        // Bearish FVG: current high < two-candles-ago low
        if (c.high < prev2.low) {
          fvgs.push({ low: c.high, high: prev2.low, direction: 'SELL', index: i });
        }
      }
    }

    // FIX Bug 3: FVG is filled when price touches ANY part of the gap, not just the far edge
    const active = fvgs.filter(fvg => {
      for (let k = fvg.index + 1; k < len; k++) {
        const c = workCandles[k];
        if (fvg.direction === 'BUY'  && c.low  <= fvg.high) return false; // any touch fills it
        if (fvg.direction === 'SELL' && c.high >= fvg.low)  return false; // any touch fills it
      }
      return true;
    });

    // Return most recent 20
    return active.slice(-20);

  } catch (err) {
    console.error('[SMC] detectFairValueGaps error:', err.message);
    return [];
  }
}

/**
 * detectLiquiditySweep(candles)
 * Returns { bullishSweep, bearishSweep, sweepCandleIndex }
 * Examines only the LAST candle for a sweep of the prior 5 candles' range.
 */
function detectLiquiditySweep(candles) {
  const result = { bullishSweep: false, bearishSweep: false, sweepCandleIndex: null };
  try {
    if (!candles || candles.length < MIN_CANDLES) return result;

    // FIX Bug 2 & 6: create a copy and use datetime for sort guard
    let workCandles = ensureAscending(candles.slice());

    const len     = workCandles.length;
    const last    = workCandles[len - 1];
    const lookback = Math.min(5, len - 1);
    const prev    = workCandles.slice(len - 1 - lookback, len - 1);
    if (prev.length === 0) return result;

    const lowestLow   = Math.min(...prev.map(c => c.low));
    const highestHigh = Math.max(...prev.map(c => c.high));

    // Bullish sweep: last candle wicks below the lowest low but closes above it
    if (last.low < lowestLow && last.close > lowestLow) {
      result.bullishSweep     = true;
      result.sweepCandleIndex = len - 1;
    }

    // Bearish sweep: last candle wicks above the highest high but closes below it
    if (last.high > highestHigh && last.close < highestHigh) {
      result.bearishSweep     = true;
      result.sweepCandleIndex = len - 1;
    }

  } catch (err) {
    console.error('[SMC] detectLiquiditySweep error:', err.message);
  }
  return result;
}

/**
 * getPremiumDiscountZone(candles)
 * Returns { equilibrium, isPremium, isDiscount, isNeutral }
 * Based on the last 50 candles dealing range.
 */
function getPremiumDiscountZone(candles) {
  const fallback = { equilibrium: 0, isPremium: false, isDiscount: false, isNeutral: true };
  try {
    if (!candles || candles.length < MIN_CANDLES) return fallback;

    const window      = candles.slice(-50);
    const highestHigh = Math.max(...window.map(c => c.high));
    const lowestLow   = Math.min(...window.map(c => c.low));
    const range       = highestHigh - lowestLow;
    if (range === 0) return fallback;

    const equilibrium  = (highestHigh + lowestLow) / 2;
    // Neutral band = ±20% of range (intentionally wide to reduce false signals)
    const neutralBand  = range * 0.20;
    const currentPrice = candles[candles.length - 1].close;

    const isNeutral  = currentPrice >= (equilibrium - neutralBand) && currentPrice <= (equilibrium + neutralBand);
    const isDiscount = currentPrice <  (equilibrium - neutralBand);
    const isPremium  = currentPrice >  (equilibrium + neutralBand);

    return { equilibrium, isPremium, isDiscount, isNeutral };

  } catch (err) {
    console.error('[SMC] getPremiumDiscountZone error:', err.message);
    return fallback;
  }
}

/**
 * evaluateHolyTrinity(candles, direction, obList, fvgList, sweepResult, pdZone)
 * Returns { holyTrinityPassed, orderBlockPresent, fvgPresent, liquiditySweepPresent,
 *           obZone, fvgZone, failReason }
 */
function evaluateHolyTrinity(candles, direction, obList, fvgList, sweepResult, pdZone) {
  const fail = (reason, ob = false, fvg = false, sweep = false) => ({
    holyTrinityPassed:     false,
    orderBlockPresent:     ob,
    fvgPresent:            fvg,
    liquiditySweepPresent: sweep,
    obZone:                null,
    fvgZone:               null,
    failReason:            reason
  });

  try {
    if (!candles || candles.length < MIN_CANDLES) return fail('insufficient candles');

    const currentPrice = candles[candles.length - 1].close;

    // Neutral zone is the FIRST gate — before direction-specific checks
    if (pdZone.isNeutral) return fail('price in neutral zone');

    // Premium/discount filter
    if (direction === 'BUY'  && !pdZone.isDiscount) return fail('price not in discount zone');
    if (direction === 'SELL' && !pdZone.isPremium)  return fail('price not in premium zone');

    // Gate 1 — Price inside an active Order Block
    const matchedOB = obList.find(ob => currentPrice >= ob.low && currentPrice <= ob.high);
    if (!matchedOB) return fail('price not inside any active OB', false, false, false);

    // Pip size — JPY pairs price ~100–200, all others < 10.
    // Safe for all 7 current pairs; latent risk only if exotic pairs (10–99) added later.
    const pipSize   = matchedOB.midpoint > 10 ? 0.01 : 0.0001;
    const pipBuffer = 20 * pipSize;

    // Gate 2 — Unfilled FVG within 20 pips of the matched OB zone
    const matchedFVG = fvgList.find(fvg => {
      const fvgMid = (fvg.low + fvg.high) / 2;
      return Math.abs(fvgMid - matchedOB.midpoint) <= pipBuffer ||
             (fvg.low <= matchedOB.high + pipBuffer && fvg.high >= matchedOB.low - pipBuffer);
    });
    if (!matchedFVG) return fail('no FVG within 20 pips of OB', true, false, false);

    // Gate 3 — Liquidity sweep in the correct direction
    const sweepOk = direction === 'BUY' ? sweepResult.bullishSweep : sweepResult.bearishSweep;
    if (!sweepOk) return fail('no liquidity sweep of correct direction', true, true, false);

    return {
      holyTrinityPassed:     true,
      orderBlockPresent:     true,
      fvgPresent:            true,
      liquiditySweepPresent: true,
      obZone:                { low: matchedOB.low,  high: matchedOB.high  },
      fvgZone:               { low: matchedFVG.low, high: matchedFVG.high },
      failReason:            null
    };

  } catch (err) {
    console.error('[SMC] evaluateHolyTrinity error:', err.message);
    return fail('internal error');
  }
}

// ---------------------------------------------------------------------------
// PART 2 — ICT KILLZONE GATING
// ---------------------------------------------------------------------------

/**
 * isInsideKillzone(now?)
 * Returns { active: bool, killzoneName: string | null }
 *
 * All windows expressed in UTC minutes. DST shifts the UTC equivalent earlier:
 *   - EU DST active  → London Open/Close windows shift 1 hour earlier in UTC
 *   - US DST active  → New York Open window shifts 1 hour earlier in UTC
 * The code correctly reflects this. Asian Open spans midnight and needs no DST adjustment.
 */
function isInsideKillzone(now) {
  try {
    const d    = now || new Date();
    const mins = d.getUTCHours() * 60 + d.getUTCMinutes();

    const usDst = isUsDst(d);
    const euDst = isEuDst(d);

    // London Open: 07:00–09:00 UTC (winter) / 06:00–08:00 UTC (EU DST)
    const londonOpenStart = euDst ? 6 * 60 : 7 * 60;
    const londonOpenEnd   = euDst ? 8 * 60 : 9 * 60;
    if (mins >= londonOpenStart && mins < londonOpenEnd) return { active: true, killzoneName: 'London Open' };

    // New York Open: 12:00–14:00 UTC (winter) / 11:00–13:00 UTC (US DST)
    const nyOpenStart = usDst ? 11 * 60 : 12 * 60;
    const nyOpenEnd   = usDst ? 13 * 60 : 14 * 60;
    if (mins >= nyOpenStart && mins < nyOpenEnd) return { active: true, killzoneName: 'New York Open' };

    // London Close: 15:00–17:00 UTC (winter) / 14:00–16:00 UTC (EU DST)
    const londonCloseStart = euDst ? 14 * 60 : 15 * 60;
    const londonCloseEnd   = euDst ? 16 * 60 : 17 * 60;
    if (mins >= londonCloseStart && mins < londonCloseEnd) return { active: true, killzoneName: 'London Close' };

    // Asian Open: 23:00–01:00 UTC — spans midnight, no DST adjustment needed
    if (mins >= 23 * 60 || mins < 1 * 60) return { active: true, killzoneName: 'Asian Open' };

    return { active: false, killzoneName: null };

  } catch (err) {
    console.error('[SMC] isInsideKillzone error:', err.message);
    return { active: false, killzoneName: null };
  }
}

// ---------------------------------------------------------------------------
// PART 3 — DXY CORRELATION FILTER
// ---------------------------------------------------------------------------

const DXY_CACHE_TTL = 6 * 60 * 60 * 1000; // 6 hours
let dxyCache = { data: null, fetchedAt: 0 };

/**
 * fetchDXYData()
 * Fetches DXY series from dollarliquidity.com. Cached for 6 hours.
 * Returns array of { date, value } sorted ascending, or null on failure.
 */
async function fetchDXYData() {
  try {
    if (dxyCache.data && (Date.now() - dxyCache.fetchedAt) < DXY_CACHE_TTL) {
      return dxyCache.data;
    }

    const response = await axios.get(
      'https://dollarliquidity.com/api/series/dollar-index?days=30',
      { timeout: 10000 }
    );

    // API may return array directly or wrapped in various shapes — handle all
    const body = response.data;
    let series = null;
    if      (Array.isArray(body))               series = body;
    else if (body && Array.isArray(body.data))   series = body.data;
    else if (body && Array.isArray(body.series)) series = body.series;
    else if (body && Array.isArray(body.values)) series = body.values;

    if (!series || series.length === 0) {
      console.warn('[DXY] Unexpected response shape:', JSON.stringify(body).slice(0, 200));
      return null;
    }

    const normalised = series
      .map(entry => {
        const value = entry.value ?? entry.close ?? entry.price ?? entry.y ?? null;
        const date  = entry.date  ?? entry.datetime ?? entry.x ?? entry.time ?? null;
        if (value === null || date === null) return null;
        return { date: String(date), value: parseFloat(value) };
      })
      .filter(e => e !== null && !isNaN(e.value));

    if (normalised.length === 0) return null;

    normalised.sort((a, b) => a.date < b.date ? -1 : 1);
    dxyCache = { data: normalised, fetchedAt: Date.now() };
    console.log(`[DXY] Fetched ${normalised.length} data points.`);
    return normalised;

  } catch (err) {
    console.warn('[DXY] fetchDXYData failed:', err.message);
    return null;
  }
}

/**
 * calculateEMA(values, period)
 * Standard EMA: k = 2/(n+1).
 * Returns null if insufficient data.
 * Note: when values.length === period exactly, returns the seed SMA (correct degenerate case).
 */
function calculateEMA(values, period) {
  if (!values || values.length < period) return null;
  const k   = 2 / (period + 1);
  let ema   = values.slice(0, period).reduce((s, v) => s + v, 0) / period;
  for (let i = period; i < values.length; i++) {
    ema = values[i] * k + ema * (1 - k);
  }
  return ema;
}

/**
 * calculateDXYTrend(dxyData)
 * Returns 'USD_STRONG' | 'USD_WEAK' | 'NEUTRAL'
 */
function calculateDXYTrend(dxyData) {
  try {
    if (!dxyData || dxyData.length < 20) return 'NEUTRAL';
    const values = dxyData.map(d => d.value);
    const ema5   = calculateEMA(values, 5);
    const ema20  = calculateEMA(values, 20);
    if (ema5 === null || ema20 === null) return 'NEUTRAL';
    if (ema5 > ema20) return 'USD_STRONG';
    if (ema5 < ema20) return 'USD_WEAK';
    return 'NEUTRAL';
  } catch (err) {
    console.error('[DXY] calculateDXYTrend error:', err.message);
    return 'NEUTRAL';
  }
}

/**
 * getDXYPenalty(dxyBias, pair, direction)
 * Returns 20 confidence penalty when signal direction opposes DXY trend, else 0.
 */
function getDXYPenalty(dxyBias, pair, direction) {
  try {
    if (!dxyBias || dxyBias === 'NEUTRAL') return 0;
    const usdBase    = ['USD/JPY', 'USD/CHF', 'USD/CAD']; // USD is the base currency
    const usdCounter = ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD']; // USD is the quote

    if (usdBase.includes(pair)) {
      if (direction === 'BUY'  && dxyBias === 'USD_WEAK')   return 20;
      if (direction === 'SELL' && dxyBias === 'USD_STRONG') return 20;
    } else if (usdCounter.includes(pair)) {
      if (direction === 'BUY'  && dxyBias === 'USD_STRONG') return 20;
      if (direction === 'SELL' && dxyBias === 'USD_WEAK')   return 20;
    }
    return 0;
  } catch (err) {
    console.error('[DXY] getDXYPenalty error:', err.message);
    return 0;
  }
}

// ---------------------------------------------------------------------------
// PART 4 — SIGNAL CLASSIFICATION HELPERS
// ---------------------------------------------------------------------------

/**
 * computeSignalTypeKey({ orderBlockPresent, fvgPresent, liquiditySweepPresent })
 * Returns integer 0–7 (bitmask).
 */
function computeSignalTypeKey(smc) {
  const bit0 = smc.orderBlockPresent     ? 1 : 0;
  const bit1 = smc.fvgPresent            ? 1 : 0;
  const bit2 = smc.liquiditySweepPresent ? 1 : 0;
  return bit0 | (bit1 << 1) | (bit2 << 2);
}

/**
 * classifySignal(trinityResult, indicatorPassed, killzone)
 * Returns 'LIVE' | 'PAPER_OUTSIDE_KILLZONE' | 'PAPER_INDICATOR_FAILED' | 'STANDARD' | 'FALLBACK'
 *
 * FALLBACK = both trinity and indicators failed. server.js intentionally suppresses
 * these (no signal stored) but logs them for visibility.
 */
function classifySignal(trinityResult, indicatorPassed, killzone) {
  const trinityPassed = trinityResult && trinityResult.holyTrinityPassed;
  if (trinityPassed && indicatorPassed  && killzone.active)  return 'LIVE';
  if (trinityPassed && indicatorPassed  && !killzone.active) return 'PAPER_OUTSIDE_KILLZONE';
  if (trinityPassed && !indicatorPassed)                     return 'PAPER_INDICATOR_FAILED';
  if (!trinityPassed && indicatorPassed)                     return 'STANDARD';
  return 'FALLBACK';
}

/**
 * getDXYCache()
 * Returns a shallow copy of the DXY cache metadata for /smc-status.
 * Returns a copy to prevent external mutation of the internal cache.
 */
function getDXYCache() {
  return { data: dxyCache.data, fetchedAt: dxyCache.fetchedAt };
}

// ---------------------------------------------------------------------------
// PART 5 — HIGHER TIMEFRAME CONFLUENCE
// ---------------------------------------------------------------------------

/**
 * getHTFBias(candles)
 * Takes H1 candles (oldest-first) and synthesizes H4 and Daily candles to
 * determine higher-timeframe trend bias via EMA(20)/EMA(50) crossover.
 * Returns { h4Bias: 'BUY'|'SELL'|'NEUTRAL', dailyBias: 'BUY'|'SELL'|'NEUTRAL' }
 */
function getHTFBias(candles) {
  const neutral = { h4Bias: 'NEUTRAL', dailyBias: 'NEUTRAL' };
  try {
    if (!candles || candles.length < 200) return neutral;

    // Synthesize H4 candles — each group of 4 consecutive H1 candles
    const h4Candles = [];
    const h4GroupSize = 4;
    const h4Complete = Math.floor(candles.length / h4GroupSize);
    for (let i = 0; i < h4Complete; i++) {
      const group = candles.slice(i * h4GroupSize, i * h4GroupSize + h4GroupSize);
      h4Candles.push({ close: group[group.length - 1].close });
    }

    // Synthesize Daily candles — each group of 24 consecutive H1 candles
    const dailyCandles = [];
    const dailyGroupSize = 24;
    const dailyComplete = Math.floor(candles.length / dailyGroupSize);
    for (let i = 0; i < dailyComplete; i++) {
      const group = candles.slice(i * dailyGroupSize, i * dailyGroupSize + dailyGroupSize);
      dailyCandles.push({ close: group[group.length - 1].close });
    }

    // Compute H4 bias
    let h4Bias = 'NEUTRAL';
    if (h4Candles.length >= 50) {
      const h4Closes = h4Candles.map(c => c.close);
      const h4Ema20  = calculateEMA(h4Closes, 20);
      const h4Ema50  = calculateEMA(h4Closes, 50);
      if (h4Ema20 !== null && h4Ema50 !== null) {
        if      (h4Ema20 > h4Ema50) h4Bias = 'BUY';
        else if (h4Ema20 < h4Ema50) h4Bias = 'SELL';
      }
    }

    // Compute Daily bias
    let dailyBias = 'NEUTRAL';
    if (dailyCandles.length >= 50) {
      const dailyCloses = dailyCandles.map(c => c.close);
      const dailyEma20  = calculateEMA(dailyCloses, 20);
      const dailyEma50  = calculateEMA(dailyCloses, 50);
      if (dailyEma20 !== null && dailyEma50 !== null) {
        if      (dailyEma20 > dailyEma50) dailyBias = 'BUY';
        else if (dailyEma20 < dailyEma50) dailyBias = 'SELL';
      }
    }

    return { h4Bias, dailyBias };

  } catch (err) {
    console.error('[SMC] getHTFBias error:', err.message);
    return neutral;
  }
}

/**
 * htfConflictCheck(direction, htfBias)
 * Returns true if EITHER h4Bias OR dailyBias directly opposes the signal direction.
 * NEUTRAL on either timeframe does NOT count as a conflict.
 * direction: 'BUY' | 'SELL'
 * htfBias: { h4Bias: string, dailyBias: string }
 */
function htfConflictCheck(direction, htfBias) {
  try {
    if (!htfBias) return false;
    if (direction === 'BUY') {
      return htfBias.h4Bias === 'SELL' || htfBias.dailyBias === 'SELL';
    }
    if (direction === 'SELL') {
      return htfBias.h4Bias === 'BUY' || htfBias.dailyBias === 'BUY';
    }
    return false;
  } catch (err) {
    console.error('[SMC] htfConflictCheck error:', err.message);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------
module.exports = {
  detectOrderBlocks,
  detectFairValueGaps,
  detectLiquiditySweep,
  getPremiumDiscountZone,
  evaluateHolyTrinity,
  isInsideKillzone,
  fetchDXYData,
  calculateDXYTrend,
  getDXYPenalty,
  computeSignalTypeKey,
  classifySignal,
  getDXYCache,
  getHTFBias,
  htfConflictCheck
};
