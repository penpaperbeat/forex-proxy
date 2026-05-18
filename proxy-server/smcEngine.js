'use strict';

const axios = require('axios');

const MIN_CANDLES = 50;
const OB_MAX_AGE_CANDLES = 50; // FIX #6: order blocks older than this are stale

// FIX #7: intelligence profile recalculates every 6 hours instead of 7 days
const INTELLIGENCE_RECALC_INTERVAL_MS = 6 * 60 * 60 * 1000;

// --- FIX #1: DST-aware killzone helper ---
function isUSDST(d) {
  const year = d.getUTCFullYear();
  const marchStart = new Date(Date.UTC(year, 2, 1));
  const marchDay = marchStart.getUTCDay();
  const dstStart = new Date(Date.UTC(year, 2, (7 - marchDay) % 7 + 8, 2));
  const novStart = new Date(Date.UTC(year, 10, 1));
  const novDay = novStart.getUTCDay();
  const dstEnd = new Date(Date.UTC(year, 10, (7 - novDay) % 7 + 1, 2));
  return d >= dstStart && d < dstEnd;
}

function isEUDST(d) {
  const year = d.getUTCFullYear();
  const marchEnd = new Date(Date.UTC(year, 2, 31, 1));
  marchEnd.setUTCDate(31 - marchEnd.getUTCDay());
  const octEnd = new Date(Date.UTC(year, 9, 31, 1));
  octEnd.setUTCDate(31 - octEnd.getUTCDay());
  return d >= marchEnd && d < octEnd;
}

function isInsideKillzone(now) {
  try {
    const d = now || new Date();
    const h = d.getUTCHours(), m = d.getUTCMinutes(), mins = h * 60 + m;
    const usDST = isUSDST(d);
    const euDST = isEUDST(d);

    const londonOpenStart = euDST ? 6 * 60 : 7 * 60;
    const londonOpenEnd   = euDST ? 8 * 60 : 9 * 60;
    const nyOpenStart     = usDST ? 12 * 60 : 13 * 60;
    const nyOpenEnd       = usDST ? 14 * 60 : 15 * 60;
    const londonCloseStart = euDST ? 14 * 60 : 15 * 60;
    const londonCloseEnd   = euDST ? 16 * 60 : 17 * 60;
    const asianOpenStart  = 23 * 60;
    const asianOpenEnd    = 1 * 60;

    if (mins >= londonOpenStart && mins < londonOpenEnd)    return { active: true, killzoneName: 'London Open' };
    if (mins >= nyOpenStart     && mins < nyOpenEnd)        return { active: true, killzoneName: 'New York Open' };
    if (mins >= londonCloseStart && mins < londonCloseEnd)  return { active: true, killzoneName: 'London Close' };
    if (mins >= asianOpenStart || mins < asianOpenEnd)      return { active: true, killzoneName: 'Asian Open' };

    return { active: false, killzoneName: null };
  } catch (err) { return { active: false, killzoneName: null }; }
}

// FIX #7 (new-7): per-pair volatility suppression — only suppress the affected pair
function isVolatilitySuppressed(pair, intelligenceProfile) {
  try {
    if (!intelligenceProfile || !pair) return { suppressed: false, reason: null };
    const profile = intelligenceProfile[pair];
    if (!profile) return { suppressed: false, reason: null };
    if (profile.volatilityRegime === 'extreme') {
      return {
        suppressed: true,
        reason: `${pair} volatility is extreme — signal suppressed for this pair only`
      };
    }
    return { suppressed: false, reason: null };
  } catch (err) {
    return { suppressed: false, reason: null };
  }
}

// FIX #6: filter order blocks older than OB_MAX_AGE_CANDLES before Trinity evaluation
function filterStaleOrderBlocks(obList, currentLen) {
  return obList.filter(ob => (currentLen - 1 - ob.index) <= OB_MAX_AGE_CANDLES);
}

function detectOrderBlocks(candles, direction) {
  try {
    if (!candles || candles.length < MIN_CANDLES) return [];
    const obs = [], len = candles.length;
    for (let i = 2; i < len; i++) {
      const c = candles[i], prev2 = candles[i - 2];
      if (direction === 'BUY') {
        if (!(c.low > prev2.high)) continue;
        const swingHigh = Math.max(...candles.slice(Math.max(0, i - 10), i).map(x => x.high));
        if (c.close <= swingHigh) continue;
        let obIdx = -1;
        for (let j = i - 1; j >= Math.max(0, i - 5); j--) { if (candles[j].close < candles[j].open) { obIdx = j; break; } }
        if (obIdx < 0) continue;
        obs.push({ low: candles[obIdx].low, high: candles[obIdx].high, midpoint: (candles[obIdx].low + candles[obIdx].high) / 2, index: obIdx, direction: 'BUY' });
      } else {
        if (!(c.high < prev2.low)) continue;
        const swingLow = Math.min(...candles.slice(Math.max(0, i - 10), i).map(x => x.low));
        if (c.close >= swingLow) continue;
        let obIdx = -1;
        for (let j = i - 1; j >= Math.max(0, i - 5); j--) { if (candles[j].close > candles[j].open) { obIdx = j; break; } }
        if (obIdx < 0) continue;
        obs.push({ low: candles[obIdx].low, high: candles[obIdx].high, midpoint: (candles[obIdx].low + candles[obIdx].high) / 2, index: obIdx, direction: 'SELL' });
      }
    }
    const active = obs.filter(ob => {
      for (let k = ob.index + 1; k < len; k++) {
        if (direction === 'BUY'  && candles[k].close < ob.midpoint) return false;
        if (direction === 'SELL' && candles[k].close > ob.midpoint) return false;
      }
      return true;
    });
    const seen = new Set(), deduped = [];
    for (let i = active.length - 1; i >= 0 && deduped.length < 10; i--) {
      if (!seen.has(active[i].index)) { seen.add(active[i].index); deduped.unshift(active[i]); }
    }
    return deduped;
  } catch (err) { console.error('[SMC] detectOrderBlocks error:', err.message); return []; }
}

function detectFairValueGaps(candles, direction) {
  try {
    if (!candles || candles.length < MIN_CANDLES) return [];
    const fvgs = [], len = candles.length;
    for (let i = 2; i < len; i++) {
      const c = candles[i], prev2 = candles[i - 2];
      if (direction === 'BUY' && c.low > prev2.high)
        fvgs.push({ low: prev2.high, high: c.low, direction: 'BUY', index: i });
      else if (direction === 'SELL' && c.high < prev2.low)
        fvgs.push({ low: c.high, high: prev2.low, direction: 'SELL', index: i });
    }
    return fvgs.filter(fvg => {
      for (let k = fvg.index + 1; k < len; k++) {
        const c = candles[k];
        if (fvg.direction === 'BUY'  && c.low  <= fvg.low)  return false;
        if (fvg.direction === 'SELL' && c.high >= fvg.high) return false;
      }
      return true;
    }).slice(-20);
  } catch (err) { console.error('[SMC] detectFairValueGaps error:', err.message); return []; }
}

// FIX new-1: look back up to 5 candles for a sweep, not just the last one
function detectLiquiditySweep(candles) {
  const result = { bullishSweep: false, bearishSweep: false, sweepCandleIndex: null };
  try {
    if (!candles || candles.length < MIN_CANDLES) return result;
    const len = candles.length;
    // FIX new-5: reduced lookback window from 20 to 10 for tighter sweep detection
    const SWEEP_LOOKBACK = 5;
    const REF_LOOKBACK = 10;

    for (let offset = 0; offset < SWEEP_LOOKBACK; offset++) {
      const idx = len - 1 - offset;
      if (idx < REF_LOOKBACK) break;
      const candle = candles[idx];
      const ref = candles.slice(idx - REF_LOOKBACK, idx);
      const lowestLow   = Math.min(...ref.map(c => c.low));
      const highestHigh = Math.max(...ref.map(c => c.high));

      if (!result.bullishSweep && candle.low < lowestLow && candle.close > lowestLow) {
        result.bullishSweep = true;
        result.sweepCandleIndex = idx;
      }
      if (!result.bearishSweep && candle.high > highestHigh && candle.close < highestHigh) {
        result.bearishSweep = true;
        result.sweepCandleIndex = idx;
      }
      if (result.bullishSweep && result.bearishSweep) break;
    }
  } catch (err) { console.error('[SMC] detectLiquiditySweep error:', err.message); }
  return result;
}

// FIX new-2: widen neutral band from 5% to 20% so consolidation zones don't hard-block signals
function getPremiumDiscountZone(candles) {
  const fallback = { equilibrium: 0, isPremium: false, isDiscount: false, isNeutral: true };
  try {
    if (!candles || candles.length < MIN_CANDLES) return fallback;
    const window = candles.slice(-50);
    const highestHigh = Math.max(...window.map(c => c.high)), lowestLow = Math.min(...window.map(c => c.low));
    const range = highestHigh - lowestLow; if (range === 0) return fallback;
    const equilibrium = (highestHigh + lowestLow) / 2;
    // FIX new-2: widened from 0.05 to 0.20 — 5% band was blocking signals during consolidation
    const neutralBand = range * 0.20;
    const currentPrice = candles[candles.length - 1].close;
    return {
      equilibrium,
      isPremium:  currentPrice > equilibrium + neutralBand,
      isDiscount: currentPrice < equilibrium - neutralBand,
      isNeutral:  currentPrice >= equilibrium - neutralBand && currentPrice <= equilibrium + neutralBand
    };
  } catch (err) { return fallback; }
}

// FIX #5: calculate entry, stop-loss and take-profit levels
function calculateSignalLevels(candles, direction) {
  try {
    const len = candles.length;
    const entry = candles[len - 1].close;

    let atr = entry * 0.001;
    if (len >= 15) {
      let trSum = 0;
      for (let i = len - 14; i < len; i++) {
        const prev = candles[i - 1];
        const tr = Math.max(
          candles[i].high - candles[i].low,
          Math.abs(candles[i].high - prev.close),
          Math.abs(candles[i].low  - prev.close)
        );
        trSum += tr;
      }
      atr = trSum / 14;
    }

    const lookback = candles.slice(Math.max(0, len - 20), len - 1);
    let stopLoss;
    if (direction === 'BUY') {
      const swingLow = Math.min(...lookback.map(c => c.low));
      stopLoss = swingLow - atr * 0.25;
    } else {
      const swingHigh = Math.max(...lookback.map(c => c.high));
      stopLoss = swingHigh + atr * 0.25;
    }

    const takeProfit = direction === 'BUY'
      ? entry + atr * 2
      : entry - atr * 2;

    return {
      entry:      parseFloat(entry.toFixed(5)),
      stopLoss:   parseFloat(stopLoss.toFixed(5)),
      takeProfit: parseFloat(takeProfit.toFixed(5)),
      atr:        parseFloat(atr.toFixed(5))
    };
  } catch (err) {
    console.error('[SMC] calculateSignalLevels error:', err.message);
    return { entry: null, stopLoss: null, takeProfit: null, atr: null };
  }
}

function evaluateHolyTrinity(candles, direction, obList, fvgList, sweepResult, pdZone) {
  const fail = (reason, ob = false, fvg = false, sweep = false) => ({
    holyTrinityPassed: false, orderBlockPresent: ob, fvgPresent: fvg,
    liquiditySweepPresent: sweep, obZone: null, fvgZone: null,
    entry: null, stopLoss: null, takeProfit: null, atr: null, failReason: reason
  });

  try {
    if (!candles || candles.length < MIN_CANDLES) return fail('insufficient candles');

    const currentPrice = candles[candles.length - 1].close;

    // FIX new-2: neutral zone no longer hard-blocks — only premium/discount mismatch blocks
    if (direction === 'BUY'  && pdZone.isPremium)  return fail('price in premium zone for BUY');
    if (direction === 'SELL' && pdZone.isDiscount) return fail('price in discount zone for SELL');

    // FIX #6: filter stale OBs before matching
    const freshOBs = filterStaleOrderBlocks(obList, candles.length);

    const matchedOB = freshOBs.find(ob => currentPrice >= ob.low && currentPrice <= ob.high);
    if (!matchedOB) return fail('price not inside any active OB');

    const pipBuffer = 20 * (matchedOB.high < 10 ? 0.01 : 0.0001);
    const matchedFVG = fvgList.find(fvg =>
      Math.abs((fvg.low + fvg.high) / 2 - matchedOB.midpoint) <= pipBuffer ||
      (fvg.low <= matchedOB.high + pipBuffer && fvg.high >= matchedOB.low - pipBuffer)
    );
    if (!matchedFVG) return fail('no FVG within 20 pips of OB', true, false, false);

    // FIX new-1: sweep now found within last 5 candles, not just the last one
    const sweepOk = direction === 'BUY' ? sweepResult.bullishSweep : sweepResult.bearishSweep;
    if (!sweepOk) return fail('no liquidity sweep of correct direction', true, true, false);

    const levels = calculateSignalLevels(candles, direction);

    return {
      holyTrinityPassed: true,
      orderBlockPresent: true, fvgPresent: true, liquiditySweepPresent: true,
      obZone:  { low: matchedOB.low,  high: matchedOB.high  },
      fvgZone: { low: matchedFVG.low, high: matchedFVG.high },
      entry:      levels.entry,
      stopLoss:   levels.stopLoss,
      takeProfit: levels.takeProfit,
      atr:        levels.atr,
      failReason: null
    };
  } catch (err) { return fail('internal error'); }
}

// FIX #2: safe integer key
function computeSignalTypeKey(smc) {
  const ob    = smc.orderBlockPresent      === true ? 1 : 0;
  const fvg   = smc.fvgPresent             === true ? 1 : 0;
  const sweep = smc.liquiditySweepPresent  === true ? 1 : 0;
  return ob | (fvg << 1) | (sweep << 2);
}

// FIX #3: FALLBACK never produces LIVE or STANDARD — always paper
function classifySignal(trinityResult, indicatorPassed, killzone) {
  const trinityPassed = trinityResult && trinityResult.holyTrinityPassed;
  if (trinityPassed && indicatorPassed && killzone.active)  return 'LIVE';
  if (trinityPassed && indicatorPassed && !killzone.active) return 'PAPER_OUTSIDE_KILLZONE';
  if (trinityPassed && !indicatorPassed)                    return 'PAPER_INDICATOR_FAILED';
  if (!trinityPassed && indicatorPassed)                    return 'STANDARD';
  return 'PAPER_INDICATOR_FAILED';
}

// --- DXY ---

const DXY_CACHE_TTL  = 6 * 60 * 60 * 1000;
const DXY_STALE_WARN = 4 * 60 * 60 * 1000;
let dxyCache = { data: null, fetchedAt: 0 };

async function fetchDXYData() {
  try {
    if (dxyCache.data && (Date.now() - dxyCache.fetchedAt) < DXY_CACHE_TTL) return dxyCache.data;
    const response = await axios.get('https://dollarliquidity.com/api/series/dollar-index?days=30', { timeout: 10000 });
    const body = response.data;
    let series = Array.isArray(body)        ? body       :
                 Array.isArray(body.data)   ? body.data  :
                 Array.isArray(body.series) ? body.series:
                 Array.isArray(body.values) ? body.values: null;
    if (!series || series.length === 0) return null;
    const normalised = series.map(e => {
      const value = e.value ?? e.close ?? e.price ?? e.y ?? null;
      const date  = e.date  ?? e.datetime ?? e.x   ?? e.time ?? null;
      if (value === null || date === null) return null;
      return { date: String(date), value: parseFloat(value) };
    }).filter(e => e !== null && !isNaN(e.value));
    if (normalised.length === 0) return null;
    normalised.sort((a, b) => a.date < b.date ? -1 : 1);
    dxyCache = { data: normalised, fetchedAt: Date.now() };
    console.log(`[DXY] Fetched ${normalised.length} data points.`);
    return normalised;
  } catch (err) {
    console.warn(`[DXY] fetchDXYData failed at ${new Date().toISOString()}:`, err.message);
    return null;
  }
}

function calculateEMA(values, period) {
  if (!values || values.length < period) return null;
  const k = 2 / (period + 1);
  let ema = values.slice(0, period).reduce((s, v) => s + v, 0) / period;
  for (let i = period; i < values.length; i++) ema = values[i] * k + ema * (1 - k);
  return ema;
}

function calculateDXYTrend(dxyData) {
  try {
    if (!dxyData || dxyData.length < 20) return 'NEUTRAL';
    const values = dxyData.map(d => d.value);
    const ema5 = calculateEMA(values, 5), ema20 = calculateEMA(values, 20);
    if (ema5 === null || ema20 === null) return 'NEUTRAL';
    if (ema5 > ema20) return 'USD_STRONG';
    if (ema5 < ema20) return 'USD_WEAK';
    return 'NEUTRAL';
  } catch (err) { return 'NEUTRAL'; }
}

function getDXYPenalty(dxyBias, pair, direction) {
  try {
    const stale = dxyCache.fetchedAt > 0 && (Date.now() - dxyCache.fetchedAt) > DXY_STALE_WARN;
    if (stale) {
      const ageMin = Math.round((Date.now() - dxyCache.fetchedAt) / 60000);
      console.warn(`[DXY] Data is ${ageMin} minutes old — applying staleness penalty to all signals.`);
    }
    const stalenessPenalty = stale ? 10 : 0;

    if (!dxyBias || dxyBias === 'NEUTRAL') return stalenessPenalty;

    const usdBase    = ['USD/JPY', 'USD/CHF', 'USD/CAD'];
    const usdCounter = ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD'];

    let directionPenalty = 0;
    if (usdBase.includes(pair)) {
      if (direction === 'BUY'  && dxyBias === 'USD_WEAK')   directionPenalty = 20;
      if (direction === 'SELL' && dxyBias === 'USD_STRONG') directionPenalty = 20;
    } else if (usdCounter.includes(pair)) {
      if (direction === 'BUY'  && dxyBias === 'USD_STRONG') directionPenalty = 20;
      if (direction === 'SELL' && dxyBias === 'USD_WEAK')   directionPenalty = 20;
    }

    return directionPenalty + stalenessPenalty;
  } catch (err) { return 0; }
}

function getDXYCache() { return dxyCache; }

module.exports = {
  detectOrderBlocks,
  detectFairValueGaps,
  detectLiquiditySweep,
  getPremiumDiscountZone,
  evaluateHolyTrinity,
  isInsideKillzone,
  isVolatilitySuppressed,
  fetchDXYData,
  calculateDXYTrend,
  getDXYPenalty,
  computeSignalTypeKey,
  classifySignal,
  getDXYCache,
  INTELLIGENCE_RECALC_INTERVAL_MS
};
