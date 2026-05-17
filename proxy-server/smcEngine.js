'use strict';

const MIN_CANDLES = 50;

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
    const active = obs.filter(ob => { for (let k = ob.index + 1; k < len; k++) { if (direction === 'BUY' && candles[k].close < ob.midpoint) return false; if (direction === 'SELL' && candles[k].close > ob.midpoint) return false; } return true; });
    const seen = new Set(), deduped = [];
    for (let i = active.length - 1; i >= 0 && deduped.length < 10; i--) { if (!seen.has(active[i].index)) { seen.add(active[i].index); deduped.unshift(active[i]); } }
    return deduped;
  } catch (err) { console.error('[SMC] detectOrderBlocks error:', err.message); return []; }
}

function detectFairValueGaps(candles, direction) {
  try {
    if (!candles || candles.length < MIN_CANDLES) return [];
    const fvgs = [], len = candles.length;
    for (let i = 2; i < len; i++) {
      const c = candles[i], prev2 = candles[i - 2];
      if (direction === 'BUY' && c.low > prev2.high) fvgs.push({ low: prev2.high, high: c.low, direction: 'BUY', index: i });
      else if (direction === 'SELL' && c.high < prev2.low) fvgs.push({ low: c.high, high: prev2.low, direction: 'SELL', index: i });
    }
    return fvgs.filter(fvg => { for (let k = fvg.index + 1; k < len; k++) { const c = candles[k]; if (fvg.direction === 'BUY' && c.low <= fvg.low) return false; if (fvg.direction === 'SELL' && c.high >= fvg.high) return false; } return true; }).slice(-20);
  } catch (err) { console.error('[SMC] detectFairValueGaps error:', err.message); return []; }
}

function detectLiquiditySweep(candles) {
  const result = { bullishSweep: false, bearishSweep: false, sweepCandleIndex: null };
  try {
    if (!candles || candles.length < MIN_CANDLES) return result;
    const len = candles.length, last = candles[len - 1], prev = candles.slice(len - 21, len - 1);
    const lowestLow = Math.min(...prev.map(c => c.low)), highestHigh = Math.max(...prev.map(c => c.high));
    if (last.low < lowestLow && last.close > lowestLow) { result.bullishSweep = true; result.sweepCandleIndex = len - 1; }
    if (last.high > highestHigh && last.close < highestHigh) { result.bearishSweep = true; result.sweepCandleIndex = len - 1; }
  } catch (err) { console.error('[SMC] detectLiquiditySweep error:', err.message); }
  return result;
}

function getPremiumDiscountZone(candles) {
  const fallback = { equilibrium: 0, isPremium: false, isDiscount: false, isNeutral: true };
  try {
    if (!candles || candles.length < MIN_CANDLES) return fallback;
    const window = candles.slice(-50);
    const highestHigh = Math.max(...window.map(c => c.high)), lowestLow = Math.min(...window.map(c => c.low));
    const range = highestHigh - lowestLow; if (range === 0) return fallback;
    const equilibrium = (highestHigh + lowestLow) / 2, neutralBand = range * 0.05, currentPrice = candles[candles.length - 1].close;
    return { equilibrium, isPremium: currentPrice > equilibrium + neutralBand, isDiscount: currentPrice < equilibrium - neutralBand, isNeutral: currentPrice >= equilibrium - neutralBand && currentPrice <= equilibrium + neutralBand };
  } catch (err) { return fallback; }
}

function evaluateHolyTrinity(candles, direction, obList, fvgList, sweepResult, pdZone) {
  const fail = (reason, ob = false, fvg = false, sweep = false) => ({ holyTrinityPassed: false, orderBlockPresent: ob, fvgPresent: fvg, liquiditySweepPresent: sweep, obZone: null, fvgZone: null, failReason: reason });
  try {
    if (!candles || candles.length < MIN_CANDLES) return fail('insufficient candles');
    const currentPrice = candles[candles.length - 1].close;
    if (direction === 'BUY' && !pdZone.isDiscount) return fail('price not in discount zone');
    if (direction === 'SELL' && !pdZone.isPremium) return fail('price not in premium zone');
    if (pdZone.isNeutral) return fail('price in neutral zone');
    const matchedOB = obList.find(ob => currentPrice >= ob.low && currentPrice <= ob.high);
    if (!matchedOB) return fail('price not inside any active OB');
    const pipBuffer = 20 * (matchedOB.high < 10 ? 0.01 : 0.0001);
    const matchedFVG = fvgList.find(fvg => Math.abs((fvg.low + fvg.high) / 2 - matchedOB.midpoint) <= pipBuffer || (fvg.low <= matchedOB.high + pipBuffer && fvg.high >= matchedOB.low - pipBuffer));
    if (!matchedFVG) return fail('no FVG within 20 pips of OB', true, false, false);
    const sweepOk = direction === 'BUY' ? sweepResult.bullishSweep : sweepResult.bearishSweep;
    if (!sweepOk) return fail('no liquidity sweep of correct direction', true, true, false);
    return { holyTrinityPassed: true, orderBlockPresent: true, fvgPresent: true, liquiditySweepPresent: true, obZone: { low: matchedOB.low, high: matchedOB.high }, fvgZone: { low: matchedFVG.low, high: matchedFVG.high }, failReason: null };
  } catch (err) { return fail('internal error'); }
}

function isInsideKillzone(now) {
  try {
    const d = now || new Date(), h = d.getUTCHours(), m = d.getUTCMinutes(), mins = h * 60 + m;
    if (mins >= 7 * 60 && mins < 9 * 60) return { active: true, killzoneName: 'London Open' };
    if (mins >= 12 * 60 && mins < 14 * 60) return { active: true, killzoneName: 'New York Open' };
    if (mins >= 15 * 60 && mins < 17 * 60) return { active: true, killzoneName: 'London Close' };
    if (mins >= 23 * 60 || mins < 1 * 60) return { active: true, killzoneName: 'Asian Open' };
    return { active: false, killzoneName: null };
  } catch (err) { return { active: false, killzoneName: null }; }
}

const DXY_CACHE_TTL = 6 * 60 * 60 * 1000;
let dxyCache = { data: null, fetchedAt: 0 };

async function fetchDXYData() {
  try {
    if (dxyCache.data && (Date.now() - dxyCache.fetchedAt) < DXY_CACHE_TTL) return dxyCache.data;
    const axios = require('axios');
    const response = await axios.get('https://dollarliquidity.com/api/series/dollar-index?days=30', { timeout: 10000 });
    const body = response.data;
    let series = Array.isArray(body) ? body : Array.isArray(body.data) ? body.data : Array.isArray(body.series) ? body.series : Array.isArray(body.values) ? body.values : null;
    if (!series || series.length === 0) return null;
    const normalised = series.map(e => { const value = e.value ?? e.close ?? e.price ?? e.y ?? null, date = e.date ?? e.datetime ?? e.x ?? e.time ?? null; if (value === null || date === null) return null; return { date: String(date), value: parseFloat(value) }; }).filter(e => e !== null && !isNaN(e.value));
    if (normalised.length === 0) return null;
    normalised.sort((a, b) => a.date < b.date ? -1 : 1);
    dxyCache = { data: normalised, fetchedAt: Date.now() };
    console.log(`[DXY] Fetched ${normalised.length} data points.`); return normalised;
  } catch (err) { console.warn('[DXY] fetchDXYData failed:', err.message); return null; }
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
    if (ema5 > ema20) return 'USD_STRONG'; if (ema5 < ema20) return 'USD_WEAK'; return 'NEUTRAL';
  } catch (err) { return 'NEUTRAL'; }
}

function getDXYPenalty(dxyBias, pair, direction) {
  try {
    if (!dxyBias || dxyBias === 'NEUTRAL') return 0;
    const usdBase = ['USD/JPY', 'USD/CHF', 'USD/CAD'], usdCounter = ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD'];
    if (usdBase.includes(pair)) { if (direction === 'BUY' && dxyBias === 'USD_WEAK') return 20; if (direction === 'SELL' && dxyBias === 'USD_STRONG') return 20; }
    else if (usdCounter.includes(pair)) { if (direction === 'BUY' && dxyBias === 'USD_STRONG') return 20; if (direction === 'SELL' && dxyBias === 'USD_WEAK') return 20; }
    return 0;
  } catch (err) { return 0; }
}

function computeSignalTypeKey(smc) { return (smc.orderBlockPresent ? 1 : 0) | (smc.fvgPresent ? 1 : 0) << 1 | (smc.liquiditySweepPresent ? 1 : 0) << 2; }

function classifySignal(trinityResult, indicatorPassed, killzone) {
  const trinityPassed = trinityResult && trinityResult.holyTrinityPassed;
  if (trinityPassed && indicatorPassed && killzone.active) return 'LIVE';
  if (trinityPassed && indicatorPassed && !killzone.active) return 'PAPER_OUTSIDE_KILLZONE';
  if (trinityPassed && !indicatorPassed) return 'PAPER_INDICATOR_FAILED';
  if (!trinityPassed && indicatorPassed) return 'STANDARD';
  return 'FALLBACK';
}

function getDXYCache() { return dxyCache; }

module.exports = { detectOrderBlocks, detectFairValueGaps, detectLiquiditySweep, getPremiumDiscountZone, evaluateHolyTrinity, isInsideKillzone, fetchDXYData, calculateDXYTrend, getDXYPenalty, computeSignalTypeKey, classifySignal, getDXYCache };
