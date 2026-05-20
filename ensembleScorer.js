'use strict';

/**
 * ensembleScorer.js
 * Main-thread interface to the ensemble worker.
 * Uses worker_threads (Node.js built-in — no npm install required).
 *
 * Public API:
 *   initEnsembleScorer(resolvedSignals)            — train if >= 100 samples
 *   scoreSignal(features)                          — Promise<number> 0-100 (null = untrained)
 *   retrainIfNeeded(resolvedSignals)               — retrain if 7 days elapsed or 50+ new signals
 *   extractFeatures(signal, paperTypeStats, sweepAge) — returns 13-element number[]
 *   getEnsembleStatus()                            — returns status object for /ensemble-status endpoint
 */

const { Worker } = require('worker_threads');
const path = require('path');

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const state = {
  modelTrained:        false,
  lastTrainedAt:       null,
  resolvedSignalCount: 0,
  trainingSampleCount: 0,
  oosAccuracy:         null,
  worker:              null,
  pendingScores:       new Map(),
  scoreCounter:        0
};

// Retrain thresholds
const RETRAIN_DAYS        = 7;
const RETRAIN_NEW_SIGNALS = 50;
let newResolvedSinceTrain = 0;

// ---------------------------------------------------------------------------
// Worker management
// ---------------------------------------------------------------------------

function spawnWorker() {
  const workerPath = path.join(__dirname, 'ensembleWorker.js');
  const w = new Worker(workerPath);

  w.on('message', (msg) => {
    if (msg.type === 'trained') {
      if (msg.error) {
        console.warn('[Ensemble] Training failed:', msg.error);
        state.modelTrained = false;
      } else {
        state.modelTrained    = true;
        state.lastTrainedAt   = new Date().toISOString();
        state.oosAccuracy     = msg.oosAccuracy;
        newResolvedSinceTrain = 0;
        console.log(`[Ensemble] Model trained. OOS accuracy: ${msg.oosAccuracy !== null ? msg.oosAccuracy + '%' : 'N/A'}. Samples: ${state.trainingSampleCount}`);
      }
    } else if (msg.type === 'scored') {
      const pending = state.pendingScores.get(msg.id);
      if (pending) {
        state.pendingScores.delete(msg.id);
        pending.resolve(msg.score);
      }
    }
  });

  w.on('error', (err) => {
    console.error('[Ensemble] Worker error:', err.message);
    for (const [, pending] of state.pendingScores) pending.resolve(50);
    state.pendingScores.clear();
    state.modelTrained = false;
    setTimeout(() => { state.worker = spawnWorker(); }, 5000);
  });

  w.on('exit', (code) => {
    if (code !== 0) {
      console.warn(`[Ensemble] Worker exited with code ${code}. Respawning...`);
      for (const [, pending] of state.pendingScores) pending.resolve(50);
      state.pendingScores.clear();
      state.modelTrained = false;
      setTimeout(() => { state.worker = spawnWorker(); }, 5000);
    }
  });

  return w;
}

// ---------------------------------------------------------------------------
// Feature extraction
// ---------------------------------------------------------------------------

/**
 * extractFeatures(signal, paperTypeStats, sweepAge)
 * signal: object from runSMCEvaluation (signalBase shape)
 * paperTypeStats: Map<number, { wins: number, total: number }> or null
 * sweepAge: number | undefined — candles since last sweep (from SMC engine, optional)
 * Returns 13-element number[]
 */
function extractFeatures(signal, paperTypeStats, sweepAge) {
  const hour = signal.generatedAt ? new Date(signal.generatedAt).getUTCHours() : 12;
  const dow  = signal.generatedAt ? new Date(signal.generatedAt).getUTCDay()   : 2;

  // Weekday 0=Mon … 4=Fri. Sunday (0) and Saturday (6) both map to 4 (Friday).
  // In practice killzone gates block weekend signals, so this is safe.
  const weekday = Math.max(0, Math.min(4, dow === 0 ? 4 : dow - 1));

  const hourSin = Math.sin(2 * Math.PI * hour / 24);
  const hourCos = Math.cos(2 * Math.PI * hour / 24);

  // [feature 8] DXY alignment
  const dxyMap     = { USD_STRONG: 1, NEUTRAL: 0, USD_WEAK: -1 };
  const dxyBias    = signal.dxyBias || 'NEUTRAL';
  const dxyEncoded = dxyMap[dxyBias] !== undefined ? dxyMap[dxyBias] : 0;

  // [feature 7] Session quality
  const sessionQualityMap = {
    'New York Open': 1.0,
    'London Open':   0.8,
    'London Close':  0.7,
    'Asian Open':    0.5
  };
  const killzoneName   = signal.killzoneName || null;
  const sessionQuality = killzoneName ? (sessionQualityMap[killzoneName] ?? 0.3) : 0.3;

  // [feature 4] OB distance in pips
  let obDistancePips = 0;
  if (signal.obZone && signal.obZone.low != null && signal.obZone.high != null) {
    const currentPrice = signal.entryPrice || signal.close || 0;
    const obMid        = (signal.obZone.low + signal.obZone.high) / 2;
    const pipFactor    = obMid > 10 ? 0.01 : 0.0001;
    obDistancePips     = Math.min(50, Math.abs(currentPrice - obMid) / pipFactor);
  }

  // [feature 5] FVG size in pips
  let fvgSizePips = 0;
  if (signal.fvgZone && signal.fvgZone.low != null && signal.fvgZone.high != null) {
    const fvgRange  = signal.fvgZone.high - signal.fvgZone.low;
    const refMid    = signal.obZone ? (signal.obZone.low + signal.obZone.high) / 2 : 1;
    const pipFactor = refMid > 10 ? 0.01 : 0.0001;
    fvgSizePips     = Math.min(50, fvgRange / pipFactor);
  }

  // [feature 6] Candles since sweep
  const candlesSinceSweep = (sweepAge !== undefined && sweepAge !== null) ? sweepAge : 5;

  // [feature 3] ATR percentile proxy
  const atrPercentile = Math.max(0, Math.min(1, ((signal.confidence || 60) - 60) / 40));

  // [feature 0] RSI
  const rsi = signal.rsi != null ? signal.rsi :
    signal.direction === 'BUY'  ? Math.max(10, 35 - ((signal.confidence || 60) - 60) / 2) :
    signal.direction === 'SELL' ? Math.min(90, 65 + ((signal.confidence || 60) - 60) / 2) : 50;

  // [feature 1] MACD histogram normalised
  const macdHistogram = signal.macd != null ? Math.max(-1, Math.min(1, signal.macd)) : 0;

  // [feature 2] ADX
  const adx = signal.adx != null ? signal.adx : 25;

  // [feature 12] Signal type win rate
  let typeWinRate = 0.5;
  if (paperTypeStats && signal.signalTypeKey != null) {
    const typeStats = paperTypeStats.get
      ? paperTypeStats.get(signal.signalTypeKey)
      : paperTypeStats[signal.signalTypeKey];
    if (typeStats && typeStats.total >= 20) {
      typeWinRate = typeStats.wins / typeStats.total;
    }
  }

  return [
    rsi,               // 0  — RSI value
    macdHistogram,     // 1  — MACD histogram (-1 to 1)
    adx,               // 2  — ADX (0-100)
    atrPercentile,     // 3  — ATR percentile proxy (0-1)
    obDistancePips,    // 4  — OB midpoint distance in pips (0-50)
    fvgSizePips,       // 5  — FVG size in pips (0-50)
    candlesSinceSweep, // 6  — Candles since last sweep
    sessionQuality,    // 7  — Killzone session quality (0.3-1.0)
    dxyEncoded,        // 8  — DXY alignment (-1, 0, 1)
    hourSin,           // 9  — Hour sine
    hourCos,           // 10 — Hour cosine
    weekday,           // 11 — Weekday (0=Mon … 4=Fri)
    typeWinRate        // 12 — Historical win rate for signal type
  ];
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

function initEnsembleScorer(resolvedSignals) {
  const count = (resolvedSignals || []).length;
  if (count < 100) {
    console.log(`[Ensemble] Insufficient resolved signals on startup (${count}). Model will activate once 100+ outcomes are available.`);
    return;
  }

  state.resolvedSignalCount = count;
  state.trainingSampleCount = count;

  if (!state.worker) state.worker = spawnWorker();

  console.log(`[Ensemble] Starting initial training with ${count} samples...`);
  state.worker.postMessage({ type: 'train', samples: resolvedSignals });
}

function scoreSignal(features) {
  if (!state.modelTrained || !state.worker) {
    return Promise.resolve(null);
  }

  return new Promise((resolve) => {
    const id = String(++state.scoreCounter);
    const timeout = setTimeout(() => {
      state.pendingScores.delete(id);
      console.warn('[Ensemble] Score timeout for id:', id);
      resolve(50);
    }, 5000);

    state.pendingScores.set(id, {
      resolve: (score) => { clearTimeout(timeout); resolve(score); },
      reject:  ()      => { clearTimeout(timeout); resolve(50);    }
    });

    state.worker.postMessage({ type: 'score', id, features });
  });
}

function retrainIfNeeded(resolvedSignals) {
  if (!resolvedSignals || resolvedSignals.length < 100) return;

  newResolvedSinceTrain++;

  const daysSinceTraining = state.lastTrainedAt
    ? (Date.now() - new Date(state.lastTrainedAt).getTime()) / (1000 * 60 * 60 * 24)
    : Infinity;

  if (daysSinceTraining >= RETRAIN_DAYS || newResolvedSinceTrain >= RETRAIN_NEW_SIGNALS) {
    if (!state.worker) state.worker = spawnWorker();
    state.trainingSampleCount = resolvedSignals.length;
    console.log(`[Ensemble] Retraining triggered. Days since last train: ${daysSinceTraining.toFixed(1)}, new resolved: ${newResolvedSinceTrain}. Samples: ${resolvedSignals.length}`);
    state.worker.postMessage({ type: 'retrain', samples: resolvedSignals });
    // Reset counter immediately so we don't flood retrain calls while training is in progress.
    // The trained handler will also reset it on success.
    newResolvedSinceTrain = 0;
  }
}

function getEnsembleStatus() {
  const nextRetrainAt = state.modelTrained && state.lastTrainedAt
    ? new Date(new Date(state.lastTrainedAt).getTime() + 7 * 24 * 60 * 60 * 1000).toISOString()
    : 'Pending minimum samples (need 100 resolved signals)';
  return {
    modelTrained:          state.modelTrained,
    lastTrainedAt:         state.lastTrainedAt,
    resolvedSignalCount:   state.resolvedSignalCount,
    trainingSampleCount:   state.trainingSampleCount,
    oosAccuracy:           state.oosAccuracy,
    newResolvedSinceTrain: newResolvedSinceTrain,
    nextRetrainAt
  };
}

module.exports = {
  initEnsembleScorer,
  scoreSignal,
  retrainIfNeeded,
  extractFeatures,
  getEnsembleStatus
};
