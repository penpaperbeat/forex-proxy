'use strict';

/**
 * ensembleScorer.js
 * Main-thread interface to the ensemble worker.
 * Uses worker_threads (Node.js built-in — no npm install required).
 *
 * Public API:
 *   initEnsembleScorer(resolvedSignals)  — train if >= 100 samples
 *   scoreSignal(features)               — Promise<number> 0-100 (50 = neutral/untrained)
 *   retrainIfNeeded(resolvedSignals)     — retrain if 7 days elapsed or 50+ new signals
 *   extractFeatures(signal, paperTypeStats) — returns 13-element number[]
 *   getEnsembleStatus()                  — returns status object for /ensemble-status endpoint
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
  // Worker lifecycle
  worker:              null,
  pendingScores:       new Map(), // id -> { resolve, reject }
  scoreCounter:        0
};

// Retrain thresholds
const RETRAIN_DAYS         = 7;
const RETRAIN_NEW_SIGNALS  = 50;
let   signalsSinceRetrain  = 0;
let   newResolvedSinceTrain = 0;

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
        state.modelTrained  = true;
        state.lastTrainedAt = new Date().toISOString();
        state.oosAccuracy   = msg.oosAccuracy;
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
    // Resolve all pending scores with neutral fallback
    for (const [id, pending] of state.pendingScores) {
      pending.resolve(50);
    }
    state.pendingScores.clear();
    state.modelTrained = false;
    // Respawn after a short delay
    setTimeout(() => {
      state.worker = spawnWorker();
    }, 5000);
  });

  w.on('exit', (code) => {
    if (code !== 0) {
      console.warn(`[Ensemble] Worker exited with code ${code}. Respawning...`);
      for (const [id, pending] of state.pendingScores) {
        pending.resolve(50);
      }
      state.pendingScores.clear();
      setTimeout(() => {
        state.worker = spawnWorker();
      }, 5000);
    }
  });

  return w;
}

// ---------------------------------------------------------------------------
// Feature extraction
// ---------------------------------------------------------------------------

/**
 * extractFeatures(signal, paperTypeStats)
 * signal: object from runSMCEvaluation (signalBase shape)
 * paperTypeStats: Map<number, { wins: number, total: number }> or null
 * Returns 13-element number[]
 */
function extractFeatures(signal, paperTypeStats) {
  const hour   = signal.generatedAt ? new Date(signal.generatedAt).getUTCHours() : 12;
  const dow    = signal.generatedAt ? new Date(signal.generatedAt).getUTCDay()   : 2;
  // day 0=Sun,6=Sat — clamp to weekday 0-4 (Mon=0)
  const weekday = Math.max(0, Math.min(4, dow === 0 ? 4 : dow - 1));

  const hourSin = Math.sin(2 * Math.PI * hour / 24);
  const hourCos = Math.cos(2 * Math.PI * hour / 24);

  // [feature 8] DXY alignment: USD_STRONG=1, NEUTRAL=0, USD_WEAK=-1
  const dxyMap     = { USD_STRONG: 1, NEUTRAL: 0, USD_WEAK: -1 };
  const dxyEncoded = dxyMap[signal.dxyBias] !== undefined ? dxyMap[signal.dxyBias] : 0;

  // [feature 7] Session quality: 1.0 = NY open (best), 0.8 = London open, 0.7 = London close, 0.5 = Asian, 0.3 = other/unknown
  const sessionQualityMap = {
    'New York Open': 1.0,
    'London Open':   0.8,
    'London Close':  0.7,
    'Asian Open':    0.5
  };
  // Guard against null/undefined killzoneName
  const killzoneName   = signal.killzoneName || null;
  const sessionQuality = killzoneName ? (sessionQualityMap[killzoneName] ?? 0.3) : 0.3;

  // [feature 4] OB distance in pips (0 if not present)
  let obDistancePips = 0;
  if (signal.obZone && signal.obZone.low != null && signal.obZone.high != null) {
    const currentPrice = signal.entryPrice || signal.close || 0;
    const obMid        = (signal.obZone.low + signal.obZone.high) / 2;
    // Pip factor: JPY pairs have obMid > 10 (prices ~100-200), others use 0.0001
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

  // [feature 6] Candles since sweep (default 5 = not recent)
  const candlesSinceSweep = 5;

  // [feature 3] ATR percentile from confidence proxy (0-1): confidence is 60-100, map to 0-1
  const atrPercentile = Math.max(0, Math.min(1, ((signal.confidence || 60) - 60) / 40));

  // [feature 0] RSI: use signal.rsi if available, otherwise estimate from confidence
  const rsi = signal.rsi != null ? signal.rsi :
    signal.direction === 'BUY'  ? Math.max(10, 35 - ((signal.confidence || 60) - 60) / 2) :
    signal.direction === 'SELL' ? Math.min(90, 65 + ((signal.confidence || 60) - 60) / 2) : 50;

  // [feature 1] MACD histogram: normalised -1 to 1, use 0 if not available
  const macdHistogram = signal.macd != null ? Math.max(-1, Math.min(1, signal.macd)) : 0;

  // [feature 2] ADX: 0-100, default 25 (neutral trend strength)
  const adx = signal.adx != null ? signal.adx : 25;

  // [feature 12] Signal type win rate from paperTypeStats
  let typeWinRate = 0.5; // default neutral
  if (paperTypeStats && signal.signalTypeKey != null) {
    const typeStats = paperTypeStats.get ? paperTypeStats.get(signal.signalTypeKey) :
                      paperTypeStats[signal.signalTypeKey];
    if (typeStats && typeStats.total >= 20) {
      typeWinRate = typeStats.wins / typeStats.total;
    }
  }

  // Feature vector — 13 elements (indices 0-12)
  return [
    rsi,               // 0  — RSI value (estimated or actual)
    macdHistogram,     // 1  — MACD histogram normalised (-1 to 1)
    adx,               // 2  — ADX trend strength (0-100)
    atrPercentile,     // 3  — ATR percentile proxy (0-1)
    obDistancePips,    // 4  — Distance from OB midpoint in pips (0-50)
    fvgSizePips,       // 5  — FVG size in pips (0-50)
    candlesSinceSweep, // 6  — Candles since last sweep (default 5)
    sessionQuality,    // 7  — Killzone session quality (0.3-1.0)
    dxyEncoded,        // 8  — DXY alignment (-1, 0, 1)
    hourSin,           // 9  — Hour of day (sine component)
    hourCos,           // 10 — Hour of day (cosine component)
    weekday,           // 11 — Weekday (0=Mon ... 4=Fri)
    typeWinRate        // 12 — Historical win rate for this signal type key
  ];
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * initEnsembleScorer(resolvedSignals)
 * resolvedSignals: Array of { features: number[], outcome: bool }
 * Trains if >= 100 samples, otherwise logs and does nothing.
 */
function initEnsembleScorer(resolvedSignals) {
  if (!resolvedSignals || resolvedSignals.length < 100) {
    console.log(`[Ensemble] Insufficient training data (${(resolvedSignals || []).length} resolved signals). Scoring disabled.`);
    return;
  }

  state.resolvedSignalCount = resolvedSignals.length;
  state.trainingSampleCount = resolvedSignals.length;

  if (!state.worker) {
    state.worker = spawnWorker();
  }

  console.log(`[Ensemble] Starting initial training with ${resolvedSignals.length} samples...`);
  state.worker.postMessage({ type: 'train', samples: resolvedSignals });
}

/**
 * scoreSignal(features)
 * Returns Promise<number> (0-100). Returns 50 if model not trained.
 * Timeout of 5s to avoid hanging the signal pipeline.
 */
function scoreSignal(features) {
  if (!state.modelTrained || !state.worker) {
    return Promise.resolve(null); // null = model not trained yet
  }

  return new Promise((resolve) => {
    const id = String(++state.scoreCounter);
    const timeout = setTimeout(() => {
      state.pendingScores.delete(id);
      console.warn('[Ensemble] Score timeout for id:', id);
      resolve(50);
    }, 5000);

    state.pendingScores.set(id, {
      resolve: (score) => {
        clearTimeout(timeout);
        resolve(score);
      },
      reject: () => {
        clearTimeout(timeout);
        resolve(50);
      }
    });

    state.worker.postMessage({ type: 'score', id, features });
  });
}

/**
 * retrainIfNeeded(resolvedSignals)
 * Triggers retrain if:
 *   - 7 or more days have passed since last training, OR
 *   - 50+ new resolved signals have accumulated
 */
function retrainIfNeeded(resolvedSignals) {
  if (!resolvedSignals || resolvedSignals.length < 100) return;

  newResolvedSinceTrain++;

  const daysSinceTraining = state.lastTrainedAt
    ? (Date.now() - new Date(state.lastTrainedAt).getTime()) / (1000 * 60 * 60 * 24)
    : Infinity;

  if (daysSinceTraining >= RETRAIN_DAYS || newResolvedSinceTrain >= RETRAIN_NEW_SIGNALS) {
    if (!state.worker) {
      state.worker = spawnWorker();
    }
    state.trainingSampleCount = resolvedSignals.length;
    console.log(`[Ensemble] Retraining triggered. Days since last train: ${daysSinceTraining.toFixed(1)}, new resolved: ${newResolvedSinceTrain}. Samples: ${resolvedSignals.length}`);
    state.worker.postMessage({ type: 'retrain', samples: resolvedSignals });
  }
}

/**
 * getEnsembleStatus()
 * Returns status object for the /ensemble-status endpoint.
 */
function getEnsembleStatus() {
  return {
    modelTrained:        state.modelTrained,
    lastTrainedAt:       state.lastTrainedAt,
    resolvedSignalCount: state.resolvedSignalCount,
    trainingSampleCount: state.trainingSampleCount,
    oosAccuracy:         state.oosAccuracy
  };
}

module.exports = {
  initEnsembleScorer,
  scoreSignal,
  retrainIfNeeded,
  extractFeatures,
  getEnsembleStatus
};
