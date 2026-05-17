'use strict';

const fs = require('fs');

const MODEL_PATH = '/data/ensemble-model.json';
const MIN_TRAINING_SAMPLES = 100;

// Internal state
let modelWeights = null;        // trained logistic regression weights
let modelFeatureNames = null;
let trainingCount = 0;
let isModelReady = false;
let lastTrainedAt = null;
let totalScored = 0;
let totalPassThrough = 0;

// FIX #15: Load persisted model on startup
try {
  if (fs.existsSync(MODEL_PATH)) {
    const raw = fs.readFileSync(MODEL_PATH, 'utf8');
    const saved = JSON.parse(raw);
    modelWeights = saved.weights;
    modelFeatureNames = saved.featureNames;
    trainingCount = saved.trainingCount || 0;
    lastTrainedAt = saved.trainedAt || null;
    isModelReady = true;
    console.log(`[ensemble] Model restored from disk. Trained on ${trainingCount} samples.`);
  }
} catch (err) {
  console.warn('[ensemble] Could not load persisted model:', err.message);
}

/**
 * Extract a fixed-length feature vector from a signal object.
 * Returns an array of numbers. Unknown/null values become 0.
 */
function extractFeatures(signal, _candles) {
  const dir = signal.direction === 'BUY' ? 1 : -1;
  const confidence = (signal.confidence || 0) / 100;
  const ob = signal.orderBlockPresent ? 1 : 0;
  const fvg = signal.fvgPresent ? 1 : 0;
  const sweep = signal.liquiditySweepPresent ? 1 : 0;
  const kz = signal.killzoneActive ? 1 : 0;

  // DXY alignment: +1 if signal aligns with DXY bias, -1 if opposed, 0 if neutral
  let dxyAlign = 0;
  const bias = signal.dxyBias || 'NEUTRAL';
  if (bias === 'BULLISH') dxyAlign = dir === 1 ? -1 : 1;  // DXY bullish = USD strong = bad for EUR/GBP BUY
  if (bias === 'BEARISH') dxyAlign = dir === 1 ? 1 : -1;

  // Hour of day (0–23 normalised)
  let hourNorm = 0;
  if (signal.generatedAt) {
    try { hourNorm = new Date(signal.generatedAt).getUTCHours() / 23; } catch (_) {}
  }

  // Trinity score: 0–3 patterns present
  const trinityScore = (ob + fvg + sweep) / 3;

  return [dir, confidence, ob, fvg, sweep, kz, dxyAlign, hourNorm, trinityScore];
}

const FEATURE_NAMES = ['direction', 'confidence', 'ob', 'fvg', 'sweep', 'killzone', 'dxyAlign', 'hourNorm', 'trinityScore'];

/**
 * Simple logistic regression trained with gradient descent.
 * Good enough for the feature set we have; keeps zero dependencies.
 */
function sigmoid(z) {
  return 1 / (1 + Math.exp(-z));
}

function trainLogisticRegression(samples, iterations = 500, lr = 0.05) {
  const n = samples[0].features.length;
  let w = new Array(n).fill(0);
  let b = 0;

  for (let iter = 0; iter < iterations; iter++) {
    let dw = new Array(n).fill(0);
    let db = 0;

    for (const { features, outcome } of samples) {
      const z = features.reduce((sum, x, i) => sum + x * w[i], b);
      const pred = sigmoid(z);
      const err = pred - (outcome ? 1 : 0);
      for (let i = 0; i < n; i++) dw[i] += err * features[i];
      db += err;
    }

    const m = samples.length;
    for (let i = 0; i < n; i++) w[i] -= (lr / m) * dw[i];
    b -= (lr / m) * db;
  }

  return { weights: w, bias: b };
}

/**
 * Train (or retrain) the ensemble scorer on resolved signal data.
 * @param {Array<{features: number[], outcome: boolean}>} trainingData
 */
function initEnsembleScorer(trainingData) {
  if (!trainingData || trainingData.length < MIN_TRAINING_SAMPLES) {
    console.log(`[ensemble] Not enough samples to train (${(trainingData || []).length}/${MIN_TRAINING_SAMPLES}). Model stays as-is.`);
    return;
  }

  // Filter out any malformed samples
  const valid = trainingData.filter(d =>
    Array.isArray(d.features) &&
    d.features.length === FEATURE_NAMES.length &&
    d.features.every(v => typeof v === 'number' && isFinite(v)) &&
    typeof d.outcome === 'boolean'
  );

  if (valid.length < MIN_TRAINING_SAMPLES) {
    console.warn(`[ensemble] Only ${valid.length} valid samples after filtering. Skipping train.`);
    return;
  }

  try {
    console.log(`[ensemble] Training on ${valid.length} samples...`);
    const result = trainLogisticRegression(valid);
    modelWeights = result.weights;
    modelFeatureNames = FEATURE_NAMES;
    trainingCount = valid.length;
    isModelReady = true;
    lastTrainedAt = new Date().toISOString();

    // FIX #15: persist model to disk
    try {
      fs.writeFileSync(MODEL_PATH, JSON.stringify({
        weights: modelWeights,
        bias: result.bias,
        featureNames: FEATURE_NAMES,
        trainingCount,
        trainedAt: lastTrainedAt
      }), 'utf8');
      console.log(`[ensemble] Model saved to disk.`);
    } catch (writeErr) {
      console.warn('[ensemble] Could not persist model:', writeErr.message);
    }

    console.log(`[ensemble] Training complete. Weights: ${modelWeights.map((w, i) => `${FEATURE_NAMES[i]}=${w.toFixed(3)}`).join(', ')}`);
  } catch (trainErr) {
    // FIX #13: surface training errors
    console.error('[ensemble] Training failed:', trainErr.message);
  }
}

// Load bias from persisted model (needed after restart)
let modelBias = 0;
try {
  if (fs.existsSync(MODEL_PATH)) {
    const saved = JSON.parse(fs.readFileSync(MODEL_PATH, 'utf8'));
    modelBias = saved.bias || 0;
  }
} catch (_) {}

/**
 * Score a signal's feature vector. Returns 0–100 or null.
 * FIX #13: errors are caught and logged, not silently swallowed.
 * FIX #14: pass-through score returned when model not ready.
 */
async function scoreSignal(features) {
  totalScored++;

  // FIX #14: graceful degradation — return confidence-based pass-through when model not ready
  if (!isModelReady || !modelWeights) {
    totalPassThrough++;
    // Return a basic score derived from features (trinityScore * confidence * 100)
    const trinity = features[8] || 0;
    const confidence = features[1] || 0;
    return Math.round((trinity * 0.4 + confidence * 0.6) * 100);
  }

  try {
    const z = features.reduce((sum, x, i) => sum + x * (modelWeights[i] || 0), modelBias);
    const prob = sigmoid(z);
    return Math.round(prob * 100);
  } catch (err) {
    // FIX #13: log scoring errors
    console.warn('[ensemble] scoreSignal error:', err.message);
    return null;
  }
}

/**
 * Returns current ensemble model status for the /ensemble-status endpoint.
 */
function getEnsembleStatus() {
  return {
    ready: isModelReady,
    trainingCount,
    lastTrainedAt,
    totalScored,
    totalPassThrough,
    featureNames: FEATURE_NAMES,
    modelWeights: isModelReady ? modelWeights : null
  };
}

module.exports = {
  extractFeatures,
  initEnsembleScorer,
  scoreSignal,
  getEnsembleStatus
};
