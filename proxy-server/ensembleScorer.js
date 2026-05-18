'use strict';

const fs = require('fs');

const MODEL_PATH = '/data/ensemble-model.json';
const MIN_TRAINING_SAMPLES = 100;

// Internal state
let modelWeights = null;
let modelBias = 0;
let modelFeatureNames = null;
let trainingCount = 0;
let isModelReady = false;
let lastTrainedAt = null;
let totalScored = 0;
let totalPassThrough = 0;

// FIX #2: load weights AND bias in a single startup block
try {
  if (fs.existsSync(MODEL_PATH)) {
    const raw = fs.readFileSync(MODEL_PATH, 'utf8');
    const saved = JSON.parse(raw);
    modelWeights      = saved.weights;
    modelBias         = saved.bias || 0;
    modelFeatureNames = saved.featureNames;
    trainingCount     = saved.trainingCount || 0;
    lastTrainedAt     = saved.trainedAt || null;
    isModelReady      = true;
    console.log(`[ensemble] Model restored from disk. Trained on ${trainingCount} samples.`);
  }
} catch (err) {
  console.warn('[ensemble] Could not load persisted model:', err.message);
}

function extractFeatures(signal, _candles) {
  const dir        = signal.direction === 'BUY' ? 1 : -1;
  const confidence = (signal.confidence || 0) / 100;
  const ob         = signal.orderBlockPresent     ? 1 : 0;
  const fvg        = signal.fvgPresent            ? 1 : 0;
  const sweep      = signal.liquiditySweepPresent ? 1 : 0;
  const kz         = signal.killzoneActive        ? 1 : 0;

  // FIX #1: corrected from BULLISH/BEARISH → USD_STRONG/USD_WEAK
  let dxyAlign = 0;
  const bias = signal.dxyBias || 'NEUTRAL';
  if (bias === 'USD_STRONG') dxyAlign = dir === 1 ? -1 : 1;
  if (bias === 'USD_WEAK')   dxyAlign = dir === 1 ? 1  : -1;

  let hourNorm = 0;
  if (signal.generatedAt) {
    try { hourNorm = new Date(signal.generatedAt).getUTCHours() / 23; } catch (_) {}
  }

  const trinityScore = (ob + fvg + sweep) / 3;

  return [dir, confidence, ob, fvg, sweep, kz, dxyAlign, hourNorm, trinityScore];
}

const FEATURE_NAMES = ['direction', 'confidence', 'ob', 'fvg', 'sweep', 'killzone', 'dxyAlign', 'hourNorm', 'trinityScore'];

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
      const z    = features.reduce((sum, x, i) => sum + x * w[i], b);
      const pred = sigmoid(z);
      const err  = pred - (outcome ? 1 : 0);
      for (let i = 0; i < n; i++) dw[i] += err * features[i];
      db += err;
    }
    const m = samples.length;
    for (let i = 0; i < n; i++) w[i] -= (lr / m) * dw[i];
    b -= (lr / m) * db;
  }

  return { weights: w, bias: b };
}

function initEnsembleScorer(trainingData) {
  if (!trainingData || trainingData.length < MIN_TRAINING_SAMPLES) {
    console.log(`[ensemble] Not enough samples to train (${(trainingData || []).length}/${MIN_TRAINING_SAMPLES}). Model stays as-is.`);
    return;
  }

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
    const result      = trainLogisticRegression(valid);
    modelWeights      = result.weights;
    modelBias         = result.bias;
    modelFeatureNames = FEATURE_NAMES;
    trainingCount     = valid.length;
    isModelReady      = true;
    lastTrainedAt     = new Date().toISOString();

    try {
      fs.writeFileSync(MODEL_PATH, JSON.stringify({
        weights:      modelWeights,
        bias:         modelBias,
        featureNames: FEATURE_NAMES,
        trainingCount,
        trainedAt:    lastTrainedAt
      }), 'utf8');
      console.log('[ensemble] Model saved to disk.');
    } catch (writeErr) {
      console.warn('[ensemble] Could not persist model:', writeErr.message);
    }

    console.log(`[ensemble] Training complete. Weights: ${modelWeights.map((w, i) => `${FEATURE_NAMES[i]}=${w.toFixed(3)}`).join(', ')}`);
  } catch (trainErr) {
    console.error('[ensemble] Training failed:', trainErr.message);
  }
}

// FIX #3: removed async — no async work done here
function scoreSignal(features) {
  totalScored++;

  if (!isModelReady || !modelWeights) {
    totalPassThrough++;
    const trinity    = features[8] || 0;
    const confidence = features[1] || 0;
    return Math.round((trinity * 0.4 + confidence * 0.6) * 100);
  }

  try {
    const z    = features.reduce((sum, x, i) => sum + x * (modelWeights[i] || 0), modelBias);
    const prob = sigmoid(z);
    return Math.round(prob * 100);
  } catch (err) {
    console.warn('[ensemble] scoreSignal error:', err.message);
    return null;
  }
}

// FIX #4+5: 'trained' added as alias for 'ready' — server.js previously read .trained which was always undefined
function getEnsembleStatus() {
  return {
    ready:           isModelReady,
    trained:         isModelReady,
    trainingCount,
    lastTrainedAt,
    totalScored,
    totalPassThrough,
    featureNames:    FEATURE_NAMES,
    modelWeights:    isModelReady ? modelWeights : null
  };
}

module.exports = {
  extractFeatures,
  initEnsembleScorer,
  scoreSignal,
  getEnsembleStatus
};
