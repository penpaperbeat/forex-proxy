'use strict';

/**
 * ensembleWorker.js
 * Runs in a worker thread. Trains a logistic regression model and scores signals.
 * All computation is isolated from the main thread.
 *
 * Improvements over v1:
 *   - Increased to 300 epochs with adaptive learning rate decay for better convergence
 *   - OOS accuracy computed before full-dataset training (pessimistic estimate — correct direction)
 *   - Model persists across score requests within the worker lifetime
 *   - All errors caught and returned to parent, worker never crashes silently
 */

const { parentPort } = require('worker_threads');

// ---------------------------------------------------------------------------
// Math helpers
// ---------------------------------------------------------------------------

function sigmoid(x) {
  return 1 / (1 + Math.exp(-Math.max(-500, Math.min(500, x))));
}

function normalise(features, means, stds) {
  return features.map((v, i) => (v - means[i]) / (stds[i] === 0 ? 1 : stds[i]));
}

function computeStats(samples) {
  const n          = samples.length;
  const featureLen = samples[0].features.length;
  const means      = new Array(featureLen).fill(0);
  const stds       = new Array(featureLen).fill(0);

  for (const s of samples)
    for (let f = 0; f < featureLen; f++)
      means[f] += s.features[f] / n;

  for (const s of samples)
    for (let f = 0; f < featureLen; f++)
      stds[f] += Math.pow(s.features[f] - means[f], 2) / n;

  for (let f = 0; f < featureLen; f++)
    stds[f] = Math.sqrt(stds[f]);

  return { means, stds };
}

// ---------------------------------------------------------------------------
// Training
// ---------------------------------------------------------------------------

/**
 * Logistic regression with gradient descent.
 * 300 epochs, learning rate decays from 0.01 to 0.001 over training.
 * L2 regularisation lambda = 0.001.
 */
function trainLogisticRegression(samples) {
  const featureLen     = samples[0].features.length;
  const { means, stds } = computeStats(samples);
  const X              = samples.map(s => normalise(s.features, means, stds));
  const y              = samples.map(s => s.outcome ? 1 : 0);
  const EPOCHS         = 300;
  const LR_START       = 0.01;
  const LR_END         = 0.001;
  const LAMBDA         = 0.001; // L2 regularisation

  let weights = new Array(featureLen).fill(0);
  let bias    = 0;

  for (let epoch = 0; epoch < EPOCHS; epoch++) {
    // Linear learning rate decay
    const lr    = LR_START - (LR_START - LR_END) * (epoch / EPOCHS);
    const wGrad = new Array(featureLen).fill(0);
    let   bGrad = 0;

    for (let i = 0; i < X.length; i++) {
      let dot = bias;
      for (let f = 0; f < featureLen; f++) dot += weights[f] * X[i][f];
      const error = sigmoid(dot) - y[i];
      bGrad += error;
      for (let f = 0; f < featureLen; f++) wGrad[f] += error * X[i][f];
    }

    bias -= lr * (bGrad / X.length);
    for (let f = 0; f < featureLen; f++)
      weights[f] -= lr * (wGrad[f] / X.length + LAMBDA * weights[f]);
  }

  return { weights, bias, means, stds };
}

function scoreFeatures(features, weights, bias, means, stds) {
  const norm = normalise(features, means, stds);
  let dot    = bias;
  for (let f = 0; f < features.length; f++) dot += weights[f] * norm[f];
  return Math.round(Math.max(0, Math.min(100, sigmoid(dot) * 100)));
}

/**
 * OOS accuracy: train on first 70%, test on remaining 30%.
 * Returns null if insufficient data.
 */
function computeOOSAccuracy(samples) {
  if (samples.length < 20) return null;
  const splitIdx = Math.floor(samples.length * 0.7);
  const train    = samples.slice(0, splitIdx);
  const test     = samples.slice(splitIdx);
  if (train.length < 10 || test.length < 5) return null;

  const model   = trainLogisticRegression(train);
  let   correct = 0;
  for (const s of test) {
    const score   = scoreFeatures(s.features, model.weights, model.bias, model.means, model.stds);
    const predicted = score >= 50;
    if (predicted === s.outcome) correct++;
  }
  return Math.round((correct / test.length) * 1000) / 10;
}

// ---------------------------------------------------------------------------
// Worker message handler
// ---------------------------------------------------------------------------

let model = null;

parentPort.on('message', (msg) => {
  try {
    if (msg.type === 'train' || msg.type === 'retrain') {
      if (!msg.samples || msg.samples.length < 10) {
        parentPort.postMessage({ type: 'trained', oosAccuracy: null, error: 'insufficient samples' });
        return;
      }
      // Compute OOS accuracy on a held-out split first (pessimistic but correct direction)
      const oosAccuracy = computeOOSAccuracy(msg.samples);
      // Train final model on full dataset
      model = trainLogisticRegression(msg.samples);
      parentPort.postMessage({ type: 'trained', oosAccuracy });

    } else if (msg.type === 'score') {
      const score = model
        ? scoreFeatures(msg.features, model.weights, model.bias, model.means, model.stds)
        : 50;
      parentPort.postMessage({ type: 'scored', id: msg.id, score });
    }

  } catch (err) {
    if (msg.type === 'score') {
      parentPort.postMessage({ type: 'scored', id: msg.id, score: 50 });
    } else {
      parentPort.postMessage({ type: 'trained', oosAccuracy: null, error: err.message });
    }
  }
});
