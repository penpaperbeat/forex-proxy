'use strict';

const { parentPort } = require('worker_threads');

function sigmoid(x) { return 1 / (1 + Math.exp(-Math.max(-500, Math.min(500, x)))); }
function normalise(features, means, stds) { return features.map((v, i) => (v - means[i]) / (stds[i] === 0 ? 1 : stds[i])); }

function computeStats(samples) {
  const n = samples.length, featureLen = samples[0].features.length;
  const means = new Array(featureLen).fill(0), stds = new Array(featureLen).fill(0);
  for (const s of samples) for (let f = 0; f < featureLen; f++) means[f] += s.features[f] / n;
  for (const s of samples) for (let f = 0; f < featureLen; f++) stds[f] += Math.pow(s.features[f] - means[f], 2) / n;
  for (let f = 0; f < featureLen; f++) stds[f] = Math.sqrt(stds[f]);
  return { means, stds };
}

function trainLogisticRegression(samples) {
  const featureLen = samples[0].features.length;
  const { means, stds } = computeStats(samples);
  const X = samples.map(s => normalise(s.features, means, stds)), y = samples.map(s => s.outcome ? 1 : 0);
  let weights = new Array(featureLen).fill(0), bias = 0;
  for (let epoch = 0; epoch < 100; epoch++) {
    let wGrad = new Array(featureLen).fill(0), bGrad = 0;
    for (let i = 0; i < X.length; i++) { let dot = bias; for (let f = 0; f < featureLen; f++) dot += weights[f] * X[i][f]; const error = sigmoid(dot) - y[i]; bGrad += error; for (let f = 0; f < featureLen; f++) wGrad[f] += error * X[i][f]; }
    bias -= 0.01 * (bGrad / X.length);
    for (let f = 0; f < featureLen; f++) weights[f] -= 0.01 * (wGrad[f] / X.length + 0.001 * weights[f]);
  }
  return { weights, bias, means, stds };
}

function scoreFeatures(features, weights, bias, means, stds) { const norm = normalise(features, means, stds); let dot = bias; for (let f = 0; f < features.length; f++) dot += weights[f] * norm[f]; return Math.round(Math.max(0, Math.min(100, sigmoid(dot) * 100))); }

function computeOOSAccuracy(samples) {
  if (samples.length < 20) return null;
  const splitIdx = Math.floor(samples.length * 0.7), train = samples.slice(0, splitIdx), test = samples.slice(splitIdx);
  if (train.length < 10 || test.length < 5) return null;
  const model = trainLogisticRegression(train);
  let correct = 0;
  for (const s of test) { if ((scoreFeatures(s.features, model.weights, model.bias, model.means, model.stds) >= 50) === s.outcome) correct++; }
  return Math.round((correct / test.length) * 1000) / 10;
}

let model = null;

parentPort.on('message', msg => {
  try {
    if (msg.type === 'train' || msg.type === 'retrain') {
      if (!msg.samples || msg.samples.length < 10) { parentPort.postMessage({ type: 'trained', weights: null, oosAccuracy: null, error: 'insufficient samples' }); return; }
      const oosAccuracy = computeOOSAccuracy(msg.samples); model = trainLogisticRegression(msg.samples);
      parentPort.postMessage({ type: 'trained', weights: model.weights, oosAccuracy });
    } else if (msg.type === 'score') {
      parentPort.postMessage({ type: 'scored', id: msg.id, score: model ? scoreFeatures(msg.features, model.weights, model.bias, model.means, model.stds) : 50 });
    }
  } catch (err) {
    if (msg.type === 'score') parentPort.postMessage({ type: 'scored', id: msg.id, score: 50 });
    else parentPort.postMessage({ type: 'trained', weights: null, oosAccuracy: null, error: err.message });
  }
});
