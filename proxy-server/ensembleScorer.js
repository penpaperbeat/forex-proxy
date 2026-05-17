'use strict';

const { Worker } = require('worker_threads');
const path = require('path');

const state = { modelTrained: false, lastTrainedAt: null, resolvedSignalCount: 0, trainingSampleCount: 0, oosAccuracy: null, worker: null, pendingScores: new Map(), scoreCounter: 0 };
const RETRAIN_DAYS = 7, RETRAIN_NEW_SIGNALS = 50;
let newResolvedSinceTrain = 0;

function spawnWorker() {
  const w = new Worker(path.join(__dirname, 'ensembleWorker.js'));
  w.on('message', msg => {
    if (msg.type === 'trained') { if (msg.error) { console.warn('[Ensemble] Training failed:', msg.error); state.modelTrained = false; } else { state.modelTrained = true; state.lastTrainedAt = new Date().toISOString(); state.oosAccuracy = msg.oosAccuracy; newResolvedSinceTrain = 0; console.log(`[Ensemble] Model trained. OOS accuracy: ${msg.oosAccuracy !== null ? msg.oosAccuracy + '%' : 'N/A'}`); } }
    else if (msg.type === 'scored') { const pending = state.pendingScores.get(msg.id); if (pending) { state.pendingScores.delete(msg.id); pending.resolve(msg.score); } }
  });
  w.on('error', err => { console.error('[Ensemble] Worker error:', err.message); for (const [, p] of state.pendingScores) p.resolve(50); state.pendingScores.clear(); state.modelTrained = false; setTimeout(() => { state.worker = spawnWorker(); }, 5000); });
  w.on('exit', code => { if (code !== 0) { for (const [, p] of state.pendingScores) p.resolve(50); state.pendingScores.clear(); setTimeout(() => { state.worker = spawnWorker(); }, 5000); } });
  return w;
}

function extractFeatures(signal, paperTypeStats) {
  const hour = signal.generatedAt ? new Date(signal.generatedAt).getUTCHours() : 12;
  const dow = signal.generatedAt ? new Date(signal.generatedAt).getUTCDay() : 2;
  const weekday = Math.max(0, Math.min(4, dow === 0 ? 4 : dow - 1));
  const dxyMap = { USD_STRONG: 1, NEUTRAL: 0, USD_WEAK: -1 };
  const sessionQualityMap = { 'New York Open': 1.0, 'London Open': 0.8, 'London Close': 0.7, 'Asian Open': 0.5 };
  const sessionQuality = signal.killzoneName ? (sessionQualityMap[signal.killzoneName] || 0.3) : 0.3;
  let obDistancePips = 0;
  if (signal.obZone && signal.obZone.low != null) { const obMid = (signal.obZone.low + signal.obZone.high) / 2; obDistancePips = Math.min(50, Math.abs((signal.entryPrice || 0) - obMid) / (obMid > 10 ? 0.01 : 0.0001)); }
  let fvgSizePips = 0;
  if (signal.fvgZone && signal.fvgZone.low != null) { const obMid = signal.obZone ? (signal.obZone.low + signal.obZone.high) / 2 : 1; fvgSizePips = Math.min(50, (signal.fvgZone.high - signal.fvgZone.low) / (obMid > 10 ? 0.01 : 0.0001)); }
  const rsi = signal.rsi != null ? signal.rsi : signal.direction === 'BUY' ? Math.max(10, 35 - ((signal.confidence || 60) - 60) / 2) : signal.direction === 'SELL' ? Math.min(90, 65 + ((signal.confidence || 60) - 60) / 2) : 50;
  let typeWinRate = 0.5;
  if (paperTypeStats && signal.signalTypeKey != null) { const ts = paperTypeStats.get ? paperTypeStats.get(signal.signalTypeKey) : paperTypeStats[signal.signalTypeKey]; if (ts && ts.total >= 20) typeWinRate = ts.wins / ts.total; }
  return [rsi, signal.macd != null ? Math.max(-1, Math.min(1, signal.macd)) : 0, signal.adx != null ? signal.adx : 25, Math.max(0, Math.min(1, ((signal.confidence || 60) - 60) / 40)), obDistancePips, fvgSizePips, 5, sessionQuality, dxyMap[signal.dxyBias] !== undefined ? dxyMap[signal.dxyBias] : 0, Math.sin(2 * Math.PI * hour / 24), Math.cos(2 * Math.PI * hour / 24), weekday, typeWinRate];
}

function initEnsembleScorer(resolvedSignals) {
  if (!resolvedSignals || resolvedSignals.length < 100) { console.log(`[Ensemble] Insufficient training data (${(resolvedSignals || []).length} signals). Scoring disabled.`); return; }
  state.resolvedSignalCount = state.trainingSampleCount = resolvedSignals.length;
  if (!state.worker) state.worker = spawnWorker();
  state.worker.postMessage({ type: 'train', samples: resolvedSignals });
}

function scoreSignal(features) {
  if (!state.modelTrained || !state.worker) return Promise.resolve(null);
  return new Promise(resolve => {
    const id = String(++state.scoreCounter);
    const timeout = setTimeout(() => { state.pendingScores.delete(id); resolve(50); }, 5000);
    state.pendingScores.set(id, { resolve: score => { clearTimeout(timeout); resolve(score); }, reject: () => { clearTimeout(timeout); resolve(50); } });
    state.worker.postMessage({ type: 'score', id, features });
  });
}

function retrainIfNeeded(resolvedSignals) {
  if (!resolvedSignals || resolvedSignals.length < 100) return;
  newResolvedSinceTrain++;
  const daysSince = state.lastTrainedAt ? (Date.now() - new Date(state.lastTrainedAt).getTime()) / (1000 * 60 * 60 * 24) : Infinity;
  if (daysSince >= RETRAIN_DAYS || newResolvedSinceTrain >= RETRAIN_NEW_SIGNALS) { if (!state.worker) state.worker = spawnWorker(); state.trainingSampleCount = resolvedSignals.length; state.worker.postMessage({ type: 'retrain', samples: resolvedSignals }); }
}

function getEnsembleStatus() { return { modelTrained: state.modelTrained, lastTrainedAt: state.lastTrainedAt, resolvedSignalCount: state.resolvedSignalCount, trainingSampleCount: state.trainingSampleCount, oosAccuracy: state.oosAccuracy }; }

module.exports = { initEnsembleScorer, scoreSignal, retrainIfNeeded, extractFeatures, getEnsembleStatus };
