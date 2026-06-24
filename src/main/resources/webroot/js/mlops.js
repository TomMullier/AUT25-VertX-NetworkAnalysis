/* ── MLOps Monitor — FlowVertex ────────────────────────────── */

const ws = new WebSocket(`ws://${location.host}/`);

/* ── state ─────────────────────────────────────────────────── */
const state = {
  total: 0,
  malicious: 0,
  benign: 0,
  // confusion matrix counters (estimated)
  tp: 0, tn: 0, fp: 0, fn: 0,
  // inference latency samples (ms) — derived from treatmentDelay
  latencies: [],
  // timeline: ring buffer of {t, benign, malicious} per 5s bucket
  timeline: [],
  currentBucket: null,
  // feature stats for drift (running mean + variance)
  features: {
    duration:    { name: 'flow duration (ms)',   vals: [], baseline: null },
    packets:     { name: 'packet count',          vals: [], baseline: null },
    bytes:       { name: 'bytes per packet',      vals: [], baseline: null },
    protocol:    { name: 'protocol distribution', vals: [], baseline: null },
  },
  // prediction log
  log: [],
  sessionStart: Date.now(),
};

const BUCKET_MS   = 5000;
const BASELINE_N  = 100;  // flows before we lock in baseline for drift
const MAX_LATENCY = 500;  // samples kept for latency histogram
const LOG_LIMIT   = 20;

/* ── WebSocket ──────────────────────────────────────────────── */
ws.onopen = () => {
  document.getElementById('ws-status').textContent = '🟢 connected';
};
ws.onclose = () => {
  document.getElementById('ws-status').textContent = '🔴 disconnected';
};
ws.onerror = () => {
  document.getElementById('ws-status').textContent = '❌ error';
};

ws.onmessage = (event) => {
  try {
    const data = JSON.parse(event.data);
    if (data.type === 'flow') ingestFlow(data);
  } catch (e) { /* ignore non-JSON */ }
};

/* ── Ingest a flow prediction ───────────────────────────────── */
function ingestFlow(flow) {
  state.total++;
  const isMalicious = flow.riskLabel === 'ML_DETECTION' || flow.riskSeverity === 'High';

  if (isMalicious) {
    state.malicious++;
    // Heuristic: if also has high packet count → likely TP, else FP candidate
    if (flow.packetCount > 10) { state.tp++; } else { state.fp++; }
  } else {
    state.benign++;
    state.tn++;
  }

  // Latency from treatmentDelay array (average of packet delays for this flow)
  if (Array.isArray(flow.treatmentDelay) && flow.treatmentDelay.length > 0) {
    const avg = flow.treatmentDelay.reduce((a, b) => a + b, 0) / flow.treatmentDelay.length;
    if (avg >= 0 && avg < 60000) {
      state.latencies.push(avg);
      if (state.latencies.length > MAX_LATENCY) state.latencies.shift();
    }
  }

  // Feature drift tracking
  if (flow.flowDurationMs != null)  trackFeature('duration', flow.flowDurationMs);
  if (flow.packetCount != null)     trackFeature('packets', flow.packetCount);
  if (flow.bytes != null && flow.packetCount > 0)
    trackFeature('bytes', flow.bytes / flow.packetCount);
  const protoNum = flow.protocol === 'TCP' ? 1 : flow.protocol === 'UDP' ? 2 : 3;
  trackFeature('protocol', protoNum);

  // Timeline bucket
  updateTimelineBucket(isMalicious);

  // Prediction log
  addToLog(flow, isMalicious);

  // Update UI
  updateMetrics();
  updateConfusionMatrix();
  updateDrift();
  updateInferenceTable();
  updatePredictionLog();
  updateCharts();
  updateSessionInfo();
}

/* ── Feature drift ──────────────────────────────────────────── */
function trackFeature(key, val) {
  const f = state.features[key];
  f.vals.push(val);
  if (f.vals.length > 500) f.vals.shift();
  // Lock baseline after first BASELINE_N observations
  if (f.vals.length === BASELINE_N && f.baseline === null) {
    f.baseline = { mean: mean(f.vals), std: std(f.vals) };
  }
}

function driftScore(key) {
  const f = state.features[key];
  if (!f.baseline || f.vals.length < BASELINE_N) return null;
  const recent = f.vals.slice(-50);
  const recentMean = mean(recent);
  const baseStd = f.baseline.std || 1;
  return Math.abs(recentMean - f.baseline.mean) / baseStd; // z-score style
}

/* ── Timeline ───────────────────────────────────────────────── */
function updateTimelineBucket(isMalicious) {
  const now = Date.now();
  const bucketKey = Math.floor(now / BUCKET_MS) * BUCKET_MS;
  if (!state.currentBucket || state.currentBucket.t !== bucketKey) {
    if (state.currentBucket) {
      state.timeline.push(state.currentBucket);
      if (state.timeline.length > 30) state.timeline.shift();
    }
    state.currentBucket = { t: bucketKey, benign: 0, malicious: 0 };
  }
  if (isMalicious) state.currentBucket.malicious++;
  else state.currentBucket.benign++;
}

/* ── Prediction log ─────────────────────────────────────────── */
function addToLog(flow, isMalicious) {
  state.log.unshift({
    time: new Date().toLocaleTimeString('fr-FR', { hour12: false }),
    key: flow.flowKey || '—',
    protocol: flow.protocol || '—',
    label: isMalicious ? 'MALICIOUS' : 'BENIGN',
    severity: flow.riskSeverity || '—',
    packets: flow.packetCount || 0,
  });
  if (state.log.length > LOG_LIMIT) state.log.pop();
}

/* ── Math helpers ───────────────────────────────────────────── */
function mean(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
function std(arr) {
  if (arr.length < 2) return 0;
  const m = mean(arr);
  return Math.sqrt(arr.reduce((a, b) => a + (b - m) ** 2, 0) / arr.length);
}
function percentile(arr, p) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}
function fmt(n, decimals = 1) {
  if (n == null || isNaN(n)) return '—';
  return n.toFixed(decimals);
}
function pct(num, den) {
  if (!den) return '—';
  return (num / den * 100).toFixed(1) + '%';
}

/* ── UI updates ─────────────────────────────────────────────── */
function updateMetrics() {
  const { total, tp, tn, fp, fn, malicious, benign } = state;
  const accuracy  = total ? (tp + tn) / total : null;
  const precision = (tp + fp) ? tp / (tp + fp) : null;
  const recall    = (tp + fn) ? tp / (tp + fn) : null;
  const f1 = (precision != null && recall != null && precision + recall)
    ? 2 * precision * recall / (precision + recall) : null;

  set('metric-accuracy',  accuracy  != null ? (accuracy  * 100).toFixed(1) + '%' : '—');
  set('metric-precision', precision != null ? (precision * 100).toFixed(1) + '%' : '—');
  set('metric-recall',    recall    != null ? (recall    * 100).toFixed(1) + '%' : '—');
  set('metric-f1',        f1        != null ? (f1        * 100).toFixed(1) + '%' : '—');
  set('metric-accuracy-sub', `last ${total} flows`);
  set('metric-malicious-rate', pct(malicious, total));
  set('metric-benign-rate',    pct(benign,    total));
  set('prediction-counter', `${total.toLocaleString()} predictions`);

  const lats = state.latencies;
  const avgL = lats.length ? mean(lats) : null;
  const p95L = lats.length ? percentile(lats, 95) : null;
  set('metric-infer-avg', avgL != null ? fmt(avgL) + ' ms' : '—');
  set('metric-infer-p95', p95L != null ? fmt(p95L) + ' ms' : '—');
}

function updateConfusionMatrix() {
  set('cm-tp', `${state.tp}<span class="cm-cell-label">TP</span>`);
  set('cm-tn', `${state.tn}<span class="cm-cell-label">TN</span>`);
  set('cm-fp', `${state.fp}<span class="cm-cell-label">FP</span>`);
  set('cm-fn', `${state.fn}<span class="cm-cell-label">FN</span>`);
  // innerHTML for the spans inside cm cells
  for (const id of ['cm-tp','cm-tn','cm-fp','cm-fn']) {
    document.getElementById(id).innerHTML = document.getElementById(id).textContent;
  }
  const val = (id, num, lbl) => {
    document.getElementById(id).innerHTML = `${num}<br><span class="cm-cell-label">${lbl}</span>`;
  };
  val('cm-tp', state.tp, 'TP');
  val('cm-tn', state.tn, 'TN');
  val('cm-fp', state.fp, 'FP');
  val('cm-fn', state.fn, 'FN');
}

function updateDrift() {
  const container = document.getElementById('drift-container');
  const keys = Object.keys(state.features);
  const anyBaseline = keys.some(k => state.features[k].baseline !== null);
  if (!anyBaseline) {
    container.innerHTML = `<div class="drift-skeleton">Computing baseline (need ${BASELINE_N} flows)... ${state.total}/${BASELINE_N}</div>`;
    return;
  }

  container.innerHTML = keys.map(key => {
    const f = state.features[key];
    const score = driftScore(key);
    if (score === null) return '';
    const pct = Math.min(100, score * 33);
    let cls, label, color;
    if (score < 1)      { cls = 'drift-stable';   label = 'stable';   color = '#639922'; }
    else if (score < 2) { cls = 'drift-shifting'; label = 'shifting'; color = '#EF9F27'; }
    else                { cls = 'drift-alert';    label = 'alert';    color = '#E24B4A'; }
    return `
      <div class="drift-item">
        <span class="drift-name">${f.name}</span>
        <div class="drift-bar-bg">
          <div class="drift-bar-fill" style="width:${pct.toFixed(0)}%;background:${color}"></div>
        </div>
        <span class="drift-badge ${cls}">${label}</span>
        <span style="font-size:11px;color:#B4B2A9;min-width:32px">z=${fmt(score)}</span>
      </div>`;
  }).join('');
}

function updateInferenceTable() {
  const lats = state.latencies;
  if (!lats.length) return;
  const set2 = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  set2('infer-min',   fmt(Math.min(...lats)) + ' ms');
  set2('infer-avg',   fmt(mean(lats)) + ' ms');
  set2('infer-p95',   fmt(percentile(lats, 95)) + ' ms');
  set2('infer-p99',   fmt(percentile(lats, 99)) + ' ms');
  set2('infer-max',   fmt(Math.max(...lats)) + ' ms');
  set2('infer-total', state.total.toLocaleString());
}

function updatePredictionLog() {
  const tbody = document.getElementById('prediction-log');
  tbody.innerHTML = state.log.map(e => `
    <tr>
      <td class="font-mono text-gray-400">${e.time}</td>
      <td class="font-mono text-gray-500 truncate max-w-xs" title="${e.key}">${e.key.slice(0, 36)}${e.key.length > 36 ? '…' : ''}</td>
      <td>${e.protocol}</td>
      <td><span class="pred-${e.label.toLowerCase()}">${e.label}</span></td>
      <td class="text-gray-400">${e.severity}</td>
      <td class="text-gray-400">${e.packets}</td>
    </tr>`).join('');
}

function updateSessionInfo() {
  const elapsed = Math.round((Date.now() - state.sessionStart) / 1000);
  const mins = Math.floor(elapsed / 60), secs = elapsed % 60;
  document.getElementById('session-info').textContent =
    `session active for ${mins}m ${secs}s — ${state.total} flows processed`;
}

/* ── Charts ─────────────────────────────────────────────────── */
let timelineChart, latencyChart;

function initCharts() {
  const ctxT = document.getElementById('timelineChart').getContext('2d');
  timelineChart = new Chart(ctxT, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: 'Benign',    data: [], borderColor: '#639922', backgroundColor: '#EAF3DE88', tension: 0.3, fill: true, pointRadius: 2 },
        { label: 'Malicious', data: [], borderColor: '#E24B4A', backgroundColor: '#FCEBEB88', tension: 0.3, fill: true, pointRadius: 2 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 10, font: { size: 11 } } } },
      scales: { x: { ticks: { font: { size: 10 }, maxRotation: 0 } }, y: { beginAtZero: true, ticks: { font: { size: 10 } } } }
    }
  });

  const ctxL = document.getElementById('latencyChart').getContext('2d');
  latencyChart = new Chart(ctxL, {
    type: 'bar',
    data: {
      labels: ['0-1', '1-2', '2-5', '5-10', '10-20', '20-50', '50+'],
      datasets: [{ label: 'flows', data: [0,0,0,0,0,0,0], backgroundColor: '#B5D4F4', borderRadius: 3 }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { font: { size: 10 } }, title: { display: true, text: 'ms', font: { size: 10 } } },
                y: { beginAtZero: true, ticks: { font: { size: 10 } } } }
    }
  });
}

function updateCharts() {
  // Timeline
  const buckets = [...state.timeline];
  if (state.currentBucket) buckets.push(state.currentBucket);
  timelineChart.data.labels = buckets.map(b => new Date(b.t).toLocaleTimeString('fr-FR', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' }));
  timelineChart.data.datasets[0].data = buckets.map(b => b.benign);
  timelineChart.data.datasets[1].data = buckets.map(b => b.malicious);
  timelineChart.update('none');

  // Latency histogram
  const bins = [0, 0, 0, 0, 0, 0, 0];
  state.latencies.forEach(l => {
    if      (l < 1)  bins[0]++;
    else if (l < 2)  bins[1]++;
    else if (l < 5)  bins[2]++;
    else if (l < 10) bins[3]++;
    else if (l < 20) bins[4]++;
    else if (l < 50) bins[5]++;
    else             bins[6]++;
  });
  latencyChart.data.datasets[0].data = bins;
  latencyChart.update('none');
}

/* ── File upload (calls /api/model/upload) ──────────────────── */
function setupUpload() {
  const zone  = document.getElementById('upload-zone');
  const input = document.getElementById('model-file-input');
  const status = document.getElementById('upload-status');

  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('drag-over');
    const file = e.dataTransfer.files[0];
    if (file) uploadModel(file);
  });
  input.addEventListener('change', () => { if (input.files[0]) uploadModel(input.files[0]); });

  async function uploadModel(file) {
    if (!file.name.endsWith('.pmml')) {
      showStatus('⚠ Only .pmml files are accepted.', 'text-yellow-600');
      return;
    }
    showStatus('⏳ Uploading model...', 'text-blue-500');
    try {
      const formData = new FormData();
      formData.append('model', file);
      const res = await fetch('/api/model/upload', { method: 'POST', body: formData });
      if (res.ok) {
        showStatus(`✔ ${file.name} uploaded. Reload to activate.`, 'text-green-600');
        addHistoryEntry(file.name);
      } else {
        showStatus(`✖ Upload failed: ${res.statusText}`, 'text-red-600');
      }
    } catch (err) {
      showStatus(`✖ Upload error: ${err.message}`, 'text-red-600');
    }
  }

  function showStatus(msg, cls) {
    status.textContent = msg;
    status.className = `mt-3 text-sm ${cls}`;
    status.classList.remove('hidden');
  }

  function addHistoryEntry(name) {
    const history = document.getElementById('model-history');
    const entry = document.createElement('div');
    entry.className = 'flex justify-between py-1 border-b border-gray-50';
    entry.innerHTML = `<span class="font-mono text-xs">${name}</span><span class="text-xs text-gray-400">uploaded ${new Date().toLocaleTimeString()}</span>`;
    history.prepend(entry);
  }
}

/* ── Helper ─────────────────────────────────────────────────── */
function set(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

/* ── Boot ───────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  setupUpload();
  // Tick session timer every second
  setInterval(updateSessionInfo, 1000);
});