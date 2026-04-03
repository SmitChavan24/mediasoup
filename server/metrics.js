const client = require('prom-client');
const os = require('os');

// ── Registry ──────────────────────────────────────────────────────────────────
const register = new client.Registry();

// Built-in Node.js metrics: memory, event-loop lag, GC, etc.
client.collectDefaultMetrics({ register, prefix: 'mediasoup_node_' });

// ── Gauges (point-in-time values) ─────────────────────────────────────────────
const activeCallsGauge = new client.Gauge({
  name: 'mediasoup_active_calls',
  help: 'Number of currently active calls',
  registers: [register],
});

const activeUsersGauge = new client.Gauge({
  name: 'mediasoup_active_users',
  help: 'Number of currently connected users, split by role',
  labelNames: ['role'],
  registers: [register],
});

const workerCountGauge = new client.Gauge({
  name: 'mediasoup_worker_count',
  help: 'Number of healthy mediasoup workers in the pool',
  registers: [register],
});

// ── Counters (monotonically increasing) ──────────────────────────────────────
const callsInitiatedCounter = new client.Counter({
  name: 'mediasoup_calls_initiated_total',
  help: 'Total number of calls initiated since server start',
  labelNames: ['type'], // 'dialOut' | 'callIn'
  registers: [register],
});

const callsEndedCounter = new client.Counter({
  name: 'mediasoup_calls_ended_total',
  help: 'Total number of calls ended since server start',
  registers: [register],
});

const connectionsCounter = new client.Counter({
  name: 'mediasoup_websocket_connections_total',
  help: 'Total WebSocket connections accepted since server start',
  registers: [register],
});

const disconnectionsCounter = new client.Counter({
  name: 'mediasoup_websocket_disconnections_total',
  help: 'Total WebSocket disconnections since server start',
  registers: [register],
});

// ── Histogram (distribution of call durations) ────────────────────────────────
const callDurationHistogram = new client.Histogram({
  name: 'mediasoup_call_duration_seconds',
  help: 'Duration of individual calls in seconds',
  // Buckets: 5s, 15s, 30s, 1m, 2m, 5m, 10m, 30m
  buckets: [5, 15, 30, 60, 120, 300, 600, 1800],
  registers: [register],
});

// ── Console snapshot (printed every LOG_INTERVAL_MS) ─────────────────────────
const LOG_INTERVAL_MS = 60_000; // every 60 s

async function printMetricsSnapshot() {
  const json = await register.getMetricsAsJSON();

  const scalar = (name) => {
    const m = json.find(m => m.name === name);
    return m?.values?.[0]?.value ?? 0;
  };

  const byLabel = (name, key, val) => {
    const m = json.find(m => m.name === name);
    return m?.values?.find(v => v.labels?.[key] === val)?.value ?? 0;
  };

  const mem = process.memoryUsage();

  const lines = [
    '╔══════════════════ [Mediasoup Metrics Snapshot] ══════════════════╗',
    `║  Timestamp     : ${new Date().toISOString()}              ║`,
    '╠═══════════════════════════════════════════════════════════════════╣',
    `║  Active Calls  : ${String(scalar('mediasoup_active_calls')).padEnd(47)}║`,
    `║  Active Users  : agents=${byLabel('mediasoup_active_users','role','agent')}  customers=${byLabel('mediasoup_active_users','role','customer')}${' '.repeat(30)}║`,
    `║  Workers       : ${String(scalar('mediasoup_worker_count')).padEnd(47)}║`,
    '╠═══════════════════════════════════════════════════════════════════╣',
    `║  Calls Total   : dialOut=${byLabel('mediasoup_calls_initiated_total','type','dialOut')}  callIn=${byLabel('mediasoup_calls_initiated_total','type','callIn')}${' '.repeat(31)}║`,
    `║  Calls Ended   : ${String(scalar('mediasoup_calls_ended_total')).padEnd(47)}║`,
    `║  WS Connects   : ${String(scalar('mediasoup_websocket_connections_total')).padEnd(47)}║`,
    `║  WS Disconnects: ${String(scalar('mediasoup_websocket_disconnections_total')).padEnd(47)}║`,
    '╠═══════════════════════════════════════════════════════════════════╣',
    `║  Mem RSS       : ${String(Math.round(mem.rss / 1024 / 1024) + ' MB').padEnd(47)}║`,
    `║  Mem Heap Used : ${String(Math.round(mem.heapUsed / 1024 / 1024) + ' MB').padEnd(47)}║`,
    `║  CPU Cores     : ${String(os.cpus().length).padEnd(47)}║`,
    `║  Uptime        : ${String(Math.round(process.uptime()) + ' s').padEnd(47)}║`,
    '╚═══════════════════════════════════════════════════════════════════╝',
  ];

  console.log(lines.join('\n'));
}

function startPeriodicLog() {
  setInterval(printMetricsSnapshot, LOG_INTERVAL_MS);
  console.log(`[metrics] Console snapshot logging every ${LOG_INTERVAL_MS / 1000}s`);
}

module.exports = {
  register,
  startPeriodicLog,
  printMetricsSnapshot,
  m: {
    activeCallsGauge,
    activeUsersGauge,
    workerCountGauge,
    callsInitiatedCounter,
    callsEndedCounter,
    connectionsCounter,
    disconnectionsCounter,
    callDurationHistogram,
  },
};
