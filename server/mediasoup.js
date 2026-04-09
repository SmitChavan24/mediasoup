const os = require('os');
const mediasoup = require('mediasoup');
function getLocalIp() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1';
}
const mediaCodecs = [
  {
    kind: 'audio',
    mimeType: 'audio/opus',
    clockRate: 48000,
    channels: 2,
  }
];
const transportOptions = {
  listenIps: [{ ip: '0.0.0.0', announcedIp: process.env.PUBLIC_IP || getLocalIp() }],
  enableUdp: true,
  enableTcp: true,
  preferUdp: true,
};
// ── Worker Pool ───────────────────────────────────────────────────────────────
// One worker per CPU core. Each worker owns one router.
// All routers use the same mediaCodecs so RTP capabilities are identical.
const workers = [];
let workerIndex = 0;
async function spawnWorker(index) {
  const worker = await mediasoup.createWorker({
    logLevel: 'warn',
    rtcMinPort: 10000,   // ← 50 000 ports shared across all workers
    rtcMaxPort: 59999,   //   comfortably handles 800+ simultaneous transports
  });
  // Auto-respawn crashed workers so the pool stays healthy
  worker.on('died', async () => {
    console.error(`[mediasoup] Worker[${index}] (pid ${worker.pid}) died — respawning in 1 s...`);
    await new Promise(r => setTimeout(r, 1000));
    workers[index] = await spawnWorker(index);
  });
  const router = await worker.createRouter({ mediaCodecs });
  console.log(`[mediasoup] Worker[${index}] (pid ${worker.pid}) ready`);
  return { worker, router };
}
async function startMediasoup() {
  const numCores = os.cpus().length;
  console.log(`[mediasoup] Spawning ${numCores} worker(s) (one per CPU core)...`);
  for (let i = 0; i < numCores; i++) {
    workers[i] = await spawnWorker(i);
  }
  console.log(`[mediasoup] All ${numCores} workers ready.`);
}
// Round-robin: each call gets the next worker in the pool
function getNextRouter() {
  const entry = workers[workerIndex % workers.length];
  workerIndex = (workerIndex + 1) % workers.length;
  return entry.router;
}
// Returns any router — used only for capability queries before a call starts
function getAnyRouter() {
  return workers[0]?.router;
}
// ── ICE servers (TURN fallback for clients behind symmetric NAT / firewalls) ──
const iceServers = [
  {
    urls: 'turn:35.154.164.61:3478',
    username: 'turnuser',
    credential: 'smit1234',
  },
];
// ── Transport factory (accepts the per-call router) ───────────────────────────
async function createTransport(router) {
  const transport = await router.createWebRtcTransport(transportOptions);
  return {
    transport,
    params: {
      id: transport.id,
      iceParameters: transport.iceParameters,
      iceCandidates: transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters,
      iceServers,
    }
  };
}

module.exports = { startMediasoup, createTransport, getNextRouter, getAnyRouter };