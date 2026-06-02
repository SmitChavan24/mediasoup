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
    rtcMinPort: parseInt(process.env.RTC_MIN_PORT, 10) || 40000,
    rtcMaxPort: parseInt(process.env.RTC_MAX_PORT, 10) || 49999,
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
// TURN is FALLBACK ONLY. Direct mediasoup UDP (host candidates) is the primary
// path. The clients who need TURN are usually on UDP-blocking networks, so the
// useful relay transports are TCP and TLS — not UDP.
const TURN_HOST = process.env.TURN_HOST;                 // public IP / domain of coturn
const TURN_TLS_HOST = process.env.TURN_TLS_HOST || TURN_HOST; // domain with a valid TLS cert
const TURN_USERNAME = process.env.TURN_USERNAME;
const TURN_CREDENTIAL = process.env.TURN_CREDENTIAL;

const iceServers = (TURN_HOST && TURN_USERNAME && TURN_CREDENTIAL)
  ? [
      { urls: `turn:${TURN_HOST}:3478?transport=udp`, username: TURN_USERNAME, credential: TURN_CREDENTIAL },
      { urls: `turn:${TURN_HOST}:3478?transport=tcp`, username: TURN_USERNAME, credential: TURN_CREDENTIAL },
      { urls: `turns:${TURN_TLS_HOST}:5349?transport=tcp`, username: TURN_USERNAME, credential: TURN_CREDENTIAL },
    ]
  : [];
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