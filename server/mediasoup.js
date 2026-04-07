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
  const localIp = getLocalIp();
  const announcedIp = process.env.PUBLIC_IP || '35.154.164.61';
  console.log(`[mediasoup] Local IP resolved to: ${localIp}`);
  console.log(`[mediasoup] Announced IP (PUBLIC_IP): ${announcedIp}`);
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
  console.log(`[mediasoup] Assigned router from Worker[${(workerIndex - 1 + workers.length) % workers.length}] (round-robin)`);
  return entry.router;
}

// Returns any router — used only for capability queries before a call starts
function getAnyRouter() {
  return workers[0]?.router;
}

// ── Transport factory (accepts the per-call router) ───────────────────────────
async function createTransport(router) {
  const announcedIp = process.env.PUBLIC_IP || '35.154.164.61';
  const options = {
    ...transportOptions,
    listenIps: [{ ip: '0.0.0.0', announcedIp }]
  };
  const transport = await router.createWebRtcTransport(options);

  console.log(`[transport] ✅ Created transport ${transport.id}`);
  console.log(`[transport]    listenIps: 0.0.0.0 → announcedIp: ${announcedIp}`);
  console.log(`[transport]    ICE candidates:`, JSON.stringify(transport.iceCandidates));
  console.log(`[transport]    DTLS params:`, JSON.stringify(transport.dtlsParameters));

  // ── Lifecycle event logging ─────────────────────────────────────────────
  transport.on('icestatechange', (iceState) => {
    console.log(`[transport] 🧊 ICE state change on ${transport.id}: ${iceState}`);
    if (iceState === 'disconnected' || iceState === 'closed') {
      console.warn(`[transport] ⚠️  ICE ${iceState} — possible network issue on transport ${transport.id}`);
    }
  });

  transport.on('dtlsstatechange', (dtlsState) => {
    console.log(`[transport] 🔒 DTLS state change on ${transport.id}: ${dtlsState}`);
    if (dtlsState === 'failed') {
      console.error(`[transport] ❌ DTLS FAILED on transport ${transport.id} — handshake did not complete`);
    } else if (dtlsState === 'closed') {
      console.warn(`[transport] 🔒 DTLS closed on transport ${transport.id}`);
    }
  });

  transport.on('sctpstatechange', (sctpState) => {
    console.log(`[transport] 📡 SCTP state change on ${transport.id}: ${sctpState}`);
  });

  transport.on('routerclose', () => {
    console.warn(`[transport] 🛑 Router closed — transport ${transport.id} will be destroyed`);
  });

  return {
    transport,
    params: {
      id: transport.id,
      iceParameters: transport.iceParameters,
      iceCandidates: transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters,
    }
  };
}

module.exports = { startMediasoup, createTransport, getNextRouter, getAnyRouter };
