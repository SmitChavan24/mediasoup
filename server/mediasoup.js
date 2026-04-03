const os = require('os');
const https = require('https');
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

// Auto-detect public IP so mobile clients over the internet can reach us
function fetchPublicIp() {
  return new Promise((resolve, reject) => {
    https.get('https://api.ipify.org', (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data.trim()));
    }).on('error', reject);
  });
}

const mediaCodecs = [
  {
    kind: 'audio',
    mimeType: 'audio/opus',
    clockRate: 48000,
    channels: 2,
  }
];

// ── Port range for WebRTC media ──────────────────────────────────────────────
// Forward these ports (UDP + TCP) on your router for mobile/internet access.
const RTC_MIN_PORT = 40000;
const RTC_MAX_PORT = 40200;

let worker, router, announcedIp;

async function startMediasoup() {
  // 1. Determine the announced IP
  //    Priority: PUBLIC_IP env var → auto-detected public IP → LAN IP fallback
  if (process.env.PUBLIC_IP) {
    announcedIp = process.env.PUBLIC_IP;
    console.log(`[mediasoup] Using PUBLIC_IP from env: ${announcedIp}`);
  } else {
    try {
      announcedIp = await fetchPublicIp();
      console.log(`[mediasoup] Auto-detected public IP: ${announcedIp}`);
    } catch (err) {
      announcedIp = getLocalIp();
      console.warn(`[mediasoup] Could not detect public IP, falling back to LAN: ${announcedIp}`);
    }
  }

  // 2. Create worker with explicit port range
  worker = await mediasoup.createWorker({
    logLevel: 'warn',
    rtcMinPort: RTC_MIN_PORT,
    rtcMaxPort: RTC_MAX_PORT,
  });

  worker.on('died', () => {
    console.error('[mediasoup] Worker died unexpectedly — exiting.');
    process.exit(1);
  });

  router = await worker.createRouter({ mediaCodecs });

  console.log(`[mediasoup] Router ready`);
  console.log(`[mediasoup] announcedIp = ${announcedIp}`);
  console.log(`[mediasoup] RTC port range = ${RTC_MIN_PORT}–${RTC_MAX_PORT} (UDP+TCP)`);
  console.log(`[mediasoup] ⚠  For mobile access, forward ports ${RTC_MIN_PORT}-${RTC_MAX_PORT} UDP+TCP on your router to this machine.`);
}

async function createTransport() {
  const transportOptions = {
    listenIps: [{ ip: '0.0.0.0', announcedIp }],
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
  };

  const transport = await router.createWebRtcTransport(transportOptions);

  return {
    transport,
    params: {
      id: transport.id,
      iceParameters: transport.iceParameters,
      iceCandidates: transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters,
      // STUN + TURN servers for NAT traversal on mobile / restrictive networks
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        {
          urls: 'turn:free.expressturn.com:3478',
          username: '000000002090563597',
          credential: '14jWHzL356cUf8wd7cupbCT4qKo=',
        },
        { urls: 'turn:numb.viagenie.ca', credential: 'muazkh', username: 'webrtc@live.com' },
        { urls: 'turn:turn.bistri.com:80', credential: 'homeo', username: 'homeo' },
        { urls: 'turn:turn.anyfirewall.com:443?transport=tcp', credential: 'webrtc', username: 'webrtc' },
      ],
    }
  };
}

module.exports = { startMediasoup, createTransport, getRouter: () => router };
