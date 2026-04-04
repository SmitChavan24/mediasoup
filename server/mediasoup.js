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
let cachedIceServers = null;

async function getIceServers() {
  if (cachedIceServers) return cachedIceServers;
  try {
    const https = require('https');
    return new Promise((resolve, reject) => {
      https.get('https://tglevels.metered.live/api/v1/turn/credentials?apiKey=89c2d061cef1700374b542ad790f36abd49d', (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            cachedIceServers = JSON.parse(data);
            resolve(cachedIceServers);
          } catch (e) {
            reject(e);
          }
        });
      }).on('error', reject);
    });
  } catch (error) {
    console.error("[mediasoup] Failed to fetch dynamic ICE servers, using fallbacks.");
    return [
      { urls: 'stun:stun.relay.metered.ca:80' },
      {
        urls: 'turn:global.relay.metered.ca:80',
        username: '1931f85ebf8e9eb113834fe3',
        credential: 'bb87UyBhpsRSlKD8',
      },
      {
        urls: 'turn:global.relay.metered.ca:80?transport=tcp',
        username: '1931f85ebf8e9eb113834fe3',
        credential: 'bb87UyBhpsRSlKD8',
      },
      {
        urls: 'turn:global.relay.metered.ca:443',
        username: '1931f85ebf8e9eb113834fe3',
        credential: 'bb87UyBhpsRSlKD8',
      },
      {
        urls: 'turns:global.relay.metered.ca:443?transport=tcp',
        username: '1931f85ebf8e9eb113834fe3',
        credential: 'bb87UyBhpsRSlKD8',
      },
    ];
  }
}

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
  const ips = [];
  if (announcedIp) {
    ips.push({ ip: '0.0.0.0', announcedIp });
  }
  // Also announce the local network IP for devices on the same WiFi
  const localIp = getLocalIp();
  if (localIp && localIp !== announcedIp) {
    ips.push({ ip: '0.0.0.0', announcedIp: localIp });
  }
  // If neither were available (e.g., error), fallback to 0.0.0.0
  if (ips.length === 0) {
    ips.push({ ip: '0.0.0.0' });
  }

  const transportOptions = {
    listenIps: ips,
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
  };

  const transport = await router.createWebRtcTransport(transportOptions);
  const iceServers = await getIceServers();

  return {
    transport,
    params: {
      id:             transport.id,
      iceParameters:  transport.iceParameters,
      iceCandidates:  transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters,
      // STUN + TURN servers for NAT traversal on mobile / restrictive networks
      iceServers,
    }
  };
}

module.exports = { startMediasoup, createTransport, getRouter: () => router };
