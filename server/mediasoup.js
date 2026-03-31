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

let worker, router;

async function startMediasoup() {
  worker = await mediasoup.createWorker({ logLevel: 'warn' });
  router = await worker.createRouter({ mediaCodecs });
  console.log('mediasoup router ready');
}

async function createTransport() {
  const transport = await router.createWebRtcTransport(transportOptions);
  return {
    transport,
    params: {
      id:             transport.id,
      iceParameters:  transport.iceParameters,
      iceCandidates:  transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters,
    }
  };
}

module.exports = { startMediasoup, createTransport, getRouter: () => router };
