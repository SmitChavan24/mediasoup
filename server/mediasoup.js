const mediasoup = require('mediasoup');

const mediaCodecs = [
  {
    kind: 'audio',
    mimeType: 'audio/opus',
    clockRate: 48000,
    channels: 2,
  }
];

const transportOptions = {
  listenIps: [{ ip: '0.0.0.0', announcedIp: '127.0.0.1' }],
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
