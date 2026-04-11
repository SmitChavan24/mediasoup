import { Device } from 'mediasoup-client';

export async function setupMonitor(socket, callId, { agentProducerId, customerProducerId }) {
  console.log('[admin-monitor] setupMonitor called', { callId, agentProducerId, customerProducerId });

  // Load device
  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );
  console.log('[admin-monitor] Got RTP capabilities');

  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });
  console.log('[admin-monitor] Device loaded');

  const pendingProducers = [];
  const audioElements = [];
  let isReady = false;

  const handleNewProducer = async ({ producerId, role }) => {
    if (!isReady) {
      console.log(`[admin-monitor] Queueing early producer: ${producerId}`);
      pendingProducers.push({ producerId, role });
      return;
    }
    console.log(`[admin-monitor] newProducer event: producerId=${producerId} role=${role}`);
    if (role === 'agent' && !agentConsumer) {
      agentConsumer = await consume(producerId, 'agent');
    } else if (role === 'customer' && !customerConsumer) {
      customerConsumer = await consume(producerId, 'customer');
    }
  };

  socket.on('newProducer', handleNewProducer);

  // Admin only needs Recv transport for monitoring, and Send for whispering
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));
  console.log('[admin-monitor] createRecvTransport response:', JSON.stringify(recvParams).slice(0, 200));

  if (recvParams.error) {
    throw new Error(`createRecvTransport failed: ${recvParams.error}`);
  }

  // Extract iceServers (STUN/TURN) from the server for NAT traversal on mobile
  const { iceServers: recvIce, ...recvRest } = recvParams;
  const recvTransport = device.createRecvTransport({ ...recvRest, iceServers: recvIce });
  console.log('[admin-monitor] recvTransport created locally, id:', recvTransport.id);

  recvTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    console.log('[admin-monitor] recvTransport connect event fired, sending connectRecvTransport...');
    socket.emit('connectRecvTransport', { dtlsParameters }, (err) => {
      console.log('[admin-monitor] connectRecvTransport response:', err || 'OK');
      if (err) return errback(new Error(err));
      cb();
    });
  });

  // ICE diagnostics
  recvTransport.on('connectionstatechange', (state) => {
    console.log(`[admin-monitor] recvTransport connection: ${state}`);
    if (state === 'failed') console.error('[admin-monitor] ⚠ recvTransport ICE FAILED');
  });

  // Helper to consume a producer
  const consume = async (producerId, label) => {
    console.log(`[admin-monitor] Consuming ${label}: producerId=${producerId}`);
    try {
      const params = await new Promise((resolve, reject) => {
        socket.emit('consume', {
          producerId,
          rtpCapabilities: device.rtpCapabilities,
          callId,
        }, (response) => {
          console.log(`[admin-monitor] consume response for ${label}:`, JSON.stringify(response).slice(0, 300));
          resolve(response);
        });

        // Timeout after 10s
        setTimeout(() => reject(new Error(`consume timeout for ${label}`)), 10000);
      });

      if (params.error) {
        console.error(`[admin-monitor] consume error for ${label}:`, params.error);
        return null;
      }

      console.log(`[admin-monitor] Creating consumer for ${label}...`);
      const consumer = await recvTransport.consume(params);
      console.log(`[admin-monitor] ✅ Consumer created for ${label}: id=${consumer.id} kind=${consumer.kind}`);

      const audio = new Audio();
      audio.setAttribute('playsinline', '');
      audio.setAttribute('autoplay', '');
      audio.srcObject = new MediaStream([consumer.track]);
      audioElements.push(audio);
      audio.play().catch(e => console.error(`[admin-monitor] audio play error for ${label}:`, e));
      console.log(`[admin-monitor] ✅ Audio element created for ${label}`);
      return consumer;
    } catch (err) {
      console.error(`[admin-monitor] ❌ Failed to consume ${label}:`, err);
      return null;
    }
  };

  // Monitor both agent and customer
  let agentConsumer = null;
  let customerConsumer = null;

  if (agentProducerId) {
    agentConsumer = await consume(agentProducerId, 'agent');
  } else {
    console.log('[admin-monitor] No agent producer yet — will wait for newProducer event');
  }

  if (customerProducerId) {
    customerConsumer = await consume(customerProducerId, 'customer');
  } else {
    console.log('[admin-monitor] No customer producer yet — will wait for newProducer event');
  }

  // Listen for new producers if they weren't ready initially
  isReady = true;
  for (const p of pendingProducers) {
    handleNewProducer(p);
  }

  let sendTransport = null;
  let whisperProducer = null;
  let whisperStream = null;

  return {
    startWhispering: async () => {
      if (!sendTransport) {
        const sendParams = await new Promise(res => socket.emit('createSendTransport', res));
        if (sendParams.error) throw new Error(`createSendTransport failed: ${sendParams.error}`);
        const { iceServers: sendIce, ...sendRest } = sendParams;
        sendTransport = device.createSendTransport({ ...sendRest, iceServers: sendIce });

        sendTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
          socket.emit('connectSendTransport', { dtlsParameters }, (err) => {
            if (err) return errback(new Error(err));
            cb();
          });
        });

        sendTransport.on('produce', ({ kind, rtpParameters }, cb, errback) => {
          socket.emit('produce', { kind, rtpParameters, callId }, ({ id, error }) => {
            if (error) return errback(new Error(error));
            cb({ id });
          });
        });
      }

      whisperStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const track = whisperStream.getAudioTracks()[0];
      whisperProducer = await sendTransport.produce({ track });
      console.log('[admin-monitor] ✅ Whisper producer created:', whisperProducer.id);
    },

    stopWhispering: () => {
      whisperProducer?.close();
      whisperProducer = null;
      whisperStream?.getTracks().forEach(t => t.stop());
      whisperStream = null;
      console.log('[admin-monitor] Stopped whispering');
    },

    close: () => {
      console.log('[admin-monitor] Closing monitor...');
      audioElements.forEach(a => a.remove());
      agentConsumer?.close();
      customerConsumer?.close();
      whisperProducer?.close();
      whisperStream?.getTracks().forEach(t => t.stop());
      recvTransport.close();
      sendTransport?.close();
      socket.off('newProducer', handleNewProducer);
    }
  };
}
