import { Device } from 'mediasoup-client';

export async function setupMonitor(socket, callId, { agentProducerId, customerProducerId }) {
  // Load device
  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );
  
  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });

  // Admin only needs Recv transport for monitoring, and Send for whispering
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));
  const recvTransport = device.createRecvTransport(recvParams);

  recvTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    socket.emit('connectRecvTransport', { dtlsParameters }, (err) => {
      if (err) return errback(err);
      cb();
    });
  });

  // Helper to consume a producer
  const consume = async (producerId) => {
    const params = await new Promise(res =>
      socket.emit('consume', {
        producerId,
        rtpCapabilities: device.rtpCapabilities,
        callId,
      }, res)
    );

    if (params.error) return null;

    const consumer = await recvTransport.consume(params);
    const stream = new MediaStream([consumer.track]);
    const audio = new Audio();
    audio.srcObject = stream;
    audio.play().catch(e => console.error('Monitor audio play error:', e));
    return consumer;
  };

  // Monitor both agent and customer
  let agentConsumer = agentProducerId ? await consume(agentProducerId) : null;
  let customerConsumer = customerProducerId ? await consume(customerProducerId) : null;

  // Listen for new producers if they weren't ready initially
  socket.on('newProducer', async ({ producerId, role }) => {
    if (role === 'agent' && !agentConsumer) {
      agentConsumer = await consume(producerId);
    } else if (role === 'customer' && !customerConsumer) {
      customerConsumer = await consume(producerId);
    }
  });

  let sendTransport = null;
  let whisperProducer = null;
  let whisperStream = null;

  return {
    startWhispering: async () => {
      if (!sendTransport) {
        const sendParams = await new Promise(res => socket.emit('createSendTransport', res));
        sendTransport = device.createSendTransport(sendParams);

        sendTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
          socket.emit('connectSendTransport', { dtlsParameters }, (err) => {
            if (err) return errback(err);
            cb();
          });
        });

        sendTransport.on('produce', ({ kind, rtpParameters }, cb, errback) => {
          socket.emit('produce', { kind, rtpParameters, callId }, ({ id, error }) => {
            if (error) return errback(error);
            cb({ id });
          });
        });
      }

      whisperStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const track = whisperStream.getAudioTracks()[0];
      whisperProducer = await sendTransport.produce({ track });
    },

    stopWhispering: () => {
      whisperProducer?.close();
      whisperProducer = null;
      whisperStream?.getTracks().forEach(t => t.stop());
      whisperStream = null;
    },

    close: () => {
      agentConsumer?.close();
      customerConsumer?.close();
      whisperProducer?.close();
      whisperStream?.getTracks().forEach(t => t.stop());
      recvTransport.close();
      sendTransport?.close();
      socket.off('newProducer');
    }
  };
}
