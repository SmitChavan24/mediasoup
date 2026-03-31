import { Device } from 'mediasoup-client';

export async function setupCall(socket, callId, onRemoteAudio) {
  // Load device with router capabilities
  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );
  
  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });

  // Create send + recv transports
  const sendParams = await new Promise(res => socket.emit('createSendTransport', res));
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));

  const sendTransport = device.createSendTransport(sendParams);
  const recvTransport = device.createRecvTransport(recvParams);

  // Wire up DTLS connect events
  sendTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    socket.emit('connectSendTransport', { dtlsParameters }, (err) => {
      if (err) return errback(err);
      cb();
    });
  });
  
  recvTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    socket.emit('connectRecvTransport', { dtlsParameters }, (err) => {
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

  // When the other party's producer is ready, consume it
  socket.on('newProducer', async ({ producerId }) => {
    const consumerParams = await new Promise(res =>
      socket.emit('consume', {
        producerId,
        rtpCapabilities: device.rtpCapabilities,
        callId,
      }, res)
    );

    if (consumerParams.error) {
      console.error('Consume error:', consumerParams.error);
      return;
    }

    const consumer = await recvTransport.consume(consumerParams);

    // Play remote audio
    const audio = new Audio();
    audio.srcObject = new MediaStream([consumer.track]);
    audio.play().catch(e => console.error('Audio play error:', e));
    
    if (onRemoteAudio) {
      onRemoteAudio(audio);
    }
  });

  // Get mic audio and produce
  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const track = stream.getAudioTracks()[0];
    await sendTransport.produce({ track });
  } catch (err) {
    console.error('Failed to get media devices:', err);
    throw err;
  }

  return { sendTransport, recvTransport, close: () => {
    stream?.getTracks().forEach(track => track.stop());
    sendTransport.close();
    recvTransport.close();
    socket.off('newProducer');
  } };
}
