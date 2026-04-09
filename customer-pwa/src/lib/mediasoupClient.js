import { Device } from 'mediasoup-client';

// Module-scope state that survives socket reconnections
let _currentCallId = null;

export function getCurrentCallId() {
  return _currentCallId;
}

export function clearCurrentCallId() {
  _currentCallId = null;
}

export async function setupCall(socket, callId, onRemoteAudio) {
  _currentCallId = callId;

  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );

  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });
  const sendParams = await new Promise(res => socket.emit('createSendTransport', res));
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));
  const sendTransport = device.createSendTransport({
    ...sendParams,
    iceServers: sendParams.iceServers,
  });
  const recvTransport = device.createRecvTransport({
    ...recvParams,
    iceServers: recvParams.iceServers,
  });
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
  socket.on('newProducer', async ({ producerId }) => {
    const consumerParams = await new Promise(res =>
      socket.emit('consume', {
        producerId,
        rtpCapabilities: device.rtpCapabilities,
        callId,
      }, res)
    );
    if (consumerParams.error) return console.error('Consume error:', consumerParams.error);
    const consumer = await recvTransport.consume(consumerParams);
    const audio = new Audio();
    audio.srcObject = new MediaStream([consumer.track]);
    audio.play().catch(e => console.error('Audio play error:', e));

    if (onRemoteAudio) {
      onRemoteAudio(audio);
    }
  });

  // Respond to server heartbeat pings
  socket.on('hb-ping', () => {
    socket.emit('hb-pong');
  });

  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const track = stream.getAudioTracks()[0];
    await sendTransport.produce({ track });
  } catch (err) {
    console.error('Failed to get media devices:', err);
    throw err;
  }
  return {
    sendTransport, recvTransport, close: () => {
      _currentCallId = null;
      stream?.getTracks().forEach(track => track.stop());
      sendTransport.close();
      recvTransport.close();
      socket.off('newProducer');
      socket.off('hb-ping');
    }
  };
}