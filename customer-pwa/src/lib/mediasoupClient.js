import { Device } from 'mediasoup-client';

export async function setupCall(socket, callId, onRemoteAudio) {
  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );
  
  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });

  const sendParams = await new Promise(res => socket.emit('createSendTransport', res));
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));

  // Extract iceServers (STUN/TURN) from the server for NAT traversal on mobile
  const { iceServers: sendIce, ...sendRest } = sendParams;
  const { iceServers: recvIce, ...recvRest } = recvParams;

  const sendTransport = device.createSendTransport({ ...sendRest, iceServers: sendIce });
  const recvTransport = device.createRecvTransport({ ...recvRest, iceServers: recvIce });

  // ── ICE / connection diagnostics ────────────────────────────────────────────
  sendTransport.on('connectionstatechange', (state) => {
    console.log(`[mediasoup] sendTransport connection: ${state}`);
    if (state === 'failed') console.error('[mediasoup] ⚠ sendTransport ICE FAILED — server ports may not be reachable');
  });
  recvTransport.on('connectionstatechange', (state) => {
    console.log(`[mediasoup] recvTransport connection: ${state}`);
    if (state === 'failed') console.error('[mediasoup] ⚠ recvTransport ICE FAILED — server ports may not be reachable');
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

  // ── Pre-warm an audio element for mobile autoplay policy ──────────────────
  // Mobile browsers block audio.play() unless triggered by a user gesture.
  // Since we're called from a click handler chain, we "warm up" audio here.
  const audioEl = new Audio();
  audioEl.setAttribute('playsinline', '');
  audioEl.setAttribute('autoplay', '');
  // Play silent to unlock audio on this page (within user gesture context)
  audioEl.srcObject = new MediaStream();
  try { await audioEl.play(); } catch (_) { /* OK — just warming up */ }

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
    console.log(`[mediasoup] Consumer created: kind=${consumer.kind} paused=${consumer.paused}`);

    // Reuse the pre-warmed audio element (already "unlocked" for autoplay)
    audioEl.srcObject = new MediaStream([consumer.track]);
    audioEl.play().catch(e => console.error('Audio play error:', e));
    
    if (onRemoteAudio) {
      onRemoteAudio(audioEl);
    }
  });

  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const track = stream.getAudioTracks()[0];
    await sendTransport.produce({ track });
    console.log('[mediasoup] Producing audio track');
  } catch (err) {
    console.error('Failed to get media devices:', err);
    throw err;
  }

  return { sendTransport, recvTransport, close: () => {
    stream?.getTracks().forEach(track => track.stop());
    sendTransport.close();
    recvTransport.close();
    audioEl.pause();
    audioEl.srcObject = null;
    socket.off('newProducer');
  } };
}
