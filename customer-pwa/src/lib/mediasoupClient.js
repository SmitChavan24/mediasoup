import { Device } from 'mediasoup-client';

const LOG_PREFIX = '[mediasoup-client:customer]';

export async function setupCall(socket, callId, onRemoteAudio) {
  console.log(`${LOG_PREFIX} ═══ setupCall START callId=${callId} ═══`);

  // ── Step 1: Load device with router capabilities ──────────────────────
  console.log(`${LOG_PREFIX} 📋 Requesting router RTP capabilities...`);
  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );
  console.log(`${LOG_PREFIX} ✅ Received RTP capabilities:`, JSON.stringify(rtpCapabilities?.codecs?.map(c => c.mimeType)));

  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });
  console.log(`${LOG_PREFIX} ✅ Device loaded. canProduce('audio')=${device.canProduce('audio')}`);

  // ── Step 2: Create send + recv transports ─────────────────────────────
  console.log(`${LOG_PREFIX} 📤 Requesting SEND transport...`);
  const sendParams = await new Promise(res => socket.emit('createSendTransport', res));
  if (sendParams.error) {
    console.error(`${LOG_PREFIX} ❌ createSendTransport failed:`, sendParams.error);
    throw new Error(sendParams.error);
  }
  console.log(`${LOG_PREFIX} ✅ SEND transport params received: id=${sendParams.id}`);
  console.log(`${LOG_PREFIX}    ICE candidates:`, JSON.stringify(sendParams.iceCandidates));

  console.log(`${LOG_PREFIX} 📥 Requesting RECV transport...`);
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));
  if (recvParams.error) {
    console.error(`${LOG_PREFIX} ❌ createRecvTransport failed:`, recvParams.error);
    throw new Error(recvParams.error);
  }
  console.log(`${LOG_PREFIX} ✅ RECV transport params received: id=${recvParams.id}`);
  console.log(`${LOG_PREFIX}    ICE candidates:`, JSON.stringify(recvParams.iceCandidates));

  const sendTransport = device.createSendTransport(sendParams);
  const recvTransport = device.createRecvTransport(recvParams);

  // ── Transport connection state logging ────────────────────────────────
  sendTransport.on('connectionstatechange', (state) => {
    console.log(`${LOG_PREFIX} 📤 SEND transport connectionstatechange: ${state}`);
    if (state === 'failed' || state === 'disconnected') {
      console.error(`${LOG_PREFIX} ❌ SEND transport ${state} — audio will NOT flow`);
    }
  });

  recvTransport.on('connectionstatechange', (state) => {
    console.log(`${LOG_PREFIX} 📥 RECV transport connectionstatechange: ${state}`);
    if (state === 'failed' || state === 'disconnected') {
      console.error(`${LOG_PREFIX} ❌ RECV transport ${state} — will NOT receive audio`);
    }
  });

  // ── Step 3: Wire up DTLS connect events ───────────────────────────────
  sendTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    console.log(`${LOG_PREFIX} 🔒 SEND transport DTLS connect triggered`);
    socket.emit('connectSendTransport', { dtlsParameters }, (err) => {
      if (err) {
        console.error(`${LOG_PREFIX} ❌ SEND transport DTLS connect failed:`, err);
        return errback(err);
      }
      console.log(`${LOG_PREFIX} ✅ SEND transport DTLS connected`);
      cb();
    });
  });

  recvTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    console.log(`${LOG_PREFIX} 🔒 RECV transport DTLS connect triggered`);
    socket.emit('connectRecvTransport', { dtlsParameters }, (err) => {
      if (err) {
        console.error(`${LOG_PREFIX} ❌ RECV transport DTLS connect failed:`, err);
        return errback(err);
      }
      console.log(`${LOG_PREFIX} ✅ RECV transport DTLS connected`);
      cb();
    });
  });

  // ── Step 4: Produce event ─────────────────────────────────────────────
  sendTransport.on('produce', ({ kind, rtpParameters }, cb, errback) => {
    console.log(`${LOG_PREFIX} 🎤 Produce event: kind=${kind} codecs=${rtpParameters?.codecs?.map(c => c.mimeType).join(', ')}`);
    socket.emit('produce', { kind, rtpParameters, callId }, ({ id, error }) => {
      if (error) {
        console.error(`${LOG_PREFIX} ❌ Produce server error:`, error);
        return errback(error);
      }
      console.log(`${LOG_PREFIX} ✅ Produce success: producerId=${id}`);
      cb({ id });
    });
  });

  // ── Step 5: When the other party's producer is ready, consume it ──────
  socket.on('newProducer', async ({ producerId }) => {
    console.log(`${LOG_PREFIX} 🔊 newProducer received: producerId=${producerId}`);
    console.log(`${LOG_PREFIX}    Requesting consume from server...`);

    const consumerParams = await new Promise(res =>
      socket.emit('consume', {
        producerId,
        rtpCapabilities: device.rtpCapabilities,
        callId,
      }, res)
    );

    if (consumerParams.error) {
      console.error(`${LOG_PREFIX} ❌ Consume error:`, consumerParams.error);
      return;
    }

    console.log(`${LOG_PREFIX} ✅ Consumer params received: id=${consumerParams.id} kind=${consumerParams.kind}`);

    const consumer = await recvTransport.consume(consumerParams);
    console.log(`${LOG_PREFIX} ✅ Consumer created locally: track.kind=${consumer.track?.kind} track.readyState=${consumer.track?.readyState} track.enabled=${consumer.track?.enabled} track.muted=${consumer.track?.muted}`);

    // Log track state changes
    consumer.track.onended = () => console.warn(`${LOG_PREFIX} ⚠️  Consumer track ENDED`);
    consumer.track.onmute = () => console.warn(`${LOG_PREFIX} ⚠️  Consumer track MUTED`);
    consumer.track.onunmute = () => console.log(`${LOG_PREFIX} ▶️  Consumer track UNMUTED`);

    // Play remote audio
    const audio = new Audio();
    audio.srcObject = new MediaStream([consumer.track]);
    document.body.appendChild(audio);
    recvTransport._audioEl = audio;

    console.log(`${LOG_PREFIX} 🔊 Attempting audio.play()...`);
    try {
      await audio.play();
      console.log(`${LOG_PREFIX} ✅ audio.play() succeeded — remote audio should be playing`);
      console.log(`${LOG_PREFIX}    audio.paused=${audio.paused} audio.muted=${audio.muted} audio.volume=${audio.volume} audio.readyState=${audio.readyState}`);
    } catch (e) {
      console.error(`${LOG_PREFIX} ❌ audio.play() FAILED (autoplay blocked?):`, e);
    }

    if (onRemoteAudio) {
      onRemoteAudio(audio);
    }
  });

  // ── Step 6: Get mic audio and produce ─────────────────────────────────
  let stream;
  try {
    console.log(`${LOG_PREFIX} 🎤 Requesting getUserMedia({ audio: true })...`);
    stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const track = stream.getAudioTracks()[0];
    console.log(`${LOG_PREFIX} ✅ getUserMedia success: track.label="${track.label}" track.readyState=${track.readyState} track.enabled=${track.enabled} track.muted=${track.muted}`);
    console.log(`${LOG_PREFIX}    Settings:`, JSON.stringify(track.getSettings()));

    // Log local track state changes
    track.onended = () => console.warn(`${LOG_PREFIX} ⚠️  Local mic track ENDED`);
    track.onmute = () => console.warn(`${LOG_PREFIX} ⚠️  Local mic track MUTED`);
    track.onunmute = () => console.log(`${LOG_PREFIX} ▶️  Local mic track UNMUTED`);

    console.log(`${LOG_PREFIX} 🎤 Calling sendTransport.produce()...`);
    await sendTransport.produce({ track });
    console.log(`${LOG_PREFIX} ✅ sendTransport.produce() completed — audio is being sent`);
  } catch (err) {
    console.error(`${LOG_PREFIX} ❌ Failed to get media devices or produce:`, err);
    throw err;
  }

  console.log(`${LOG_PREFIX} ═══ setupCall COMPLETE callId=${callId} ═══`);

  return {
    sendTransport, recvTransport, close: () => {
      console.log(`${LOG_PREFIX} 🛑 Closing call resources for callId=${callId}`);
      stream?.getTracks().forEach(track => track.stop());
      sendTransport.close();
      recvTransport._audioEl && recvTransport._audioEl.remove();
      recvTransport.close();
      socket.off('newProducer');
      console.log(`${LOG_PREFIX} 🧹 Call resources cleaned up`);
    }
  };
}
