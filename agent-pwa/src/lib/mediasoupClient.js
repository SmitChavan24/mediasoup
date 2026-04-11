import { Device } from 'mediasoup-client';

const CALL_STATE_KEY = 'voip_active_call';

// Module-scope state that survives socket reconnections
let _currentCallId = null;

export function getCurrentCallId() {
  // Check localStorage first (survives tab close)
  if (!_currentCallId) {
    try {
      const stored = JSON.parse(localStorage.getItem(CALL_STATE_KEY));
      if (stored?.callId) _currentCallId = stored.callId;
    } catch { }
  }
  return _currentCallId;
}

export function getPreviousSocketId() {
  try {
    const stored = JSON.parse(localStorage.getItem(CALL_STATE_KEY));
    return stored?.socketId || null;
  } catch { return null; }
}

export function clearCurrentCallId() {
  _currentCallId = null;
  localStorage.removeItem(CALL_STATE_KEY);
}

function persistCallState(callId, socketId) {
  localStorage.setItem(CALL_STATE_KEY, JSON.stringify({ callId, socketId }));
}

export async function setupCall(socket, callId, onRemoteAudio) {
  _currentCallId = callId;
  persistCallState(callId, socket.id);

  let isReady = false;
  let currentDevice = null;
  let currentRecvTransport = null;
  const pendingProducers = [];
  const audioElements = [];

  const handleNewProducer = async ({ producerId }) => {
    if (!isReady || !currentDevice || !currentRecvTransport) {
      console.log(`[mediasoup] Queueing early producer: ${producerId}`);
      pendingProducers.push(producerId);
      return;
    }

    const consumerParams = await new Promise(res =>
      socket.emit('consume', {
        producerId,
        rtpCapabilities: currentDevice.rtpCapabilities,
        callId,
      }, res)
    );
    if (consumerParams.error) return console.error('Consume error:', consumerParams.error);

    const consumer = await currentRecvTransport.consume(consumerParams);
    const audio = new Audio();
    audio.srcObject = new MediaStream([consumer.track]);
    audio.autoplay = true;
    audio.volume = 1.0;
    audio.style.display = 'none';
    document.body.appendChild(audio);
    audioElements.push(audio);

    try {
      await audio.play();
      console.log('[mediasoup] ✅ Remote audio playing');
    } catch (e) {
      console.warn('[mediasoup] ⚠️ Audio play blocked, retrying on user gesture:', e);
      const resumeAudio = () => {
        audio.play().then(() => {
          console.log('[mediasoup] ✅ Audio resumed after user gesture');
        }).catch(() => { });
        document.removeEventListener('click', resumeAudio);
        document.removeEventListener('touchstart', resumeAudio);
      };
      document.addEventListener('click', resumeAudio);
      document.addEventListener('touchstart', resumeAudio);
    }

    if (onRemoteAudio) {
      onRemoteAudio(audio);
    }
  };

  // Register listener immediately to avoid missing events during setup async calls
  socket.on('newProducer', handleNewProducer);

  const rtpCapabilities = await new Promise(res =>
    socket.emit('getRouterRtpCapabilities', res)
  );

  const device = new Device();
  await device.load({ routerRtpCapabilities: rtpCapabilities });
  currentDevice = device;

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
  currentRecvTransport = recvTransport;

  // Wire up DTLS connect events (with guards to prevent duplicate connect)
  let sendConnected = false;
  sendTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    if (sendConnected) {
      console.warn('[mediasoup] ⚠️ SEND transport connect already called — skipping duplicate');
      return cb();
    }
    sendConnected = true;
    socket.emit('connectSendTransport', { dtlsParameters }, (err) => {
      if (err) {
        sendConnected = false;
        return errback(err);
      }
      cb();
    });
  });

  let recvConnected = false;
  recvTransport.on('connect', ({ dtlsParameters }, cb, errback) => {
    if (recvConnected) {
      console.warn('[mediasoup] ⚠️ RECV transport connect already called — skipping duplicate');
      return cb();
    }
    recvConnected = true;
    socket.emit('connectRecvTransport', { dtlsParameters }, (err) => {
      if (err) {
        recvConnected = false;
        return errback(err);
      }
      cb();
    });
  });
  sendTransport.on('produce', ({ kind, rtpParameters }, cb, errback) => {
    socket.emit('produce', { kind, rtpParameters, callId }, ({ id, error }) => {
      if (error) return errback(error);
      cb({ id });
    });
  });

  isReady = true;
  for (const pid of pendingProducers) {
    handleNewProducer({ producerId: pid });
  }


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
  return {
    sendTransport, recvTransport, close: () => {
      _currentCallId = null;
      audioElements.forEach(a => a.remove());
      stream?.getTracks().forEach(track => track.stop());
      sendTransport.close();
      recvTransport.close();
      socket.off('newProducer', handleNewProducer);
    }
  };
}
