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

export async function setupCall(socket, callId, onRemoteAudio, authToken) {
  _currentCallId = callId;
  persistCallState(callId, socket.id);

  let isReady = false;
  let currentDevice = null;
  let currentRecvTransport = null;
  const pendingProducers = [];
  const audioElements = [];

  // ── Call recording: mix local mic + remote audio into one MediaRecorder ──
  const recordingBaseUrl = (socket.io && socket.io.uri) || '';
  let recAudioCtx = null;
  let recMixDest = null;
  let recRecorder = null;
  let recChunks = [];

  const ensureRecMixer = () => {
    if (recMixDest) return;
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      recAudioCtx = new Ctx();
      recMixDest = recAudioCtx.createMediaStreamDestination();
    } catch (e) {
      console.warn('[rec] AudioContext unavailable, recording disabled', e);
    }
  };

  const startRecorder = () => {
    if (recRecorder || !recMixDest) return;
    let options;
    if (typeof MediaRecorder !== 'undefined' &&
        MediaRecorder.isTypeSupported &&
        MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
      options = { mimeType: 'audio/webm;codecs=opus' };
    }
    try {
      recRecorder = new MediaRecorder(recMixDest.stream, options);
    } catch (e) {
      console.warn('[rec] MediaRecorder init failed', e);
      return;
    }
    recRecorder.ondataavailable = (e) => {
      if (e.data && e.data.size > 0) recChunks.push(e.data);
    };
    recRecorder.start(2000);
    console.log('[rec] ▶️ recording started');
  };

  const addTrackToRecording = (track) => {
    ensureRecMixer();
    if (!recMixDest) return;
    try {
      const src = recAudioCtx.createMediaStreamSource(new MediaStream([track]));
      src.connect(recMixDest);
    } catch (e) {
      console.warn('[rec] failed to add track to recording', e);
      return;
    }
    startRecorder();
  };

  const stopRecordingAndUpload = () => {
    if (!recRecorder) return;
    const mimeType = recRecorder.mimeType || 'audio/webm';
    recRecorder.onstop = () => {
      try { if (recAudioCtx) recAudioCtx.close(); } catch { }
      const blob = new Blob(recChunks, { type: mimeType });
      recChunks = [];
      if (!blob.size || !recordingBaseUrl) return;
      fetch(`${recordingBaseUrl}/api/recordings/${callId}`, {
        method: 'POST',
        headers: {
          'Content-Type': mimeType,
          ...(authToken ? { Authorization: `Bearer ${authToken}` } : {}),
        },
        body: blob,
      })
        .then(() => console.log('[rec] ⬆️ recording uploaded'))
        .catch((e) => console.error('[rec] upload failed', e));
    };
    try { recRecorder.stop(); } catch { }
    recRecorder = null;
  };

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
    addTrackToRecording(consumer.track);

    // In-DOM <audio> with playsinline, not `new Audio()` — a bare Audio object
    // stays silent on iOS and the browser won't even pull RTP for it.
    const audio = document.createElement('audio');
    audio.srcObject = new MediaStream([consumer.track]);
    audio.autoplay = true;
    audio.muted = false;
    audio.volume = 1.0;
    audio.setAttribute('playsinline', 'true');
    audio.setAttribute('webkit-playsinline', 'true');
    audio.style.display = 'none';
    document.body.appendChild(audio);
    audioElements.push(audio);

    const tryPlay = async () => {
      try { await audio.play(); return true; } catch (e) { return false; }
    };
    if (!(await tryPlay())) {
      console.warn('[mediasoup] ⚠️ Audio play blocked — retrying on next gesture');
      const resumeAudio = async () => {
        if (await tryPlay()) {
          document.removeEventListener('click', resumeAudio);
          document.removeEventListener('touchstart', resumeAudio);
        }
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
  if (sendParams.error) throw new Error(`createSendTransport failed: ${sendParams.error}`);
  const recvParams = await new Promise(res => socket.emit('createRecvTransport', res));
  if (recvParams.error) throw new Error(`createRecvTransport failed: ${recvParams.error}`);
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
    // Check available devices first
    const devices = await navigator.mediaDevices.enumerateDevices();
    const audioInputs = devices.filter(d => d.kind === 'audioinput');
    console.log('[mediasoup] Available audio inputs:', audioInputs.map(d => `${d.label || 'unnamed'} (${d.deviceId?.slice(0,8)})`));

    if (audioInputs.length === 0) {
      throw new Error('No microphone found. Please connect a mic and try again.');
    }

    stream = await navigator.mediaDevices.getUserMedia({
      audio: { deviceId: audioInputs[0].deviceId ? { ideal: audioInputs[0].deviceId } : undefined }
    });
    const track = stream.getAudioTracks()[0];
    addTrackToRecording(track);
    await sendTransport.produce({ track });
  } catch (err) {
    // Tear down whatever was already created before rethrowing. Otherwise a
    // failure here (most often the first-call mic-permission prompt) leaks the
    // transports on both client and server, and the retry collides with the
    // stale ones — the "MID already exists" / "Call not found" produce errors
    // that made the first call fail and disconnect the panel.
    console.error('Failed to get media devices:', err);
    try { stream?.getTracks().forEach(t => t.stop()); } catch (_) {}
    try { if (recRecorder) recRecorder.stop(); } catch (_) {}
    try { if (recAudioCtx) recAudioCtx.close(); } catch (_) {}
    try { sendTransport.close(); } catch (_) {}
    try { recvTransport.close(); } catch (_) {}
    try { socket.off('newProducer', handleNewProducer); } catch (_) {}
    _currentCallId = null;
    isReady = false;
    throw err;
  }
  return {
    sendTransport, recvTransport, close: () => {
      _currentCallId = null;
      stopRecordingAndUpload();
      audioElements.forEach(a => a.remove());
      stream?.getTracks().forEach(track => track.stop());
      sendTransport.close();
      recvTransport.close();
      socket.off('newProducer', handleNewProducer);
    }
  };
}
