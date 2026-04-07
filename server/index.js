require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { startMediasoup, createTransport, getNextRouter, getAnyRouter } = require('./mediasoup');
const { register, m, startPeriodicLog, printMetricsSnapshot } = require('./metrics');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// In-memory call state
// calls[callId] = { agentSocket, customerSocket, transports, producers, consumers }
const calls = {};

// Presence tracking
// Map socket.id -> { id, username, role }
const activeUsers = new Map();

function broadcastPresence() {
  const agents = [];
  const customers = [];

  for (const [id, user] of activeUsers.entries()) {
    if (user.role === 'agent') {
      agents.push(user);
    } else if (user.role === 'customer') {
      customers.push(user);
    }
  }

  // Update presence gauges
  m.activeUsersGauge.set({ role: 'agent' }, agents.length);
  m.activeUsersGauge.set({ role: 'customer' }, customers.length);

  // Send the live roster to everyone
  io.emit('presenceUpdate', { agents, customers });
}

async function main() {
  await startMediasoup();

  // Set worker count gauge once the pool is ready
  m.workerCountGauge.set(require('os').cpus().length);

  io.on('connection', (socket) => {
    m.connectionsCounter.inc();
    const { role, username } = socket.handshake.query;

    if (role && username) {
      console.log(`[connect] ✅ ${role} connected: ${username} (${socket.id})`);

      activeUsers.set(socket.id, {
        id: socket.id,
        username,
        role
      });

      if (role === 'agent') {
        socket.join('agents');
      } else if (role === 'customer') {
        socket.join('customers');
      }

      broadcastPresence();
    } else {
      console.warn(`[connect] ⚠️  Socket ${socket.id} connected without role/username`);
    }

    // ── STEP 1: Client requests router RTP capabilities ──────────────────
    // Use the call's assigned router if available; otherwise any router
    // (all workers share identical RTP capabilities via the same mediaCodecs).
    socket.on('getRouterRtpCapabilities', (cb) => {
      const callId = socket._callId;
      const router = (callId && calls[callId]?.router) || getAnyRouter();
      console.log(`[rtp] 📋 getRouterRtpCapabilities for socket=${socket.id} callId=${callId || 'none'} routerId=${router?.id}`);
      cb(router.rtpCapabilities);
    });

    // ── STEP 2: Create a send (produce) transport ─────────────────────────
    socket.on('createSendTransport', async (cb) => {
      try {
        const callId = socket._callId;
        const router = (callId && calls[callId]?.router) || getAnyRouter();
        console.log(`[transport] 📤 Creating SEND transport for socket=${socket.id} callId=${callId || 'none'} routerId=${router?.id}`);
        const { transport, params } = await createTransport(router);
        socket._sendTransport = transport;
        console.log(`[transport] ✅ SEND transport created: ${transport.id} for socket=${socket.id}`);
        cb(params);
      } catch (err) {
        console.error(`[transport] ❌ Failed to create SEND transport for socket=${socket.id}:`, err);
        cb({ error: err.message });
      }
    });

    // ── STEP 3: Create a recv (consume) transport ─────────────────────────
    socket.on('createRecvTransport', async (cb) => {
      try {
        const callId = socket._callId;
        const router = (callId && calls[callId]?.router) || getAnyRouter();
        console.log(`[transport] 📥 Creating RECV transport for socket=${socket.id} callId=${callId || 'none'} routerId=${router?.id}`);
        const { transport, params } = await createTransport(router);
        socket._recvTransport = transport;
        console.log(`[transport] ✅ RECV transport created: ${transport.id} for socket=${socket.id}`);
        cb(params);
      } catch (err) {
        console.error(`[transport] ❌ Failed to create RECV transport for socket=${socket.id}:`, err);
        cb({ error: err.message });
      }
    });

    // ── STEP 4: Connect send transport (DTLS handshake) ───────────────────
    socket.on('connectSendTransport', async ({ dtlsParameters }, cb) => {
      try {
        console.log(`[dtls] 🔒 connectSendTransport for socket=${socket.id} transportId=${socket._sendTransport?.id}`);
        console.log(`[dtls]    DTLS role: ${dtlsParameters?.role}, fingerprints: ${dtlsParameters?.fingerprints?.length || 0}`);
        await socket._sendTransport.connect({ dtlsParameters });
        console.log(`[dtls] ✅ SEND transport connected for socket=${socket.id}`);
        cb();
      } catch (err) {
        console.error(`[dtls] ❌ SEND transport connect FAILED for socket=${socket.id}:`, err);
        cb(err.message);
      }
    });

    // ── STEP 5: Connect recv transport ────────────────────────────────────
    socket.on('connectRecvTransport', async ({ dtlsParameters }, cb) => {
      try {
        console.log(`[dtls] 🔒 connectRecvTransport for socket=${socket.id} transportId=${socket._recvTransport?.id}`);
        console.log(`[dtls]    DTLS role: ${dtlsParameters?.role}, fingerprints: ${dtlsParameters?.fingerprints?.length || 0}`);
        await socket._recvTransport.connect({ dtlsParameters });
        console.log(`[dtls] ✅ RECV transport connected for socket=${socket.id}`);
        cb();
      } catch (err) {
        console.error(`[dtls] ❌ RECV transport connect FAILED for socket=${socket.id}:`, err);
        cb(err.message);
      }
    });

    // ── STEP 6: Client starts producing audio ─────────────────────────────
    socket.on('produce', async ({ kind, rtpParameters, callId }, cb) => {
      try {
        console.log(`[produce] 🎤 Produce request: socket=${socket.id} kind=${kind} callId=${callId}`);
        console.log(`[produce]    RTP codecs: ${rtpParameters?.codecs?.map(c => c.mimeType).join(', ')}`);
        console.log(`[produce]    sendTransport exists: ${!!socket._sendTransport}, id=${socket._sendTransport?.id}`);

        const producer = await socket._sendTransport.produce({ kind, rtpParameters });
        socket._producer = producer;
        console.log(`[produce] ✅ Producer created: id=${producer.id} kind=${producer.kind} paused=${producer.paused}`);

        // Log producer lifecycle events
        producer.on('transportclose', () => {
          console.warn(`[produce] 🛑 Producer ${producer.id} — transport closed`);
        });
        producer.on('score', (score) => {
          console.log(`[produce] 📊 Producer ${producer.id} score:`, JSON.stringify(score));
        });

        const call = calls[callId];
        if (!call) {
          console.error(`[produce] ❌ Call not found: callId=${callId}`);
          return cb({ error: 'Call not found' });
        }

        // Tell the other party to consume this new producer
        const otherSocket = role === 'agent' ? call.customerSocket : call.agentSocket;
        if (otherSocket) {
          console.log(`[produce] 📢 Emitting newProducer to other party socket=${otherSocket.id} producerId=${producer.id}`);
          otherSocket.emit('newProducer', { producerId: producer.id });
        } else {
          console.warn(`[produce] ⚠️  No other party socket found in call ${callId} — newProducer NOT sent`);
        }

        cb({ id: producer.id });
      } catch (err) {
        console.error(`[produce] ❌ Produce FAILED for socket=${socket.id}:`, err);
        cb({ error: err.message });
      }
    });

    // ── STEP 7: Other party consumes the producer ─────────────────────────
    socket.on('consume', async ({ producerId, rtpCapabilities, callId }, cb) => {
      try {
        console.log(`[consume] 🔊 Consume request: socket=${socket.id} producerId=${producerId} callId=${callId}`);
        const router = (callId && calls[callId]?.router) || getAnyRouter();

        const canConsume = router.canConsume({ producerId, rtpCapabilities });
        console.log(`[consume] canConsume=${canConsume} routerId=${router?.id}`);

        if (!canConsume) {
          console.error(`[consume] ❌ Cannot consume: producerId=${producerId} — RTP capabilities mismatch or producer not found`);
          return cb({ error: 'Cannot consume' });
        }

        console.log(`[consume]    recvTransport exists: ${!!socket._recvTransport}, id=${socket._recvTransport?.id}`);
        const consumer = await socket._recvTransport.consume({
          producerId,
          rtpCapabilities,
          paused: false,
        });
        socket._consumer = consumer;
        console.log(`[consume] ✅ Consumer created: id=${consumer.id} kind=${consumer.kind} producerId=${producerId} paused=${consumer.paused}`);

        // Log consumer lifecycle events
        consumer.on('transportclose', () => {
          console.warn(`[consume] 🛑 Consumer ${consumer.id} — transport closed`);
        });
        consumer.on('producerclose', () => {
          console.warn(`[consume] 🛑 Consumer ${consumer.id} — producer closed`);
        });
        consumer.on('producerpause', () => {
          console.warn(`[consume] ⏸️  Consumer ${consumer.id} — producer paused`);
        });
        consumer.on('producerresume', () => {
          console.log(`[consume] ▶️  Consumer ${consumer.id} — producer resumed`);
        });
        consumer.on('score', (score) => {
          console.log(`[consume] 📊 Consumer ${consumer.id} score:`, JSON.stringify(score));
        });

        cb({
          id: consumer.id,
          producerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
        });
      } catch (err) {
        console.error(`[consume] ❌ Consume FAILED for socket=${socket.id}:`, err);
        cb({ error: err.message });
      }
    });

    // ── CALL INITIATION ───────────────────────────────────────────────────

    // Any-to-Any Direct Dial (Agent->Customer or Customer->Agent)
    socket.on('dialOut', ({ targetId }, cb) => {
      const callId = `call_${Date.now()}`;
      const targetSocket = io.sockets.sockets.get(targetId);

      if (!targetSocket) {
        console.warn(`[dialOut] ⚠️  Target ${targetId} not found or offline`);
        return cb({ error: 'Target user not found or offline.' });
      }

      const callerUser = activeUsers.get(socket.id);

      // Assign a dedicated router from the pool for this call (round-robin)
      const router = getNextRouter();

      // Assume the caller is agent and target is customer by default,
      // but flip it if the caller is the customer.
      if (callerUser.role === 'customer') {
        calls[callId] = { agentSocket: targetSocket, customerSocket: socket, router, startTime: Date.now() };
      } else {
        calls[callId] = { agentSocket: socket, customerSocket: targetSocket, router, startTime: Date.now() };
      }

      socket._callId = callId;
      targetSocket._callId = callId;

      // Metrics
      m.callsInitiatedCounter.inc({ type: 'dialOut' });
      m.activeCallsGauge.inc();

      console.log(`[dialOut] 📞 ${callerUser.username} (${callerUser.role}) → ${targetId} | callId=${callId} | routerId=${router.id}`);

      targetSocket.emit('incomingCall', {
        callId,
        from: callerUser.username,
        role: callerUser.role
      });

      cb({ callId });
    });

    // Generic "Call First Available" fallback (Customer to anyone)
    socket.on('callIn', () => {
      const callId = `call_${Date.now()}`;
      const router = getNextRouter();
      // Assign a dedicated router from the pool for this call (round-robin)
      calls[callId] = { customerSocket: socket, agentSocket: null, router, startTime: Date.now() };
      socket._callId = callId;

      const callerUser = activeUsers.get(socket.id);

      // Metrics
      m.callsInitiatedCounter.inc({ type: 'callIn' });
      m.activeCallsGauge.inc();

      console.log(`[callIn] 📞 General queue call from: ${callerUser.username} | callId=${callId} | routerId=${router.id}`);

      // Notify all agents
      io.to('agents').emit('incomingCall', {
        callId,
        from: callerUser.username,
        role: 'customer'
      });
    });

    // Receive acceptance
    socket.on('acceptCall', ({ callId }, cb) => {
      const call = calls[callId];
      if (!call) {
        console.warn(`[acceptCall] ⚠️  Call not found or ended: callId=${callId}`);
        return cb({ error: 'Call not found or ended' });
      }

      // If this was a general 'callIn' and the agent is answering, assign the agent socket
      if (role === 'agent' && !call.agentSocket) {
        call.agentSocket = socket;
        socket._callId = callId;
      }

      console.log(`[acceptCall] ✅ Call ${callId} accepted by socket=${socket.id} (${role})`);
      console.log(`[acceptCall]    agentSocket=${call.agentSocket?.id || 'none'} customerSocket=${call.customerSocket?.id || 'none'}`);

      // Tell the other party to start setupCall too
      const other = socket === call.agentSocket ? call.customerSocket : call.agentSocket;
      if (other) {
        console.log(`[acceptCall] 📢 Emitting callAccepted to other party socket=${other.id}`);
        other.emit('callAccepted', { callId });
      } else {
        console.warn(`[acceptCall] ⚠️  No other party found for callId=${callId}`);
      }

      cb({ callId });
    });

    // Request presence on explicit call, just in case
    socket.on('getPresence', () => {
      const agents = [];
      const customers = [];
      for (const user of activeUsers.values()) {
        if (user.role === 'agent') agents.push(user);
        if (user.role === 'customer') customers.push(user);
      }
      socket.emit('presenceUpdate', { agents, customers });
    });

    // ── HANGUP ────────────────────────────────────────────────────────────
    socket.on('hangup', () => {
      console.log(`[hangup] 📱 Hangup requested by socket=${socket.id}`);
      endCall(socket);
    });

    socket.on('disconnect', () => {
      m.disconnectionsCounter.inc();
      console.log(`[disconnect] 🔌 Socket ${socket.id} disconnecting...`);
      endCall(socket);
      if (activeUsers.has(socket.id)) {
        console.log(`[disconnect] User disconnected: ${activeUsers.get(socket.id).username} (${socket.id})`);
        activeUsers.delete(socket.id);
        broadcastPresence();
      }
    });
  });

  function endCall(socket) {
    const callId = socket._callId;
    if (!callId || !calls[callId]) return;
    const call = calls[callId];

    // Record call duration and update counters
    let durationSec = 0;
    if (call.startTime) {
      durationSec = (Date.now() - call.startTime) / 1000;
      m.callDurationHistogram.observe(durationSec);
    }
    m.callsEndedCounter.inc();
    m.activeCallsGauge.dec();

    console.log(`[endCall] 🛑 callId=${callId} ended (duration: ${durationSec.toFixed(1)}s)`);

    // Log cleanup details
    const other = socket === call.agentSocket ? call.customerSocket : call.agentSocket;

    console.log(`[endCall]    Caller socket=${socket.id}: producer=${!!socket._producer} consumer=${!!socket._consumer} sendT=${!!socket._sendTransport} recvT=${!!socket._recvTransport}`);
    if (other) {
      console.log(`[endCall]    Other  socket=${other.id}: producer=${!!other._producer} consumer=${!!other._consumer} sendT=${!!other._sendTransport} recvT=${!!other._recvTransport}`);
    }

    // Notify the other party
    other?.emit('callEnded');

    // Unbind call IDs
    if (call.agentSocket) call.agentSocket._callId = null;
    if (call.customerSocket) call.customerSocket._callId = null;

    // Clean up mediasoup resources for caller
    try { socket._producer?.close(); } catch (_) { }
    try { socket._consumer?.close(); } catch (_) { }
    try { socket._sendTransport?.close(); } catch (_) { }
    try { socket._recvTransport?.close(); } catch (_) { }
    socket._producer = socket._consumer = socket._sendTransport = socket._recvTransport = null;

    // Clean up mediasoup resources for other party
    if (other) {
      try { other._producer?.close(); } catch (_) { }
      try { other._consumer?.close(); } catch (_) { }
      try { other._sendTransport?.close(); } catch (_) { }
      try { other._recvTransport?.close(); } catch (_) { }
      other._producer = other._consumer = other._sendTransport = other._recvTransport = null;
    }

    delete calls[callId];
    console.log(`[endCall] 🧹 Cleaned up all resources for callId=${callId}`);
  }

  app.get('/', (_req, res) => {
    res.json({ message: 'Mediasoup server is running.' });
  });

  // ── Prometheus scrape endpoint ────────────────────────────────────────────
  app.get('/metrics', async (_req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  });

  // ── Immediate snapshot on startup + periodic console logging ─────────────
  startPeriodicLog();
  server.listen(3000, () => {
    console.log('Server running on port 3000');
    console.log('Prometheus metrics available at http://localhost:3000/metrics');
    // Print an initial snapshot right away so the first log isn't 60s away
    printMetricsSnapshot();
  });
}

main().catch(console.error);
