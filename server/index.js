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
      console.log(`[${role}] connected: ${username} (${socket.id})`);

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
    }

    // ── STEP 1: Client requests router RTP capabilities ──────────────────
    // Use the call's assigned router if available; otherwise any router
    // (all workers share identical RTP capabilities via the same mediaCodecs).
    socket.on('getRouterRtpCapabilities', (cb) => {
      const callId = socket._callId;
      const router = (callId && calls[callId]?.router) || getAnyRouter();
      cb(router.rtpCapabilities);
    });

    // ── STEP 2: Create a send (produce) transport ─────────────────────────
    socket.on('createSendTransport', async (cb) => {
      const callId = socket._callId;
      const router = (callId && calls[callId]?.router) || getAnyRouter();
      const { transport, params } = await createTransport(router);
      socket._sendTransport = transport;
      cb(params);
    });

    // ── STEP 3: Create a recv (consume) transport ─────────────────────────
    socket.on('createRecvTransport', async (cb) => {
      const callId = socket._callId;
      const router = (callId && calls[callId]?.router) || getAnyRouter();
      const { transport, params } = await createTransport(router);
      socket._recvTransport = transport;
      cb(params);
    });

    // ── STEP 4: Connect send transport (DTLS handshake) ───────────────────
    socket.on('connectSendTransport', async ({ dtlsParameters }, cb) => {
      await socket._sendTransport.connect({ dtlsParameters });
      cb();
    });

    // ── STEP 5: Connect recv transport ────────────────────────────────────
    socket.on('connectRecvTransport', async ({ dtlsParameters }, cb) => {
      await socket._recvTransport.connect({ dtlsParameters });
      cb();
    });

    // ── STEP 6: Client starts producing audio ─────────────────────────────
    socket.on('produce', async ({ kind, rtpParameters, callId }, cb) => {
      const producer = await socket._sendTransport.produce({ kind, rtpParameters });
      socket._producer = producer;

      const call = calls[callId];
      if (!call) return cb({ error: 'Call not found' });

      // Tell the other party to consume this new producer
      const otherSocket = role === 'agent' ? call.customerSocket : call.agentSocket;
      if (otherSocket) {
        otherSocket.emit('newProducer', { producerId: producer.id });
      }

      cb({ id: producer.id });
    });

    // ── STEP 7: Other party consumes the producer ─────────────────────────
    socket.on('consume', async ({ producerId, rtpCapabilities, callId }, cb) => {
      const router = (callId && calls[callId]?.router) || getAnyRouter();
      if (!router.canConsume({ producerId, rtpCapabilities })) {
        return cb({ error: 'Cannot consume' });
      }
      const consumer = await socket._recvTransport.consume({
        producerId,
        rtpCapabilities,
        paused: false,
      });
      socket._consumer = consumer;
      cb({
        id: consumer.id,
        producerId,
        kind: consumer.kind,
        rtpParameters: consumer.rtpParameters,
      });
    });

    // ── CALL INITIATION ───────────────────────────────────────────────────

    // Any-to-Any Direct Dial (Agent->Customer or Customer->Agent)
    socket.on('dialOut', ({ targetId }, cb) => {
      const callId = `call_${Date.now()}`;
      const targetSocket = io.sockets.sockets.get(targetId);

      if (!targetSocket) return cb({ error: 'Target user not found or offline.' });

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

      console.log(`[dialOut] ${callerUser.username} → ${targetId} | callId=${callId}`);

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
      // Assign a dedicated router from the pool for this call (round-robin)
      calls[callId] = { customerSocket: socket, agentSocket: null, router: getNextRouter(), startTime: Date.now() };
      socket._callId = callId;

      const callerUser = activeUsers.get(socket.id);

      // Metrics
      m.callsInitiatedCounter.inc({ type: 'callIn' });
      m.activeCallsGauge.inc();

      console.log(`[callIn] General queue call from: ${callerUser.username} | callId=${callId}`);

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
      if (!call) return cb({ error: 'Call not found or ended' });

      // If this was a general 'callIn' and the agent is answering, assign the agent socket
      if (role === 'agent' && !call.agentSocket) {
        call.agentSocket = socket;
        socket._callId = callId;
      }

      console.log(`[acceptCall] Subscribed to call ${callId}`);

      // Tell the other party to start setupCall too
      const other = socket === call.agentSocket ? call.customerSocket : call.agentSocket;
      other?.emit('callAccepted', { callId });

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
    socket.on('hangup', () => endCall(socket));

    socket.on('disconnect', () => {
      m.disconnectionsCounter.inc();
      endCall(socket);
      if (activeUsers.has(socket.id)) {
        console.log(`User disconnected: ${activeUsers.get(socket.id).username}`);
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
    if (call.startTime) {
      const durationSec = (Date.now() - call.startTime) / 1000;
      m.callDurationHistogram.observe(durationSec);
    }
    m.callsEndedCounter.inc();
    m.activeCallsGauge.dec();

    console.log(`[endCall] callId=${callId} ended`);

    // Notify the other party
    const other = socket === call.agentSocket ? call.customerSocket : call.agentSocket;
    other?.emit('callEnded');

    // Unbind call IDs
    if (call.agentSocket) call.agentSocket._callId = null;
    if (call.customerSocket) call.customerSocket._callId = null;

    // Clean up mediasoup resources for caller
    try { socket._producer?.close(); } catch (_) {}
    try { socket._consumer?.close(); } catch (_) {}
    try { socket._sendTransport?.close(); } catch (_) {}
    try { socket._recvTransport?.close(); } catch (_) {}
    socket._producer = socket._consumer = socket._sendTransport = socket._recvTransport = null;

    // Clean up mediasoup resources for other party
    if (other) {
      try { other._producer?.close(); } catch (_) {}
      try { other._consumer?.close(); } catch (_) {}
      try { other._sendTransport?.close(); } catch (_) {}
      try { other._recvTransport?.close(); } catch (_) {}
      other._producer = other._consumer = other._sendTransport = other._recvTransport = null;
    }

    delete calls[callId];
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
