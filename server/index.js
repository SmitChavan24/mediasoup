require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { startMediasoup, createTransport, getNextRouter, getAnyRouter } = require('./mediasoup');
const { register, m, startPeriodicLog, printMetricsSnapshot } = require('./metrics');
const {
  createRedisClients,
  cleanupStaleState,
  GRACE_PERIOD_SECONDS,

  setUser,
  getUser,
  updateUser,
  deleteUser,

  addToPresence,
  removeFromPresence,
  getPresenceList,
  getPresenceCount,

  setCall,
  getCall,
  updateCall,
  deleteCall,
  getAllActiveCalls,

  setGracePeriod,
  getGracePeriod,
  deleteGracePeriod,

  subscribeToExpirations,
} = require('./redisState');
const { attachHeartbeat } = require('./heartbeat');
const { registerUser, loginUser, verifyToken } = require('./auth');
const { createPool, initDatabase } = require('./db');
const { insertCallRecord, updateCallRecord, getCallHistory, getUserIdByUsername } = require('./callHistory');

const app = express();
app.use(cors());
app.use(express.json());

const webpush = require('web-push');

// Initialize Web Push with generated VAPID keys
webpush.setVapidDetails(
  'mailto:support@mediasoup-app.com',
  'BIS3SfRv_zL4c4e6LEQqD7r4x3gBbHxnGX_TUXQ0DfXXOLsov4LmKGaMiNPTXvCR1Xlkcc4wTQFe_67XcOXrgkM',
  'HMzH48TX5r1xlJbJZO4zy9defFVLcKQhUTZthr8MHVA'
);

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// ── Socket.IO Authentication Middleware ───────────────────────────────────────
// Every socket connection MUST provide a valid JWT token.
io.use((socket, next) => {
  const token = socket.handshake.auth?.token;
  if (!token) {
    return next(new Error('Authentication required. Please login first.'));
  }
  const decoded = verifyToken(token);
  if (!decoded) {
    return next(new Error('Invalid or expired token. Please login again.'));
  }
  // Attach user info to the socket for use in all event handlers
  socket.user = decoded; // { userId, username, role }
  next();
});

// ── Local mediasoup transport/producer/consumer refs ──────────────────────────
// These are C++ objects that cannot be serialised into Redis and MUST live in
// the same process.  We index them by socket.id so we can clean them up.
const localTransports = {};   // socketId → { sendTransport, recvTransport }
const localProducers = {};   // socketId → producer
const localConsumers = {};   // socketId → consumer

// ── Per-call router mapping ──────────────────────────────────────────────────
// Each call MUST use a single router for all its transports (send/recv for both
// parties).  Routers are C++ objects that can't live in Redis, so we keep a
// local map.  The router is assigned once when the call is created (dialOut /
// callIn) and reused for every createTransport / consume within that call.
const callRouters = {};       // callId → router
const pendingRings = {};      // callId → { interval, sub, targetDbId }
const globalOfflineTargets = {}; // targetDbId → { callId, callerUsername, callerRole, targetRole }

// ── Helpers ──────────────────────────────────────────────────────────────────

function clearPendingRings(callId) {
  if (callId && pendingRings[callId]) {
    clearInterval(pendingRings[callId].interval);
    webpush.sendNotification(pendingRings[callId].sub, JSON.stringify({ type: 'cancelCall' })).catch(() => { });
    if (pendingRings[callId].targetDbId) {
      delete globalOfflineTargets[pendingRings[callId].targetDbId];
    }
    delete pendingRings[callId];
  }
}

async function broadcastPresence() {
  const [agents, customers] = await Promise.all([
    getPresenceList('agent'),
    getPresenceList('customer'),
  ]);

  // Update Prometheus gauges
  m.activeUsersGauge.set({ role: 'agent' }, agents.length);
  m.activeUsersGauge.set({ role: 'customer' }, customers.length);

  // Send the live roster to everyone
  io.emit('presenceUpdate', { agents, customers });
}

/**
 * Full cleanup when a user is gone for good (hangup, grace expired, etc.)
 * Closes mediasoup transports/producers/consumers and removes all Redis state.
 */
async function fullCleanup(socketId) {
  const user = await getUser(socketId);
  if (!user) return;

  const callId = user.callId;

  // Close mediasoup C++ objects for this socket
  closeLocalMediasoup(socketId);

  // If they were in a call, clean up the call
  if (callId && callId !== '') {
    const call = await getCall(callId);
    if (call) {
      // Find the other party
      const otherSocketId = call.agentSocketId === socketId
        ? call.customerSocketId
        : call.agentSocketId;

      // Notify the other party
      if (otherSocketId) {
        const otherSocket = io.sockets.sockets.get(otherSocketId);
        if (otherSocket) {
          otherSocket.emit('callEnded');
        }

        // Close the other party's mediasoup resources too
        closeLocalMediasoup(otherSocketId);

        // Clear the other party's callId
        await updateUser(otherSocketId, { callId: '' });
      }

      // Record call duration + metrics
      let durationSec = 0;
      if (call.startTime) {
        durationSec = (Date.now() - parseInt(call.startTime, 10)) / 1000;
        m.callDurationHistogram.observe(durationSec);
      }
      m.callsEndedCounter.inc();
      m.activeCallsGauge.dec();

      console.log(`[endCall] 🛑 callId=${callId} ended (duration: ${durationSec.toFixed(1)}s)`);

      // ── Persist call history (fullCleanup path) ──────────────────────
      try {
        await updateCallRecord(callId, {
          ended_at: true,
          duration_sec: durationSec,
        });
      } catch (err) {
        console.error(`[fullCleanup] ❌ Failed to update call history:`, err);
      }

      // Notify any admins monitoring this call
      io.to(`monitor:${callId}`).emit('callEnded');
      // Clean up monitoring admins' mediasoup resources
      const monitorRoom = io.sockets.adapter.rooms.get(`monitor:${callId}`);
      if (monitorRoom) {
        for (const adminSocketId of monitorRoom) {
          closeLocalMediasoup(adminSocketId);
          await updateUser(adminSocketId, { callId: '' });
        }
      }

      clearPendingRings(callId);
      delete callRouters[callId];
      await deleteCall(callId);
    }
  }

  // Remove from presence and delete user
  await removeFromPresence(user.role, socketId);
  await deleteUser(socketId);
  await deleteGracePeriod(socketId);

  await broadcastPresence();

  // Notify admin dashboards
  io.to('admins').emit('callsUpdated');

  console.log(`[cleanup] 🧹 Full cleanup complete for socket=${socketId} user=${user.username}`);
}

function closeLocalMediasoup(socketId) {
  const transports = localTransports[socketId];
  if (transports) {
    try { transports.sendTransport?.close(); } catch (_) { }
    try { transports.recvTransport?.close(); } catch (_) { }
    delete localTransports[socketId];
  }
  try { localProducers[socketId]?.close(); } catch (_) { }
  try { localConsumers[socketId]?.close(); } catch (_) { }
  delete localProducers[socketId];
  delete localConsumers[socketId];
}

/**
 * endCall — called on explicit hangup.  Cleans up both parties immediately
 * (no grace period since this is an intentional hangup).
 */
async function endCall(socket) {
  const user = await getUser(socket.id);
  if (!user || !user.callId) return;

  const callId = user.callId;
  const call = await getCall(callId);
  if (!call) return;

  // Record call duration + metrics
  let durationSec = 0;
  if (call.startTime) {
    durationSec = (Date.now() - parseInt(call.startTime, 10)) / 1000;
    m.callDurationHistogram.observe(durationSec);
  }
  m.callsEndedCounter.inc();
  m.activeCallsGauge.dec();

  console.log(`[endCall] 🛑 callId=${callId} ended (duration: ${durationSec.toFixed(1)}s)`);

  // ── Persist call history ──────────────────────────────────────────────
  try {
    await updateCallRecord(callId, {
      ended_at: true,
      duration_sec: durationSec,
    });
  } catch (err) {
    console.error(`[endCall] ❌ Failed to update call history:`, err);
  }

  // Find the other party
  const otherSocketId = call.agentSocketId === socket.id
    ? call.customerSocketId
    : call.agentSocketId;

  // Notify + cleanup other party
  if (otherSocketId) {
    const otherSocket = io.sockets.sockets.get(otherSocketId);
    if (otherSocket) {
      otherSocket.emit('callEnded');
    }
    closeLocalMediasoup(otherSocketId);
    await updateUser(otherSocketId, { callId: '' });
  }

  // Cleanup this socket
  closeLocalMediasoup(socket.id);
  await updateUser(socket.id, { callId: '' });

  // Clear push loop
  clearPendingRings(callId);
  await updateUser(socket.id, { callId: '' });

  // Notify any admins monitoring this call
  io.to(`monitor:${callId}`).emit('callEnded');
  const monitorRoom = io.sockets.adapter.rooms.get(`monitor:${callId}`);
  if (monitorRoom) {
    for (const adminSocketId of monitorRoom) {
      closeLocalMediasoup(adminSocketId);
      await updateUser(adminSocketId, { callId: '' });
    }
  }

  // Delete the call and its router reference
  delete callRouters[callId];
  await deleteCall(callId);

  console.log(`[endCall] 🧹 Cleaned up all resources for callId=${callId}`);

  // Notify admin dashboards
  io.to('admins').emit('callsUpdated');
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // 0. Initialize MySQL database
  createPool();
  await initDatabase();

  // 1. Connect to Redis and clean stale state
  createRedisClients();
  await cleanupStaleState();

  // 2. Start mediasoup workers
  await startMediasoup();
  m.workerCountGauge.set(require('os').cpus().length);

  // 3. Subscribe to Redis grace-period expirations
  subscribeToExpirations(async (socketId) => {
    console.log(`[grace] ⏰ Grace period expired for ${socketId} — running full cleanup`);
    await fullCleanup(socketId);
  });

  // 4. Socket.IO connection handler
  io.on('connection', async (socket) => {
    m.connectionsCounter.inc();
    // User info is guaranteed by the auth middleware
    const { role, username } = socket.user;

    console.log(`[connect] ✅ ${role} connected: ${username} (${socket.id})`);

    await setUser(socket.id, { username, role, status: 'connected' });
    await addToPresence(role, socket.id);

    if (role === 'agent') {
      socket.join('agents');
    } else if (role === 'customer') {
      socket.join('customers');
    } else if (role === 'admin') {
      socket.join('admins');
    }

    // ── Check Offline Call Handoff ──────────────────────────────────────────
    // If this user was dialed while offline, immediately alert them!
    const userDbId = socket.user.userId;
    if (userDbId && globalOfflineTargets[userDbId]) {
      const offlinePending = globalOfflineTargets[userDbId];

      // Assign the call to this specific socket!
      await updateUser(socket.id, { callId: offlinePending.callId });

      const updates = offlinePending.targetRole === 'customer'
        ? { customerSocketId: socket.id }
        : { agentSocketId: socket.id };
      await updateCall(offlinePending.callId, updates);

      // Alert the user's frontend UI
      socket.emit('incomingCall', {
        callId: offlinePending.callId,
        from: offlinePending.callerUsername,
        role: offlinePending.callerRole
      });

      // Clear the OS push loop so their phone stops buzzing natively
      clearPendingRings(offlinePending.callId);

      // Alert the Caller UI that the target has opened the app!
      const activeCall = await getCall(offlinePending.callId);
      if (activeCall) {
        const callerSocketId = offlinePending.targetRole === 'customer' ? activeCall.agentSocketId : activeCall.customerSocketId;
        if (callerSocketId) {
          const callerSocket = io.sockets.sockets.get(callerSocketId);
          if (callerSocket) callerSocket.emit('callStateUpdate', 'Calling...');
        }
      }

      delete globalOfflineTargets[userDbId];
    }

    // Admins don't appear in agent/customer presence — only broadcast for agents & customers
    await broadcastPresence();

    // Attach application-level heartbeat
    attachHeartbeat(socket, async (timedOutSocket) => {
      console.log(`[heartbeat] 💀 Heartbeat timeout for ${timedOutSocket.id} — triggering disconnect`);
      timedOutSocket.disconnect(true);
    });

    // ── Reconnection: client tries to restore a previous session ───────────
    socket.on('reconnectSession', async ({ previousSocketId, callId }, cb) => {
      console.log(`[reconnect] 🔄 Reconnect attempt: newSocket=${socket.id} prevSocket=${previousSocketId} callId=${callId}`);

      const gracedCallId = await getGracePeriod(previousSocketId);

      if (gracedCallId && gracedCallId === callId) {
        // ✅ Grace period still active — restore session
        console.log(`[reconnect] ✅ Grace period active — restoring session`);

        const call = await getCall(callId);
        if (!call) {
          console.warn(`[reconnect] ⚠️  Call ${callId} no longer exists`);
          return cb({ success: false, reason: 'call_ended' });
        }

        const user = await getUser(previousSocketId);
        if (!user) {
          console.warn(`[reconnect] ⚠️  User data for ${previousSocketId} not found`);
          return cb({ success: false, reason: 'session_expired' });
        }

        // Migrate user data to new socket ID
        await deleteUser(previousSocketId);
        await removeFromPresence(user.role, previousSocketId);
        closeLocalMediasoup(previousSocketId);

        await setUser(socket.id, { username: user.username, role: user.role, status: 'connected' });
        await updateUser(socket.id, { callId });
        await addToPresence(user.role, socket.id);

        // Update call record with new socket ID
        if (call.agentSocketId === previousSocketId) {
          await updateCall(callId, { agentSocketId: socket.id });
        } else {
          await updateCall(callId, { customerSocketId: socket.id });
        }

        // Cancel grace period
        await deleteGracePeriod(previousSocketId);

        // Join appropriate room
        if (user.role === 'agent') socket.join('agents');
        else socket.join('customers');

        // Notify the other party
        const updatedCall = await getCall(callId);
        const otherSocketId = updatedCall.agentSocketId === socket.id
          ? updatedCall.customerSocketId
          : updatedCall.agentSocketId;

        if (otherSocketId) {
          const otherSocket = io.sockets.sockets.get(otherSocketId);
          if (otherSocket) {
            otherSocket.emit('participantReconnected', { userId: socket.id });
          }
        }

        await broadcastPresence();

        cb({ success: true, callId, role: user.role, username: user.username });
      } else {
        // ❌ Grace period expired or not found
        console.log(`[reconnect] ❌ No active grace period — must rejoin fresh`);
        cb({ success: false, reason: 'grace_expired' });
      }
    });

    // ── STEP 1: Client requests router RTP capabilities ──────────────────
    socket.on('getRouterRtpCapabilities', async (cb) => {
      const user = await getUser(socket.id);
      const callId = user?.callId;
      let router;

      if (callId) {
        const call = await getCall(callId);
        if (call) {
          router = getAnyRouter(); // All routers share identical RTP caps
        }
      }
      router = router || getAnyRouter();

      console.log(`[rtp] 📋 getRouterRtpCapabilities for socket=${socket.id} callId=${callId || 'none'} routerId=${router?.id}`);
      cb(router.rtpCapabilities);
    });

    // ── STEP 2: Create a send (produce) transport ─────────────────────────
    socket.on('createSendTransport', async (cb) => {
      try {
        const user = await getUser(socket.id);
        const callId = user?.callId;
        // Use the call's assigned router — all transports in a call MUST share one router
        const router = (callId && callRouters[callId]) || getAnyRouter();

        console.log(`[transport] 📤 Creating SEND transport for socket=${socket.id} callId=${callId || 'none'} routerId=${router?.id}`);
        const { transport, params } = await createTransport(router);

        if (!localTransports[socket.id]) localTransports[socket.id] = {};
        localTransports[socket.id].sendTransport = transport;

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
        const user = await getUser(socket.id);
        const callId = user?.callId;
        // Use the call's assigned router — all transports in a call MUST share one router
        const router = (callId && callRouters[callId]) || getAnyRouter();

        console.log(`[transport] 📥 Creating RECV transport for socket=${socket.id} callId=${callId || 'none'} routerId=${router?.id}`);
        const { transport, params } = await createTransport(router);

        if (!localTransports[socket.id]) localTransports[socket.id] = {};
        localTransports[socket.id].recvTransport = transport;

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
        const sendTransport = localTransports[socket.id]?.sendTransport;
        if (!sendTransport) {
          console.warn(`[dtls] ⚠️ SEND transport not found for socket=${socket.id} — skipping connect (transport may have been cleaned up)`);
          return cb('Transport not found');
        }
        console.log(`[dtls] 🔒 connectSendTransport for socket=${socket.id} transportId=${sendTransport.id}`);
        console.log(`[dtls]    DTLS role: ${dtlsParameters?.role}, fingerprints: ${dtlsParameters?.fingerprints?.length || 0}`);
        await sendTransport.connect({ dtlsParameters });
        console.log(`[dtls] ✅ SEND transport connected for socket=${socket.id}`);
        cb();
      } catch (err) {
        if (err.message && err.message.includes('connect() already called')) {
          console.warn(`[dtls] ⚠️ SEND transport connect() already called for socket=${socket.id} — ignoring duplicate`);
          return cb();
        }
        console.error(`[dtls] ❌ SEND transport connect FAILED for socket=${socket.id}:`, err);
        cb(err.message);
      }
    });

    // ── STEP 5: Connect recv transport ────────────────────────────────────
    socket.on('connectRecvTransport', async ({ dtlsParameters }, cb) => {
      try {
        const recvTransport = localTransports[socket.id]?.recvTransport;
        if (!recvTransport) {
          console.warn(`[dtls] ⚠️ RECV transport not found for socket=${socket.id} — skipping connect (transport may have been cleaned up)`);
          return cb('Transport not found');
        }
        console.log(`[dtls] 🔒 connectRecvTransport for socket=${socket.id} transportId=${recvTransport.id}`);
        console.log(`[dtls]    DTLS role: ${dtlsParameters?.role}, fingerprints: ${dtlsParameters?.fingerprints?.length || 0}`);
        await recvTransport.connect({ dtlsParameters });
        console.log(`[dtls] ✅ RECV transport connected for socket=${socket.id}`);
        cb();
      } catch (err) {
        if (err.message && err.message.includes('connect() already called')) {
          console.warn(`[dtls] ⚠️ RECV transport connect() already called for socket=${socket.id} — ignoring duplicate`);
          return cb();
        }
        console.error(`[dtls] ❌ RECV transport connect FAILED for socket=${socket.id}:`, err);
        cb(err.message);
      }
    });

    // ── STEP 6: Client starts producing audio ─────────────────────────────
    socket.on('produce', async ({ kind, rtpParameters, callId }, cb) => {
      try {
        console.log(`[produce] 🎤 Produce request: socket=${socket.id} kind=${kind} callId=${callId}`);
        console.log(`[produce]    RTP codecs: ${rtpParameters?.codecs?.map(c => c.mimeType).join(', ')}`);

        const sendTransport = localTransports[socket.id]?.sendTransport;
        if (!sendTransport) {
          console.warn(`[produce] ⚠️ SEND transport not found for socket=${socket.id} — cannot produce (transport may have been cleaned up)`);
          return cb({ error: 'Transport not found' });
        }
        console.log(`[produce]    sendTransport exists: true, id=${sendTransport.id}`);

        const producer = await sendTransport.produce({ kind, rtpParameters });
        localProducers[socket.id] = producer;
        console.log(`[produce] ✅ Producer created: id=${producer.id} kind=${producer.kind} paused=${producer.paused}`);

        // Log producer lifecycle events
        producer.on('transportclose', () => {
          console.warn(`[produce] 🛑 Producer ${producer.id} — transport closed`);
        });
        producer.on('score', (score) => {
          console.log(`[produce] 📊 Producer ${producer.id} score:`, JSON.stringify(score));
        });

        const call = await getCall(callId);
        if (!call) {
          console.error(`[produce] ❌ Call not found: callId=${callId}`);
          return cb({ error: 'Call not found' });
        }

        const user = await getUser(socket.id);

        // Tell the other party to consume this new producer
        const otherSocketId = user?.role === 'agent' ? call.customerSocketId : call.agentSocketId;
        if (otherSocketId) {
          const otherSocket = io.sockets.sockets.get(otherSocketId);
          if (otherSocket) {
            console.log(`[produce] 📢 Emitting newProducer to other party socket=${otherSocketId} producerId=${producer.id}`);
            otherSocket.emit('newProducer', { producerId: producer.id });
          } else {
            console.warn(`[produce] ⚠️  Other party socket ${otherSocketId} not found in io.sockets — newProducer NOT sent`);
          }
        } else if (user?.role !== 'admin') {
          console.warn(`[produce] ⚠️  No other party socket found in call ${callId} — newProducer NOT sent`);
        }

        // Notify monitoring admins about this new producer (include role for the admin client)
        if (user?.role !== 'admin') {
          io.to(`monitor:${callId}`).emit('newProducer', { producerId: producer.id, role: user?.role });
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
        // MUST use the same router the producer was created on
        const router = (callId && callRouters[callId]) || getAnyRouter();

        // Verify router exists
        if (!router) {
          console.warn(`[consume] ⚠️ No router found for callId=${callId} — cannot consume`);
          return cb({ error: 'Router not found' });
        }

        // Check if the producer still exists before trying canConsume
        let canConsume = false;
        try {
          canConsume = router.canConsume({ producerId, rtpCapabilities });
        } catch (routerErr) {
          console.log(`[consume] ℹ️  Cannot consume producerId=${producerId} (likely ended): ${routerErr.message}`);
          return cb({ error: 'Producer not found or closed' });
        }
        console.log(`[consume] canConsume=${canConsume} routerId=${router.id}`);

        if (!canConsume) {
          console.log(`[consume] ℹ️  Cannot consume producerId=${producerId} (likely already closed or true RTP mismatch)`);
          return cb({ error: 'Producer already closed' });
        }

        const recvTransport = localTransports[socket.id]?.recvTransport;
        if (!recvTransport) {
          console.warn(`[consume] ⚠️ RECV transport not found for socket=${socket.id} — cannot consume (transport may have been cleaned up)`);
          return cb({ error: 'Transport not found' });
        }
        console.log(`[consume]    recvTransport exists: true, id=${recvTransport.id}`);
        const consumer = await recvTransport.consume({
          producerId,
          rtpCapabilities,
          paused: false,
        });
        localConsumers[socket.id] = consumer;
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
    socket.on('dialOut', async ({ targetId, targetUserId }, cb) => {
      const callId = `call_${Date.now()}`;

      let targetSocket = null;
      let targetDbId = targetUserId;
      let targetUsername = null;
      let targetRole = null;

      // Find by given socket.id (legacy)
      if (targetId) {
        targetSocket = io.sockets.sockets.get(targetId);
        if (targetSocket) {
          targetDbId = targetSocket.user?.userId;
          targetUsername = targetSocket.user?.username;
          targetRole = targetSocket.user?.role;
        }
      }
      // If passing DbId (offline calling scenario) search across active sockets
      else if (targetDbId) {
        for (const [id, s] of io.sockets.sockets.entries()) {
          if (s.user?.userId === targetDbId) {
            targetSocket = s;
            targetId = id;
            targetUsername = s.user?.username;
            targetRole = s.user?.role;
            break;
          }
        }
      }

      // Check if target is already on a call via active socket presence
      if (targetId) {
        const activeTargetUser = await getUser(targetId);
        if (activeTargetUser?.callId && activeTargetUser.callId !== '') {
          console.log(`[dialOut] 📵 Target ${activeTargetUser.username} is busy (callId=${activeTargetUser.callId})`);
          const callerUser = await getUser(socket.id);
          try {
            await insertCallRecord({
              callId,
              callerName: callerUser?.username || null,
              callerRole: callerUser?.role || null,
              calleeName: activeTargetUser.username,
              calleeRole: activeTargetUser.role,
            });
            await updateCallRecord(callId, { status: 'missed', ended_at: true });
          } catch (err) { }
          return cb({ error: `${activeTargetUser.username} is on another call`, busy: true });
        }
      }

      const callerUser = await getUser(socket.id);
      const router = getNextRouter();
      callRouters[callId] = router;  // Store locally — C++ object can't go to Redis

      // Build call record
      const callData = {
        routerIndex: 0,
        startTime: Date.now(),
      };

      if (callerUser.role === 'customer') {
        callData.agentSocketId = targetId || 'offline_target';
        callData.customerSocketId = socket.id;
      } else {
        callData.agentSocketId = socket.id;
        callData.customerSocketId = targetId || 'offline_target';
      }

      await setCall(callId, callData);
      await updateUser(socket.id, { callId });
      if (targetId) {
        await updateUser(targetId, { callId });
      }

      // Metrics
      m.callsInitiatedCounter.inc({ type: 'dialOut' });
      m.activeCallsGauge.inc();

      console.log(`[dialOut] 📞 ${callerUser.username} (${callerUser.role}) → ${targetId} | callId=${callId}`);

      // ── Persist call history ──────────────────────────────────────────
      // Look up target DB user robustly
      let finalTargetUser = null;
      if (targetDbId) {
        const { getPool } = require('./db');
        const [rows] = await getPool().query('SELECT id, username, role, push_subscription FROM users WHERE id = ?', [targetDbId]);
        if (rows.length > 0) finalTargetUser = rows[0];
      }

      try {
        await insertCallRecord({
          callId,
          callerName: callerUser.username,
          callerRole: callerUser.role,
          calleeName: targetUsername || finalTargetUser?.username || null,
          calleeRole: targetRole || finalTargetUser?.role || null,
        });
      } catch (err) {
        console.error(`[dialOut] ❌ Failed to insert call history:`, err);
      }

      // ── Dispatch Incoming Call (Socket or Web Push) ────────────────────
      if (targetSocket) {
        // Target is online via WebSocket
        targetSocket.emit('incomingCall', {
          callId,
          from: callerUser.username,
          role: callerUser.role
        });
      } else if (finalTargetUser && finalTargetUser.push_subscription) {
        // Target is offline but has a Push Subscription
        console.log(`[dialOut] 📡 Dispatching Web Push Notification to offline user ${finalTargetUser.username}`);

        const sub = finalTargetUser.push_subscription;
        const payload = JSON.stringify({
          type: 'incomingCall',
          callId,
          from: callerUser.username,
          role: callerUser.role,
          message: `Incoming call from ${callerUser.username}`
        });

        const sendPush = async () => {
          try {
            await webpush.sendNotification(sub, payload);
            return true;
          } catch (e) {
            console.error(`[dialOut] ❌ Failed to send Web Push`, e);
            if (e.statusCode === 410 || e.statusCode === 404 || String(e).includes('expired') || String(e).includes('unsubscribed')) {
              console.log(`[dialOut] 🧹 Invalid push subscription for user ${targetDbId}. Deleting from DB.`);
              const { getPool } = require('./db');
              getPool().execute('UPDATE users SET push_subscription = NULL WHERE id = ?', [targetDbId]).catch(() => { });
            }
            return false;
          }
        };

        const success = await sendPush();
        if (!success) {
          // Clean up the dangling call record since they are unreachable
          delete callRouters[callId];
          await deleteCall(callId);
          await updateUser(socket.id, { callId: '' });
          await updateCallRecord(callId, { status: 'missed', ended_at: true });
          return cb({ error: `User is unreachable (Push notification denied or expired).`, pushFailed: true });
        }

        // Successfully sent initial push, start re-notifying to simulate ringing
        globalOfflineTargets[targetDbId] = {
          callId,
          callerUsername: callerUser.username,
          callerRole: callerUser.role,
          targetRole: finalTargetUser.role
        };

        pendingRings[callId] = {
          sub,
          targetDbId,
          interval: setInterval(sendPush, 1000)
        };
      } else {
        // Target is totally offline and no push sub
        console.log(`[dialOut] 📵 Target completely unreachable (no socket, no push)`);
        await updateCallRecord(callId, { status: 'missed', ended_at: true });
        return cb({ error: 'Target user is completely offline and unreachable.' });
      }

      cb({ callId });

      // Notify admin dashboards about new call
      io.to('admins').emit('callsUpdated');
    });

    // Generic "Call First Available" fallback (Customer to anyone)
    socket.on('callIn', async () => {
      const callId = `call_${Date.now()}`;
      const callerUser = await getUser(socket.id);
      const router = getNextRouter();
      callRouters[callId] = router;  // Store locally — C++ object can't go to Redis

      await setCall(callId, {
        customerSocketId: socket.id,
        agentSocketId: '',
        routerIndex: 0,
        startTime: Date.now(),
      });
      await updateUser(socket.id, { callId });

      // Metrics
      m.callsInitiatedCounter.inc({ type: 'callIn' });
      m.activeCallsGauge.inc();

      console.log(`[callIn] 📞 General queue call from: ${callerUser.username} | callId=${callId}`);

      // ── Persist call history ──────────────────────────────────────────
      try {
        await insertCallRecord({
          callId,
          callerName: callerUser.username,
          callerRole: 'customer',
          calleeName: null,
          calleeRole: null,
        });
      } catch (err) {
        console.error(`[callIn] ❌ Failed to insert call history:`, err);
      }

      // Notify all agents
      io.to('agents').emit('incomingCall', {
        callId,
        from: callerUser.username,
        role: 'customer'
      });

      // Notify admin dashboards about new call
      io.to('admins').emit('callsUpdated');
    });

    // Receive acceptance
    socket.on('acceptCall', async ({ callId }, cb) => {
      const call = await getCall(callId);
      if (!call) {
        console.warn(`[acceptCall] ⚠️  Call not found or ended: callId=${callId}`);
        return cb({ error: 'Call not found or ended' });
      }

      const user = await getUser(socket.id);

      // If this was a general 'callIn' and the agent is answering, assign the agent socket
      if (user.role === 'agent' && !call.agentSocketId) {
        await updateCall(callId, { agentSocketId: socket.id });
        await updateUser(socket.id, { callId });
      }

      console.log(`[acceptCall] ✅ Call ${callId} accepted by socket=${socket.id} (${user.role})`);

      // Mark as accepted in Redis so admin sees 'Connected'
      await updateCall(callId, { accepted: 'true' });
      io.to('admins').emit('callsUpdated');

      // Clear any pending push ring loops natively
      clearPendingRings(callId);

      // ── Update call history: mark accepted ──────────────────────────────
      try {
        const historyUpdate = { status: 'completed', answered_at: true };
        // For callIn, fill callee info now that we know who answered
        if (user.role === 'agent' && !call.agentSocketId) {
          historyUpdate.callee_name = user.username;
          historyUpdate.callee_role = 'agent';
          historyUpdate.callee_id = await getUserIdByUsername(user.username);
        }
        await updateCallRecord(callId, historyUpdate);
      } catch (err) {
        console.error(`[acceptCall] ❌ Failed to update call history:`, err);
      }

      // Tell the other party to start setupCall too
      const updatedCall = await getCall(callId);
      const otherSocketId = socket.id === updatedCall.agentSocketId
        ? updatedCall.customerSocketId
        : updatedCall.agentSocketId;

      if (otherSocketId) {
        const otherSocket = io.sockets.sockets.get(otherSocketId);
        if (otherSocket) {
          console.log(`[acceptCall] 📢 Emitting callAccepted to other party socket=${otherSocketId}`);
          otherSocket.emit('callAccepted', { callId });
        } else {
          console.warn(`[acceptCall] ⚠️  Other party socket ${otherSocketId} not found`);
        }
      } else {
        console.warn(`[acceptCall] ⚠️  No other party found for callId=${callId}`);
      }

      cb({ callId });
    });

    // ── REJECT CALL ──────────────────────────────────────────────────────
    socket.on('rejectCall', async ({ callId }, cb) => {
      console.log(`[rejectCall] ❌ Call ${callId} rejected by socket=${socket.id}`);

      // Update call history DB
      try {
        await updateCallRecord(callId, {
          status: 'rejected',
          ended_at: true,
        });
      } catch (err) {
        console.error(`[rejectCall] ❌ Failed to update call history:`, err);
      }

      // Get the call to find the other party (the caller)
      const call = await getCall(callId);
      if (call) {
        const user = await getUser(socket.id);
        // Determine who the caller is (the other party)
        const callerSocketId = call.agentSocketId === socket.id
          ? call.customerSocketId
          : call.agentSocketId;

        // Notify the caller that their call was rejected
        if (callerSocketId) {
          const callerSocket = io.sockets.sockets.get(callerSocketId);
          if (callerSocket) {
            callerSocket.emit('callRejected', {
              callId,
              rejectedBy: user?.username || 'Unknown',
            });
          }
          // Clear caller's call state
          closeLocalMediasoup(callerSocketId);
          await updateUser(callerSocketId, { callId: '' });
        }

        // Clear rejector's call state
        closeLocalMediasoup(socket.id);
        await updateUser(socket.id, { callId: '' });

        // Clean up call + router
        clearPendingRings(callId);
        delete callRouters[callId];
        await deleteCall(callId);

        // Metrics
        m.callsEndedCounter.inc();
        m.activeCallsGauge.dec();

        // Notify admins
        io.to('admins').emit('callsUpdated');
      }

      if (typeof cb === 'function') cb({ ok: true });
    });

    // Request presence on explicit call
    socket.on('getPresence', async () => {
      const [agents, customers] = await Promise.all([
        getPresenceList('agent'),
        getPresenceList('customer'),
      ]);
      socket.emit('presenceUpdate', { agents, customers });
    });

    // ── ADMIN: Get live calls ─────────────────────────────────────────────
    socket.on('getLiveCalls', async (cb) => {
      try {
        const calls = await getAllActiveCalls();
        console.log(`[admin] 📊 getLiveCalls requested by socket=${socket.id} — ${calls.length} active calls`);
        cb(calls);
      } catch (err) {
        console.error(`[admin] ❌ getLiveCalls failed:`, err);
        cb([]);
      }
    });

    // ── ADMIN: Monitor a call (silent listen) ─────────────────────────────
    socket.on('monitorCall', async ({ callId }, cb) => {
      try {
        const user = await getUser(socket.id);
        if (!user || user.role !== 'admin') {
          return cb({ error: 'Only admins can monitor calls.' });
        }

        const call = await getCall(callId);
        if (!call) {
          return cb({ error: 'Call not found.' });
        }

        // If admin was monitoring another call, leave that room first
        if (user.callId && user.callId !== '' && user.callId !== callId) {
          socket.leave(`monitor:${user.callId}`);
          closeLocalMediasoup(socket.id);
        }

        // Bind admin to this call's router and join monitor room
        await updateUser(socket.id, { callId });
        socket.join(`monitor:${callId}`);

        // Get producer IDs for both parties
        const agentProducerId = localProducers[call.agentSocketId]?.id || null;
        const customerProducerId = localProducers[call.customerSocketId]?.id || null;

        console.log(`[admin] 🎧 ${user.username} monitoring callId=${callId} agent=${agentProducerId || 'n/a'} customer=${customerProducerId || 'n/a'}`);

        cb({ agentProducerId, customerProducerId });
      } catch (err) {
        console.error(`[admin] ❌ monitorCall failed:`, err);
        cb({ error: err.message });
      }
    });

    // ── ADMIN: Stop monitoring ─────────────────────────────────────────────
    socket.on('stopMonitoring', async () => {
      const user = await getUser(socket.id);
      if (!user || user.role !== 'admin') return;

      if (user.callId && user.callId !== '') {
        socket.leave(`monitor:${user.callId}`);
        closeLocalMediasoup(socket.id);
        await updateUser(socket.id, { callId: '' });
        console.log(`[admin] 🔇 ${user.username} stopped monitoring callId=${user.callId}`);
      }
    });

    // ── HANGUP ────────────────────────────────────────────────────────────
    socket.on('hangup', async () => {
      console.log(`[hangup] 📱 Hangup requested by socket=${socket.id}`);
      await endCall(socket);
    });

    // ── DISCONNECT (with grace period) ────────────────────────────────────
    socket.on('disconnect', async () => {
      m.disconnectionsCounter.inc();
      console.log(`[disconnect] 🔌 Socket ${socket.id} disconnecting...`);

      const user = await getUser(socket.id);
      if (!user) return;

      if (user.role === 'admin') {
        // Admin disconnecting — just clean up monitoring, never end the call
        if (user.callId && user.callId !== '') {
          socket.leave(`monitor:${user.callId}`);
          closeLocalMediasoup(socket.id);
          console.log(`[disconnect] Admin ${user.username} stopped monitoring callId=${user.callId}`);
        }
        await removeFromPresence('admin', socket.id);
        await deleteUser(socket.id);
        return;
      }

      if (user.callId && user.callId !== '') {
        // User was in a call — start grace period instead of immediate cleanup
        console.log(`[disconnect] ⏱️  Starting ${GRACE_PERIOD_SECONDS}s grace period for ${user.username} (callId=${user.callId})`);

        await updateUser(socket.id, {
          status: 'disconnected',
          disconnectedAt: String(Date.now()),
        });

        await setGracePeriod(socket.id, user.callId);

        // Broadcast updated presence so other users see this user as offline
        await broadcastPresence();

        // Notify the other party that this user may reconnect
        const call = await getCall(user.callId);
        if (call) {
          const otherSocketId = call.agentSocketId === socket.id
            ? call.customerSocketId
            : call.agentSocketId;

          if (otherSocketId) {
            const otherSocket = io.sockets.sockets.get(otherSocketId);
            if (otherSocket) {
              otherSocket.emit('participantDisconnected', {
                userId: socket.id,
                username: user.username,
                gracePeriod: GRACE_PERIOD_SECONDS,
              });
            }
          }
        }
      } else {
        // Not in a call — immediate cleanup
        console.log(`[disconnect] User disconnected: ${user.username} (${socket.id})`);
        await removeFromPresence(user.role, socket.id);
        await deleteUser(socket.id);
        await broadcastPresence();
      }
    });
  });

  // ── Express routes ─────────────────────────────────────────────────────────
  app.get('/', (_req, res) => {
    res.json({ message: 'Mediasoup server is running.', redis: 'connected' });
  });

  // ── Auth REST endpoints ───────────────────────────────────────────────────
  app.post('/api/register', async (req, res) => {
    try {
      const { username, phone, password, role } = req.body;
      if (!username || !phone || !password || !role) {
        return res.status(400).json({ error: 'All fields are required: username, phone, password, role.' });
      }
      const result = await registerUser(username, phone, password, role);
      console.log(`[auth] ✅ Registered user: ${result.username} (${result.role}) phone=${result.phone}`);
      res.json(result);
    } catch (err) {
      console.error(`[auth] ❌ Register failed:`, err.message);
      res.status(400).json({ error: err.message });
    }
  });

  app.post('/api/login', async (req, res) => {
    try {
      const { phone, password } = req.body;
      if (!phone || !password) {
        return res.status(400).json({ error: 'Phone and password are required.' });
      }
      const result = await loginUser(phone, password);
      console.log(`[auth] ✅ Login: ${result.username} (${result.role}) phone=${result.phone}`);
      res.json(result);
    } catch (err) {
      console.error(`[auth] ❌ Login failed:`, err.message);
      res.status(401).json({ error: err.message });
    }
  });

  // Verify token endpoint (for frontend auto-login check)
  app.post('/api/verify-token', (req, res) => {
    const { token } = req.body;
    if (!token) return res.status(400).json({ valid: false });
    const decoded = verifyToken(token);
    if (!decoded) return res.status(401).json({ valid: false });
    res.json({ valid: true, user: { userId: decoded.userId, username: decoded.username, role: decoded.role } });
  });

  // JWT auth middleware for REST routes
  function authMiddleware(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Authorization header required.' });
    }
    const decoded = verifyToken(authHeader.slice(7));
    if (!decoded) return res.status(401).json({ error: 'Invalid or expired token.' });
    req.user = decoded;
    next();
  }

  // ── Web Push Subscriptions ────────────────────────────────────────────────
  app.get('/api/vapid-public-key', (req, res) => {
    res.json({ publicKey: 'BIS3SfRv_zL4c4e6LEQqD7r4x3gBbHxnGX_TUXQ0DfXXOLsov4LmKGaMiNPTXvCR1Xlkcc4wTQFe_67XcOXrgkM' });
  });

  app.post('/api/push-subscribe', authMiddleware, async (req, res) => {
    try {
      const subscription = req.body;
      const { getPool } = require('./db');
      const pool = getPool();
      await pool.execute('UPDATE users SET push_subscription = ? WHERE id = ?', [JSON.stringify(subscription), req.user.userId]);
      console.log(`[push] ✅ Saved push subscription for userId=${req.user.userId}`);
      res.status(201).json({ success: true });
    } catch (e) {
      console.error('[push] ❌ Failed to save subscription', e);
      res.status(500).json({ error: 'Failed to save subscription' });
    }
  });

  // ── Fetch Customers (for Agent dialer) ──────────────────────────────────
  app.get('/api/customers', authMiddleware, async (req, res) => {
    try {
      if (req.user.role !== 'agent' && req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Forbidden' });
      }
      const { getPool } = require('./db');
      // Return ALL customers (even offline ones) so push notifications can reach them
      const [rows] = await getPool().query('SELECT id, username, phone FROM users WHERE role = "customer"');
      res.json(rows);
    } catch (e) {
      res.status(500).json({ error: 'Failed to fetch customers' });
    }
  });

  // ── Call History REST Endpoints ──────────────────────────────────────────

  // JWT auth middleware for REST routes
  function authMiddleware(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Authorization header required.' });
    }
    const decoded = verifyToken(authHeader.slice(7));
    if (!decoded) return res.status(401).json({ error: 'Invalid or expired token.' });
    req.user = decoded;
    next();
  }

  // Admin: full call history with filters & pagination
  app.get('/api/call-history', authMiddleware, async (req, res) => {
    try {
      if (req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Admin access required.' });
      }
      const result = await getCallHistory({
        userId: req.query.userId || null,
        status: req.query.status || null,
        from: req.query.from || null,
        to: req.query.to || null,
        search: req.query.search || null,
        page: req.query.page || 1,
        limit: req.query.limit || 20,
      });
      res.json(result);
    } catch (err) {
      console.error('[api] ❌ GET /api/call-history failed:', err);
      res.status(500).json({ error: 'Failed to fetch call history.' });
    }
  });

  // Agent / Customer: own call history
  app.get('/api/my-calls', authMiddleware, async (req, res) => {
    try {
      const result = await getCallHistory({
        userId: req.user.userId,
        status: req.query.status || null,
        from: req.query.from || null,
        to: req.query.to || null,
        page: req.query.page || 1,
        limit: req.query.limit || 20,
      });
      res.json(result);
    } catch (err) {
      console.error('[api] ❌ GET /api/my-calls failed:', err);
      res.status(500).json({ error: 'Failed to fetch call history.' });
    }
  });

  // ── Prometheus scrape endpoint ────────────────────────────────────────────
  app.get('/metrics', async (_req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  });

  // ── Redis health endpoint ─────────────────────────────────────────────────
  app.get('/redis-health', async (_req, res) => {
    try {
      const { getRedis } = require('./redisState');
      const r = getRedis();
      const pong = await r.ping();
      const userCount = await r.keys('user:*');
      const callCount = await r.keys('call:*');
      const graceCount = await r.keys('grace:*');
      res.json({
        status: 'ok',
        ping: pong,
        users: userCount.length,
        activeCalls: callCount.length,
        gracePeriods: graceCount.length,
      });
    } catch (err) {
      res.status(500).json({ status: 'error', message: err.message });
    }
  });

  // ── Immediate snapshot on startup + periodic console logging ─────────────
  startPeriodicLog();
  server.listen(3000, () => {
    console.log('Server running on port 3000');
    console.log('Prometheus metrics available at http://localhost:3000/metrics');
    console.log('Redis health available at http://localhost:3000/redis-health');
    // Print an initial snapshot right away so the first log isn't 60s away
    printMetricsSnapshot();
  });
}

main().catch(console.error);
