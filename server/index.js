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
const { createPool, initDatabase, getPool } = require('./db');

/* Pilot allowlist: comma-separated PWA user ids that may be called while
   testing. Empty (the default) means no restriction — so clearing the env var
   is how this is switched off when the pilot ends. */
const PILOT_USERS = new Set(
  String(process.env.VOIP_PILOT_USERS || '')
    .split(',').map((s) => s.trim()).filter(Boolean)
);
console.log(
  PILOT_USERS.size
    ? `[pilot] 🔒 dial-out restricted to ${PILOT_USERS.size} test user(s): ${[...PILOT_USERS].join(', ')}`
    : '[pilot] ⚠️  NO allowlist — any customer in the app can be dialled'
);
const mysql2 = require('mysql2/promise');
const { insertCallRecord, updateCallRecord, getCallHistory, getUserIdByUsername, setRecordingPath } = require('./callHistory');
const fs = require('fs');
const path = require('path');

// Directory where uploaded call recordings are stored.
const RECORDINGS_DIR = path.join(__dirname, 'recordings');
fs.mkdirSync(RECORDINGS_DIR, { recursive: true });

let tgPool;       // AWS Remote DB (Names & Info)
let localTgPool;  // Local DB (Push Subscriptions & PWA Operations)

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
io.use(async (socket, next) => {
  const token = socket.handshake.auth?.token;
  if (!token) {
    if (socket.handshake.auth?.isPwaClient) {
      const { userId, username } = socket.handshake.auth;
      let finalUsername = username || 'Customer';

      if (userId && !userId.startsWith('guest_') && tgPool) {
        try {
          // Get real name and phone from remote AWS DB (dbt_user)
          const [tgRows] = await tgPool.query('SELECT COALESCE(CONCAT(first_name, " ", last_name), username, phone) as name FROM dbt_user WHERE user_id = ? LIMIT 1', [userId]);
          if (tgRows && tgRows.length > 0 && tgRows[0].name) {
            finalUsername = tgRows[0].name; // Use real name or fallback to phone
          }
        } catch (err) {
          console.error('[auth] Failed to lookup PWA user in AWS DB:', err.message);
        }
      }

      socket.user = {
        userId: userId || `guest_${Date.now()}`,
        username: finalUsername,
        role: 'customer'
      };
      return next();
    }
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
const PUSH_DIAL_TIMEOUT_MS = parseInt(process.env.PUSH_DIAL_TIMEOUT_MS, 10) || 60000;

// ── Round-robin agent assignment ─────────────────────────────────────────────
// `callIn` (the general queue) rings one agent at a time in rotation rather than
// broadcasting to everyone. If an agent rejects or doesn't answer within
// RING_TIMEOUT_MS we advance to the next free agent. If no agent is free the
// call waits in `callQueue` until one frees up or connects.
const RING_TIMEOUT_MS = parseInt(process.env.RING_TIMEOUT_MS, 10) || 20000;
const ringTimers = {};        // callId → setTimeout (per-agent ring window)
const ringingAgents = {};     // agentSocketId → callId (agent currently being rung)
const callQueue = [];         // FIFO of callIds waiting for a free agent
let rrCursor = 0;             // rotating pointer across the available-agent list

// ── Helpers ──────────────────────────────────────────────────────────────────

function clearPendingRings(callId) {
  if (callId && pendingRings[callId]) {
    if (pendingRings[callId].interval) clearInterval(pendingRings[callId].interval);
    if (pendingRings[callId].timeout) clearTimeout(pendingRings[callId].timeout);
    if (pendingRings[callId].sub) {
      webpush.sendNotification(pendingRings[callId].sub, JSON.stringify({ type: 'cancelCall' })).catch(() => { });
    }
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

  // Enrich the roster with live call pairing so every agent can see which
  // customer is on a call and with which agent. Only connected (accepted)
  // calls count as "busy".
  const activeCalls = await getAllActiveCalls();
  const callByCustomerSocket = {};
  const callByAgentSocket = {};
  for (const c of activeCalls) {
    if (!c.accepted) continue;
    if (c.customerSocketId) callByCustomerSocket[c.customerSocketId] = c;
    if (c.agentSocketId) callByAgentSocket[c.agentSocketId] = c;
  }
  for (const cust of customers) {
    const call = callByCustomerSocket[cust.id];
    if (call) {
      cust.onCall = true;
      cust.withAgent = call.agent;
      cust.withAgentSocketId = call.agentSocketId;
    }
  }
  for (const ag of agents) {
    const call = callByAgentSocket[ag.id];
    if (call) {
      ag.onCall = true;
      ag.withCustomer = call.customer;
    }
  }

  // Only surface customers whose app is actually in the foreground — a
  // backgrounded PWA still holds a socket but can't be rung, so showing it as
  // "online" just sends agents to voicemail. Someone already on a call is kept
  // visible so the roster still shows them as busy.
  const reachableCustomers = customers.filter((c) => c.foreground || c.onCall);

  // Update Prometheus gauges
  m.activeUsersGauge.set({ role: 'agent' }, agents.length);
  m.activeUsersGauge.set({ role: 'customer' }, reachableCustomers.length);

  // Send the live roster to everyone
  io.emit('presenceUpdate', { agents, customers: reachableCustomers });
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
      // If a queue call is still ringing an agent, notify them to dismiss the banner
      if (call.ringingAgent) {
        const ringingSocket = io.sockets.sockets.get(call.ringingAgent);
        if (ringingSocket) {
          ringingSocket.emit('incomingCallCancelled', { callId });
        }
      }
      clearRing(callId);
      removeFromQueue(callId);

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
        const historyUpdate = {
          ended_at: true,
          duration_sec: durationSec,
          ended_by: user.username + ' (disconnected)',
        };

        // For queue calls that were never accepted, record the last ringing agent
        if (call.ringingAgent && call.accepted !== 'true') {
          const ringingUser = await getUser(call.ringingAgent);
          if (ringingUser) {
            historyUpdate.callee_name = ringingUser.username;
            historyUpdate.callee_role = ringingUser.role;
          }
        }

        await updateCallRecord(callId, historyUpdate);
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
      clearRing(callId);
      removeFromQueue(callId);
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

  // Freed an agent and/or removed a waiting customer — re-balance the queue.
  await processQueue();

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

// ── Round-robin helpers ───────────────────────────────────────────────────────

function clearRingTimer(callId) {
  if (ringTimers[callId]) {
    clearTimeout(ringTimers[callId]);
    delete ringTimers[callId];
  }
}

function clearRingingForCall(callId) {
  for (const [agentId, cid] of Object.entries(ringingAgents)) {
    if (cid === callId) delete ringingAgents[agentId];
  }
}

// Stop ringing a call entirely: cancel its timer and release any rung agent.
function clearRing(callId) {
  clearRingTimer(callId);
  clearRingingForCall(callId);
}

function removeFromQueue(callId) {
  const i = callQueue.indexOf(callId);
  if (i !== -1) callQueue.splice(i, 1);
}

// Connected agents who are not in a call, not already being rung, and not excluded.
async function getAvailableAgents(excludeIds = []) {
  const agents = await getPresenceList('agent'); // already filtered to status='connected'
  const out = [];
  for (const a of agents) {
    if (excludeIds.includes(a.id)) continue;
    if (!io.sockets.sockets.has(a.id)) continue;
    const u = await getUser(a.id);
    if (u && (!u.callId || u.callId === '')) out.push(a);
  }
  return out;
}

// Ring the next eligible agent for a queue-mode call, or park it in the queue.
async function ringNextAgent(callId, callerUsername) {
  const call = await getCall(callId);
  if (!call || call.accepted === 'true') {
    clearRing(callId);
    return;
  }

  // Customer gone while we were routing — tear the call down.
  if (!call.customerSocketId || !io.sockets.sockets.has(call.customerSocketId)) {
    clearRing(callId);
    removeFromQueue(callId);
    delete callRouters[callId];
    await deleteCall(callId);
    m.activeCallsGauge.dec();
    return;
  }

  // Release the previously-rung agent for this call, then pick a fresh one.
  clearRing(callId);

  const tried = (call.triedAgents || '').split(',').filter(Boolean);
  const exclude = [...tried, ...Object.keys(ringingAgents)];
  const available = await getAvailableAgents(exclude);

  if (available.length === 0) {
    // Nobody to ring right now — wait. Reset tried so a freshly-freed agent rings cleanly.
    await updateCall(callId, { ringingAgent: '', triedAgents: '' });
    if (!callQueue.includes(callId)) callQueue.push(callId);
    const cust = io.sockets.sockets.get(call.customerSocketId);
    if (cust) cust.emit('callStateUpdate', 'Waiting for an agent…');
    console.log(`[rr] 🕓 callId=${callId} queued — no free agent (queue=${callQueue.length})`);

    // Re-try after 5s — the agent who just declined is now free again
    setTimeout(() => processQueue(), 5000);
    return;
  }

  const agent = available[rrCursor % available.length];
  rrCursor = (rrCursor + 1) % 1e6;

  ringingAgents[agent.id] = callId;
  await updateCall(callId, {
    ringingAgent: agent.id,
    triedAgents: [...tried, agent.id].join(','),
  });
  removeFromQueue(callId);

  const agentSocket = io.sockets.sockets.get(agent.id);
  if (!agentSocket) {
    // Vanished between the presence read and now — try the next one immediately.
    return ringNextAgent(callId, callerUsername);
  }

  console.log(`[rr] 🔔 Ringing ${agent.username} (${agent.id}) for callId=${callId}`);
  agentSocket.emit('incomingCall', { callId, from: callerUsername, role: 'customer' });
  const cust = io.sockets.sockets.get(call.customerSocketId);
  if (cust) cust.emit('callStateUpdate', 'Ringing…');

  ringTimers[callId] = setTimeout(async () => {
    console.log(`[rr] ⏰ ${agent.username} did not answer callId=${callId} — advancing`);
    const s = io.sockets.sockets.get(agent.id);
    if (s) s.emit('incomingCallCancelled', { callId });
    await ringNextAgent(callId, callerUsername);
  }, RING_TIMEOUT_MS);
}

// An agent just became free/connected — try to assign any waiting calls.
async function processQueue() {
  if (callQueue.length === 0) return;
  for (const callId of [...callQueue]) {
    const available = await getAvailableAgents(Object.keys(ringingAgents));
    if (available.length === 0) break;

    const call = await getCall(callId);
    if (!call || call.accepted === 'true') {
      removeFromQueue(callId);
      continue;
    }
    if (!call.customerSocketId || !io.sockets.sockets.has(call.customerSocketId)) {
      removeFromQueue(callId);
      delete callRouters[callId];
      await deleteCall(callId);
      m.activeCallsGauge.dec();
      continue;
    }

    const customerUser = await getUser(call.customerSocketId);
    await ringNextAgent(callId, customerUser?.username || 'Customer');
  }
}

/**
 * endCall — called on explicit hangup.  Cleans up both parties immediately
 * (no grace period since this is an intentional hangup).
 */
// Close any open callback requests for these PWA user ids and tell both panels
// to drop them. Called when a call ends so a callback clears the moment its
// call is over — no manual "Done", and it works no matter who hung up. Agent
// ids never match a callback row, so passing them is a harmless no-op.
async function autoCloseCallbacksForUsers(userIds, callId) {
  const ids = [...new Set((userIds || []).map((u) => u && String(u)).filter(Boolean))];
  if (!ids.length) return;
  try {
    const pool = getPool();
    for (const uid of ids) {
      const [open] = await pool.execute(
        `SELECT id FROM callback_requests WHERE pwa_user_id=? AND status IN ('pending','claimed')`,
        [uid]
      );
      for (const r of open) {
        await pool.execute(
          `UPDATE callback_requests SET status='done', closed_at=NOW(), last_call_id=?
             WHERE id=? AND status IN ('pending','claimed')`,
          [String(callId || '').slice(0, 64) || null, r.id]
        );
        const [rows] = await pool.execute('SELECT * FROM callback_requests WHERE id=?', [r.id]);
        if (rows[0]) {
          io.to('agents').emit('callbacksUpdated', rows[0]);
          io.to('admins').emit('callbacksUpdated', rows[0]);
        }
      }
    }
  } catch (e) {
    console.error('[callback] auto-close on call end failed:', e.message);
  }
}

async function endCall(socket) {
  const user = await getUser(socket.id);
  if (!user || !user.callId) return;

  const callId = user.callId;
  const call = await getCall(callId);
  if (!call) return;

  // If a queue call is still ringing an agent, notify them to dismiss the banner
  if (call.ringingAgent) {
    const ringingSocket = io.sockets.sockets.get(call.ringingAgent);
    if (ringingSocket) {
      ringingSocket.emit('incomingCallCancelled', { callId });
    }
  }

  clearRing(callId);
  removeFromQueue(callId);

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
    const historyUpdate = {
      ended_at: true,
      duration_sec: durationSec,
      ended_by: user.username,
    };

    // For queue calls that were never accepted, record the last ringing agent
    if (call.ringingAgent && call.accepted !== 'true') {
      const ringingUser = await getUser(call.ringingAgent);
      if (ringingUser) {
        historyUpdate.callee_name = ringingUser.username;
        historyUpdate.callee_role = ringingUser.role;
      }
    }

    await updateCallRecord(callId, historyUpdate);
  } catch (err) {
    console.error(`[endCall] ❌ Failed to update call history:`, err);
  }

  // Find the other party
  const otherSocketId = call.agentSocketId === socket.id
    ? call.customerSocketId
    : call.agentSocketId;

  // Notify + cleanup other party
  const otherUser = otherSocketId ? await getUser(otherSocketId) : null;
  if (otherSocketId) {
    const otherSocket = io.sockets.sockets.get(otherSocketId);
    if (otherSocket) {
      otherSocket.emit('callEnded');
    }
    closeLocalMediasoup(otherSocketId);
    await updateUser(otherSocketId, { callId: '' });
  }

  // Clear the customer's callback request now that the call is over — from
  // whichever side hung up. Fire-and-forget so it never delays teardown.
  autoCloseCallbacksForUsers([user.userId, otherUser?.userId], callId);

  // Cleanup this socket
  closeLocalMediasoup(socket.id);
  await updateUser(socket.id, { callId: '' });

  // Clear push loop
  clearPendingRings(callId);

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

  // An agent likely just freed up — service any queued calls.
  await processQueue();

  // Refresh the roster so the busy badge clears for this customer/agent.
  await broadcastPresence();
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // 0. Initialize MySQL database
  createPool();
  await initDatabase();

  // 0b. Initialize TG Level PWA database pool (Remote)
  tgPool = mysql2.createPool({
    host: process.env.TG_DB_HOST,
    user: process.env.TG_DB_USER,
    password: process.env.TG_DB_PASSWORD,
    database: process.env.TG_DB_NAME,
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
  });
  console.log('[db] ✅ TG Level PWA database pool (Remote) created');

  // 0c. Initialize TG Level PWA database pool (Local)
  localTgPool = mysql2.createPool({
    host: process.env.LOCAL_TG_DB_HOST,
    user: process.env.LOCAL_TG_DB_USER,
    password: process.env.LOCAL_TG_DB_PASSWORD,
    database: process.env.LOCAL_TG_DB_NAME,
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
  });
  console.log('[db] ✅ TG Level PWA database pool (Local) created');

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

  // 3b. Stale call garbage collector — sweep every 5 min
  setInterval(async () => {
    try {
      const { getRedis } = require('./redisState');
      const r = getRedis();
      const callKeys = await r.keys('call:*');
      if (callKeys.length === 0) return;

      for (const key of callKeys) {
        const callId = key.replace('call:', '');
        const call = await getCall(callId);
        if (!call) continue;

        const age = Date.now() - parseInt(call.startTime || '0', 10);
        const agentExists = call.agentSocketId && io.sockets.sockets.has(call.agentSocketId);
        const customerExists = call.customerSocketId && io.sockets.sockets.has(call.customerSocketId);

        // If call is older than 2 min and neither party is connected, clean it up
        if (age > 120000 && !agentExists && !customerExists) {
          console.log(`[gc] 🧹 Cleaning stale call: ${callId} (age=${Math.round(age/1000)}s)`);
          clearPendingRings(callId);
          clearRing(callId);
          removeFromQueue(callId);
          delete callRouters[callId];
          await deleteCall(callId);
          m.activeCallsGauge.dec();
        }
      }
    } catch (err) {
      console.error('[gc] ❌ Stale call sweep error:', err.message);
    }
  }, 5 * 60 * 1000);

  // 4. Socket.IO connection handler
  io.on('connection', async (socket) => {
    m.connectionsCounter.inc();
    // User info is guaranteed by the auth middleware
    const { role, username } = socket.user;

    console.log(`[connect] ✅ ${role} connected: ${username} (${socket.id})`);

    await setUser(socket.id, { username, role, status: 'connected' });
    await addToPresence(role, socket.id);

    // Foreground tracking. A connected socket is NOT the same as a reachable
    // one: a backgrounded PWA keeps its socket alive but cannot ring. So online
    // means "app in the foreground", reported by the client below. Assume true
    // on connect (they just opened it); the client corrects within a tick.
    socket.data.foreground = true;

    if (role === 'agent') {
      socket.join('agents');
    } else if (role === 'customer') {
      socket.join('customers');
    } else if (role === 'admin') {
      socket.join('admins');
    }

    // Client reports foreground/background via document.visibilityState.
    socket.on('visibility', async ({ visible } = {}) => {
      const fg = Boolean(visible);
      if (socket.data.foreground === fg) return;
      socket.data.foreground = fg;
      try { await updateUser(socket.id, { foreground: fg ? '1' : '0' }); } catch {}
      broadcastPresence();
    });

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

    // A newly-connected agent can take a waiting call.
    if (role === 'agent') await processQueue();

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

        // Connection handler already called setUser + addToPresence for the new socket.
        // Just migrate the callId to the already-registered new socket.
        await updateUser(socket.id, { callId });

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
            otherSocket.emit('callStateUpdate', 'Connected');
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

      /* PILOT GUARD — remove by clearing VOIP_PILOT_USERS in .env.
         The agent portal lists every customer currently in the app, which
         during a pilot means several thousand real people are one tap away
         from an unexpected incoming call. While the allowlist is set, only
         those user ids can be dialled. Enforced here rather than by hiding
         rows in the panel: a filtered UI is a suggestion, this is a rule. */
      if (PILOT_USERS.size) {
        const intended = String(
          targetUserId || io.sockets.sockets.get(targetId)?.user?.userId || ''
        );
        if (!PILOT_USERS.has(intended)) {
          console.warn(`[dialOut] ⛔ blocked call to ${intended || 'unknown'} — not in pilot allowlist`);
          return cb({
            error: 'Pilot mode: this customer is not on the test list.',
            blocked: true,
          });
        }
      }

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
              callType: 'direct',
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

      console.log(`[dialOut] 📞 ${callerUser.username} (${callerUser.role}) → ${targetUsername || targetDbId || 'offline'} | callId=${callId}`);

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
          callType: 'direct',
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
          m.activeCallsGauge.dec();
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
          interval: setInterval(sendPush, 1000),
          timeout: setTimeout(async () => {
            console.log(`[dialOut] ⏰ Push dial timeout (${PUSH_DIAL_TIMEOUT_MS/1000}s) for callId=${callId}`);
            clearPendingRings(callId);
            delete callRouters[callId];
            await deleteCall(callId);
            await updateUser(socket.id, { callId: '' });
            await updateCallRecord(callId, { status: 'missed', ended_at: true });
            m.activeCallsGauge.dec();
            socket.emit('callEnded');
            io.to('admins').emit('callsUpdated');
          }, PUSH_DIAL_TIMEOUT_MS)
        };
      } else if (targetDbId && localTgPool) {
        // Target is an offline PWA Customer, try OneSignal
        console.log(`[dialOut] 📡 Checking Push Subscription for offline PWA user ${targetDbId}`);

        const sendOneSignalPush = async () => {
          try {
            const [pushRows] = await localTgPool.query(
              `SELECT onesignal_id FROM users WHERE user_id = ? AND onesignal_id IS NOT NULL AND push_status IN ('active', 'Subscribed') LIMIT 1`,
              [targetDbId]
            );

            if (pushRows.length > 0) {
              const oneSignalId = pushRows[0].onesignal_id;
              console.log(`[dialOut] 📡 Dispatching OneSignal Push to onesignal_id=${oneSignalId}`);

              const payload = {
                app_id: process.env.ONESIGNAL_APP_ID,
                include_player_ids: [oneSignalId],
                headings: { en: `Incoming Call` },
                contents: { en: `Incoming call from ${callerUser.username}` },
                data: {
                  type: 'incomingCall',
                  callId,
                  from: callerUser.username,
                  role: callerUser.role
                },
                priority: 10,
                url: `https://app.tglevels.in/call?callId=${callId}`,
                ios_sound: "default",
                android_sound: "default",
                chrome_web_icon: "https://bullionscan.io/tg-level-icon.png",
                chrome_web_badge: "https://bullionscan.io/tg-level-icon.png",
                large_icon: "https://bullionscan.io/tg-level-icon.png",
                small_icon: "https://bullionscan.io/tg-level-icon.png"
              };

              const response = await fetch("https://onesignal.com/api/v1/notifications", {
                method: 'POST',
                headers: {
                  "Content-Type": "application/json; charset=utf-8",
                  "Authorization": `Basic ${process.env.ONESIGNAL_API_KEY}`
                },
                body: JSON.stringify(payload)
              });

              const result = await response.json();
              if (response.ok && result.recipients > 0) {
                return true;
              }
            }
            return false;
          } catch (e) {
            console.error(`[dialOut] ❌ OneSignal API error:`, e);
            return false;
          }
        };

        const success = await sendOneSignalPush();
        if (!success) {
          delete callRouters[callId];
          await deleteCall(callId);
          await updateUser(socket.id, { callId: '' });
          await updateCallRecord(callId, { status: 'missed', ended_at: true });
          m.activeCallsGauge.dec();
          return cb({ error: `User is unreachable (Push notification denied or expired).`, pushFailed: true });
        }

        // Successfully sent push via OneSignal
        globalOfflineTargets[targetDbId] = {
          callId,
          callerUsername: callerUser.username,
          callerRole: callerUser.role,
          targetRole: 'customer'
        };

        // We don't setInterval for OneSignal to avoid rate limits.
        // Auto-cancel after timeout if unanswered.
        pendingRings[callId] = {
          targetDbId,
          interval: null,
          timeout: setTimeout(async () => {
            console.log(`[dialOut] ⏰ Push dial timeout (${PUSH_DIAL_TIMEOUT_MS/1000}s) for callId=${callId}`);
            clearPendingRings(callId);
            delete callRouters[callId];
            await deleteCall(callId);
            await updateUser(socket.id, { callId: '' });
            await updateCallRecord(callId, { status: 'missed', ended_at: true });
            m.activeCallsGauge.dec();
            socket.emit('callEnded');
            io.to('admins').emit('callsUpdated');
          }, PUSH_DIAL_TIMEOUT_MS)
        };
      } else {
        // Target is totally offline and no push sub
        console.log(`[dialOut] 📵 Target completely unreachable (no socket, no push)`);
        delete callRouters[callId];
        await deleteCall(callId);
        await updateUser(socket.id, { callId: '' });
        await updateCallRecord(callId, { status: 'missed', ended_at: true });
        m.activeCallsGauge.dec();
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
          callType: 'queue',
        });
      } catch (err) {
        console.error(`[callIn] ❌ Failed to insert call history:`, err);
      }

      // Round-robin: ring one agent at a time instead of broadcasting to all
      await updateCall(callId, { mode: 'queue' });
      await ringNextAgent(callId, callerUser.username);

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

      // Round-robin guard: for queue calls, only the agent currently being rung
      // may accept — a timed-out agent must not steal a call already routed on.
      if (call.mode === 'queue' && call.ringingAgent && call.ringingAgent !== socket.id) {
        return cb({ error: 'This call has already been routed to another agent.' });
      }
      clearRing(callId);
      removeFromQueue(callId);

      // If this was a general 'callIn' and the agent is answering, assign the agent socket
      if (user.role === 'agent' && !call.agentSocketId) {
        await updateCall(callId, { agentSocketId: socket.id });
        await updateUser(socket.id, { callId });
      }

      console.log(`[acceptCall] ✅ Call ${callId} accepted by socket=${socket.id} (${user.role})`);

      // Mark as accepted in Redis so admin sees 'Connected'
      await updateCall(callId, { accepted: 'true' });
      io.to('admins').emit('callsUpdated');

      // Refresh the roster so all agents see this customer is now busy.
      await broadcastPresence();

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

      if (typeof cb === 'function') {
        cb({ callId });
      }
    });

    // ── REJECT CALL ──────────────────────────────────────────────────────
    socket.on('rejectCall', async ({ callId }, cb) => {
      console.log(`[rejectCall] ❌ Call ${callId} rejected by socket=${socket.id}`);

      // Round-robin: an agent passing on a queued call doesn't end it — ring the
      // next agent and keep the customer waiting. The agent was never assigned
      // (callId is only set on accept), so there's no agent state to clear.
      const rrCall = await getCall(callId);
      if (rrCall && rrCall.mode === 'queue' && rrCall.accepted !== 'true') {
        clearRing(callId);
        const customerUser = await getUser(rrCall.customerSocketId);
        await ringNextAgent(callId, customerUser?.username || 'Customer');
        if (typeof cb === 'function') cb({ ok: true });
        return;
      }

      // Update call history DB
      const rejectUser = await getUser(socket.id);
      try {
        await updateCallRecord(callId, {
          status: 'rejected',
          ended_at: true,
          ended_by: rejectUser?.username || 'Unknown',
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

        // Broadcast updated presence so customer list refreshes immediately
        await broadcastPresence();
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
        clearRing(user.callId);
        removeFromQueue(user.callId);

        // If the call never connected (still ringing/queued), there's nothing to
        // reconnect to — clean up immediately and re-balance the queue, no grace.
        const dcCall = await getCall(user.callId);
        if (dcCall && dcCall.accepted !== 'true') {
          console.log(`[disconnect] 🚪 ${user.username} left before connect — cleaning up callId=${user.callId}`);
          clearPendingRings(user.callId);
          delete callRouters[user.callId];
          await deleteCall(user.callId);
          m.activeCallsGauge.dec();
          await removeFromPresence(user.role, socket.id);
          await deleteUser(socket.id);
          await broadcastPresence();
          io.to('admins').emit('callsUpdated');
          await processQueue();
          return;
        }

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

  // ── Fetch Customers from TG Level PWA database ─────────────────────────────
  app.get('/api/customers', authMiddleware, async (req, res) => {
    try {
      if (req.user.role !== 'agent' && req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Forbidden' });
      }
      // Query remote AWS DB (dbt_user) for all active customers
      const [rows] = await tgPool.query(
        `SELECT user_id AS id, 
                COALESCE(CONCAT(first_name, ' ', last_name), username, phone) AS username, 
                phone,
                first_name,
                last_name,
                email,
                image,
                0 AS hasPush
         FROM dbt_user 
         WHERE status = 1 
         ORDER BY created DESC`
      );

      // Now query local PWA DB (tg_level.users) to see who has active push subscriptions
      if (rows.length > 0 && localTgPool) {
        try {
          const userIds = rows.map(r => r.id).filter(Boolean);
          if (userIds.length > 0) {
            const [pushRows] = await localTgPool.query(
              `SELECT user_id FROM users WHERE onesignal_id IS NOT NULL AND push_status IN ('active', 'Subscribed') AND user_id IN (?)`,
              [userIds]
            );
            
            const activePushUsers = new Set(pushRows.map(r => r.user_id));
            rows.forEach(r => {
              if (activePushUsers.has(r.id)) {
                r.hasPush = 1;
              }
            });
          }
        } catch (err) {
          console.error('[api] ⚠️ Failed to fetch push status from local PWA DB:', err.message);
        }
      }

      // Filter to ONLY return users who actually have an active push subscription
      const subscribedUsersOnly = rows.filter(r => r.hasPush === 1);

      res.json(subscribedUsersOnly);
    } catch (e) {
      console.error('[api] ❌ Failed to fetch TG Level customers:', e.message);
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

  // ── Call recording upload ─────────────────────────────────────────────────
  // The agent client records the mixed call audio (local mic + remote) and
  // POSTs the raw blob here on hangup. Body is the audio bytes (audio/webm).
  app.post(
    '/api/recordings/:callId',
    authMiddleware,
    express.raw({ type: 'audio/*', limit: '100mb' }),
    async (req, res) => {
      try {
        const rawCallId = req.params.callId || '';
        // Guard against path traversal — callIds are like "call_1712696400000".
        if (!/^[A-Za-z0-9_-]+$/.test(rawCallId)) {
          return res.status(400).json({ error: 'Invalid callId.' });
        }
        if (!req.body || !req.body.length) {
          return res.status(400).json({ error: 'Empty recording body.' });
        }
        const fileName = `${rawCallId}.webm`;
        const filePath = path.join(RECORDINGS_DIR, fileName);
        await fs.promises.writeFile(filePath, req.body);
        const relPath = `recordings/${fileName}`;
        await setRecordingPath(rawCallId, relPath);
        console.log(`[api] 🎙️ Saved recording for callId=${rawCallId} (${req.body.length} bytes)`);
        res.json({ success: true, path: relPath });
      } catch (err) {
        console.error('[api] ❌ POST /api/recordings failed:', err);
        res.status(500).json({ error: 'Failed to save recording.' });
      }
    }
  );

  // Serve recordings for admin playback. Access is via the non-guessable callId
  // filename; only admins are linked to these paths in the call-history UI.
  app.use('/recordings', express.static(RECORDINGS_DIR));

  /* ── Callback requests ──────────────────────────────────────────────────
     The customer never rings an agent. They leave a request, an agent picks
     it up when convenient and dials out. Everything about the request and the
     resulting call stays in this local database; the ONE thing that leaves is
     a single "customer requested a callback" line injected into their Support
     Board thread by PWA_NOTIFY, which is the only writer to that remote DB.

     Created server-to-server: the PWA authenticates the customer with its own
     session cookie and then calls this with the internal key, so a browser can
     never post a callback for someone else's number.
     ---------------------------------------------------------------------- */
  const INTERNAL_KEY = process.env.INTERNAL_API_KEY || '';

  function requireInternalKey(req, res, next) {
    if (!INTERNAL_KEY || req.get('x-internal-key') !== INTERNAL_KEY) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
  }

  const last10 = (v) => String(v == null ? '' : v).replace(/\D/g, '').slice(-10);

  /* Which customers are reachable in the app right now.
     Read straight off the live socket set rather than redis: PWA clients
     authenticate with their PWA user_id (see the isPwaClient branch above),
     and that id is on socket.user but is not part of the presence hash. This
     is the same information without widening the stored state. */
  function onlineCustomerIds() {
    const ids = new Set();
    for (const [, s] of io.of('/').sockets) {
      // foreground !== false: a socket mid-handshake hasn't reported yet and is
      // treated as reachable until it says otherwise. A backgrounded PWA
      // (foreground === false) is excluded — it can't actually be rung.
      if (s.user && s.user.role === 'customer' && s.user.userId && s.data.foreground !== false) {
        ids.add(String(s.user.userId));
      }
    }
    return ids;
  }

  app.post('/api/callback', requireInternalKey, async (req, res) => {
    try {
      const phone = last10(req.body?.phone);
      if (phone.length !== 10) {
        return res.status(400).json({ error: 'Valid phone required.' });
      }
      const pwaUserId = String(req.body?.user_id || '').slice(0, 100) || null;
      const name = String(req.body?.name || '').slice(0, 120) || null;

      const pool = getPool();
      let row;
      try {
        const [ins] = await pool.execute(
          `INSERT INTO callback_requests (phone, pwa_user_id, name, source)
           VALUES (?, ?, ?, 'pwa')`,
          [phone, pwaUserId, name]
        );
        const [rows] = await pool.execute(
          'SELECT * FROM callback_requests WHERE id = ?', [ins.insertId]
        );
        row = rows[0];
      } catch (e) {
        // The unique index on open_key is what enforces one open request per
        // customer — a double tap loses the race in the database, not here.
        if (e && e.code === 'ER_DUP_ENTRY') {
          const [rows] = await pool.execute(
            `SELECT * FROM callback_requests
              WHERE phone = ? AND status IN ('pending','claimed') LIMIT 1`,
            [phone]
          );
          return res.json({ success: true, duplicate: true, request: rows[0] || null });
        }
        throw e;
      }

      // Tell whoever is watching. Missing it is survivable — the panels also
      // poll GET /api/callbacks, so a closed laptop does not lose a request.
      io.to('agents').emit('callbackRequested', row);
      io.to('admins').emit('callbackRequested', row);

      console.log(`[callback] 📞 requested by ${phone} (id ${row.id})`);
      res.json({ success: true, duplicate: false, request: row });
    } catch (err) {
      console.error('[api] ❌ POST /api/callback failed:', err);
      res.status(500).json({ error: 'Failed to create callback request.' });
    }
  });

  // Callback lookup for the agent panel. Agents do NOT get an open list — they
  // must pass ?phone=<digits>, and only that number's open callbacks come back
  // (server-enforced, so it's a rule not just a hidden UI). Without a phone the
  // response is empty. The admin panel passes ?all=1 to still see the full
  // queue for oversight.
  app.get('/api/callbacks', authMiddleware, async (req, res) => {
    try {
      const status = ['pending', 'claimed', 'done', 'cancelled'].includes(req.query.status)
        ? req.query.status : null;
      const phoneDigits = String(req.query.phone || '').replace(/\D/g, '').slice(-10);
      const wantAll = req.query.all === '1' || req.query.all === 'true';
      const pool = getPool();

      // Agents must search by phone; no phone + not admin-all → return nothing.
      if (!phoneDigits && !wantAll) {
        return res.json({ success: true, count: 0, requests: [] });
      }

      const statusClause = status ? 'status = ?' : "status IN ('pending','claimed')";
      const params = status ? [status] : [];
      let sql = `SELECT * FROM callback_requests WHERE ${statusClause}`;
      if (phoneDigits) {
        sql += ' AND RIGHT(REPLACE(phone," ",""),10) = ?';
        params.push(phoneDigits);
      }
      sql += ' ORDER BY requested_at ASC LIMIT 500';
      const [rows] = await pool.execute(sql, params);

      const online = onlineCustomerIds();
      res.json({
        success: true,
        count: rows.length,
        // `online` is what tells an agent whether dialling out will actually
        // ring anything — a closed PWA cannot be rung.
        requests: rows.map((r) => ({
          ...r,
          online: Boolean(r.pwa_user_id && online.has(String(r.pwa_user_id))),
        })),
      });
    } catch (err) {
      console.error('[api] ❌ GET /api/callbacks failed:', err);
      res.status(500).json({ error: 'Failed to fetch callbacks.' });
    }
  });

  // Claim / release / close. Claiming is a conditional UPDATE so two agents
  // grabbing the same request at once cannot both win.
  app.post('/api/callbacks/:id/:action', authMiddleware, async (req, res) => {
    try {
      const id = Number(req.params.id);
      const action = req.params.action;
      if (!Number.isInteger(id) || id < 1) {
        return res.status(400).json({ error: 'Bad id.' });
      }
      const pool = getPool();
      let result;

      if (action === 'claim') {
        [result] = await pool.execute(
          `UPDATE callback_requests
              SET status='claimed', claimed_by=?, claimed_at=NOW()
            WHERE id=? AND status='pending'`,
          [req.user.userId, id]
        );
        if (!result.affectedRows) {
          return res.status(409).json({ error: 'Already taken by someone else.' });
        }
      } else if (action === 'release') {
        [result] = await pool.execute(
          `UPDATE callback_requests
              SET status='pending', claimed_by=NULL, claimed_at=NULL
            WHERE id=? AND status='claimed' AND claimed_by=?`,
          [id, req.user.userId]
        );
        if (!result.affectedRows) {
          return res.status(409).json({ error: 'Not yours to release.' });
        }
      } else if (action === 'close') {
        const outcome = String(req.body?.outcome || '').slice(0, 255) || null;
        [result] = await pool.execute(
          `UPDATE callback_requests
              SET status='done', closed_at=NOW(), outcome=?, last_call_id=?
            WHERE id=? AND status IN ('pending','claimed')`,
          [outcome, String(req.body?.call_id || '').slice(0, 64) || null, id]
        );
        if (!result.affectedRows) {
          return res.status(409).json({ error: 'Already closed.' });
        }
      } else {
        return res.status(400).json({ error: 'Unknown action.' });
      }

      const [rows] = await pool.execute('SELECT * FROM callback_requests WHERE id=?', [id]);
      io.to('agents').emit('callbacksUpdated', rows[0]);
      io.to('admins').emit('callbacksUpdated', rows[0]);
      res.json({ success: true, request: rows[0] });
    } catch (err) {
      console.error('[api] ❌ callback action failed:', err);
      res.status(500).json({ error: 'Action failed.' });
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
  const PORT = parseInt(process.env.PORT, 10) || 3005;
  server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Prometheus metrics available at http://localhost:${PORT}/metrics`);
    console.log(`Redis health available at http://localhost:${PORT}/redis-health`);
    // Print an initial snapshot right away so the first log isn't 60s away
    printMetricsSnapshot();
  });
}

main().catch(console.error);
