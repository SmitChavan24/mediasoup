/**
 * Redis State Manager for Mediasoup Signaling
 *
 * Replaces in-memory `calls` object and `activeUsers` Map with Redis-backed
 * state. Provides a clean abstraction so the rest of the server never touches
 * Redis directly.
 *
 * Key Schema
 * ──────────
 *   user:{socketId}        → Hash  { username, role, status, callId }
 *   call:{callId}          → Hash  { agentSocketId, customerSocketId, routerIndex, startTime }
 *   presence:agents        → Set   of socket IDs
 *   presence:customers     → Set   of socket IDs
 *   presence:admins        → Set   of socket IDs
 *   grace:{socketId}       → String (callId)  TTL 30s
 */

const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const GRACE_PERIOD_SECONDS = parseInt(process.env.GRACE_PERIOD_SECONDS, 10) || 30;

// ── Redis Clients ────────────────────────────────────────────────────────────
// We need two clients: one for normal commands, one for Pub/Sub subscriptions
// (ioredis enters subscriber mode and can no longer run regular commands).

let redis;
let redisSub;

function createRedisClients() {
  redis = new Redis(REDIS_URL, { lazyConnect: false, maxRetriesPerRequest: 3 });
  redisSub = new Redis(REDIS_URL, { lazyConnect: false, maxRetriesPerRequest: 3 });

  redis.on('connect', () => console.log('[redis] ✅ Command client connected'));
  redis.on('error', (err) => console.error('[redis] ❌ Command client error:', err.message));

  redisSub.on('connect', () => console.log('[redis] ✅ Subscriber client connected'));
  redisSub.on('error', (err) => console.error('[redis] ❌ Subscriber client error:', err.message));

  return { redis, redisSub };
}

function getRedis() {
  return redis;
}

// ── Startup Cleanup ──────────────────────────────────────────────────────────
// Wipe any orphaned state from a previous crash / restart.

async function cleanupStaleState() {
  console.log('[redis] 🧹 Cleaning up stale state from previous session...');

  const userKeys = await redis.keys('user:*');
  const callKeys = await redis.keys('call:*');
  const graceKeys = await redis.keys('grace:*');

  const pipeline = redis.pipeline();

  for (const key of [...userKeys, ...callKeys, ...graceKeys]) {
    pipeline.del(key);
  }
  pipeline.del('presence:agents');
  pipeline.del('presence:customers');
  pipeline.del('presence:admins');

  await pipeline.exec();

  const total = userKeys.length + callKeys.length + graceKeys.length;
  console.log(`[redis] 🧹 Removed ${total} stale keys + presence sets`);
}

// ── User Operations ──────────────────────────────────────────────────────────

async function setUser(socketId, { username, role, status = 'connected' }) {
  await redis.hmset(`user:${socketId}`, {
    username,
    role,
    status,
    callId: '',
    disconnectedAt: '',
  });
}

async function getUser(socketId) {
  const data = await redis.hgetall(`user:${socketId}`);
  if (!data || !data.username) return null;
  return data;
}

async function updateUser(socketId, fields) {
  const entries = Object.entries(fields);
  if (entries.length === 0) return;
  await redis.hmset(`user:${socketId}`, fields);
}

async function deleteUser(socketId) {
  await redis.del(`user:${socketId}`);
}

// ── Presence Operations ──────────────────────────────────────────────────────

async function addToPresence(role, socketId) {
  await redis.sadd(`presence:${role}s`, socketId);
}

async function removeFromPresence(role, socketId) {
  await redis.srem(`presence:${role}s`, socketId);
}

/**
 * Returns an array of { id, username, role } for every connected user of the
 * given role.  Reads each user hash individually — fine for rosters of a few
 * hundred users; for thousands, consider a Lua script.
 */
async function getPresenceList(role) {
  const ids = await redis.smembers(`presence:${role}s`);
  if (ids.length === 0) return [];

  const pipeline = redis.pipeline();
  for (const id of ids) {
    pipeline.hgetall(`user:${id}`);
  }
  const results = await pipeline.exec();

  const users = [];
  for (let i = 0; i < results.length; i++) {
    const [err, data] = results[i];
    if (!err && data && data.username && data.status === 'connected') {
      users.push({ id: ids[i], username: data.username, role: data.role });
    }
  }
  return users;
}

async function getPresenceCount(role) {
  return redis.scard(`presence:${role}s`);
}

// ── Call Operations ──────────────────────────────────────────────────────────

async function setCall(callId, { agentSocketId, customerSocketId, routerIndex, startTime }) {
  await redis.hmset(`call:${callId}`, {
    agentSocketId: agentSocketId || '',
    customerSocketId: customerSocketId || '',
    routerIndex: String(routerIndex),
    startTime: String(startTime),
    accepted: 'false',
  });
}

async function getCall(callId) {
  const data = await redis.hgetall(`call:${callId}`);
  if (!data || !data.startTime) return null;
  return data;
}

async function updateCall(callId, fields) {
  const mapped = {};
  for (const [k, v] of Object.entries(fields)) {
    mapped[k] = String(v ?? '');
  }
  await redis.hmset(`call:${callId}`, mapped);
}

async function deleteCall(callId) {
  await redis.del(`call:${callId}`);
}

/**
 * Returns all active calls with agent/customer user info.
 * Used by the admin dashboard's getLiveCalls handler.
 */
async function getAllActiveCalls() {
  const callKeys = await redis.keys('call:*');
  if (callKeys.length === 0) return [];

  const pipeline = redis.pipeline();
  for (const key of callKeys) {
    pipeline.hgetall(key);
  }
  const results = await pipeline.exec();

  const calls = [];
  for (let i = 0; i < results.length; i++) {
    const [err, data] = results[i];
    if (err || !data || !data.startTime) continue;

    const callId = callKeys[i].replace('call:', '');

    // Look up agent and customer usernames
    let agentName = 'Unknown';
    let customerName = 'Unknown';

    if (data.agentSocketId) {
      const agent = await redis.hgetall(`user:${data.agentSocketId}`);
      if (agent && agent.username) agentName = agent.username;
    }
    if (data.customerSocketId) {
      const customer = await redis.hgetall(`user:${data.customerSocketId}`);
      if (customer && customer.username) customerName = customer.username;
    }

    calls.push({
      id: callId,
      agent: agentName,
      customer: customerName,
      agentSocketId: data.agentSocketId,
      customerSocketId: data.customerSocketId,
      startTime: parseInt(data.startTime, 10),
      accepted: data.accepted === 'true',
    });
  }
  return calls;
}

// ── Grace Period ─────────────────────────────────────────────────────────────

async function setGracePeriod(socketId, callId) {
  await redis.set(`grace:${socketId}`, callId, 'EX', GRACE_PERIOD_SECONDS);
}

async function getGracePeriod(socketId) {
  return redis.get(`grace:${socketId}`);
}

async function deleteGracePeriod(socketId) {
  await redis.del(`grace:${socketId}`);
}

// ── Keyspace Notifications (expired keys) ────────────────────────────────────

/**
 * Subscribe to Redis key-expiry events for grace:* keys.
 * When a grace period expires without reconnection, `callback(socketId)` fires.
 */
function subscribeToExpirations(callback) {
  redisSub.subscribe('__keyevent@0__:expired');
  redisSub.on('message', (_channel, expiredKey) => {
    if (expiredKey.startsWith('grace:')) {
      const socketId = expiredKey.slice('grace:'.length);
      console.log(`[redis] ⏰ Grace period expired for socket=${socketId}`);
      callback(socketId);
    }
  });
  console.log('[redis] 👂 Subscribed to keyspace expiration events');
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  createRedisClients,
  getRedis,
  cleanupStaleState,
  GRACE_PERIOD_SECONDS,

  // Users
  setUser,
  getUser,
  updateUser,
  deleteUser,

  // Presence
  addToPresence,
  removeFromPresence,
  getPresenceList,
  getPresenceCount,

  // Calls
  setCall,
  getCall,
  updateCall,
  deleteCall,
  getAllActiveCalls,

  // Grace period
  setGracePeriod,
  getGracePeriod,
  deleteGracePeriod,

  // Subscriptions
  subscribeToExpirations,
};
