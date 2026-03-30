const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { startMediasoup, createTransport, getRouter } = require('./mediasoup');

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

  // Send the live roster to everyone
  io.emit('presenceUpdate', { agents, customers });
}

async function main() {
  await startMediasoup();

  io.on('connection', (socket) => {
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
    socket.on('getRouterRtpCapabilities', (cb) => {
      cb(getRouter().rtpCapabilities);
    });

    // ── STEP 2: Create a send (produce) transport ─────────────────────────
    socket.on('createSendTransport', async (cb) => {
      const { transport, params } = await createTransport();
      socket._sendTransport = transport;
      cb(params);
    });

    // ── STEP 3: Create a recv (consume) transport ─────────────────────────
    socket.on('createRecvTransport', async (cb) => {
      const { transport, params } = await createTransport();
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
      const router = getRouter();
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
        id:            consumer.id,
        producerId,
        kind:          consumer.kind,
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
      
      // Assume the caller is agent and target is customer by default,
      // but flip it if the caller is the customer.
      if (callerUser.role === 'customer') {
        calls[callId] = { agentSocket: targetSocket, customerSocket: socket };
      } else {
        calls[callId] = { agentSocket: socket, customerSocket: targetSocket };
      }
      
      socket._callId = callId;
      targetSocket._callId = callId;

      console.log(`[dialOut] ${callerUser.username} is dialing target ${targetId}`);

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
      calls[callId] = { customerSocket: socket, agentSocket: null };
      socket._callId = callId;
      
      const callerUser = activeUsers.get(socket.id);
      console.log(`[callIn] General queue call from: ${callerUser.username}`);

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

    console.log(`Ending call ${callId}`);

    // Notify the other party
    const other = socket === call.agentSocket ? call.customerSocket : call.agentSocket;
    other?.emit('callEnded');
    
    // Unbind call IDs
    if (call.agentSocket) call.agentSocket._callId = null;
    if (call.customerSocket) call.customerSocket._callId = null;

    // Clean up mediasoup resources for caller
    socket._producer?.close();
    socket._consumer?.close();
    socket._sendTransport?.close();
    socket._recvTransport?.close();
    
    // Clean up mediasoup resources for other
    if (other) {
      other._producer?.close();
      other._consumer?.close();
      other._sendTransport?.close();
      other._recvTransport?.close();
    }

    delete calls[callId];
  }

  server.listen(3000, () => console.log('Server running on port 3000'));
}

main().catch(console.error);
