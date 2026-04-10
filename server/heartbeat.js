/**
 * WebSocket Heartbeat Monitor
 *
 * Sends periodic pings through Socket.IO to detect stale connections that
 * the TCP keep-alive might miss (e.g. mobile device going to sleep, network
 * switch, etc.).
 *
 * This is a supplement to Socket.IO's built-in pingTimeout — it adds
 * application-level visibility and logging.
 */

const HEARTBEAT_INTERVAL_MS = parseInt(process.env.HEARTBEAT_INTERVAL_MS, 10) || 5000;
const HEARTBEAT_TIMEOUT_MS = parseInt(process.env.HEARTBEAT_TIMEOUT_MS, 10) || 30000;

/**
 * Attach heartbeat monitoring to every new socket connection.
 *
 * @param {import('socket.io').Socket} socket — individual socket
 * @param {Function} onTimeout — called with (socket) when heartbeat fails
 */
function attachHeartbeat(socket, onTimeout) {
  let lastPong = Date.now();
  let intervalId = null;

  // Client responds with 'hb-pong' when it receives 'hb-ping'
  socket.on('hb-pong', () => {
    lastPong = Date.now();
  });

  intervalId = setInterval(() => {
    const elapsed = Date.now() - lastPong;

    if (elapsed > HEARTBEAT_TIMEOUT_MS) {
      console.warn(`[heartbeat] 💀 Socket ${socket.id} heartbeat timeout (${elapsed}ms > ${HEARTBEAT_TIMEOUT_MS}ms)`);
      clearInterval(intervalId);
      onTimeout(socket);
      return;
    }

    // Send ping
    socket.emit('hb-ping');
  }, HEARTBEAT_INTERVAL_MS);

  // Clean up interval when socket disconnects normally
  socket.on('disconnect', () => {
    clearInterval(intervalId);
  });
}

module.exports = {
  attachHeartbeat,
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_TIMEOUT_MS,
};
