import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupCall, getCurrentCallId, clearCurrentCallId } from './lib/mediasoupClient';

const SERVER_URL = '';

function App() {
  const [username, setUsername] = useState('');
  const [loggedIn, setLoggedIn] = useState(false);

  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [activeCall, setActiveCall] = useState(null);
  const [incomingCall, setIncomingCall] = useState(null);
  const [reconnecting, setReconnecting] = useState(false);
  const [peerDisconnected, setPeerDisconnected] = useState(null);

  // Roster state
  const [activeAgents, setActiveAgents] = useState([]);

  const callRef = useRef(null);
  const previousSocketId = useRef(null);

  const handleLogin = (e) => {
    e.preventDefault();
    if (username.trim()) {
      setLoggedIn(true);
      connectSocket();
    }
  };

  const connectSocket = () => {
    const s = io(SERVER_URL, {
      query: { role: 'customer', username: username.trim() },
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: 10,
      extraHeaders: {
        'ngrok-skip-browser-warning': 'true'
      }
    });

    s.on('connect', () => {
      console.log(`[customer] Connected with socket.id=${s.id}`);

      // If we were reconnecting and had an active call, try to restore
      if (reconnecting && previousSocketId.current && getCurrentCallId()) {
        const callId = getCurrentCallId();
        console.log(`[customer] Attempting session reconnection: prev=${previousSocketId.current} callId=${callId}`);

        s.emit('reconnectSession', {
          previousSocketId: previousSocketId.current,
          callId,
        }, async (res) => {
          if (res.success) {
            console.log('[customer] ✅ Session restored!');
            setReconnecting(false);
            setPeerDisconnected(null);

            // Re-setup media
            try {
              const callTransports = await setupCall(s, res.callId, () => {
                console.log('Remote audio playing (reconnected)');
              });
              callRef.current = callTransports;
            } catch (err) {
              console.error('[customer] Failed to restore media:', err);
              endCall();
            }
          } else {
            console.log(`[customer] ❌ Session restore failed: ${res.reason} — cleaning up`);
            setReconnecting(false);
            clearCurrentCallId();
            handleCallCleanup();
          }
        });
      }

      previousSocketId.current = s.id;
      setConnected(true);
      s.emit('getPresence'); // Fetch current roster
    });

    s.on('disconnect', () => {
      setConnected(false);

      // If we had an active call, enter reconnecting state
      if (activeCall || getCurrentCallId()) {
        setReconnecting(true);
        console.log('[customer] Socket disconnected during active call — will attempt reconnection');
      }
    });

    s.on('presenceUpdate', ({ agents }) => {
      setActiveAgents(agents);
    });

    // When an agent calls this customer, or general queue connects
    s.on('incomingCall', ({ callId, from, role }) => {
      setIncomingCall({ callId, from, role });
    });

    s.on('callEnded', () => {
      handleCallCleanup();
    });

    // Handle peer disconnect/reconnect notifications
    s.on('participantDisconnected', ({ username: peerName, gracePeriod }) => {
      setPeerDisconnected({ username: peerName, gracePeriod });
    });

    s.on('participantReconnected', () => {
      setPeerDisconnected(null);
    });

    // Respond to server heartbeat
    s.on('hb-ping', () => {
      s.emit('hb-pong');
    });

    setSocket(s);
  };

  const handleCallCleanup = () => {
    if (callRef.current) {
      callRef.current.close();
      callRef.current = null;
    }
    clearCurrentCallId();
    setActiveCall(null);
    setIncomingCall(null);
    setReconnecting(false);
    setPeerDisconnected(null);
  };

  const callAnyAgent = () => {
    setActiveCall({ state: 'Waiting for an agent...', withUser: 'Next Available' });

    socket.emit('callIn');

    socket.once('callAccepted', async ({ callId }) => {
      setActiveCall({ callId, state: 'Connected', withUser: 'Agent' });

      try {
        const callTransports = await setupCall(socket, callId, () => {
          console.log('Playing remote audio');
        });
        callRef.current = callTransports;
      } catch (err) {
        alert('Could not setup media devices. Check permissions.');
        endCall();
      }
    });
  };

  const callSpecificAgent = (agentId, agentName) => {
    socket.emit('dialOut', { targetId: agentId }, (res) => {
      if (res.error) return alert(res.error);

      const { callId } = res;
      setActiveCall({ callId, withUser: agentName, state: 'Calling...' });

      socket.once('callAccepted', async () => {
        setActiveCall({ callId, withUser: agentName, state: 'Connected' });
        try {
          const callTransports = await setupCall(socket, callId, () => {
            console.log('Remote audio playing');
          });
          callRef.current = callTransports;
        } catch (err) {
          alert('Could not setup media devices. Check permissions.');
          endCall();
        }
      });
    });
  };

  const acceptIncomingCall = () => {
    if (!incomingCall) return;
    const { callId, from } = incomingCall;

    socket.emit('acceptCall', { callId }, async (res) => {
      if (res.error) return alert(res.error);

      setActiveCall({ callId, state: 'Connected', withUser: from });
      setIncomingCall(null);

      try {
        const callTransports = await setupCall(socket, callId, () => {
          console.log('Playing remote audio');
        });
        callRef.current = callTransports;
      } catch (err) {
        alert('Could not setup media devices. Check permissions.');
        endCall();
      }
    });
  };

  const endCall = () => {
    socket?.emit('hangup');
    handleCallCleanup();
  };

  if (!loggedIn) {
    return (
      <div className="login-container">
        <form onSubmit={handleLogin} className="login-box">
          <h2>Customer Login</h2>
          <p>Please enter your name to connect</p>
          <input
            type="text"
            placeholder="Your Name"
            value={username}
            onChange={e => setUsername(e.target.value)}
            autoFocus
            required
          />
          <button type="submit">Connect to System</button>
        </form>
      </div>
    );
  }

  return (
    <div className="app-container">
      <div className="header">
        <h1>Customer Support</h1>
        <div className="status">
          <div className={`status-indicator ${connected ? 'connected' : reconnecting ? 'reconnecting' : ''}`}></div>
          {reconnecting
            ? 'Reconnecting...'
            : connected
              ? `Connected as ${username}`
              : 'Connecting...'}
        </div>
      </div>

      {!activeCall ? (
        <>
          {incomingCall && (
            <div className="card incoming-call">
              <div className="card-title">Incoming Call</div>
              <div style={{ color: 'var(--text-primary)', marginBottom: 8, fontWeight: 'bold' }}>
                Call from {incomingCall.from}
              </div>
              <div className="incoming-call-actions">
                <button className="success" onClick={acceptIncomingCall}>Accept</button>
                <button className="danger" onClick={() => setIncomingCall(null)}>Decline</button>
              </div>
            </div>
          )}

          {!incomingCall && (
            <>
              <div className="card" style={{ textAlign: 'center', padding: '24px 24px' }}>
                <button
                  className="success"
                  style={{ width: '100%', padding: '16px', fontSize: '18px', borderRadius: '16px', marginBottom: '8px' }}
                  onClick={callAnyAgent}
                >
                  Call First Available Agent
                </button>
                <p style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>or pick a specific agent below</p>
              </div>

              <div className="card roster">
                <div className="card-title">Online Agents ({activeAgents.length})</div>
                {activeAgents.length === 0 ? (
                  <p style={{ color: 'var(--text-secondary)' }}>No agents are currently online.</p>
                ) : (
                  <ul className="user-list">
                    {activeAgents.map(agent => (
                      <li key={agent.id} className="user-item">
                        <div className="user-info">
                          <span className="user-name">{agent.username}</span>
                          <span className="user-role">Support Agent</span>
                        </div>
                        <button className="dial-btn primary" onClick={() => callSpecificAgent(agent.id, agent.username)}>
                          Call
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </>
          )}
        </>
      ) : (
        <div className="card">
          <div className="call-active-state">
            <div className="avatar">CS</div>
            <div className="card-title">Call with {activeCall.withUser}</div>
            <div style={{ color: 'var(--text-primary)', fontWeight: 'bold' }}>{activeCall.state}</div>
            {peerDisconnected && (
              <div style={{ color: '#ffab00', fontSize: '14px', marginTop: 4 }}>
                ⚠️ {peerDisconnected.username} disconnected — waiting {peerDisconnected.gracePeriod}s for reconnection...
              </div>
            )}
            <div className="call-timer">00:00</div>
            <button className="danger" onClick={endCall}>End Call</button>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
