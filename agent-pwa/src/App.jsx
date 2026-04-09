import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupCall, getCurrentCallId, clearCurrentCallId } from './lib/mediasoupClient';

const SERVER_URL = '';

function App() {
  const [username, setUsername] = useState('');
  const [loggedIn, setLoggedIn] = useState(false);

  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [incomingCall, setIncomingCall] = useState(null);
  const [activeCall, setActiveCall] = useState(null);
  const [reconnecting, setReconnecting] = useState(false);
  const [peerDisconnected, setPeerDisconnected] = useState(null);

  // Roster state
  const [activeCustomers, setActiveCustomers] = useState([]);

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
      query: { role: 'agent', username: username.trim() },
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
      console.log(`[agent] Connected with socket.id=${s.id}`);

      // If we were reconnecting and had an active call, try to restore
      if (reconnecting && previousSocketId.current && getCurrentCallId()) {
        const callId = getCurrentCallId();
        console.log(`[agent] Attempting session reconnection: prev=${previousSocketId.current} callId=${callId}`);

        s.emit('reconnectSession', {
          previousSocketId: previousSocketId.current,
          callId,
        }, async (res) => {
          if (res.success) {
            console.log('[agent] ✅ Session restored!');
            setReconnecting(false);
            setPeerDisconnected(null);

            // Re-setup media
            try {
              const callTransports = await setupCall(s, res.callId, () => {
                console.log('Remote audio playing (reconnected)');
              });
              callRef.current = callTransports;
            } catch (err) {
              console.error('[agent] Failed to restore media:', err);
              endCall();
            }
          } else {
            console.log(`[agent] ❌ Session restore failed: ${res.reason} — cleaning up`);
            setReconnecting(false);
            clearCurrentCallId();
            handleCallCleanup();
          }
        });
      }

      previousSocketId.current = s.id;
      setConnected(true);
      s.emit('getPresence'); // Request initial presence state
    });

    s.on('disconnect', () => {
      setConnected(false);

      // If we had an active call, enter reconnecting state
      if (activeCall || getCurrentCallId()) {
        setReconnecting(true);
        console.log('[agent] Socket disconnected during active call — will attempt reconnection');
      }
    });

    s.on('presenceUpdate', ({ customers }) => {
      setActiveCustomers(customers);
    });

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

  const acceptCall = () => {
    if (!incomingCall) return;
    const { callId, from } = incomingCall;

    socket.emit('acceptCall', { callId }, async (res) => {
      if (res.error) {
        alert(res.error);
        setIncomingCall(null);
        return;
      }
      setActiveCall({ callId, withUser: from, state: 'Connected' });
      setIncomingCall(null);

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
  };

  const dialCustomer = (customerId, customerName) => {
    socket.emit('dialOut', { targetId: customerId }, (res) => {
      if (res.error) return alert(res.error);

      const { callId } = res;
      setActiveCall({ callId, withUser: customerName, state: 'Calling...' });

      socket.once('callAccepted', async () => {
        setActiveCall({ callId, withUser: customerName, state: 'Connected' });
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

  const endCall = () => {
    socket?.emit('hangup');
    handleCallCleanup();
  };

  if (!loggedIn) {
    return (
      <div className="login-container">
        <form onSubmit={handleLogin} className="login-box">
          <h2>Agent Login</h2>
          <p>Please enter your name to connect</p>
          <input
            type="text"
            placeholder="Agent Name"
            value={username}
            onChange={e => setUsername(e.target.value)}
            autoFocus
            required
          />
          <button type="submit">Join Support Queue</button>
        </form>
      </div>
    );
  }

  return (
    <div className="app-container">
      <div className="header">
        <h1>Agent Portal</h1>
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
              <div style={{ color: 'white', marginBottom: 8 }}>Call from {incomingCall.from}</div>
              <div className="incoming-call-actions">
                <button className="success" onClick={acceptCall}>Accept</button>
                <button className="danger" onClick={() => setIncomingCall(null)}>Decline</button>
              </div>
            </div>
          )}

          <div className="card roster">
            <div className="card-title">Active Customers ({activeCustomers.length})</div>
            {activeCustomers.length === 0 ? (
              <p style={{ color: 'var(--text-secondary)' }}>No customers currently online.</p>
            ) : (
              <ul className="user-list">
                {activeCustomers.map(customer => (
                  <li key={customer.id} className="user-item">
                    <div className="user-info">
                      <span className="user-name">{customer.username}</span>
                      <span className="user-role">Customer</span>
                    </div>
                    <button className="dial-btn" onClick={() => dialCustomer(customer.id, customer.username)}>
                      Call
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </>
      ) : (
        <div className="card">
          <div className="call-active-state">
            <div className="avatar">{activeCall.withUser.charAt(0).toUpperCase()}</div>
            <div className="card-title">Call with {activeCall.withUser}</div>
            <div style={{ color: 'white' }}>{activeCall.state}</div>
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
