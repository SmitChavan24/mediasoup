import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupCall, getCurrentCallId, clearCurrentCallId, getPreviousSocketId } from './lib/mediasoupClient';

const SERVER_URL = '';
const STORAGE_KEY = 'voip_customer_session';

function getStoredSession() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch { return null; }
}

function storeSession(session) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
}

function clearSession() {
  localStorage.removeItem(STORAGE_KEY);
}

function App() {
  // Auth state
  const [authMode, setAuthMode] = useState('login');
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState('');
  const [session, setSession] = useState(null);
  const [checkingSession, setCheckingSession] = useState(true);

  // Form fields
  const [formUsername, setFormUsername] = useState('');
  const [formPhone, setFormPhone] = useState('');
  const [formPassword, setFormPassword] = useState('');
  const [formConfirmPassword, setFormConfirmPassword] = useState('');

  // App state
  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [activeCall, setActiveCall] = useState(null);
  const [incomingCall, setIncomingCall] = useState(null);
  const [reconnecting, setReconnecting] = useState(false);
  const [peerDisconnected, setPeerDisconnected] = useState(null);
  const [activeAgents, setActiveAgents] = useState([]);

  const callRef = useRef(null);
  const previousSocketId = useRef(null);
  const socketRef = useRef(null);

  // On mount: check for existing session
  useEffect(() => {
    const stored = getStoredSession();
    if (stored && stored.token) {
      fetch(`${SERVER_URL}/api/verify-token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: stored.token }),
      })
        .then(r => r.json())
        .then(data => {
          if (data.valid && data.user.role === 'customer') {
            setSession(stored);
          } else {
            clearSession();
          }
        })
        .catch(() => clearSession())
        .finally(() => setCheckingSession(false));
    } else {
      setCheckingSession(false);
    }
  }, []);

  useEffect(() => {
    if (session && !socketRef.current) {
      connectSocket(session);
    }
  }, [session]);

  const handleRegister = async (e) => {
    e.preventDefault();
    setAuthError('');
    if (formPassword !== formConfirmPassword) {
      setAuthError('Passwords do not match.');
      return;
    }
    setAuthLoading(true);
    try {
      const res = await fetch(`${SERVER_URL}/api/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: formUsername.trim(),
          phone: formPhone.trim(),
          password: formPassword,
          role: 'customer',
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Registration failed');
      const sess = { token: data.token, username: data.username, phone: data.phone, role: data.role };
      storeSession(sess);
      setSession(sess);
    } catch (err) {
      setAuthError(err.message);
    } finally {
      setAuthLoading(false);
    }
  };

  const handleLogin = async (e) => {
    e.preventDefault();
    setAuthError('');
    setAuthLoading(true);
    try {
      const res = await fetch(`${SERVER_URL}/api/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          phone: formPhone.trim(),
          password: formPassword,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Login failed');
      if (data.role !== 'customer') throw new Error('This login is for customers only.');
      const sess = { token: data.token, username: data.username, phone: data.phone, role: data.role };
      storeSession(sess);
      setSession(sess);
    } catch (err) {
      setAuthError(err.message);
    } finally {
      setAuthLoading(false);
    }
  };

  const handleLogout = () => {
    if (socketRef.current) {
      socketRef.current.disconnect();
      socketRef.current = null;
    }
    clearSession();
    setSession(null);
    setSocket(null);
    setConnected(false);
    setActiveCall(null);
    setIncomingCall(null);
    setActiveAgents([]);
    handleCallCleanup();
  };

  const connectSocket = (sess) => {
    const s = io(SERVER_URL, {
      auth: { token: sess.token },
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

      // Check for active call to reconnect (works for both network drops and tab close)
      const prevSocketId = previousSocketId.current || getPreviousSocketId();
      const prevCallId = getCurrentCallId();

      if (prevSocketId && prevCallId) {
        console.log(`[customer] Attempting session reconnection: prev=${prevSocketId} callId=${prevCallId}`);
        s.emit('reconnectSession', {
          previousSocketId: prevSocketId,
          callId: prevCallId,
        }, async (res) => {
          if (res.success) {
            setReconnecting(false);
            setPeerDisconnected(null);
            setActiveCall({ callId: res.callId, withUser: res.username || 'Agent', state: 'Connected' });
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
            setReconnecting(false);
            clearCurrentCallId();
            handleCallCleanup();
          }
        });
      }

      previousSocketId.current = s.id;
      setConnected(true);
      s.emit('getPresence');
    });

    s.on('connect_error', (err) => {
      console.error('[customer] Socket connect error:', err.message);
      if (err.message.includes('expired') || err.message.includes('Invalid')) {
        handleLogout();
      }
    });

    s.on('disconnect', () => {
      setConnected(false);
      if (activeCall || getCurrentCallId()) {
        setReconnecting(true);
      }
    });

    s.on('presenceUpdate', ({ agents }) => {
      setActiveAgents(agents);
    });

    s.on('incomingCall', ({ callId, from, role }) => {
      setIncomingCall({ callId, from, role });
    });

    s.on('callEnded', () => {
      handleCallCleanup();
    });

    s.on('participantDisconnected', ({ username: peerName, gracePeriod }) => {
      setPeerDisconnected({ username: peerName, gracePeriod });
    });

    s.on('participantReconnected', () => {
      setPeerDisconnected(null);
    });

    s.on('hb-ping', () => {
      s.emit('hb-pong');
    });

    socketRef.current = s;
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

  if (checkingSession) {
    return (
      <div className="login-container">
        <div className="login-box">
          <div className="loading-spinner"></div>
          <p style={{ color: 'var(--text-secondary)', marginTop: 16 }}>Checking session...</p>
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <div className="login-container">
        <div className="login-box">
          <h2>Customer Support</h2>
          <p>Sign in with your phone number</p>

          <div className="auth-tabs">
            <button
              className={`auth-tab ${authMode === 'login' ? 'active' : ''}`}
              onClick={() => { setAuthMode('login'); setAuthError(''); }}
            >Login</button>
            <button
              className={`auth-tab ${authMode === 'register' ? 'active' : ''}`}
              onClick={() => { setAuthMode('register'); setAuthError(''); }}
            >Register</button>
          </div>

          {authError && <div className="auth-error">{authError}</div>}

          {authMode === 'login' ? (
            <form onSubmit={handleLogin}>
              <input
                type="tel"
                placeholder="Phone Number"
                value={formPhone}
                onChange={e => setFormPhone(e.target.value)}
                autoFocus
                required
              />
              <input
                type="password"
                placeholder="Password"
                value={formPassword}
                onChange={e => setFormPassword(e.target.value)}
                required
              />
              <button type="submit" disabled={authLoading}>
                {authLoading ? 'Signing in...' : 'Sign In'}
              </button>
            </form>
          ) : (
            <form onSubmit={handleRegister}>
              <input
                type="text"
                placeholder="Username"
                value={formUsername}
                onChange={e => setFormUsername(e.target.value)}
                autoFocus
                required
              />
              <input
                type="tel"
                placeholder="Phone Number"
                value={formPhone}
                onChange={e => setFormPhone(e.target.value)}
                required
              />
              <input
                type="password"
                placeholder="Password (min 6 chars)"
                value={formPassword}
                onChange={e => setFormPassword(e.target.value)}
                required
                minLength={6}
              />
              <input
                type="password"
                placeholder="Confirm Password"
                value={formConfirmPassword}
                onChange={e => setFormConfirmPassword(e.target.value)}
                required
              />
              <button type="submit" disabled={authLoading}>
                {authLoading ? 'Creating Account...' : 'Create Account'}
              </button>
            </form>
          )}
        </div>
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
              ? `Connected as ${session.username}`
              : 'Connecting...'}
        </div>
        <button className="logout-btn" onClick={handleLogout}>Logout</button>
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
