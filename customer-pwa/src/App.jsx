import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupCall, getCurrentCallId, clearCurrentCallId, getPreviousSocketId } from './lib/mediasoupClient';
import { ringtone } from './lib/ringtone';

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

/* ── Helpers ─────────────────────────────────────────────────────────────── */
function fmtDuration(sec) {
  if (!sec || sec <= 0) return '—';
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

function fmtDateTime(dt) {
  if (!dt) return '—';
  const d = new Date(dt);
  return d.toLocaleString(undefined, {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

function StatusBadge({ status }) {
  const colors = {
    completed: { bg: 'rgba(16,185,129,0.15)', color: '#34d399', label: 'Completed' },
    missed:    { bg: 'rgba(251,191,36,0.15)', color: '#fbbf24', label: 'Missed' },
    rejected:  { bg: 'rgba(239,68,68,0.15)',  color: '#f87171', label: 'Rejected' },
  };
  const c = colors[status] || colors.missed;
  return (
    <span style={{
      background: c.bg, color: c.color,
      padding: '2px 8px', borderRadius: '12px', fontSize: '11px', fontWeight: 600,
    }}>{c.label}</span>
  );
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

  // Call history state
  const [showHistory, setShowHistory] = useState(false);
  const [callHistory, setCallHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyTotalPages, setHistoryTotalPages] = useState(1);

  // Call timer
  const [callElapsed, setCallElapsed] = useState(0);
  const callStartTimeRef = useRef(null);

  const callRef = useRef(null);
  const previousSocketId = useRef(null);
  const socketRef = useRef(null);

  // Incoming call ringtone
  useEffect(() => {
    if (incomingCall) {
      ringtone.start();
    } else {
      ringtone.stop();
    }
    return () => ringtone.stop();
  }, [incomingCall]);

  // Live call timer
  useEffect(() => {
    if (activeCall && activeCall.state === 'Connected') {
      if (!callStartTimeRef.current) callStartTimeRef.current = Date.now();
      const interval = setInterval(() => {
        setCallElapsed(Math.floor((Date.now() - callStartTimeRef.current) / 1000));
      }, 1000);
      return () => clearInterval(interval);
    } else if (!activeCall) {
      callStartTimeRef.current = null;
      setCallElapsed(0);
    }
  }, [activeCall?.state, activeCall]);

  const fmtTimer = (sec) => {
    const m = String(Math.floor(sec / 60)).padStart(2, '0');
    const s = String(sec % 60).padStart(2, '0');
    return `${m}:${s}`;
  };

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

  useEffect(() => {
    // After logging in and connecting successfully, register for Push Notifications
    if (session && connected && 'serviceWorker' in navigator && 'PushManager' in window) {
      registerPushProtocol();
    }
  }, [session, connected]);

  async function registerPushProtocol() {
    try {
      if (Notification.permission === 'default') {
        const perm = await Notification.requestPermission();
        if (perm !== 'granted') return;
      } else if (Notification.permission === 'denied') {
        return;
      }

      const registration = await navigator.serviceWorker.ready;
      let subscription = await registration.pushManager.getSubscription();

      if (!subscription) {
        const vapidRes = await fetch(`${SERVER_URL}/api/vapid-public-key`);
        const { publicKey } = await vapidRes.json();
        const convertedVapidKey = urlBase64ToUint8Array(publicKey);

        subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: convertedVapidKey
        });
      }

      await fetch(`${SERVER_URL}/api/push-subscribe`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${session.token}`
        },
        body: JSON.stringify(subscription)
      });
      console.log('[push] ✅ Registered Push Subscription on server');
    } catch (err) {
      console.error('[push] ❌ Push setup failed:', err);
    }
  }

  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding).replace(/\-/g, '+').replace(/_/g, '/');
    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    for (let i = 0; i < rawData.length; ++i) {
      outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
  }

  // Fetch call history when toggled
  useEffect(() => {
    if (showHistory && session) fetchHistory();
  }, [showHistory, historyPage]);

  // Handle page visibility to prevent mobile heartbeat timeouts
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'hidden') {
        // Disconnect gracefully if not in a call
        if (!activeCall && !incomingCall && socketRef.current) {
          console.log('[customer] App backgrounded - gracefully disconnecting socket');
          socketRef.current.disconnect();
        }
      } else if (document.visibilityState === 'visible') {
        // Reconnect when returning to foreground
        if (socketRef.current && socketRef.current.disconnected && session) {
          console.log('[customer] App foregrounded - reconnecting socket');
          socketRef.current.connect();
        }
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, [activeCall, incomingCall, session]);

  const fetchHistory = async () => {
    if (!session) return;
    setHistoryLoading(true);
    try {
      const params = new URLSearchParams({ page: historyPage, limit: 10 });
      const res = await fetch(`${SERVER_URL}/api/my-calls?${params}`, {
        headers: { Authorization: `Bearer ${session.token}` },
      });
      const data = await res.json();
      if (res.ok) {
        setCallHistory(data.rows || []);
        setHistoryTotalPages(data.totalPages || 1);
      }
    } catch (err) {
      console.error('[customer] Failed to fetch call history:', err);
    } finally {
      setHistoryLoading(false);
    }
  };

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

    s.on('callRejected', ({ rejectedBy }) => {
      console.log(`[customer] Call rejected by ${rejectedBy}`);
      // Remove the pending callAccepted listener since the call was rejected
      s.off('callAccepted');
      // Briefly show "Rejected" on the call screen before returning to main
      setActiveCall(prev => prev ? { ...prev, state: 'Call Rejected' } : null);
      setTimeout(() => {
        handleCallCleanup();
      }, 2000);
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
      if (res.error) {
        if (res.busy) {
          // Show call screen with busy state, then return to main after 3s
          setActiveCall({ withUser: agentName, state: 'On Another Call' });
          setTimeout(() => handleCallCleanup(), 3000);
          return;
        }
        return alert(res.error);
      }
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

  const declineIncomingCall = () => {
    if (!incomingCall) return;
    socket.emit('rejectCall', { callId: incomingCall.callId });
    setIncomingCall(null);
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
    <div className="app-shell">
      {/* ── Top Bar ────────────────────────────────────────────────────── */}
      <header className="topbar">
        <div className="topbar-left">
          <span className="topbar-title">Customer Support</span>
          <div className="topbar-status">
            <span className={`status-dot ${connected ? 'online' : ''}`}></span>
            {reconnecting ? 'Reconnecting...' : connected ? session.username : 'Connecting...'}
          </div>
        </div>
        <button className="logout-btn" onClick={handleLogout}>Logout</button>
      </header>

      {!activeCall ? (
        <>
          {/* ── Incoming Call Banner ────────────────────────────────── */}
          {incomingCall && (
            <div style={{ padding: '24px 24px 0' }}>
              <div className="incoming-banner">
                <div className="incoming-header">
                  <span className="incoming-label">📞 Incoming Call</span>
                </div>
                <div className="incoming-from">{incomingCall.from}</div>
                <div className="incoming-actions">
                  <button className="btn-accept" onClick={acceptIncomingCall}>Accept</button>
                  <button className="btn-decline" onClick={declineIncomingCall}>Decline</button>
                </div>
              </div>
            </div>
          )}

          {!incomingCall && (
            <>
              {/* ── Call Any Agent ──────────────────────────────────── */}
              <div style={{ padding: '20px 24px 0' }}>
                <button className="call-any-btn" onClick={callAnyAgent}>
                  📞 Call First Available Agent
                </button>
                <p className="call-any-hint">or pick a specific agent below</p>
              </div>

              {/* ── Nav Tabs ────────────────────────────────────────── */}
              <nav className="nav-tabs">
                <button className={`nav-tab ${!showHistory ? 'active' : ''}`} onClick={() => setShowHistory(false)}>
                  🧑‍💼 Agents ({activeAgents.length})
                </button>
                <button className={`nav-tab ${showHistory ? 'active' : ''}`} onClick={() => setShowHistory(true)}>
                  📋 Call History
                </button>
              </nav>

              <div className="main-content">
                {!showHistory ? (
                  <>
                    {activeAgents.length === 0 ? (
                      <div className="empty-state">
                        <div className="empty-icon">🧑‍💼</div>
                        <p>No agents are currently online.</p>
                      </div>
                    ) : (
                      <ul className="user-list">
                        {activeAgents.map(agent => (
                          <li key={agent.id} className="user-card">
                            <div className="user-info">
                              <span className="user-name">{agent.username}</span>
                              <span className="user-role-tag">Support Agent</span>
                            </div>
                            <button className="call-btn" onClick={() => callSpecificAgent(agent.id, agent.username)}>
                              📞 Call
                            </button>
                          </li>
                        ))}
                      </ul>
                    )}
                  </>
                ) : (
                  <>
                    <div className="section-title">My Call History</div>
                    {historyLoading ? (
                      <div className="empty-state"><div className="loading-spinner"></div></div>
                    ) : callHistory.length === 0 ? (
                      <div className="empty-state">
                        <div className="empty-icon">📋</div>
                        <p>No calls yet.</p>
                      </div>
                    ) : (
                      <ul className="history-list">
                        {callHistory.map(row => (
                          <li key={row.id} className="history-item">
                            <div className="history-item-main">
                              <span className="history-peer">{row.caller_name === session.username ? (row.callee_name || '—') : (row.caller_name || '—')}</span>
                              <StatusBadge status={row.status} />
                            </div>
                            <div className="history-item-meta">
                              <span>📅 {fmtDateTime(row.started_at)}</span>
                              <span>⏱️ {fmtDuration(row.duration_sec)}</span>
                            </div>
                          </li>
                        ))}
                      </ul>
                    )}
                    {historyTotalPages > 1 && (
                      <div className="pagination">
                        <button disabled={historyPage <= 1} onClick={() => setHistoryPage(p => p - 1)}>← Prev</button>
                        <span className="page-info">Page {historyPage}/{historyTotalPages}</span>
                        <button disabled={historyPage >= historyTotalPages} onClick={() => setHistoryPage(p => p + 1)}>Next →</button>
                      </div>
                    )}
                  </>
                )}
              </div>
            </>
          )}
        </>
      ) : (
        /* ── Active Call Screen ──────────────────────────────────────── */
        <div className="call-screen">
          <div className="call-avatar">CS</div>
          <div className="call-with-name">{activeCall.withUser}</div>
          <div className={`call-state ${activeCall.state !== 'Connected' ? 'ringing' : ''}`}>
            {activeCall.state}
          </div>
          {peerDisconnected && (
            <div className="call-warning">
              ⚠️ {peerDisconnected.username} disconnected — waiting {peerDisconnected.gracePeriod}s...
            </div>
          )}
          <div className="call-timer">{fmtTimer(callElapsed)}</div>
          <button className="hangup-btn" onClick={endCall}>End Call</button>
        </div>
      )}
    </div>
  );
}

export default App;
