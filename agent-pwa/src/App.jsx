import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupCall, getCurrentCallId, clearCurrentCallId, getPreviousSocketId } from './lib/mediasoupClient';
import { ringtone } from './lib/ringtone';

// Env-driven (Vite, baked at build) so a test build can target a staging voip
// domain. Default = production. Set VITE_SERVER_URL to override at build time.
const SERVER_URL = import.meta.env.VITE_SERVER_URL || 'https://voip.tglevels.in';

const STORAGE_KEY = 'voip_agent_session';

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
    missed: { bg: 'rgba(251,191,36,0.15)', color: '#fbbf24', label: 'Missed' },
    rejected: { bg: 'rgba(239,68,68,0.15)', color: '#f87171', label: 'Rejected' },
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
  const [authMode, setAuthMode] = useState('login'); // 'login' | 'register'
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState('');
  const [session, setSession] = useState(null); // { token, username, phone, role }
  const [checkingSession, setCheckingSession] = useState(true);

  // Form fields
  const [formUsername, setFormUsername] = useState('');
  const [formPhone, setFormPhone] = useState('');
  const [formPassword, setFormPassword] = useState('');
  const [formConfirmPassword, setFormConfirmPassword] = useState('');

  // App state
  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [incomingCall, setIncomingCall] = useState(null);
  const [activeCall, setActiveCall] = useState(null);
  const [reconnecting, setReconnecting] = useState(false);
  const [peerDisconnected, setPeerDisconnected] = useState(null);
  const [activeCustomers, setActiveCustomers] = useState([]);
  const [callbacks, setCallbacks] = useState([]); // pending/claimed callback requests


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
  const dialingRef = useRef(false);
  const activeCallbackIdRef = useRef(null); // callback being handled by the current call

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

  // On mount: check for existing session in localStorage
  useEffect(() => {
    const stored = getStoredSession();
    if (stored && stored.token) {
      // Verify the token is still valid
      fetch(`${SERVER_URL}/api/verify-token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: stored.token }),
      })
        .then(r => r.json())
        .then(data => {
          if (data.valid && data.user.role === 'agent') {
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

  // When session is set, connect socket
  useEffect(() => {
    if (session && !socketRef.current) {
      connectSocket(session);
    }
  }, [session]);

  // Fetch call history when toggled
  useEffect(() => {
    if (showHistory && session) fetchHistory();
  }, [showHistory, historyPage]);



  // Handle page visibility to prevent mobile heartbeat timeouts
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'hidden') {
        // Disconnect gracefully if not in a call
        // if (!activeCall && !incomingCall && socketRef.current) {
        //   console.log('[agent] App backgrounded - gracefully disconnecting socket');
        //   socketRef.current.disconnect();
        // }
      } else if (document.visibilityState === 'visible') {
        // Reconnect when returning to foreground
        // if (socketRef.current && socketRef.current.disconnected && session) {
        //   console.log('[agent] App foregrounded - reconnecting socket');
        //   socketRef.current.connect();
        // }
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
      console.error('[agent] Failed to fetch call history:', err);
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
          role: 'agent',
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
      if (data.role !== 'agent') throw new Error('This login is for agents only.');
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
    setActiveCustomers([]);
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
      console.log(`[agent] Connected with socket.id=${s.id}`);

      // Check for active call to reconnect (works for both network drops and tab close)
      const prevSocketId = previousSocketId.current || getPreviousSocketId();
      const prevCallId = getCurrentCallId();

      if (prevSocketId && prevCallId) {
        console.log(`[agent] Attempting session reconnection: prev=${prevSocketId} callId=${prevCallId}`);

        s.emit('reconnectSession', {
          previousSocketId: prevSocketId,
          callId: prevCallId,
        }, async (res) => {
          if (res.success) {
            console.log('[agent] ✅ Session restored!');
            setReconnecting(false);
            setPeerDisconnected(null);
            setActiveCall({ callId: res.callId, withUser: res.username || 'Peer', state: 'Connected' });
            try {
              const callTransports = await setupCall(s, res.callId, () => {
                console.log('Remote audio playing (reconnected)');
              }, session?.token);
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
      s.emit('getPresence');

      // Load the callback queue on every (re)connect so a closed laptop never
      // loses requests that arrived while it was away.
      fetch(`${SERVER_URL}/api/callbacks`, { headers: { Authorization: `Bearer ${session?.token}` } })
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => { if (d?.requests) setCallbacks(d.requests); })
        .catch(() => {});
    });

    s.on('connect_error', (err) => {
      console.error('[agent] Socket connect error:', err.message);
      if (err.message.includes('expired') || err.message.includes('Invalid')) {
        handleLogout();
      }
    });

    s.on('disconnect', () => {
      setConnected(false);
      if (activeCall || getCurrentCallId()) {
        setReconnecting(true);
        console.log('[agent] Socket disconnected during active call — will attempt reconnection');
      }
    });

    s.on('presenceUpdate', ({ customers }) => {
      setActiveCustomers(customers);
    });

    // A customer just asked for a callback — surfaces here and on admin at once.
    s.on('callbackRequested', (row) => {
      setCallbacks((prev) => (prev.some((c) => c.id === row.id) ? prev : [{ ...row, online: true }, ...prev]));
    });
    // Claimed / released / closed by any agent — keep every panel in sync.
    s.on('callbacksUpdated', (row) => {
      setCallbacks((prev) =>
        ['done', 'cancelled'].includes(row.status)
          ? prev.filter((c) => c.id !== row.id)
          : prev.map((c) => (c.id === row.id ? { ...c, ...row } : c))
      );
    });

    s.on('incomingCall', ({ callId, from, role }) => {
      setIncomingCall({ callId, from, role });
    });

    // Round-robin moved this call on to another agent — drop our popup.
    s.on('incomingCallCancelled', ({ callId }) => {
      setIncomingCall((prev) => (prev && prev.callId === callId ? null : prev));
    });

    s.on('callStateUpdate', (newState) => {
      setActiveCall((prev) => prev ? { ...prev, state: newState } : null);
    });

    s.on('callEnded', () => {
      handleCallCleanup();
    });

    s.on('callRejected', ({ rejectedBy }) => {
      console.log(`[agent] Call rejected by ${rejectedBy}`);
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
    // The call for a callback just ended — close that request so it clears from
    // every panel automatically (no manual "Done").
    if (activeCallbackIdRef.current) {
      closeCallback(activeCallbackIdRef.current);
      activeCallbackIdRef.current = null;
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
        }, session?.token);
        callRef.current = callTransports;
      } catch (err) {
        console.error('[agent] setupCall failed:', err);
        alert(`Call setup failed: ${err.message}`);
        endCall();
      }
    });
  };

  const declineCall = () => {
    if (!incomingCall) return;
    // Notify server this call was rejected
    socket.emit('rejectCall', { callId: incomingCall.callId });
    setIncomingCall(null);
  };

  // Shared dial routine. `payload` is either { targetId } (a live socket, from
  // the Customers list) or { targetUserId } (a user id, from a callback request
  // — the server resolves it to their live socket, or pushes if offline).
  const beginDial = (payload, customerName) => {
    if (dialingRef.current) return; // Prevent double-dial
    dialingRef.current = true;

    socket.emit('dialOut', payload, (res) => {
      dialingRef.current = false;
      if (res.error) {
        // Dial never connected — keep the callback in the queue (don't let the
        // cleanup below close it) so the agent can retry.
        activeCallbackIdRef.current = null;
        if (res.busy) {
          setActiveCall({ withUser: customerName, state: 'On Another Call' });
          setTimeout(() => handleCallCleanup(), 3000);
          return;
        }
        return alert(res.error);
      }

      const { callId } = res;
      setActiveCall({ callId, withUser: customerName, state: 'Calling...' });

      socket.once('callAccepted', async () => {
        setActiveCall({ callId, withUser: customerName, state: 'Connected' });
        try {
          // Pass the agent's token so the recording upload is authenticated —
          // /api/recordings is auth-protected, and without it the POST 401s and
          // the recording is silently lost (recording_path stays NULL).
          const callTransports = await setupCall(socket, callId, () => {
            console.log('Remote audio playing');
          }, session?.token);
          callRef.current = callTransports;
        } catch (err) {
          console.error('[agent] setupCall failed:', err);
          alert(`Call setup failed: ${err.message}`);
          endCall();
        }
      });
    });
  };

  const dialCustomer = (customerSocketId, customerName) => beginDial({ targetId: customerSocketId }, customerName);
  const dialByUserId = (userId, customerName) => beginDial({ targetUserId: userId }, customerName);

  // Mark a callback handled — removes it from every panel's queue.
  const closeCallback = async (id) => {
    try {
      await fetch(`${SERVER_URL}/api/callbacks/${id}/close`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${session?.token}` },
        body: JSON.stringify({}),
      });
    } catch { /* the callbacksUpdated event will reconcile */ }
  };

  const endCall = () => {
    socket?.emit('hangup');
    handleCallCleanup();
  };

  // Loading state while checking stored session
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

  // Auth screen
  if (!session) {
    return (
      <div className="login-container">
        <div className="login-box">
          <h2>Agent Portal</h2>
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
          <span className="topbar-title">Agent Portal</span>
          <div className="topbar-status">
            <span className={`status-dot ${connected ? 'online' : ''}`}></span>
            {reconnecting ? 'Reconnecting...' : connected ? session.username : 'Connecting...'}
          </div>
        </div>
        <button className="logout-btn" onClick={handleLogout}>Logout</button>
      </header>

      {!activeCall ? (
        <>
          {/* ── Nav Tabs ──────────────────────────────────────────────── */}
          <nav className="nav-tabs">
            <button className={`nav-tab ${!showHistory ? 'active' : ''}`} onClick={() => setShowHistory(false)}>
              📞 Callbacks ({callbacks.length})
            </button>
            <button className={`nav-tab ${showHistory ? 'active' : ''}`} onClick={() => setShowHistory(true)}>
              📋 Call History
            </button>
          </nav>

          <div className="main-content">
            {/* ── Incoming Call Banner ──────────────────────────────── */}
            {incomingCall && (
              <div className="incoming-banner">
                <div className="incoming-header">
                  <span className="incoming-label">📞 Incoming Call</span>
                </div>
                <div className="incoming-from">{incomingCall.from}</div>
                <div className="incoming-actions">
                  <button className="btn-accept" onClick={acceptCall}>Accept</button>
                  <button className="btn-decline" onClick={declineCall}>Decline</button>
                </div>
              </div>
            )}

            {/* ── Customers Tab ─────────────────────────────────────── */}
            {!showHistory ? (
              <>
                {/* Callback requests only — nothing else (no online-users list). */}
                {callbacks.length === 0 ? (
                  <div className="empty-state">
                    <div className="empty-icon">📞</div>
                    <p>No callback requests.</p>
                  </div>
                ) : incomingCall ? null : (
                  <ul className="user-list">
                    {callbacks.map((cb) => {
                      const name = cb.name && cb.name !== 'USER' && !/^pwaguest/i.test(cb.name)
                        ? cb.name : (cb.phone || 'Customer');
                      const isOnline = !!cb.online;
                      return (
                        <li key={cb.id} className="user-card" style={{ borderLeft: '3px solid #1E9B22' }}>
                          <div className="user-info" style={{ flexDirection: 'column', alignItems: 'flex-start', gap: '2px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                              <span className="user-name">{name}</span>
                              <span className={`status-dot ${isOnline ? 'online' : ''}`} title={isOnline ? 'Online' : 'Offline'}></span>
                              {cb.status === 'claimed' && <span className="user-role-tag" style={{ marginLeft: 0 }}>Claimed</span>}
                            </div>
                            <span style={{ fontSize: '11px', color: 'var(--text-secondary)', opacity: 0.8 }}>
                              {cb.phone} · {fmtDateTime(cb.requested_at)}
                            </span>
                          </div>
                          {/* No "Done" — the request auto-clears when the call ends. */}
                          <button
                            className="call-btn"
                            onClick={() => { activeCallbackIdRef.current = cb.id; dialByUserId(cb.pwa_user_id, name); }}
                          >
                            📞 Call
                          </button>
                        </li>
                      );
                    })}
                  </ul>
                )}
              </>
            ) : (
              /* ── History Tab ───────────────────────────────────────── */
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
      ) : (
        /* ── Active Call Screen ──────────────────────────────────────── */
        <div className="call-screen">
          <div className="call-avatar">{activeCall.withUser.charAt(0).toUpperCase()}</div>
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
