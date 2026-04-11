import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupMonitor } from './lib/mediasoupClient';

const SERVER_URL = '';
const STORAGE_KEY = 'voip_admin_session';

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
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

function fmtTime(dt) {
  if (!dt) return '—';
  const d = new Date(dt);
  return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
}

function StatusBadge({ status }) {
  const colors = {
    completed: { bg: 'rgba(16,185,129,0.2)', color: '#34d399', label: '✅ Completed' },
    missed:    { bg: 'rgba(251,191,36,0.2)', color: '#fbbf24', label: '📞 Missed' },
    rejected:  { bg: 'rgba(239,68,68,0.2)',  color: '#f87171', label: '❌ Rejected' },
  };
  const c = colors[status] || colors.missed;
  return (
    <span style={{
      background: c.bg, color: c.color,
      padding: '4px 12px', borderRadius: '20px', fontSize: '12px', fontWeight: 600,
      whiteSpace: 'nowrap',
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
  const [activeCalls, setActiveCalls] = useState([]);
  const [monitoredCall, setMonitoredCall] = useState(null);

  // View: 'live' or 'history'
  const [currentView, setCurrentView] = useState('live');

  // Call history state
  const [history, setHistory] = useState([]);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyTotalPages, setHistoryTotalPages] = useState(1);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [filterStatus, setFilterStatus] = useState('');
  const [filterSearch, setFilterSearch] = useState('');
  const [filterFrom, setFilterFrom] = useState('');
  const [filterTo, setFilterTo] = useState('');

  const monitorRef = useRef(null);
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
          if (data.valid && data.user.role === 'admin') {
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

  // Fetch history when switching to history tab or changing filters/page
  useEffect(() => {
    if (currentView === 'history' && session) {
      fetchHistory();
    }
  }, [currentView, historyPage, filterStatus, filterFrom, filterTo]);

  // Handle page visibility to prevent mobile heartbeat timeouts
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'hidden') {
        // Disconnect gracefully if not monitoring a call
        if (!monitoredCall && socketRef.current) {
          console.log('[admin] App backgrounded - gracefully disconnecting socket');
          socketRef.current.disconnect();
        }
      } else if (document.visibilityState === 'visible') {
        // Reconnect when returning to foreground
        if (socketRef.current && socketRef.current.disconnected && session) {
          console.log('[admin] App foregrounded - reconnecting socket');
          socketRef.current.connect();
        }
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, [monitoredCall, session]);

  const fetchHistory = async (searchOverride) => {
    if (!session) return;
    setHistoryLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('page', historyPage);
      params.set('limit', 15);
      if (filterStatus) params.set('status', filterStatus);
      if (filterFrom) params.set('from', filterFrom);
      if (filterTo) params.set('to', filterTo);
      const s = searchOverride !== undefined ? searchOverride : filterSearch;
      if (s) params.set('search', s);

      const res = await fetch(`${SERVER_URL}/api/call-history?${params}`, {
        headers: { Authorization: `Bearer ${session.token}` },
      });
      const data = await res.json();
      if (res.ok) {
        setHistory(data.rows || []);
        setHistoryTotal(data.total || 0);
        setHistoryTotalPages(data.totalPages || 1);
      }
    } catch (err) {
      console.error('[admin] Failed to fetch call history:', err);
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
          role: 'admin',
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
      if (data.role !== 'admin') throw new Error('This login is for admins only.');
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
    setActiveCalls([]);
    handleMonitorCleanup();
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
      setConnected(true);
      refreshCalls(s);
    });

    s.on('connect_error', (err) => {
      console.error('[admin] Socket connect error:', err.message);
      if (err.message.includes('expired') || err.message.includes('Invalid')) {
        handleLogout();
      }
    });

    s.on('disconnect', () => {
      setConnected(false);
    });

    s.on('presenceUpdate', () => {
      refreshCalls(s);
    });

    s.on('callsUpdated', () => {
      refreshCalls(s);
    });

    s.on('callEnded', () => {
      handleMonitorCleanup();
    });

    s.on('hb-ping', () => {
      s.emit('hb-pong');
    });

    socketRef.current = s;
    setSocket(s);
  };

  const refreshCalls = (s) => {
    s.emit('getLiveCalls', (calls) => {
      setActiveCalls(calls);
    });
  };

  const handleMonitorCleanup = () => {
    if (monitorRef.current) {
      monitorRef.current.close();
      monitorRef.current = null;
    }
    socket?.emit('stopMonitoring');
    setMonitoredCall(null);
  };

  const startMonitoring = (call) => {
    socket.emit('monitorCall', { callId: call.id }, async (res) => {
      if (res.error) return alert(res.error);

      setMonitoredCall({ ...call, isWhispering: false });

      try {
        const monitor = await setupMonitor(socket, call.id, {
          agentProducerId: res.agentProducerId,
          customerProducerId: res.customerProducerId
        });
        monitorRef.current = monitor;
      } catch (err) {
        alert('Could not monitor call.');
        handleMonitorCleanup();
      }
    });
  };

  const toggleWhisper = async () => {
    if (!monitorRef.current) return;

    if (monitoredCall.isWhispering) {
      monitorRef.current.stopWhispering();
      setMonitoredCall({ ...monitoredCall, isWhispering: false });
    } else {
      try {
        await monitorRef.current.startWhispering();
        setMonitoredCall({ ...monitoredCall, isWhispering: true });
      } catch (err) {
        alert('Could not start whispering.');
      }
    }
  };

  const stopMonitoring = () => {
    handleMonitorCleanup();
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
          <h2>Admin Dashboard</h2>
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

  /* ══════════════════════════════════════════════════════════════════════
   * MAIN DASHBOARD — single-column, full-width, clean layout
   * ══════════════════════════════════════════════════════════════════════ */
  return (
    <div className="admin-dashboard">
      {/* ── Top Bar ──────────────────────────────────────────────────────── */}
      <header className="admin-topbar">
        <div className="admin-topbar-left">
          <h1 className="admin-title">Admin Dashboard</h1>
          <div className="admin-status">
            <span className={`status-dot ${connected ? 'online' : ''}`}></span>
            {connected ? session.username : 'Connecting...'}
          </div>
        </div>
        <button className="logout-btn" onClick={handleLogout}>Logout</button>
      </header>

      {/* ── Nav Tabs ─────────────────────────────────────────────────────── */}
      <nav className="admin-nav">
        <button className={`admin-nav-btn ${currentView === 'live' ? 'active' : ''}`} onClick={() => setCurrentView('live')}>
          📞 Live Calls {activeCalls.length > 0 && <span className="badge">{activeCalls.length}</span>}
        </button>
        <button className={`admin-nav-btn ${currentView === 'history' ? 'active' : ''}`} onClick={() => setCurrentView('history')}>
          📋 Call History
        </button>
      </nav>

      {/* ── Monitoring Banner ────────────────────────────────────────────── */}
      {monitoredCall && (
        <div className="monitor-banner">
          <div className="monitor-info">
            <span className="monitor-label">🎧 Monitoring</span>
            <span className="monitor-names">{monitoredCall.agent} ↔ {monitoredCall.customer}</span>
            <span className="monitor-state">
              {monitoredCall.isWhispering ? '🎤 Whispering to Agent' : '👂 Listening'}
            </span>
          </div>
          <div className="monitor-actions">
            <button
              className={monitoredCall.isWhispering ? 'btn-danger-sm' : 'btn-success-sm'}
              onClick={toggleWhisper}
            >
              {monitoredCall.isWhispering ? 'Stop Whisper' : 'Whisper'}
            </button>
            <button className="btn-secondary-sm" onClick={stopMonitoring}>Stop</button>
          </div>
        </div>
      )}

      {/* ── Content ──────────────────────────────────────────────────────── */}
      <main className="admin-content">

        {/* ── LIVE CALLS VIEW ────────────────────────────────────────── */}
        {currentView === 'live' && (
          <>
            {activeCalls.length === 0 ? (
              <div className="empty-state">
                <div className="empty-icon">📞</div>
                <h2>No Active Calls</h2>
                <p>When agents and customers start calling, active calls will appear here.</p>
              </div>
            ) : (
              <div className="live-calls-grid">
                {activeCalls.map(call => (
                  <div
                    key={call.id}
                    className={`live-call-card ${monitoredCall?.id === call.id ? 'monitoring' : ''} ${!call.accepted ? 'pending' : ''}`}
                    onClick={() => call.accepted && monitoredCall?.id !== call.id && startMonitoring(call)}
                  >
                    <div className="live-call-header">
                      {call.accepted ? (
                        <>
                          <span className="live-dot connected-dot"></span>
                          <span className="live-label connected-label">CONNECTED</span>
                        </>
                      ) : (
                        <>
                          <span className="live-dot pending-dot"></span>
                          <span className="live-label pending-label">PENDING</span>
                        </>
                      )}
                    </div>
                    <div className="live-call-parties">
                      <div className="live-party">
                        <span className="party-role agent-role">Agent</span>
                        <span className="party-name">{call.agent || '—'}</span>
                      </div>
                      <span className="call-arrow">↔</span>
                      <div className="live-party">
                        <span className="party-role customer-role">Customer</span>
                        <span className="party-name">{call.customer || '—'}</span>
                      </div>
                    </div>
                    {monitoredCall?.id === call.id && (
                      <div className="live-monitoring-badge">● Monitoring</div>
                    )}
                    {monitoredCall?.id !== call.id && call.accepted && (
                      <div className="live-click-hint">Click to monitor</div>
                    )}
                    {!call.accepted && (
                      <div className="live-pending-hint">⏳ Ringing… waiting for answer</div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </>
        )}

        {/* ── CALL HISTORY VIEW ──────────────────────────────────────── */}
        {currentView === 'history' && (
          <>
            <div className="history-header">
              <h2 className="section-title">
                Call History
                {historyTotal > 0 && <span className="total-badge">{historyTotal} total</span>}
              </h2>
            </div>

            {/* ── Filter Bar ────────────────────────────────────────── */}
            <div className="filter-row">
              <div className="filter-group">
                <label>Search</label>
                <input
                  type="text"
                  placeholder="Search by name..."
                  value={filterSearch}
                  onChange={e => setFilterSearch(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') { setHistoryPage(1); fetchHistory(); } }}
                  className="filter-input"
                />
              </div>
              <div className="filter-group">
                <label>Status</label>
                <select value={filterStatus} onChange={e => { setFilterStatus(e.target.value); setHistoryPage(1); }} className="filter-select">
                  <option value="">All</option>
                  <option value="completed">Completed</option>
                  <option value="missed">Missed</option>
                  <option value="rejected">Rejected</option>
                </select>
              </div>
              <div className="filter-group">
                <label>From</label>
                <input type="date" value={filterFrom} onChange={e => { setFilterFrom(e.target.value); setHistoryPage(1); }} className="filter-input" />
              </div>
              <div className="filter-group">
                <label>To</label>
                <input type="date" value={filterTo} onChange={e => { setFilterTo(e.target.value); setHistoryPage(1); }} className="filter-input" />
              </div>
              <div className="filter-group" style={{ alignSelf: 'flex-end' }}>
                <button className="btn-primary-sm" onClick={() => { setHistoryPage(1); fetchHistory(); }}>Search</button>
              </div>
            </div>

            {/* ── History Cards (mobile-friendly) ───────────────────── */}
            {historyLoading ? (
              <div className="empty-state"><div className="loading-spinner"></div></div>
            ) : history.length === 0 ? (
              <div className="empty-state">
                <div className="empty-icon">📋</div>
                <h2>No Records Found</h2>
                <p>Try adjusting your filters or make some calls first.</p>
              </div>
            ) : (
              <div className="history-cards">
                {history.map(row => (
                  <div key={row.id} className="history-card">
                    <div className="hc-row hc-row-top">
                      <div className="hc-direction">
                        <div className="hc-person">
                          <span className="hc-label">Called by</span>
                          <span className="hc-name">{row.caller_name || '—'}</span>
                          {row.caller_role && <span className={`role-tag ${row.caller_role}`}>{row.caller_role}</span>}
                        </div>
                        <span className="hc-arrow-big">→</span>
                        <div className="hc-person">
                          <span className="hc-label">Received by</span>
                          <span className="hc-name">{row.callee_name || '—'}</span>
                          {row.callee_role && <span className={`role-tag ${row.callee_role}`}>{row.callee_role}</span>}
                        </div>
                      </div>
                      <StatusBadge status={row.status} />
                    </div>
                    <div className="hc-row hc-row-bottom">
                      <span className="hc-detail">📅 {fmtDateTime(row.started_at)}</span>
                      <span className="hc-detail">⏱️ {fmtDuration(row.duration_sec)}</span>
                      {row.ended_at && <span className="hc-detail">🏁 Ended {fmtTime(row.ended_at)}</span>}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* ── Pagination ──────────────────────────────────────── */}
            {historyTotalPages > 1 && (
              <div className="pagination">
                <button disabled={historyPage <= 1} onClick={() => setHistoryPage(p => p - 1)}>← Prev</button>
                <span className="page-info">Page {historyPage} of {historyTotalPages}</span>
                <button disabled={historyPage >= historyTotalPages} onClick={() => setHistoryPage(p => p + 1)}>Next →</button>
              </div>
            )}
          </>
        )}
      </main>
    </div>
  );
}

export default App;
