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

  return (
    <div className="app-container" style={{ display: 'flex', height: '100vh', padding: 0 }}>
      {/* Sidebar */}
      <div className="sidebar" style={{ width: '300px', borderRight: '1px solid #444', padding: '20px', background: '#1a1a1a' }}>
        <h3 style={{ color: 'white' }}>Live Calls ({activeCalls.length})</h3>
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {activeCalls.map(call => (
            <li key={call.id} style={{ 
              padding: '15px', 
              marginBottom: '10px', 
              background: monitoredCall?.id === call.id ? '#333' : '#222',
              borderRadius: '8px',
              cursor: 'pointer'
            }} onClick={() => monitoredCall?.id !== call.id && startMonitoring(call)}>
              <div style={{ color: '#eee', fontWeight: 'bold' }}>{call.agent} ↔ {call.customer}</div>
              <div style={{ color: '#888', fontSize: '12px' }}>ID: {call.id}</div>
              {monitoredCall?.id === call.id && (
                <div style={{ color: '#4caf50', fontSize: '12px', marginTop: '5px' }}>● Monitoring</div>
              )}
            </li>
          ))}
        </ul>
        {activeCalls.length === 0 && <p style={{ color: '#666' }}>No active calls.</p>}
      </div>

      {/* Main Panel */}
      <div className="main-panel" style={{ flex: 1, padding: '40px', background: '#121212' }}>
        <div className="header" style={{ marginBottom: '40px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h1>Admin Dashboard</h1>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <div className="status">
              <div className={`status-indicator ${connected ? 'connected' : ''}`}></div>
              {connected ? `Connected as ${session.username}` : 'Connecting...'}
            </div>
            <button className="logout-btn" onClick={handleLogout}>Logout</button>
          </div>
        </div>

        {monitoredCall ? (
          <div className="card" style={{ maxWidth: '600px', margin: '0 auto', textAlign: 'center' }}>
            <div className="card-title">Monitoring: {monitoredCall.agent} ↔ {monitoredCall.customer}</div>
            <div style={{ margin: '30px 0' }}>
              <div style={{ color: '#4caf50', fontSize: '1.2rem', marginBottom: '10px' }}>
                {monitoredCall.isWhispering ? 'Whispering to Agent...' : 'Listening to both parties...'}
              </div>
              <div style={{ color: '#aaa' }}>
                Admin speech is NOT heard by the Customer.
              </div>
            </div>
            
            <div style={{ display: 'flex', gap: '20px', justifyContent: 'center' }}>
              <button 
                className={monitoredCall.isWhispering ? 'danger' : 'success'} 
                onClick={toggleWhisper}
              >
                {monitoredCall.isWhispering ? 'Stop Whispering' : 'Speak to Agent'}
              </button>
              <button className="secondary" onClick={stopMonitoring}>Stop Monitoring</button>
            </div>
          </div>
        ) : (
          <div style={{ textAlign: 'center', marginTop: '100px', color: '#666' }}>
            <div style={{ fontSize: '48px', marginBottom: '20px' }}>🎧</div>
            <h2>Select a call from the sidebar to start monitoring</h2>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
