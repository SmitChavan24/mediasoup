import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupMonitor } from './lib/mediasoupClient';

const SERVER_URL = 'http://localhost:3000'; // Update with your server URL

function App() {
  const [username, setUsername] = useState('');
  const [loggedIn, setLoggedIn] = useState(false);

  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [activeCalls, setActiveCalls] = useState([]);
  const [monitoredCall, setMonitoredCall] = useState(null);

  const monitorRef = useRef(null);

  const handleLogin = (e) => {
    e.preventDefault();
    if (username.trim()) {
      setLoggedIn(true);
      connectSocket();
    }
  };

  const connectSocket = () => {
    const s = io(SERVER_URL, {
      query: { role: 'admin', username: username.trim() },
      transports: ['websocket', 'polling']
    });

    s.on('connect', () => {
      setConnected(true);
      refreshCalls(s);
    });

    s.on('disconnect', () => {
      setConnected(false);
    });

    s.on('presenceUpdate', () => {
      refreshCalls(s);
    });

    s.on('callEnded', () => {
      handleMonitorCleanup();
    });

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

  if (!loggedIn) {
    return (
      <div className="login-container">
        <form onSubmit={handleLogin} className="login-box">
          <h2>Admin Login</h2>
          <p>Enter your name to monitor calls</p>
          <input
            type="text"
            placeholder="Admin Name"
            value={username}
            onChange={e => setUsername(e.target.value)}
            autoFocus
            required
          />
          <button type="submit">Enter Dashboard</button>
        </form>
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
        <div className="header" style={{ marginBottom: '40px', display: 'flex', justifyContent: 'space-between' }}>
          <h1>Admin Dashboard</h1>
          <div className="status">
            <div className={`status-indicator ${connected ? 'connected' : ''}`}></div>
            {connected ? `Connected as ${username}` : 'Connecting...'}
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
