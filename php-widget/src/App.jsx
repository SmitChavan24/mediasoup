import React, { useEffect, useState, useRef } from 'react';
import { io } from 'socket.io-client';
import { setupCall, getCurrentCallId, clearCurrentCallId, getPreviousSocketId } from './lib/mediasoupClient';
import { ringtone } from './lib/ringtone';

const SERVER_URL = 'https://tglevels.me'; // Hardcoded for widget

export default function App({ userId, username, phone }) {
  const [session, setSession] = useState(null);
  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [activeCall, setActiveCall] = useState(null);
  const [incomingCall, setIncomingCall] = useState(null);
  const [reconnecting, setReconnecting] = useState(false);
  const [peerDisconnected, setPeerDisconnected] = useState(null);
  const [callbackRequested, setCallbackRequested] = useState(false);

  const [callElapsed, setCallElapsed] = useState(0);
  const callStartTimeRef = useRef(null);
  const callRef = useRef(null);
  const previousSocketId = useRef(null);
  const socketRef = useRef(null);

  // Incoming call ringtone
  useEffect(() => {
    if (incomingCall) ringtone.start();
    else ringtone.stop();
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

  // On mount: auto-login
  useEffect(() => {
    if (!userId) return; // Need at least userId to auto-login
    
    fetch(`${SERVER_URL}/api/guest-login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username || 'Customer', phone: phone || '', phpUserId: userId }),
    })
      .then(r => r.json())
      .then(data => {
        if (data.token) {
          setSession({ token: data.token, username: data.username, phone: data.phone, role: data.role });
        } else {
          console.error('[widget] Guest login error:', data.error);
        }
      })
      .catch(err => console.error('[widget] Auto-login failed', err));
  }, [userId, username, phone]);

  useEffect(() => {
    if (session && !socketRef.current) {
      connectSocket(session);
    }
  }, [session]);

  useEffect(() => {
    // Only register push notifications on production domain (not localhost)
    const isProduction = window.location.hostname !== 'localhost' && 
                         !window.location.hostname.startsWith('127.');
    if (session && connected && isProduction && 'serviceWorker' in navigator && 'PushManager' in window) {
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

      const registration = await navigator.serviceWorker.register('/sw.js');
      await navigator.serviceWorker.ready;
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

  const connectSocket = (sess) => {
    const s = io(SERVER_URL, {
      auth: { token: sess.token },
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: 10,
    });

    s.on('connect', () => {
      console.log(`[widget] Connected with socket.id=${s.id}`);

      const prevSocketId = previousSocketId.current || getPreviousSocketId();
      const prevCallId = getCurrentCallId();

      if (prevSocketId && prevCallId) {
        s.emit('reconnectSession', { previousSocketId: prevSocketId, callId: prevCallId }, async (res) => {
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
              console.error('[widget] Failed to restore media:', err);
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

    s.on('disconnect', () => {
      setConnected(false);
      if (activeCall || getCurrentCallId()) {
        setReconnecting(true);
      }
    });

    s.on('incomingCall', ({ callId, from, role }) => {
      setIncomingCall({ callId, from, role });
      setCallbackRequested(false); // Clear callback state when called
    });

    s.on('callEnded', () => {
      handleCallCleanup();
    });

    s.on('callStateUpdate', (newState) => {
      setActiveCall((prev) => prev ? { ...prev, state: newState } : null);
    });

    s.on('participantDisconnected', ({ username: peerName, gracePeriod }) => {
      setPeerDisconnected({ username: peerName, gracePeriod });
    });

    s.on('participantReconnected', () => {
      setPeerDisconnected(null);
    });

    s.on('hb-ping', () => s.emit('hb-pong'));

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

  const requestCallback = () => {
    if (!socket) return;
    socket.emit('requestCallback', (res) => {
      if (res && res.success) setCallbackRequested(true);
      else alert(res?.error || 'Failed to request callback');
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

  // If no session or user, render nothing
  if (!session) return null;

  return (
    <>
      {/* ── Floating Callback Button ──────────────────────────────────────── */}
      {!activeCall && !incomingCall && (
        <div style={{
          position: 'fixed',
          bottom: '80px',
          right: '20px',
          zIndex: 9999
        }}>
          <button 
            onClick={requestCallback}
            style={{
              padding: '14px 24px',
              borderRadius: '30px',
              border: 'none',
              background: callbackRequested ? '#10b981' : '#3b82f6',
              color: '#fff',
              fontSize: '16px',
              fontWeight: '600',
              boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
              cursor: callbackRequested ? 'default' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '8px'
            }}
            disabled={callbackRequested}
          >
            {callbackRequested ? '🛎️ Callback Requested' : '📞 Request Callback'}
          </button>
        </div>
      )}

      {/* ── Incoming Call Overlay ────────────────────────────────────────── */}
      {incomingCall && (
        <div style={{
          position: 'fixed',
          top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(0,0,0,0.8)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 10000
        }}>
          <div style={{
            background: '#fff',
            padding: '30px',
            borderRadius: '16px',
            textAlign: 'center',
            width: '320px',
            boxShadow: '0 10px 25px rgba(0,0,0,0.2)'
          }}>
            <h2 style={{ margin: '0 0 10px', fontSize: '20px', color: '#1f2937' }}>📞 Incoming Call</h2>
            <p style={{ margin: '0 0 24px', color: '#6b7280', fontSize: '16px' }}>{incomingCall.from} is calling you...</p>
            <div style={{ display: 'flex', gap: '12px' }}>
              <button 
                onClick={declineIncomingCall}
                style={{ flex: 1, padding: '12px', borderRadius: '8px', border: 'none', background: '#ef4444', color: '#fff', fontWeight: 'bold', cursor: 'pointer' }}
              >
                Decline
              </button>
              <button 
                onClick={acceptIncomingCall}
                style={{ flex: 1, padding: '12px', borderRadius: '8px', border: 'none', background: '#10b981', color: '#fff', fontWeight: 'bold', cursor: 'pointer' }}
              >
                Accept
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Active Call Overlay ──────────────────────────────────────────── */}
      {activeCall && (
        <div style={{
          position: 'fixed',
          bottom: '20px',
          right: '20px',
          background: '#fff',
          padding: '20px',
          borderRadius: '16px',
          width: '280px',
          boxShadow: '0 10px 25px rgba(0,0,0,0.15)',
          zIndex: 10000,
          textAlign: 'center'
        }}>
          <div style={{ 
            width: '60px', height: '60px', borderRadius: '30px', 
            background: '#3b82f6', color: '#fff', 
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: '24px', margin: '0 auto 12px' 
          }}>
            {activeCall.withUser.charAt(0).toUpperCase()}
          </div>
          <h3 style={{ margin: '0 0 4px', fontSize: '18px', color: '#1f2937' }}>{activeCall.withUser}</h3>
          <p style={{ margin: '0 0 16px', color: '#6b7280', fontSize: '14px' }}>
            {activeCall.state === 'Connected' ? fmtTimer(callElapsed) : activeCall.state}
          </p>
          {peerDisconnected && (
            <p style={{ color: '#ef4444', fontSize: '12px', marginBottom: '12px' }}>
              Reconnecting...
            </p>
          )}
          <button 
            onClick={endCall}
            style={{ 
              width: '100%', padding: '12px', borderRadius: '8px', 
              border: 'none', background: '#ef4444', color: '#fff', 
              fontWeight: 'bold', cursor: 'pointer' 
            }}
          >
            End Call
          </button>
        </div>
      )}
    </>
  );
}
