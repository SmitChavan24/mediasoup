import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'

const scriptTag = document.currentScript || document.querySelector('script[data-user-id]');
const userId = scriptTag?.getAttribute('data-user-id') || '';
const username = scriptTag?.getAttribute('data-username') || 'Customer';
const phone = scriptTag?.getAttribute('data-phone') || '';

let rootEl = document.getElementById('tglevels-call-widget');
if (!rootEl) {
  rootEl = document.createElement('div');
  rootEl.id = 'tglevels-call-widget';
  document.body.appendChild(rootEl);
}

ReactDOM.createRoot(rootEl).render(
  <React.StrictMode>
    <App userId={userId} username={username} phone={phone} />
  </React.StrictMode>,
)
