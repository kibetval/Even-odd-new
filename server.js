require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Serve files from repo root so index.html (at repo root) is served
app.use(express.static(__dirname));
app.use(express.json());

const PORT = process.env.PORT || 3000;
const DERIV_API_TOKEN = process.env.DERIV_API_TOKEN || '';
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089';

let DEFAULT_SYMBOL = 'R_10';

function createDerivConnection(clientSocket) {
  const wsUrl = `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`;
  console.log('[server] connecting to Deriv:', wsUrl);

  const derivWs = new WebSocket(wsUrl);
  let activeSubscriptionId = null;
  let pingInterval = null;
  let reconnectTimer = null;

  function startPing() {
    if (pingInterval) clearInterval(pingInterval);
    pingInterval = setInterval(() => {
      if (derivWs.readyState === WebSocket.OPEN) {
        try { derivWs.send(JSON.stringify({ ping: 1 })); } catch (e) { }
      }
    }, 30000);
  }

  derivWs.on('open', () => {
    console.log('[server] Deriv WS open');
    // authorize if token provided
    if (DERIV_API_TOKEN) {
      derivWs.send(JSON.stringify({ authorize: DERIV_API_TOKEN }));
    }
    startPing();
  });

  derivWs.on('message', (raw) => {
    let msg = null;
    try {
      msg = JSON.parse(raw);
    } catch (err) {
      console.warn('[server] non-json message from deriv:', raw.toString().slice(0,200));
      return;
    }

    // track active subscription id when provided
    if (msg.subscription && msg.subscription.id) {
      activeSubscriptionId = msg.subscription.id;
    }

    // if client is open forward the raw message
    if (clientSocket && clientSocket.readyState === WebSocket.OPEN) {
      try {
        clientSocket.send(JSON.stringify(msg));
      } catch (e) {
        console.warn('[server] failed to forward message to client', e.message);
      }
    }
  });

  derivWs.on('close', (code, reason) => {
    console.log('[server] Deriv WS closed', code, reason && reason.toString());
    if (pingInterval) clearInterval(pingInterval);
    // attempt reconnect after delay
    reconnectTimer = setTimeout(() => {
      // create a new connection and replace behavior by mutating this object's handlers is complex
      // We'll notify the client to reconnect and let frontend request a new connection from server via new ws connection
      if (clientSocket && clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(JSON.stringify({ type: 'status', connected: false, message: 'deriv_closed' }));
      }
    }, 3000);
  });

  derivWs.on('error', (err) => {
    console.error('[server] Deriv WS error', err && err.message);
    if (clientSocket && clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.send(JSON.stringify({ type: 'deriv_error', error: err.message || String(err) }));
    }
  });

  // expose a helper to request market data (forgets previous subscription automatically)
  derivWs.requestMarketData = function(symbol) {
    if (derivWs.readyState !== WebSocket.OPEN) {
      console.warn('[server] deriv ws not open yet');
      return;
    }

    try {
      if (activeSubscriptionId) {
        derivWs.send(JSON.stringify({ forget: activeSubscriptionId }));
        activeSubscriptionId = null;
      }

      derivWs.send(JSON.stringify({
        ticks_history: symbol,
        adjust_start_time: 1,
        count: 1000,
        end: 'latest',
        start: 1,
        style: 'ticks'
      }));

      derivWs.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));

      // inform client
      if (clientSocket && clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(JSON.stringify({ type: 'status', connected: true, message: `subscribed:${symbol}` }));
      }
    } catch (e) {
      console.error('[server] failed to request market data', e.message);
    }
  };

  derivWs.closeConnection = function() {
    if (pingInterval) clearInterval(pingInterval);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    try { derivWs.close(); } catch (e) {}
  };

  return derivWs;
}

// WebSocket bridge to the frontend UI
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('[server] frontend connected');
  // Create an associated Deriv connection per frontend client
  const derivConn = createDerivConnection(ws);

  // once deriv connection opens, subscribe to default symbol
  // if deriv already open, request immediately, otherwise will subscribe on open via authorize flow
  derivConn.once('open', () => {
    derivConn.requestMarketData(DEFAULT_SYMBOL);
  });

  // If deriv is already open quickly, request
  if (derivConn.readyState === WebSocket.OPEN) derivConn.requestMarketData(DEFAULT_SYMBOL);

  ws.on('message', (message) => {
    let msg = null;
    try { msg = JSON.parse(message); } catch (e) { console.warn('[server] bad client json', e.message); return; }

    if (msg.action === 'change_symbol' && msg.symbol) {
      console.log('[server] client requested symbol change to', msg.symbol);
      DEFAULT_SYMBOL = msg.symbol;
      if (derivConn && derivConn.readyState === WebSocket.OPEN) {
        derivConn.requestMarketData(msg.symbol);
      }
    }

    // support other client messages if needed
  });

  ws.on('close', () => {
    console.log('[server] frontend disconnected');
    try { derivConn.closeConnection(); } catch (e) {}
  });

  ws.on('error', (err) => {
    console.warn('[server] frontend ws error', err && err.message);
  });
});

server.listen(PORT, () => {
  console.log(`[server] Signal Analyzer Bot running on http://localhost:${PORT}`);
});
