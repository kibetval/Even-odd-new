require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const PORT = process.env.PORT || 3000;
const DERIV_API_TOKEN = process.env.DERIV_API_TOKEN || '';
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089'; // Default Deriv test app ID

// In-memory state for connected web clients
let activeWs = null;
let currentSymbol = 'R_10';
let marketSubscribed = false;

function connectToDeriv(clientSocket) {
  // Use public or authorized endpoint based on token availability
  const wsUrl = `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`;
  console.log(`Connecting to Deriv WebSocket: ${wsUrl}`);
  
  const derivWs = new WebSocket(wsUrl);

  derivWs.on('open', () => {
    console.log('Connected to Deriv WebSocket API');
    
    // Authorize if token is provided
    if (DERIV_API_TOKEN) {
      derivWs.send(JSON.stringify({ authorize: DERIV_API_TOKEN }));
    } else {
      // If no token, request history & ticks publicly
      requestMarketData(derivWs, currentSymbol);
    }
  });

  derivWs.on('message', (data) => {
    try {
      const response = JSON.parse(data);
      
      if (response.msg_type === 'authorize') {
        if (response.error) {
          console.error('Authorization failed:', response.error.message);
        } else {
          console.log(`Authorized successfully for user: ${response.authorize.email}`);
          requestMarketData(derivWs, currentSymbol);
        }
      }

      // Forward tick and history data to frontend UI
      if (clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(JSON.stringify(response));
      }
    } catch (err) {
      console.error('Error parsing Deriv message:', err);
    }
  });

  derivWs.on('close', () => {
    console.log('Deriv WebSocket closed. Reconnecting in 5s...');
    setTimeout(() => connectToDeriv(clientSocket), 5000);
  });

  derivWs.on('error', (err) => {
    console.error('Deriv WebSocket error:', err);
  });

  return derivWs;
}

function requestMarketData(derivWs, symbol) {
  if (derivWs.readyState !== WebSocket.OPEN) return;

  console.log(`Fetching history and subscribing to ticks for: ${symbol}`);
  
  // 1. Get 1000 tick history for initial probability analysis
  derivWs.send(JSON.stringify({
    ticks_history: symbol,
    adjust_start_time: 1,
    count: 1000,
    end: 'latest',
    start: 1,
    style: 'ticks'
  }));

  // 2. Subscribe to real-time tick stream
  derivWs.send(JSON.stringify({
    ticks: symbol,
    subscribe: 1
  }));
}

// WebSocket bridge to the frontend UI
const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
  console.log('Frontend dashboard connected');
  activeWs = connectToDeriv(ws);

  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message);
      if (msg.action === 'change_symbol' && msg.symbol) {
        currentSymbol = msg.symbol;
        if (activeWs && activeWs.readyState === WebSocket.OPEN) {
          requestMarketData(activeWs, currentSymbol);
        }
      }
    } catch (e) {
      console.error('Failed to parse frontend message:', e);
    }
  });

  ws.on('close', () => {
    console.log('Frontend dashboard disconnected');
    if (activeWs) activeWs.close();
  });
});

server.listen(PORT, () => {
  console.log(`Signal Analyzer Bot running on http://localhost:${PORT}`);
});
  
