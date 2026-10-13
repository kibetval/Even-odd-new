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
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089';

let currentSymbol = 'R_10';

function connectToDeriv(clientSocket) {
  // Correct official Deriv public WebSocket connection string
  const wsUrl = `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`;
  console.log(`Connecting to Deriv WebSocket: ${wsUrl}`);
  
  const derivWs = new WebSocket(wsUrl);

  derivWs.on('open', () => {
    console.log('Connected to Deriv WebSocket API');
    
    // Authorize if token is provided, otherwise fetch public stream
    if (DERIV_API_TOKEN) {
      derivWs.send(JSON.stringify({ authorize: DERIV_API_TOKEN }));
    } else {
      requestMarketData(derivWs, currentSymbol);
    }

    // Keep connection alive with periodic pings every 30 seconds
    const pingInterval = setInterval(() => {
      if (derivWs.readyState === WebSocket.OPEN) {
        derivWs.send(JSON.stringify({ ping: 1 }));
      } else {
        clearInterval(pingInterval);
      }
    }, 30000);

    derivWs.on('close', () => clearInterval(pingInterval));
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

      // Forward responses to browser frontend
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
  
  // 1. Get 1000 tick history for initial calculation
  derivWs.send(JSON.stringify({
    ticks_history: symbol,
    adjust_start_time: 1,
    count: 1000,
    end: 'latest',
    start: 1,
    style: 'ticks'
  }));

  // 2. Subscribe to real-time ticks
  derivWs.send(JSON.stringify({
    ticks: symbol,
    subscribe: 1
  }));
}

// WebSocket bridge to the frontend UI
const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
  console.log('Frontend dashboard connected');
  const activeDerivWs = connectToDeriv(ws);

  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message);
      if (msg.action === 'change_symbol' && msg.symbol) {
        currentSymbol = msg.symbol;
        if (activeDerivWs && activeDerivWs.readyState === WebSocket.OPEN) {
          requestMarketData(activeDerivWs, currentSymbol);
        }
      }
    } catch (e) {
      console.error('Failed to parse frontend message:', e);
    }
  });

  ws.on('close', () => {
    console.log('Frontend dashboard disconnected');
    if (activeDerivWs) activeDerivWs.close();
  });
});

server.listen(PORT, () => {
  console.log(`Signal Analyzer Bot running on http://localhost:${PORT}`);
});