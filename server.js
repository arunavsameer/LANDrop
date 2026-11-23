const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) {
        res.writeHead(500);
        return res.end('Error loading index.html');
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocketServer({ server, maxPayload: 128 * 1024 * 1024 });
const peers = new Map(); // id -> { ws, id, name, device, ip }

function broadcastPeers() {
  const list = Array.from(peers.values()).map(({ id, name, device, ip }) => ({
    id, name, device, ip
  }));
  const payload = JSON.stringify({ type: 'PEERS', peers: list });
  for (const peer of peers.values()) {
    if (peer.ws.readyState === 1) peer.ws.send(payload);
  }
}

wss.on('connection', (ws, req) => {
  const rawIp = req.socket.remoteAddress || '';
  const ip = rawIp.replace(/^.*:/, '') || 'local';
  let peerId = null;

  ws.on('message', (data, isBinary) => {
    // Binary relay support (if WebRTC UDP is blocked by router AP isolation)
    if (isBinary) {
      // First 36 bytes = target peer UUID (ASCII)
      const targetId = data.subarray(0, 36).toString('utf8');
      const target = peers.get(targetId);
      if (target && target.ws.readyState === 1) {
        target.ws.send(data.subarray(36), { binary: true });
      }
      return;
    }

    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'REGISTER') {
        peerId = msg.id;
        peers.set(peerId, {
          ws,
          id: peerId,
          name: (msg.name || 'anonymous').slice(0, 32),
          device: msg.device || 'Desktop',
          ip
        });
        broadcastPeers();
      } else if (msg.target && peers.has(msg.target)) {
        const dest = peers.get(msg.target);
        if (dest.ws.readyState === 1) {
          dest.ws.send(JSON.stringify({ ...msg, from: peerId }));
        }
      }
    } catch (e) {
      console.error('Malformed message:', e.message);
    }
  });

  ws.on('close', () => {
    if (peerId && peers.has(peerId)) {
      peers.delete(peerId);
      broadcastPeers();
    }
  });
});

function getLanIps() {
  const nets = os.networkInterfaces();
  const results = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        results.push({ iface: name, address: net.address });
      }
    }
  }
  return results;
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n=== LANdrop Local Node Active ===`);
  console.log(`Local:   http://localhost:${PORT}`);
  for (const { iface, address } of getLanIps()) {
    console.log(`LAN (${iface}): http://${address}:${PORT}`);
  }
  console.log(`=================================\n`);
});