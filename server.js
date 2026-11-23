const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer } = require('ws');
const multicastDns = require('multicast-dns');
const QRCode = require('qrcode');

const PORT = Number(process.env.PORT) || 3000;
const MDNS_HOST = 'landrop.local';
const INDEX_PATH = path.join(__dirname, 'index.html');

// Cache index.html in memory and refresh automatically on file modification
let indexHtmlCache = null;
function loadIndexHtml() {
  try {
    indexHtmlCache = fs.readFileSync(INDEX_PATH);
  } catch (err) {
    console.error('Failed to read index.html:', err.message);
  }
}
loadIndexHtml();
fs.watchFile(INDEX_PATH, { interval: 1000 }, () => loadIndexHtml());

// Cache generated SVG QR codes by URL
const qrSvgCache = new Map();
async function getQrSvg(url) {
  if (qrSvgCache.has(url)) return qrSvgCache.get(url);
  const svg = await QRCode.toString(url, {
    type: 'svg',
    margin: 2,
    color: { dark: '#0b0d10', light: '#ffffff' }
  });
  qrSvgCache.set(url, svg);
  return svg;
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    if (!indexHtmlCache) loadIndexHtml();
    if (!indexHtmlCache) {
      res.writeHead(500);
      return res.end('Error loading index.html');
    }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache'
    });
    res.end(indexHtmlCache);
  } else if (req.url === '/api/qr') {
    try {
      const lanIps = getLanIps();
      const targets = lanIps.length
        ? lanIps.map(({ iface, address }) => ({
            iface,
            url: `http://${address}:${PORT}`
          }))
        : [{ iface: 'local', url: `http://localhost:${PORT}` }];

      const items = await Promise.all(
        targets.map(async ({ iface, url }) => ({
          iface,
          url,
          svg: await getQrSvg(url)
        }))
      );
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ items }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'QR generation failed' }));
    }
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocketServer({ server, maxPayload: 128 * 1024 * 1024 });
const peers = new Map(); // id -> { ws, id, name, device, ip }

function broadcastPeers() {
  const list = Array.from(peers.values()).map(({ id, name, device, ip }) => ({
    id,
    name,
    device,
    ip
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
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (data, isBinary) => {
    // Binary relay support (if WebRTC UDP is blocked by router AP isolation)
    if (isBinary) {
      if (data.length <= 36) return;
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
      if (msg.type === 'REGISTER' && typeof msg.id === 'string') {
        peerId = msg.id;
        const nextName = (msg.name || 'anonymous').slice(0, 32);
        const nextDevice = msg.device || 'Desktop';
        const existing = peers.get(peerId);
        const changed =
          !existing ||
          existing.ws !== ws ||
          existing.name !== nextName ||
          existing.device !== nextDevice ||
          existing.ip !== ip;

        peers.set(peerId, {
          ws,
          id: peerId,
          name: nextName,
          device: nextDevice,
          ip
        });

        if (changed) {
          broadcastPeers();
        }
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
      const current = peers.get(peerId);
      if (current && current.ws === ws) {
        peers.delete(peerId);
        broadcastPeers();
      }
    }
  });
});

// Protocol-level ping/pong keepalive (replaces noisy 20s REGISTER broadcasts)
const heartbeatTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  }
}, 25000);

wss.on('close', () => clearInterval(heartbeatTimer));

function getLanIps() {
  const nets = os.networkInterfaces();
  const results = [];
  const rank = (iface, ip) => {
    if (/^(wl|wlan|wifi|en|eth)/i.test(iface)) return 0;
    if (
      /^(tailscale|tun|tap|wg|docker|veth|virbr|zt|vmnet|vboxnet|br-)/i.test(
        iface
      )
    ) {
      return 2;
    }
    const [a, b] = ip.split('.').map(Number);
    if (a === 100 && b >= 64 && b <= 127) return 2;
    return 1;
  };
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        results.push({ iface: name, address: net.address });
      }
    }
  }
  return results.sort(
    (a, b) => rank(a.iface, a.address) - rank(b.iface, b.address)
  );
}

function startMdns() {
  const mdns = multicastDns();
  const buildAnswers = () =>
    getLanIps().map(({ address }) => ({
      name: MDNS_HOST,
      type: 'A',
      ttl: 120,
      flush: true,
      data: address
    }));

  mdns.on('error', (err) => {
    console.warn(`mDNS disabled (${err.message})`);
  });

  mdns.on('query', (query, rinfo) => {
    const wantsHost = query.questions.some(
      (q) =>
        q.name.toLowerCase() === MDNS_HOST &&
        (q.type === 'A' || q.type === 'ANY')
    );
    if (!wantsHost) return;
    const answers = buildAnswers();
    if (!answers.length) return;
    if (rinfo && rinfo.port !== 5353) {
      mdns.respond({ answers }, rinfo);
    } else {
      mdns.respond({ answers });
    }
  });

  const initial = buildAnswers();
  if (initial.length) mdns.respond({ answers: initial });
  return mdns;
}

server.listen(PORT, '0.0.0.0', async () => {
  startMdns();
  const lanIps = getLanIps();
  console.log(`\n=== LANdrop Local Node Active ===`);
  console.log(`Local:   http://localhost:${PORT}`);
  console.log(`mDNS:    http://${MDNS_HOST}:${PORT}`);
  for (const { iface, address } of lanIps) {
    console.log(`LAN (${iface}): http://${address}:${PORT}`);
  }
  const primaryUrl = lanIps.length
    ? `http://${lanIps[0].address}:${PORT}`
    : `http://localhost:${PORT}`;
  console.log(`\nScan to open (${primaryUrl}):\n`);
  try {
    const qrTerminal = await QRCode.toString(primaryUrl, {
      type: 'terminal',
      small: true
    });
    console.log(qrTerminal.trimEnd());
  } catch (err) {
    console.warn('Could not render terminal QR:', err.message);
  }
  console.log(`\n=================================\n`);
});
