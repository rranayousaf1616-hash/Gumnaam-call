// Matching + signaling server. Audio server se nahi guzarta (peer-to-peer / TURN).
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/config.json') {
    const ice = [{ urls: 'stun:stun.l.google.com:19302' }];
    if (process.env.TURN_URL) {
      ice.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER, credential: process.env.TURN_PASS });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers: ice }));
  }
  if (url === '/' || url === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(404); return res.end('index.html nahi mila'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
let waiting = null;

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const broadcastCount = () => { const n = wss.clients.size; wss.clients.forEach(c => send(c, { type: 'online', n })); };

function unpair(ws, notify) {
  if (waiting === ws) waiting = null;
  const p = ws.partner;
  if (p) { p.partner = null; ws.partner = null; if (notify) send(p, { type: 'partner-left' }); }
}

wss.on('connection', ws => {
  ws.partner = null; ws.alive = true; ws.lastFind = 0;
  ws.on('pong', () => { ws.alive = true; });
  broadcastCount();
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'find') {
      if (Date.now() - ws.lastFind < 700) return; // spam rok
      ws.lastFind = Date.now();
      unpair(ws, true);
      if (waiting && waiting !== ws && waiting.readyState === 1) {
        const a = waiting; waiting = null;
        a.partner = ws; ws.partner = a;
        send(a, { type: 'matched', initiator: true });
        send(ws, { type: 'matched', initiator: false });
      } else waiting = ws;
    } else if (m.type === 'signal' && ws.partner) {
      send(ws.partner, { type: 'signal', data: m.data });
    } else if (m.type === 'leave') {
      unpair(ws, true);
    } else if (m.type === 'report') {
      console.log(new Date().toISOString(), 'REPORT', String(m.reason || '').slice(0, 100));
      unpair(ws, true);
    }
  });
  ws.on('close', () => { unpair(ws, true); broadcastCount(); });
});

setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); });
}, 30000);

server.listen(PORT, () => console.log('Gumnaam Call chal raha hai: port ' + PORT));
