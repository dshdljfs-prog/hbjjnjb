'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const RECONNECT_MS = 45000;
const origins = (process.env.CORS_ORIGINS || '*').split(',').map(s => s.trim()).filter(Boolean);
const allowAll = origins.includes('*');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: allowAll ? '*' : origins } });

app.use((req, res, next) => {
  const o = req.headers.origin;
  if (allowAll) res.setHeader('Access-Control-Allow-Origin', '*');
  else if (o && origins.includes(o)) res.setHeader('Access-Control-Allow-Origin', o);
  next();
});
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.send('ok'));

// إعدادات ICE: STUN مجاني + TURN اختياري من متغيرات البيئة (لا أسرار في الواجهة)
app.get('/api/ice', (_req, res) => {
  const iceServers = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(',').map(s => s.trim()),
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }
  res.json({ iceServers });
});

/* ---------- أدوات ---------- */
const rooms = new Map();
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const genCode = () => {
  let c;
  do { c = Array.from({ length: 5 }, () => ALPHA[crypto.randomInt(ALPHA.length)]).join(''); } while (rooms.has(c));
  return c;
};
const clean = (s, n) => String(s == null ? '' : s).replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, n);
const byId = (room, id) => [...room.players.values()].find(p => p.id === id);

function view(room) {
  return {
    code: room.code, name: room.name, isPublic: room.isPublic, max: room.max,
    ownerId: room.ownerId, status: room.game ? 'playing' : 'lobby',
    players: [...room.players.values()].map(p => ({
      id: p.id, name: p.name, ready: p.ready, online: p.online, inVoice: p.inVoice, muted: p.muted
    })),
    game: room.game
  };
}
const emitRoom = r => io.to(r.code).emit('room:state', view(r));
const publicList = () => [...rooms.values()].filter(r => r.isPublic).map(r => ({
  code: r.code, name: r.name, count: r.players.size, max: r.max, status: r.game ? 'playing' : 'lobby'
}));
const emitList = () => io.emit('rooms:list', publicList());
const sys = (room, text) => io.to(room.code).emit('chat:msg', { system: true, text, t: Date.now() });

function ctx(socket) {
  const room = rooms.get(socket.data.code);
  if (!room) return {};
  const p = room.players.get(socket.data.token);
  if (!p || p.socketId !== socket.id) return {};
  return { room, p };
}

function removePlayer(room, token) {
  const p = room.players.get(token);
  if (!p) return;
  clearTimeout(p.timer);
  if (p.inVoice) io.to(room.code).emit('voice:left', { id: p.id });
  room.players.delete(token);
  if (room.game && Object.values(room.game.players).includes(p.id)) {
    room.game = null;
    room.players.forEach(x => { x.ready = false; });
    sys(room, 'انتهت المباراة لأن أحد اللاعبين غادر');
  }
  if (!room.players.size) {
    rooms.delete(room.code);
  } else {
    if (room.ownerId === p.id) {
      const n = [...room.players.values()].find(x => x.online) || [...room.players.values()][0];
      room.ownerId = n.id;
      sys(room, `${n.name} أصبح مالك الغرفة`);
    }
    emitRoom(room);
  }
  emitList();
}

function leaveCurrent(socket) {
  const room = rooms.get(socket.data.code);
  if (room) removePlayer(room, socket.data.token);
  if (socket.data.code) socket.leave(socket.data.code);
  socket.data.code = null;
}

/* ---------- XO ---------- */
const LINES = [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]];
function winner(b) {
  for (const [a, c, d] of LINES) if (b[a] && b[a] === b[c] && b[a] === b[d]) return { sym: b[a], line: [a, c, d] };
  return null;
}
function newXO(x, o, prev) {
  return {
    type: 'xo', board: Array(9).fill(null), players: { X: x, O: o }, turn: 'X',
    status: 'playing', line: null, winner: null, rematch: [],
    scores: prev ? prev.scores : { [x]: 0, [o]: 0 }, draws: prev ? prev.draws : 0, round: prev ? prev.round + 1 : 1
  };
}

/* ---------- Socket.IO ---------- */
io.on('connection', socket => {
  socket.emit('rooms:list', publicList());
  socket.data.msgs = [];

  socket.on('room:create', (d, cb) => {
    d = d || {};
    const token = clean(d.token, 64);
    if (token.length < 16) return cb && cb({ ok: false, error: 'رمز غير صالح' });
    leaveCurrent(socket);
    const room = {
      code: genCode(), name: clean(d.roomName, 30) || 'غرفة جديدة', isPublic: !!d.isPublic,
      max: Math.min(6, Math.max(2, parseInt(d.max, 10) || 2)), ownerId: null,
      players: new Map(), bans: new Set(), game: null
    };
    rooms.set(room.code, room);
    joinRoom(socket, room, token, clean(d.name, 20) || 'لاعب', cb, true);
  });

  socket.on('room:join', (d, cb) => {
    d = d || {};
    const token = clean(d.token, 64);
    const room = rooms.get(clean(d.code, 8).toUpperCase());
    if (token.length < 16) return cb && cb({ ok: false, error: 'رمز غير صالح' });
    if (!room) return cb && cb({ ok: false, error: 'الغرفة غير موجودة' });
    joinRoom(socket, room, token, clean(d.name, 20) || 'لاعب', cb, false);
  });

  function joinRoom(sock, room, token, name, cb, isNew) {
    if (room.bans.has(token)) return cb && cb({ ok: false, error: 'تم طردك من هذه الغرفة' });
    let p = room.players.get(token);
    if (sock.data.code && sock.data.code !== room.code) leaveCurrent(sock);
    if (!p) {
      if (room.players.size >= room.max) return cb && cb({ ok: false, error: 'الغرفة ممتلئة' });
      p = { token, id: crypto.randomBytes(5).toString('hex'), name, ready: false, online: true,
            socketId: sock.id, inVoice: false, muted: false, timer: null };
      room.players.set(token, p);
      if (isNew) room.ownerId = p.id;
      sys(room, `${name} انضم إلى الغرفة`);
    } else {
      clearTimeout(p.timer);
      p.socketId = sock.id; p.online = true; p.name = name;
    }
    sock.data.token = token; sock.data.code = room.code;
    sock.join(room.code);
    cb && cb({ ok: true, id: p.id, code: room.code });
    emitRoom(room); emitList();
  }

  socket.on('room:leave', () => leaveCurrent(socket));

  socket.on('room:ready', () => {
    const { room, p } = ctx(socket);
    if (!room || room.game) return;
    p.ready = !p.ready; emitRoom(room);
  });

  socket.on('room:kick', id => {
    const { room, p } = ctx(socket);
    if (!room || p.id !== room.ownerId) return;
    const t = byId(room, id);
    if (!t || t.id === p.id) return;
    room.bans.add(t.token);
    if (t.socketId) {
      const s = io.sockets.sockets.get(t.socketId);
      if (s) { s.emit('kicked'); s.leave(room.code); s.data.code = null; }
    }
    sys(room, `تم طرد ${t.name}`);
    removePlayer(room, t.token);
  });

  socket.on('game:start', (_d, cb) => {
    const { room, p } = ctx(socket);
    if (!room) return;
    const fail = error => cb && cb({ ok: false, error });
    if (p.id !== room.ownerId) return fail('مالك الغرفة فقط يبدأ المباراة');
    if (room.game) return fail('المباراة تعمل بالفعل');
    const online = [...room.players.values()].filter(x => x.online);
    if (online.length < 2) return fail('تحتاج لاعبين اثنين على الأقل');
    if (online.some(x => x.id !== room.ownerId && !x.ready)) return fail('ليس كل اللاعبين جاهزين');
    room.game = newXO(online[0].id, online[1].id, null);
    emitRoom(room); emitList();
    cb && cb({ ok: true });
  });

  socket.on('game:stop', () => {
    const { room, p } = ctx(socket);
    if (!room || p.id !== room.ownerId || !room.game) return;
    room.game = null;
    room.players.forEach(x => { x.ready = false; });
    emitRoom(room); emitList();
  });

  socket.on('xo:move', i => {
    const { room, p } = ctx(socket);
    const g = room && room.game;
    i = Number(i);
    if (!g || g.status !== 'playing' || !Number.isInteger(i) || i < 0 || i > 8 || g.board[i]) return;
    if (g.players[g.turn] !== p.id) return;
    g.board[i] = g.turn;
    const w = winner(g.board);
    if (w) { g.status = 'won'; g.line = w.line; g.winner = p.id; g.scores[p.id] = (g.scores[p.id] || 0) + 1; }
    else if (g.board.every(Boolean)) { g.status = 'draw'; g.draws++; }
    else g.turn = g.turn === 'X' ? 'O' : 'X';
    emitRoom(room);
  });

  socket.on('xo:rematch', () => {
    const { room, p } = ctx(socket);
    const g = room && room.game;
    if (!g || g.status === 'playing' || !Object.values(g.players).includes(p.id)) return;
    if (!g.rematch.includes(p.id)) g.rematch.push(p.id);
    if (g.rematch.length >= 2) room.game = newXO(g.players.O, g.players.X, g); // تبديل من يبدأ
    emitRoom(room);
  });

  /* الشات */
  socket.on('chat:send', text => {
    const { room, p } = ctx(socket);
    if (!room) return;
    const now = Date.now();
    socket.data.msgs = socket.data.msgs.filter(t => now - t < 3000);
    if (socket.data.msgs.length >= 5) return;
    socket.data.msgs.push(now);
    text = clean(text, 300);
    if (!text) return;
    io.to(room.code).emit('chat:msg', { id: p.id, name: p.name, text, t: now });
  });

  /* الصوت: إشارات WebRTC (Mesh) */
  socket.on('voice:join', (_d, cb) => {
    const { room, p } = ctx(socket);
    if (!room) return;
    const peers = [...room.players.values()].filter(x => x.inVoice && x.online && x.id !== p.id).map(x => x.id);
    p.inVoice = true; p.muted = false;
    cb && cb({ peers });
    emitRoom(room);
  });
  socket.on('voice:leave', () => {
    const { room, p } = ctx(socket);
    if (!room || !p.inVoice) return;
    p.inVoice = false; p.muted = false;
    io.to(room.code).emit('voice:left', { id: p.id });
    emitRoom(room);
  });
  socket.on('voice:state', d => {
    const { room, p } = ctx(socket);
    if (!room || !p.inVoice) return;
    p.muted = !!(d && d.muted); emitRoom(room);
  });
  socket.on('voice:signal', d => {
    const { room, p } = ctx(socket);
    if (!room || !p.inVoice || !d || typeof d.data !== 'object') return;
    const t = byId(room, d.to);
    if (t && t.inVoice && t.socketId) io.to(t.socketId).emit('voice:signal', { from: p.id, data: d.data });
  });

  socket.on('disconnect', () => {
    const { room, p } = ctx(socket);
    if (!room) return;
    p.online = false; p.socketId = null;
    if (p.inVoice) { p.inVoice = false; io.to(room.code).emit('voice:left', { id: p.id }); }
    p.timer = setTimeout(() => removePlayer(room, p.token), RECONNECT_MS);
    emitRoom(room);
  });
});

server.listen(PORT, () => console.log(`Boda Games running on http://localhost:${PORT}`));
