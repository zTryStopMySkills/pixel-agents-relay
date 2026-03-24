import { createServer } from 'http';
import { WebSocketServer } from 'ws';

const PORT = process.env.PORT ?? 8080;
const PING_INTERVAL_MS = 15_000;
const JOIN_TIMEOUT_MS = 30_000;
const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function randomCode(len) {
  let s = '';
  for (let i = 0; i < len; i++) s += CHARS[Math.floor(Math.random() * CHARS.length)];
  return s;
}

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch { /* ignore */ }
  }
}

/**
 * rooms: Map<roomCode, {
 *   host: WebSocket,
 *   hostDisplayName: string,
 *   guests: Map<peerId, { ws, displayName }>,
 *   pending: Map<requestId, { ws, displayName, timer }>,
 *   pendingCounter: number
 * }>
 *
 * socketRole: WeakMap<WebSocket, { role: 'host'|'guest'|'pending', roomCode, peerId? }>
 */
const rooms = new Map();
const socketRole = new WeakMap();

function broadcastRoom(room, msg, excludePeerId = null) {
  const raw = JSON.stringify(msg);
  if (excludePeerId !== 'host') {
    if (room.host.readyState === room.host.OPEN) {
      try { room.host.send(raw); } catch { /* ignore */ }
    }
  }
  for (const [pid, { ws }] of room.guests) {
    if (pid === excludePeerId) continue;
    if (ws.readyState === ws.OPEN) {
      try { ws.send(raw); } catch { /* ignore */ }
    }
  }
}

function broadcastGuests(room, msg, excludePeerId = null) {
  const raw = JSON.stringify(msg);
  for (const [pid, { ws }] of room.guests) {
    if (pid === excludePeerId) continue;
    if (ws.readyState === ws.OPEN) {
      try { ws.send(raw); } catch { /* ignore */ }
    }
  }
}

function cleanupSocket(ws) {
  const role = socketRole.get(ws);
  if (!role) return;
  socketRole.delete(ws);

  const room = rooms.get(role.roomCode);
  if (!room) return;

  if (role.role === 'host') {
    // Notify all guests and pending that room closed
    const msg = JSON.stringify({ type: 'peerLeft', peerId: 'host', reason: 'disconnect' });
    for (const { ws: gWs } of room.guests.values()) {
      try { if (gWs.readyState === gWs.OPEN) gWs.send(msg); gWs.terminate(); } catch { /* ignore */ }
    }
    for (const { ws: pWs, timer } of room.pending.values()) {
      clearTimeout(timer);
      try { pWs.terminate(); } catch { /* ignore */ }
    }
    rooms.delete(role.roomCode);
    console.log(`[Relay] Room ${role.roomCode} closed (host disconnected)`);
  } else if (role.role === 'guest') {
    const peerId = role.peerId;
    room.guests.delete(peerId);
    const peerLeft = { type: 'peerLeft', peerId, reason: 'disconnect' };
    send(room.host, peerLeft);
    broadcastGuests(room, peerLeft, peerId);
    console.log(`[Relay] Guest ${peerId} left room ${role.roomCode}`);
  }
  // pending connections are cleaned up when timer fires or host rejects
}

const httpServer = createServer((_req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Pixel Agents Relay OK\n');
});

const wss = new WebSocketServer({ server: httpServer, maxPayload: 10 * 1024 * 1024 /* 10 MB */ });

wss.on('connection', (ws, req) => {
  const remoteIp = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
    ?? req.socket.remoteAddress
    ?? 'unknown';

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    // ── Host creates a room ──────────────────────────────────────────────
    if (msg.type === 'hostHello') {
      if (socketRole.has(ws)) return; // already registered
      const roomCode = randomCode(6);
      rooms.set(roomCode, {
        host: ws,
        hostDisplayName: String(msg.displayName ?? 'Host').slice(0, 64),
        workspaceFolder: msg.workspaceFolder ?? null,
        guests: new Map(),
        pending: new Map(),
        pendingCounter: 0,
      });
      socketRole.set(ws, { role: 'host', roomCode });
      send(ws, { type: 'roomCreated', roomCode });
      console.log(`[Relay] Room ${roomCode} created by ${msg.displayName}`);
      return;
    }

    // ── Guest wants to join ──────────────────────────────────────────────
    if (msg.type === 'hello') {
      if (socketRole.has(ws)) return;
      const room = rooms.get(msg.roomCode);
      if (!room) {
        send(ws, { type: 'joinRejected', reason: 'Room not found' });
        ws.terminate();
        return;
      }
      if (room.pending.size >= 10) {
        send(ws, { type: 'joinRejected', reason: 'Too many pending requests' });
        ws.terminate();
        return;
      }
      const requestId = `req_${++room.pendingCounter}`;
      const timer = setTimeout(() => {
        room.pending.delete(requestId);
        socketRole.delete(ws);
        send(ws, { type: 'joinRejected', reason: 'Timeout' });
        ws.terminate();
        send(room.host, { type: 'joinRequestExpired', requestId });
      }, JOIN_TIMEOUT_MS);

      const displayName = String(msg.displayName ?? 'Guest').slice(0, 64);
      room.pending.set(requestId, { ws, displayName, timer });
      socketRole.set(ws, { role: 'pending', roomCode: msg.roomCode });
      send(room.host, { type: 'joinRequest', requestId, displayName, remoteIp });
      console.log(`[Relay] Join request ${requestId} from ${msg.displayName} for room ${msg.roomCode}`);
      return;
    }

    const role = socketRole.get(ws);
    if (!role) return;
    const room = rooms.get(role.roomCode);
    if (!room) return;

    // ── Host accepts a pending guest ─────────────────────────────────────
    if (msg.type === 'acceptPeer' && role.role === 'host') {
      const pending = room.pending.get(msg.requestId);
      if (!pending) return;
      clearTimeout(pending.timer);
      room.pending.delete(msg.requestId);

      const peerId = randomCode(6);
      room.guests.set(peerId, { ws: pending.ws, displayName: pending.displayName });
      socketRole.set(pending.ws, { role: 'guest', roomCode: role.roomCode, peerId });

      // Send welcome to the guest
      const existingPeers = Array.from(room.guests.entries())
        .filter(([id]) => id !== peerId)
        .map(([id, g]) => ({ peerId: id, displayName: g.displayName }));
      send(pending.ws, {
        type: 'welcome',
        peerId,
        roomCode: role.roomCode,
        hostDisplayName: room.hostDisplayName,
        workspaceFolder: room.workspaceFolder,
        peers: existingPeers,
        agents: [],
      });

      // Broadcast peerJoined to everyone (host + other guests)
      broadcastRoom(room, { type: 'peerJoined', peer: { peerId, displayName: pending.displayName }, agents: [] }, peerId);
      console.log(`[Relay] Guest ${peerId} (${pending.displayName}) joined room ${role.roomCode}`);
      return;
    }

    // ── Host rejects a pending guest ─────────────────────────────────────
    if (msg.type === 'rejectPeer' && role.role === 'host') {
      const pending = room.pending.get(msg.requestId);
      if (!pending) return;
      clearTimeout(pending.timer);
      room.pending.delete(msg.requestId);
      send(pending.ws, { type: 'joinRejected', reason: 'rejected' });
      pending.ws.terminate();
      console.log(`[Relay] Request ${msg.requestId} rejected`);
      return;
    }

    // ── agentEvent relay ─────────────────────────────────────────────────
    if (msg.type === 'agentEvent') {
      const event = msg.event ?? {};
      // Unicast: snapshot chunks/complete targeted at a specific peer
      if (event.targetPeerId && (event.kind === 'workspaceSnapshotChunk' || event.kind === 'workspaceSnapshotComplete')) {
        const targetWs = event.targetPeerId === 'host'
          ? room.host
          : room.guests.get(event.targetPeerId)?.ws ?? null;
        if (targetWs) send(targetWs, { type: 'agentEvent', peerId: role.peerId ?? 'host', event });
        return;
      }
      // Unicast: snapshot request goes to host only
      if (event.kind === 'workspaceSnapshotRequest') {
        send(room.host, { type: 'agentEvent', peerId: role.peerId ?? 'host', event });
        return;
      }
      if (role.role === 'host') {
        broadcastGuests(room, { type: 'agentEvent', peerId: 'host', event: msg.event });
      } else if (role.role === 'guest') {
        broadcastRoom(room, { type: 'agentEvent', peerId: role.peerId, event: msg.event }, role.peerId);
      }
      return;
    }

    // ── agentSnapshot relay ──────────────────────────────────────────────
    if (msg.type === 'agentSnapshot') {
      if (role.role === 'host') {
        broadcastGuests(room, { type: 'agentSnapshot', peerId: 'host', agents: msg.agents });
      } else if (role.role === 'guest') {
        broadcastRoom(room, { type: 'agentSnapshot', peerId: role.peerId, agents: msg.agents }, role.peerId);
      }
      return;
    }

    // ── layoutSync: host → specific guest only ───────────────────────────
    if (msg.type === 'layoutSync' && role.role === 'host') {
      const target = room.guests.get(msg.targetPeerId)?.ws;
      if (target) send(target, { type: 'remoteLayoutSync', layout: msg.layout });
      return;
    }

    // ── transcriptRequest: any peer → another peer ───────────────────────
    if (msg.type === 'transcriptRequest') {
      const targetPeerId = String(msg.targetPeerId ?? '');
      let target = null;
      if (targetPeerId === 'host') {
        target = room.host;
      } else {
        target = room.guests.get(targetPeerId)?.ws ?? null;
      }
      if (target) {
        send(target, { ...msg, fromPeerId: role.peerId ?? 'host' });
      }
      return;
    }

    // ── transcriptResponse: owner → requestor ────────────────────────────
    if (msg.type === 'transcriptResponse') {
      const targetPeerId = String(msg.targetPeerId ?? '');
      let target = null;
      if (targetPeerId === 'host') {
        target = room.host;
      } else {
        target = room.guests.get(targetPeerId)?.ws ?? null;
      }
      if (target) send(target, msg);
      return;
    }

    // ── annotationBroadcast: host only → all guests ───────────────────────
    if (msg.type === 'annotationBroadcast' && role.role === 'host') {
      broadcastGuests(room, msg);
      return;
    }

    // ── chatMessage: public broadcast or private routing ─────────────────────
    if (msg.type === 'chatMessage') {
      const outMsg = {
        type: 'chatMessage',
        id: String(msg.id ?? ''),
        fromPeerId: role.peerId ?? 'host',
        fromName: String(msg.fromName ?? '').slice(0, 64),
        text: String(msg.text ?? '').slice(0, 2000),
        toPeerId: msg.toPeerId,
        timestamp: typeof msg.timestamp === 'number' ? msg.timestamp : Date.now(),
      };
      if (msg.toPeerId) {
        // Private: route to target only
        const target = msg.toPeerId === 'host'
          ? room.host
          : room.guests.get(msg.toPeerId)?.ws ?? null;
        if (target) send(target, outMsg);
      } else {
        // Public: broadcast to all except sender
        broadcastRoom(room, outMsg, role.peerId ?? 'host');
      }
      return;
    }

    // ── kickPeer: host only → remove a guest ─────────────────────────────
    if (msg.type === 'kickPeer' && role.role === 'host') {
      const targetId = String(msg.peerId ?? '');
      const target = room.guests.get(targetId);
      if (target) {
        const peerLeft = { type: 'peerLeft', peerId: targetId, reason: 'kicked' };
        send(target.ws, peerLeft);
        target.ws.terminate();
        room.guests.delete(targetId);
        // Notify remaining participants
        broadcastRoom(room, peerLeft);
        console.log(`[Relay] Guest ${targetId} kicked from room ${role.roomCode}`);
      }
      return;
    }

    // ── remoteExecRequest: guest → host ──────────────────────────────────
    if (msg.type === 'remoteExecRequest' && role.role === 'guest') {
      send(room.host, { ...msg, fromPeerId: role.peerId });
      return;
    }

    // ── remoteExecResponse: host → specific guest ─────────────────────────
    if (msg.type === 'remoteExecResponse' && role.role === 'host') {
      const targetPeerId = String(msg.targetPeerId ?? '');
      const target = room.guests.get(targetPeerId)?.ws ?? null;
      if (target) send(target, msg);
      return;
    }

    // ── pong ─────────────────────────────────────────────────────────────
    if (msg.type === 'pong') return;
  });

  ws.on('close', () => cleanupSocket(ws));
  ws.on('error', () => { /* close fires after error */ });
});

// Ping all connected sockets every PING_INTERVAL_MS
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.readyState === ws.OPEN) {
      send(ws, { type: 'ping', seq: Date.now() });
    }
  }
}, PING_INTERVAL_MS);

httpServer.listen(PORT, () => {
  console.log(`[Relay] Pixel Agents relay listening on port ${PORT}`);
});
