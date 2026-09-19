'use strict';
/**
 * WebSocket Gateway
 * ──────────────────
 * Standalone service that owns ALL socket connections.
 * Has zero business logic — it is a pure transport bridge:
 *
 *   Browser  ←→  Socket.io  ←→  Redis Pub/Sub  ←→  Chat Service
 *
 * What it does:
 *   • Authenticates sockets by reading sessions from the shared Redis session store
 *   • On connect: calls Chat Service's internal REST to get the user's room list
 *   • Subscribes to chat.deliver and chat.member channels → forwards to sockets
 *   • Forwards chat:send / chat:read to the Chat Service's /internal/chat/* and
 *     acks the browser with the result; edit/delete (legacy) ride Redis
 *   • Handles typing indicators and presence entirely in-process (no DB, no Chat Service)
 *
 * Required env vars: see .env.example
 */

require('dotenv').config();

const http      = require('http');
const express   = require('express');
const { Server } = require('socket.io');
const Redis     = require('ioredis');
const session   = require('express-session');
const jwt       = require('jsonwebtoken');
// const { default: RedisStore } = require('connect-redis');
const { RedisStore } = require('connect-redis');

// ── Env ───────────────────────────────────────────────────────────────────────
const REDIS_URL           = process.env.REDIS_URL;
const CHAT_SERVICE_URL    = process.env.CHAT_SERVICE_URL;   // e.g. http://localhost:3000
const SCHOOL_BACKEND_URL  = process.env.SCHOOL_BACKEND_URL || CHAT_SERVICE_URL;
const INTERNAL_SECRET     = process.env.INTERNAL_SECRET;
const SESSION_SECRET      = process.env.SESSION_SECRET  || 'fallback_secret';
const JWT_SECRET          = process.env.JWT_SECRET;
const PORT                = process.env.PORT             || 4000;
const ALLOWED_ORIGINS     = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

if (!REDIS_URL)        { console.error('FATAL: REDIS_URL is required');        process.exit(1); }
if (!CHAT_SERVICE_URL) { console.error('FATAL: CHAT_SERVICE_URL is required'); process.exit(1); }
if (!INTERNAL_SECRET)  { console.error('FATAL: INTERNAL_SECRET is required');  process.exit(1); }
// The React app and the mobile app authenticate sockets with the API's JWT.
// Without this secret every one of those handshakes is rejected and the app
// silently loses live chat + live notifications — so refuse to start quietly.
if (!JWT_SECRET) {
    console.error('FATAL: JWT_SECRET is required — it must match JWT_SECRET in the school-backend .env,');
    console.error('       otherwise every token-authenticated socket is rejected as "Unauthenticated".');
    process.exit(1);
}

// ── Redis clients ─────────────────────────────────────────────────────────────
function _makeRedis(name) {
    const c = new Redis(REDIS_URL, {
        retryStrategy: times => Math.min(times * 100, 3000),
        maxRetriesPerRequest: 3,
        enableReadyCheck: true,
    });
    c.on('error',   e  => console.error(`[Redis:${name}] error:`, e.message));
    c.on('connect', () => console.log(`✅ Redis:${name} connected`));
    return c;
}

const pubClient     = _makeRedis('pub');
const subClient     = _makeRedis('sub');
const sessionRedis  = _makeRedis('session');
const presenceRedis = _makeRedis('presence');

// ── Shared session store ──────────────────────────────────────────────────────
// Must use the SAME prefix / secret as the Chat Service so the gateway can
// read sessions created by the main app.
// const sessionStore = new RedisStore({ client: sessionRedis, prefix: 'sess:' });
const sessionStore = new RedisStore({
    client: sessionRedis,
    prefix: 'sess:',
});

const sessionMiddleware = session({
    store:             sessionStore,
    secret:            SESSION_SECRET,
    resave:            false,
    saveUninitialized: false,
    cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000, sameSite: 'lax' },
});

// ── Express (health check only) ───────────────────────────────────────────────
const app = express();
app.use(express.json());

app.get('/health', (_, res) => res.json({
    ok:      true,
    service: 'websocket-gateway',
    sockets: io ? io.engine.clientsCount : 0,
}));

const server = http.createServer(app);

// ── Socket.io server ──────────────────────────────────────────────────────────
const io = new Server(server, {
    cors: {
        origin:      ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS : true,
        credentials: true,
    },
    transports: ['websocket', 'polling'],
});

// Plug the shared session middleware into Socket.io's engine so
// socket.request.session is populated before our auth middleware runs.
io.engine.use(sessionMiddleware);

// ── Socket.io auth middleware ─────────────────────────────────────────────────
// Accepts either a JWT token (school-backend users) or a Redis session (chat users).
io.use(async (socket, next) => {
    const token = socket.handshake.auth?.token;
    if (token) {
        try {
            const decoded = jwt.verify(token, JWT_SECRET);
            // The sign-in chooser's short-lived `purpose` tokens are not sessions
            // (the API refuses them too) — never let one open a socket.
            if (decoded.purpose) return next(new Error('Unauthenticated — not a session token'));
            socket.userId   = String(decoded.userId);
            socket.userRole = decoded.role     ? String(decoded.role)     : '';
            socket.schoolId = decoded.schoolId ? String(decoded.schoolId) : '';
            socket.authType = 'jwt';

            // Tokens issued before role/school were added to the payload carry
            // only userId. Room sync and chat.send both need the school, so
            // resolve the rest from the backend instead of joining no rooms.
            if (!socket.userRole || !socket.schoolId) {
                const ctx = await _getUserContext(socket.userId);
                if (ctx) {
                    socket.userRole = socket.userRole || ctx.role     || 'unknown';
                    socket.schoolId = socket.schoolId || ctx.schoolId || '';
                }
            }
            return next();
        } catch (err) {
            console.warn(`[Gateway] token rejected (${err.message}) — trying session auth`);
        }
    }
    const sess = socket.request.session;
    if (!sess || !sess.userId) {
        return next(new Error('Unauthenticated — no valid session or token'));
    }
    socket.userId   = String(sess.userId);
    socket.userRole = sess.userRole  || 'unknown';
    socket.schoolId = String(sess.schoolId);
    socket.authType = 'session';
    next();
});

// ── In-memory presence tracking ───────────────────────────────────────────────
// userId → Set<socketId> — lets us know when the LAST socket for a user disconnects
const userSockets = new Map();

function _addSocket(userId, socketId) {
    if (!userSockets.has(userId)) userSockets.set(userId, new Set());
    userSockets.get(userId).add(socketId);
}

function _removeSocket(userId, socketId) {
    const set = userSockets.get(userId);
    if (!set) return;
    set.delete(socketId);
    if (set.size === 0) userSockets.delete(userId);
}

function _isOnline(userId) {
    const set = userSockets.get(userId);
    return !!(set && set.size > 0);
}

// ── Internal REST helpers ─────────────────────────────────────────────────────
async function _getUserChats(userId, schoolId) {
    const url = `${CHAT_SERVICE_URL}/internal/user-chats?userId=${encodeURIComponent(userId)}&schoolId=${encodeURIComponent(schoolId)}`;
    const res = await fetch(url, {
        headers: { 'x-internal-secret': INTERNAL_SECRET },
        signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`/internal/user-chats returned HTTP ${res.status}`);
    const body = await res.json();
    return body.chatIds || [];
}

// Refresh the user's lastSeenAt in the DB — driven by the socket lifecycle
// (connect / presence heartbeat / disconnect) instead of client HTTP polling.
function _bumpLastSeen(userId) {
    const url = `${SCHOOL_BACKEND_URL}/internal/user-seen?userId=${encodeURIComponent(userId)}`;
    fetch(url, {
        method:  'POST',
        headers: { 'x-internal-secret': INTERNAL_SECRET },
        signal:  AbortSignal.timeout(3000),
    }).catch(() => { /* presence is best-effort */ });
}

// Role + school for a user whose token predates those claims
async function _getUserContext(userId) {
    try {
        const url = `${SCHOOL_BACKEND_URL}/internal/user-context?userId=${encodeURIComponent(userId)}`;
        const res = await fetch(url, {
            headers: { 'x-internal-secret': INTERNAL_SECRET },
            signal: AbortSignal.timeout(3000),
        });
        if (!res.ok) return null;
        return await res.json();
    } catch (err) {
        console.warn(`[Gateway] user-context lookup failed for ${userId}:`, err.message);
        return null;
    }
}

async function _getNotificationCount(userId) {
    try {
        const url = `${SCHOOL_BACKEND_URL}/internal/user-notification-count?userId=${encodeURIComponent(userId)}`;
        const res = await fetch(url, {
            headers: { 'x-internal-secret': INTERNAL_SECRET },
            signal: AbortSignal.timeout(3000),
        });
        if (!res.ok) return 0;
        const body = await res.json();
        return body.count ?? 0;
    } catch {
        return 0;
    }
}

// Chat commands (send / read) go to the backend over HTTP so the browser gets a
// real answer: the saved message, or the reason it was refused. Fan-out to the
// room still comes back over Redis (chat.deliver).
async function _chatCommand(path, body) {
    try {
        const res = await fetch(`${CHAT_SERVICE_URL}/internal/chat/${path}`, {
            method:  'POST',
            headers: { 'x-internal-secret': INTERNAL_SECRET, 'content-type': 'application/json' },
            body:    JSON.stringify(body),
            signal:  AbortSignal.timeout(8000),
        });
        const out = await res.json().catch(() => null);
        if (out && typeof out.ok === 'boolean') return out;
        return { ok: false, status: res.status, message: 'Chat service unavailable' };
    } catch (err) {
        console.warn(`[Gateway] /internal/chat/${path} failed:`, err.message);
        return { ok: false, status: 503, message: 'Chat service unavailable' };
    }
}

// ── Redis publish helper ──────────────────────────────────────────────────────
function _publish(channel, data) {
    pubClient.publish(channel, JSON.stringify(data)).catch((err) => {
        console.error(`[Gateway] publish(${channel}) failed:`, err.message);
    });
}

// ── Presence helpers ──────────────────────────────────────────────────────────
async function _markOnline(userId, chatIds) {
    await presenceRedis.set(`presence:${userId}`, '1', 'EX', 35).catch(() => {});
    for (const chatId of chatIds) {
        io.to(`chat:${chatId}`).emit('chat:user_online', { userId });
    }
}

async function _markOffline(userId, chatIds) {
    await presenceRedis.del(`presence:${userId}`).catch(() => {});
    for (const chatId of chatIds) {
        io.to(`chat:${chatId}`).emit('chat:user_offline', { userId });
    }
}

// ── Connection handler ────────────────────────────────────────────────────────
// Every listener is attached synchronously, BEFORE the room lookup is awaited.
// A socket.io event that arrives while no listener exists is dropped, and the
// moment of (re)connecting is exactly when a browser flushes the messages it
// queued while offline — they used to vanish here.
io.on('connection', (socket) => {
    const { userId, userRole, schoolId } = socket;

    _addSocket(userId, socket.id);

    // Personal room — used by publishToUser() from Chat/Notification Service
    socket.join(`user:${userId}`);

    // ── Presence heartbeat (keeps Redis TTL alive + lastSeenAt fresh every 25 s) ──
    const heartbeatTimer = setInterval(() => {
        presenceRedis.set(`presence:${userId}`, '1', 'EX', 35).catch(() => {});
        _bumpLastSeen(userId);
    }, 25_000);

    // ── Inbound chat commands ─────────────────────────────────────────────────
    // chat:send / chat:read take an optional ack callback. With one, the caller
    // hears back { ok, data } or { ok:false, message } — the browser keeps its
    // optimistic bubble pending until then and falls back to REST on a timeout
    // (safe: the backend dedupes on clientId). Without one, a failure arrives
    // as a chat:error event instead.

    // Token bucket per socket: bursts of 30 sends, refilled at one per 400 ms.
    const BUCKET = 30, REFILL_MS = 400;
    let sendTokens = BUCKET;
    let refilledAt = Date.now();
    const takeSendToken = () => {
        const now = Date.now();
        const earned = Math.floor((now - refilledAt) / REFILL_MS);
        if (earned > 0) {
            sendTokens = Math.min(BUCKET, sendTokens + earned);
            refilledAt = sendTokens === BUCKET ? now : refilledAt + earned * REFILL_MS;
        }
        if (sendTokens <= 0) return false;
        sendTokens -= 1;
        return true;
    };

    const reply = (ack, result) => {
        if (typeof ack === 'function') return ack(result);
        if (!result.ok) socket.emit('chat:error', { message: result.message, clientId: result.clientId || null });
    };

    socket.on('chat:send', async (data, ack) => {
        const clientId = data?.clientId || data?.tempId || null;
        if (!data || !data.chatId) return reply(ack, { ok: false, status: 400, message: 'chatId is required', clientId });
        if (!takeSendToken()) {
            return reply(ack, { ok: false, status: 429, message: 'You are sending messages too quickly', clientId });
        }
        const out = await _chatCommand('send', {
            userId,
            chatId:      data.chatId,
            content:     data.content,
            type:        data.type || 'text',
            replyTo:     data.replyTo || null,
            attachments: data.attachments || [],
            isForwarded: !!data.isForwarded,
            clientId,
        });
        reply(ack, out.ok ? out : { ...out, clientId });
    });

    socket.on('chat:read', async (data, ack) => {
        if (!data || !data.chatId) {
            if (typeof ack === 'function') ack({ ok: false, status: 400, message: 'chatId is required' });
            return;
        }
        const out = await _chatCommand('read', { userId, chatId: data.chatId, messageId: data.messageId || null });
        if (typeof ack === 'function') ack(out);   // a failed read receipt is not worth a toast
    });

    // Edit / delete still ride Redis for older clients; current ones use REST.
    socket.on('chat:edit', (data) => {
        if (!data || !data.messageId || !data.content) return;
        _publish('chat.edit', { messageId: data.messageId, senderId: userId, content: data.content });
    });

    socket.on('chat:delete', (data) => {
        if (!data || !data.messageId) return;
        _publish('chat.delete', { messageId: data.messageId, senderId: userId, senderRole: userRole });
    });

    // ── Typing indicators (handled entirely in gateway — no Chat Service round-trip) ──
    // Only relayed into rooms this socket is actually in, so nobody can make a
    // conversation they are not part of show "typing…".
    socket.on('chat:typing', ({ chatId } = {}) => {
        if (!chatId || !socket.rooms.has(`chat:${chatId}`)) return;
        socket.to(`chat:${chatId}`).emit('chat:typing', { chatId, userId });
    });

    socket.on('chat:stop_typing', ({ chatId } = {}) => {
        if (!chatId || !socket.rooms.has(`chat:${chatId}`)) return;
        socket.to(`chat:${chatId}`).emit('chat:stop_typing', { chatId, userId });
    });

    // ── Disconnect ────────────────────────────────────────────────────────────
    // Rooms are emptied before 'disconnect' fires — remember them here so the
    // offline notice also reaches conversations joined after connecting.
    let lastRooms = [];
    socket.on('disconnecting', () => {
        lastRooms = [...socket.rooms].filter((r) => r.startsWith('chat:')).map((r) => r.slice(5));
    });

    socket.on('disconnect', async () => {
        clearInterval(heartbeatTimer);
        _removeSocket(userId, socket.id);

        if (!_isOnline(userId)) {
            // Last socket for this user — announce offline + record final "last seen"
            await _markOffline(userId, lastRooms);
            _bumpLastSeen(userId);
        }
    });

    // ── Async setup (listeners are already live) ──────────────────────────────
    (async () => {
        // Push initial unread notification count
        _getNotificationCount(userId).then(count => {
            socket.emit('notification:unread_count', { count });
        });

        // Join all chat rooms the user belongs to
        let chatIds = [];
        try {
            chatIds = await _getUserChats(userId, schoolId);
            if (socket.connected) for (const chatId of chatIds) socket.join(`chat:${chatId}`);
        } catch (err) {
            console.error(`[Gateway] room-sync failed for user ${userId}:`, err.message);
        }
        if (!socket.connected) return;

        // From here on nothing aimed at this socket's rooms can be missed — the
        // browser catches up on anything older (REST, ?after=) when it hears this.
        socket.emit('chat:ready', { rooms: chatIds.length });

        await _markOnline(userId, chatIds);
        _bumpLastSeen(userId);
    })().catch((err) => console.error('[Gateway] connection setup failed:', err.message));
});

// ── Redis Pub/Sub: inbound from Chat Service & Notification Service ───────────
const CH_DELIVER       = 'chat.deliver';
const CH_MEMBER        = 'chat.member';
const CH_NOTIF_COUNT   = 'notification.count';

subClient.subscribe(CH_DELIVER, CH_MEMBER, CH_NOTIF_COUNT, (err) => {
    if (err) {
        console.error('[Gateway] Redis subscribe failed:', err.message);
        return;
    }
    console.log(`✅ Gateway subscribed to [${CH_DELIVER}, ${CH_MEMBER}, ${CH_NOTIF_COUNT}]`);
});

subClient.on('message', (channel, raw) => {
    try {
        const payload = JSON.parse(raw);
        if (channel === CH_DELIVER)     _onDeliver(payload);
        if (channel === CH_MEMBER)      _onMember(payload);
        if (channel === CH_NOTIF_COUNT) _onNotifCount(payload);
    } catch (err) {
        console.error('[Gateway] Redis message parse error:', err.message);
    }
});

/**
 * Forward notification unread count to a specific user's sockets.
 * Payload: { userId, count }
 */
function _onNotifCount({ userId, count }) {
    if (!userId) return;
    io.to(`user:${userId}`).emit('notification:unread_count', { count });
}

/**
 * Deliver an event to a Socket.io room or a specific user's sockets.
 * Payload: { target: 'room'|'user', targetId, event, data }
 */
function _onDeliver({ target, targetId, event, data }) {
    if (!target || !targetId || !event) return;
    io.to(targetId).emit(event, data);
}

/**
 * Tell a user's sockets to join or leave a room.
 * Called when Chat Service creates a new chat or removes a member.
 * Payload: { action: 'join'|'leave', userId, chatId }
 */
function _onMember({ action, userId, chatId }) {
    if (!action || !userId || !chatId) return;
    const room = `chat:${chatId}`;
    // Synchronous on purpose: a new conversation's first message is often in
    // the same Redis read as its join, and an awaited fetchSockets() let that
    // delivery run before the join — the recipient never saw it.
    if (action === 'join')  io.in(`user:${userId}`).socketsJoin(room);
    if (action === 'leave') io.in(`user:${userId}`).socketsLeave(room);
}

// ── Start ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`🔌 WebSocket Gateway running on port ${PORT}`);
    console.log(`   Chat Service : ${CHAT_SERVICE_URL}`);
    console.log(`   CORS origins : ${ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS.join(', ') : '(all)'}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
});
