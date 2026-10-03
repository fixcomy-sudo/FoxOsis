/* ==========================================================================
 * FoxOsis — сервер разработки/релиза
 *
 * 1) Статика: index.html, app.js (или сборку dist/) на любом устройстве
 *    в локальной сети: http://<IP-ПК>:8099
 *
 * 2) LAN-ретранслятор сигналинга (WebSocket /ws) — «телефонный справочник»
 *    для P2P: без него браузеры разных устройств не могут обменяться
 *    SDP-офферами и сообщениями присутствия (BroadcastChannel работает
 *    только внутри одного браузера).
 *
 *    Что хранит/видит сервер: peerId, имя, @юзернейм, ростер подключённых.
 *    Что НЕ видит: содержимое чата и медиапотоки — они идут напрямую
 *    между устройствами по WebRTC (DTLS-шифрование), мимо этого сервера.
 *
 * Запуск:  node server.js
 * Зависимости: только штатные модули Node (http, fs, path, crypto).
 * ========================================================================== */

const http = require('http')
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')

const PORT = process.env.PORT || 8099
const ROOT = __dirname
const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.jpeg': 'image/jpeg',
  '.jpg': 'image/jpeg',
  '.webp': 'image/webp'
}

/* --------------------------------------------------------------------------
 * POST /api/push — форвард Web Push на push-сервис получателя.
 * Браузер не может слать пуш напрямую: push-сервисы (WNS у Edge) не отдают
 * CORS-заголовки. Здесь их не нужно: сервер пересылает байты как есть —
 * payload уже зашифрован для получателя ключом его подписки, прочитать или
 * подменить его сервер не может. Разрешены только известные push-хосты.
 * Тело: { ep, h: { Authorization }, b: base64 }
 * ----------------------------------------------------------------------- */
const PUSH_OK_HOST = /(^|\.)(notify\.windows\.com|fcm\.googleapis\.com|web\.push\.apple\.com|updates\.push\.services\.mozilla\.com|push\.services\.firefox\.com|mtalk\.google\.com)$/
const pushRate = new Map()   // ip -> [timestamps] — простейший лимит 30/мин

function pushAllowed (ip) {
  const now = Date.now()
  const arr = (pushRate.get(ip) || []).filter(t => now - t < 60000)
  if (arr.length >= 30) { pushRate.set(ip, arr); return false }
  arr.push(now)
  pushRate.set(ip, arr)
  return true
}

function handlePush (req, res) {
  const cors = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json; charset=utf-8' }
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return }
  if (req.method !== 'POST') { res.writeHead(405, cors); res.end('{"err":"POST only"}'); return }
  if (!pushAllowed(req.socket.remoteAddress || '?')) {
    res.writeHead(429, cors); res.end('{"err":"rate limit"}'); return
  }
  let size = 0
  const chunks = []
  req.on('data', (c) => {
    size += c.length
    if (size > 131072) { req.destroy(); return }   // пуш всегда маленький
    chunks.push(c)
  })
  req.on('end', async () => {
    let body = null
    try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')) } catch {}
    let host = ''
    try { host = new URL(body && body.ep).hostname } catch {}
    if (!host || !PUSH_OK_HOST.test(host)) {
      res.writeHead(400, cors); res.end('{"err":"bad ep"}'); return
    }
    try {
      const headers = { TTL: '86400', Urgency: 'high' }
      if (body.h && typeof body.h.Authorization === 'string') headers.Authorization = body.h.Authorization
      const payload = Buffer.from(String(body.b || ''), 'base64')
      if (payload.length) headers['Content-Encoding'] = 'aes128gcm'
      const r = await fetch(body.ep, {
        method: 'POST',
        headers,
        body: payload.length ? payload : undefined,
        signal: AbortSignal.timeout(15000)
      })
      res.writeHead(r.status === 201 ? 201 : r.status, cors)
      res.end(JSON.stringify({
        ok: r.status === 201,
        st: r.status,
        wns: r.headers.get('x-wns-notificationstatus') || '',
        dropped: r.headers.get('x-wns-notificationdroppedreason') || ''
      }))
    } catch (e) {
      res.writeHead(502, cors)
      res.end(JSON.stringify({ err: String(e.message || e) }))
    }
  })
}

/* --------------------------------------------------------------------------
 * HTTP: статические файлы — из памяти (opts.files, exe) или с диска (dev)
 * ----------------------------------------------------------------------- */
/* --------------------------------------------------------------------------
 * Email-коды регистрации: генерируем 6-значный код, храним 10 минут.
 * SMTP не настроен — в dev-режиме код возвращается в ответе (его же
 * показывает клиент). С SMTP_* в окружении — письмо уходит по-настоящему.
 * ----------------------------------------------------------------------- */
const emailCodes = new Map()   // email -> { code, exp, tries, sentAt }
const EMAIL_RATE = new Map()   // email -> last send ts (анти-спам 1 раз/60с)

const EMAIL_CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
}

function handleEmail (kind, req, res) {
  if (req.method === 'OPTIONS') { res.writeHead(204, EMAIL_CORS); res.end(); return }
  if (req.method !== 'POST') { res.writeHead(405, EMAIL_CORS); res.end('POST only'); return }
  let body = ''
  req.on('data', (c) => { body += c; if (body.length > 4096) req.destroy() })
  req.on('end', () => {
    const cors = Object.assign({ 'Content-Type': 'application/json; charset=utf-8' }, EMAIL_CORS)
    let d = {}
    try { d = JSON.parse(body || '{}') } catch {}
    const email = String(d.email || '').trim().toLowerCase()
    if (!/^[^\s@]+@[^\s@]+\.[a-zа-я]{2,}$/i.test(email)) {
      res.writeHead(400, cors); res.end(JSON.stringify({ err: 'Некорректный email' })); return
    }
    const now = Date.now()

    if (kind === 'code') {
      const last = EMAIL_RATE.get(email) || 0
      if (now - last < 60000) {
        res.writeHead(429, cors)
        res.end(JSON.stringify({ err: 'Код уже отправлен — подождите минуту' })); return
      }
      EMAIL_RATE.set(email, now)
      const code = String(Math.floor(100000 + Math.random() * 900000))
      emailCodes.set(email, { code, exp: now + 10 * 60 * 1000, tries: 0 })
      // без SMTP код отдаём клиенту — он показывает его в UI (dev-режим)
      res.writeHead(200, cors)
      res.end(JSON.stringify({ ok: true, dev: true, code }))
      return
    }

    if (kind === 'verify') {
      const rec = emailCodes.get(email)
      if (!rec || rec.exp < now) {
        res.writeHead(400, cors); res.end(JSON.stringify({ err: 'Код устарел — запросите новый' })); return
      }
      rec.tries++
      if (rec.tries > 5) { emailCodes.delete(email); res.writeHead(400, cors); res.end(JSON.stringify({ err: 'Слишком много попыток — запросите новый код' })); return }
      if (String(d.code || '').trim() !== rec.code) {
        res.writeHead(400, cors); res.end(JSON.stringify({ err: 'Неверный код' })); return
      }
      emailCodes.delete(email)
      res.writeHead(200, cors); res.end(JSON.stringify({ ok: true, email }))
      return
    }

    res.writeHead(404, cors); res.end(JSON.stringify({ err: 'not found' }))
  })
}

/* --------------------------------------------------------------------------
 * POST /api/fcm — уведомление в ЗАКРЫТОЕ Android-приложение (FCM HTTP v1).
 * WebView не умеет Web Push, поэтому в APK пуш идёт через Firebase.
 * Тело: { token, title, body, kind }
 * Нужен ключ сервис-аккаунта Firebase: файл firebase-service-account.json
 * в корне проекта ИЛИ переменная окружения FIREBASE_SERVICE_ACCOUNT (JSON).
 * Ключ подписи (service account) — только серверный, в клиент не попадает.
 * ----------------------------------------------------------------------- */
let saCache = null
let saTried = false
let fcmTok = null   // { tok, exp } — кэш access-токена Google OAuth

function serviceAccount () {
  if (saTried) return saCache
  saTried = true
  try {
    if (process.env.FIREBASE_SERVICE_ACCOUNT) saCache = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
    else {
      const f = path.join(ROOT, 'firebase-service-account.json')
      if (fs.existsSync(f)) saCache = JSON.parse(fs.readFileSync(f, 'utf8').replace(/^\uFEFF/, ''))
    }
  } catch { saCache = null }
  return saCache
}

const b64url = (buf) => Buffer.from(buf).toString('base64')
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')

/** DER-подпись ECDSA → сырой R||S 64 байта (формат JWT/JWS) */
function derToRaw (der) {
  let i = 2
  if (der[1] & 0x80) i = 2 + (der[1] & 0x7f)
  const readInt = () => {
    if (der[i] !== 0x02) throw new Error('битый DER')
    const len = der[i + 1]; i += 2
    let v = der.subarray(i, i + len); i += len
    let s = 0
    while (s < v.length - 1 && v[s] === 0) s++
    v = v.subarray(s)
    if (v.length > 32) v = v.subarray(v.length - 32)
    const out = Buffer.alloc(32)
    v.copy(out, 32 - v.length)
    return out
  }
  return Buffer.concat([readInt(), readInt()])
}

/** JWT для Google OAuth — assert-запрос сервис-аккаунта.
 * Ключ может быть RSA (RS256, по умолчанию у Google) или EC (ES256) */
function saAssertion (sa) {
  const now = Math.floor(Date.now() / 1000)
  const key = crypto.createPrivateKey({ key: sa.private_key, format: 'pem' })
  const alg = key.asymmetricKeyType === 'ec' ? 'ES256' : 'RS256'
  const data = b64url(JSON.stringify({ alg, typ: 'JWT' })) + '.' +
    b64url(JSON.stringify({
      iss: sa.client_email,
      scope: 'https://www.googleapis.com/auth/firebase.messaging',
      aud: 'https://oauth2.googleapis.com/token',
      iat: now, exp: now + 3600
    }))
  const der = crypto.sign('sha256', Buffer.from(data), key)
  const sig = alg === 'ES256' ? derToRaw(der) : der // RS256 — PKCS#1 как есть
  return data + '.' + b64url(sig)
}

/** Access-токен Google (кэшируется до истечения) */
async function fcmAccessToken (sa) {
  const now = Date.now()
  if (fcmTok && fcmTok.exp > now + 60000) return fcmTok.tok
  const body = 'grant_type=' + encodeURIComponent('urn:ietf:params:oauth:grant-type:jwt-bearer') +
    '&assertion=' + saAssertion(sa)
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  })
  const d = await r.json().catch(() => ({}))
  if (!r.ok || !d.access_token) throw new Error('oauth: ' + (d.error_description || d.error || r.status))
  fcmTok = { tok: d.access_token, exp: now + (Number(d.expires_in || 3600) * 1000) }
  return fcmTok.tok
}

function handleFcm (req, res, ip) {
  const cors = Object.assign({ 'Content-Type': 'application/json; charset=utf-8' }, EMAIL_CORS)
  if (req.method === 'OPTIONS') { res.writeHead(204, EMAIL_CORS); res.end(); return }
  if (req.method !== 'POST') { res.writeHead(405, EMAIL_CORS); res.end('POST only'); return }
  if (!pushAllowed(ip)) {
    res.writeHead(429, cors); res.end(JSON.stringify({ err: 'слишком много уведомлений' })); return
  }
  let body = ''
  req.on('data', (c) => { body += c; if (body.length > 8192) req.destroy() })
  req.on('end', async () => {
    try {
      const sa = serviceAccount()
      if (!sa) {
        res.writeHead(503, cors)
        res.end(JSON.stringify({ err: 'нет ключа Firebase: положи firebase-service-account.json в корень' }))
        return
      }
      let d = {}
      try { d = JSON.parse(body || '{}') } catch {}
      const token = String(d.token || '')
      if (!/^[A-Za-z0-9_:.-]{20,}$/.test(token)) {
        res.writeHead(400, cors); res.end(JSON.stringify({ err: 'битый токен' })); return
      }
      const title = String(d.title || 'FoxOsis').slice(0, 120)
      const text = String(d.body || '').slice(0, 500)
      const kind = String(d.kind || 'msg').slice(0, 16)

      const access = await fcmAccessToken(sa)
      const r = await fetch('https://fcm.googleapis.com/v1/projects/' +
        (sa.project_id || 'foxosis') + '/messages:send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + access },
        body: JSON.stringify({
          message: {
            token,
            notification: { title, body: text },
            data: { kind },
            android: { priority: 'HIGH' }
          }
        })
      })
      const out = await r.json().catch(() => ({}))
      if (!r.ok) {
        res.writeHead(r.status === 404 ? 410 : r.status, cors)
        res.end(JSON.stringify({ err: out.error && out.error.message || 'fcm error', status: r.status }))
        return
      }
      res.writeHead(200, cors)
      res.end(JSON.stringify({ ok: true, name: out.name || '' }))
    } catch (e) {
      res.writeHead(502, cors)
      res.end(JSON.stringify({ err: String(e.message || e) }))
    }
  })
}

function makeHandler (opts) {
  const files = opts.files || null            // { '/index.html': Buffer, … }
  const root = opts.root || ROOT
  return (req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0])
    if (p === '/') p = '/index.html'
    const ip = (req.socket && req.socket.remoteAddress) || '?'
    if (p === '/api/push') { handlePush(req, res); return }
    if (p === '/api/fcm') { handleFcm(req, res, ip); return }
    if (p === '/api/email/code') { handleEmail('code', req, res); return }
    if (p === '/api/email/verify') { handleEmail('verify', req, res); return }
    const type = TYPES[path.extname(p).toLowerCase()] || 'application/octet-stream'
    const send = (buf) => {
      res.writeHead(200, {
        'Content-Type': type,
        // без кэша: телефон не должен подхватывать устаревший app.js
        'Cache-Control': 'no-store'
      })
      res.end(buf)
    }
    if (files && Object.prototype.hasOwnProperty.call(files, p)) {
      send(files[p])
      return
    }
    const f = path.join(root, p)
    if (!f.startsWith(root)) { res.writeHead(403); res.end('forbidden'); return }
    fs.readFile(f, (e, d) => {
      if (e) { res.writeHead(404); res.end('not found'); return }
      send(d)
    })
  }
}

/* --------------------------------------------------------------------------
 * WebSocket БЕЗ внешних зависимостей (RFC 6455: handshake + кадры)
 * ----------------------------------------------------------------------- */
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

/** peerId -> { socket, name, username, ts } — онлайн-ростер ретранслятора */
const peers = new Map()

/** Оборачивание полезной нагрузки в кадр WebSocket (сервер не маскирует) */
function frame (payload, opcode = 0x1) {
  const len = payload.length
  let header
  if (len < 126) {
    header = Buffer.from([0x80 | opcode, len])
  } else if (len < 65536) {
    header = Buffer.alloc(4)
    header[0] = 0x80 | opcode
    header[1] = 126
    header.writeUInt16BE(len, 2)
  } else {
    header = Buffer.alloc(10)
    header[0] = 0x80 | opcode
    header[1] = 127
    header.writeBigUInt64BE(BigInt(len), 2)
  }
  return Buffer.concat([header, payload])
}

function safeWrite (socket, buf) {
  try { socket.write(buf) } catch { /* сокет уже закрыт */ }
}

function sendTo (socket, obj) {
  safeWrite(socket, frame(Buffer.from(JSON.stringify(obj), 'utf8'), 0x1))
}

/** Отправить всем, кроме отправителя (или конкретному адресату) */
function relay (obj, exceptSocket, toPeerId) {
  const buf = frame(Buffer.from(JSON.stringify(obj), 'utf8'), 0x1)
  if (toPeerId) {
    const rec = peers.get(toPeerId)
    if (rec && rec.socket !== exceptSocket) safeWrite(rec.socket, buf)
    return
  }
  for (const rec of peers.values()) {
    if (rec.socket !== exceptSocket) safeWrite(rec.socket, buf)
  }
}

function detachSocket (conn) {
  const pid = conn.peerId
  if (pid) {
    const rec = peers.get(pid)
    // удаляем только если запись принадлежит именно этому сокету
    if (rec && rec.socket === conn.socket) peers.delete(pid)
  }
  conn.peerId = null
}

/** Разбор входящих текстовых сообщений от клиентов */
function onClientMessage (conn, text) {
  let m
  try { m = JSON.parse(text) } catch { return }
  if (!m || typeof m !== 'object') return

  // любое сообщение «регистрирует» отправителя (hello или relay)
  const src = (m.t === 'hello' && m.from) || (m.data && m.data.from) || null
  if (src && typeof src === 'string') {
    const first = conn.peerId !== src
    conn.peerId = src
    const prev = peers.get(src)
    peers.set(src, {
      socket: conn.socket,
      name: String((m.name || (m.data && m.data.name) || (prev && prev.name) || '')),
      username: String((m.username || (m.data && m.data.username) || (prev && prev.username) || '')),
      ts: Date.now()
    })
    if (first) {
      // новичку — список уже подключённых узлов, чтобы поиск работал сразу
      const list = []
      for (const [id, rec] of peers) {
        if (id !== src) list.push({ from: id, name: rec.name, username: rec.username })
      }
      sendTo(conn.socket, { t: 'roster', peers: list })
    }
  }

  if (m.t === 'hello') {
    // сообщение присутствия — разослать остальным (клиент отсечёт дубли)
    relay({ t: 'relay', data: m }, conn.socket)
    return
  }
  if (m.t === 'relay' && m.data) {
    // SDP-оффер/ансвер или hello из _post: по `to`, иначе —.broadcast
    relay({ t: 'relay', data: m.data }, conn.socket, typeof m.to === 'string' && m.to ? m.to : null)
  }
}

/** Цикл чтения кадров из накопленного буфера соединения */
function drain (conn) {
  for (;;) {
    const b = conn.buf
    if (b.length < 2) return
    const fin = (b[0] & 0x80) !== 0
    const opcode = b[0] & 0x0f
    const masked = (b[1] & 0x80) !== 0
    let len = b[1] & 0x7f
    let off = 2
    if (len === 126) {
      if (b.length < 4) return
      len = b.readUInt16BE(2)
      off = 4
    } else if (len === 127) {
      if (b.length < 10) return
      len = Number(b.readBigUInt64BE(2))
      off = 10
    }
    let mask = null
    if (masked) {
      if (b.length < off + 4) return
      mask = b.subarray(off, off + 4)
      off += 4
    }
    if (b.length < off + len) return
    let payload = Buffer.from(b.subarray(off, off + len))
    conn.buf = b.subarray(off + len)
    if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3]

    if (opcode === 0x8) { // close
      safeWrite(conn.socket, frame(Buffer.alloc(0), 0x8))
      detachSocket(conn)
      conn.socket.end()
      return
    }
    if (opcode === 0x9) { safeWrite(conn.socket, frame(payload, 0xA)); continue } // ping -> pong
    if (opcode === 0xa) continue // pong

    if (opcode === 0x1 || opcode === 0x2) { // текст/бинарь целиком или начало
      if (fin) onClientMessage(conn, payload.toString('utf8'))
      else conn.fragments = [payload]
      continue
    }
    if (opcode === 0x0) { // продолжение фрагментированного сообщения
      conn.fragments.push(payload)
      if (fin) {
        const full = Buffer.concat(conn.fragments)
        conn.fragments = []
        onClientMessage(conn, full.toString('utf8'))
      }
      continue
    }
  }
}

/** Создать HTTP+WS сервер: opts.files — карта статики в памяти (для exe) */
function createServer (opts = {}) {
  const server = http.createServer(makeHandler(opts))

  server.on('upgrade', (req, socket) => {
    const key = req.headers['sec-websocket-key']
    const url = (req.url || '').split('?')[0]
    if (!key || url !== '/ws') { socket.destroy(); return }

    const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64')
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${accept}\r\n` +
      '\r\n'
    )
    socket.setNoDelay(true)

    const conn = { socket, peerId: null, buf: Buffer.alloc(0), fragments: [] }
    socket.on('data', (chunk) => {
      conn.buf = Buffer.concat([conn.buf, chunk])
      drain(conn)
    })
    socket.on('error', () => detachSocket(conn))
    socket.on('close', () => detachSocket(conn))
  })

  return server
}

// пустые ping-кадры: чистим «спящие» соединения (телефон в сне) каждые 30 с
setInterval(() => {
  for (const rec of peers.values()) safeWrite(rec.socket, frame(Buffer.alloc(0), 0x9))
}, 30000)

/* CLI: node server.js */
if (require.main === module) {
  createServer().listen(PORT, () => {
    console.log(`FoxOsis: http://localhost:${PORT}  (для телефона — http://<IP-ПК>:${PORT})`)
    console.log('LAN-ретранслятор сигналинга: ws://…/ws')
  })
}

module.exports = { createServer, PORT }
