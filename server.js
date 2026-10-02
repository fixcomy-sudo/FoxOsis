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
 * HTTP: статические файлы — из памяти (opts.files, exe) или с диска (dev)
 * ----------------------------------------------------------------------- */
function makeHandler (opts) {
  const files = opts.files || null            // { '/index.html': Buffer, … }
  const root = opts.root || ROOT
  return (req, res) => {
    let p = decodeURIComponent(req.url.split('?')[0])
    if (p === '/') p = '/index.html'
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
