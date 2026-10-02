/**
 * ============================================================================
 * FoxOsis — каркас децентрализованного мессенджера
 * ============================================================================
 * Архитектура (всё в одном модуле, разбито на логические секции):
 *
 *   1.  ИМПОРТЫ / КОНФИГ      — libp2p и помощники (ESM с esm.sh)
 *   2.  УТИЛИТЫ                — кодирование, время, таймеры, WebRTC-хелперы
 *   3.  Store                  — хранение профиля и чатов в localStorage
 *   4.  KeyStore               — стабильный Ed25519-ключ узла (мой "логин")
 *   5.  UI                     — весь DOM: список «Недавние», чат, звонки
 *   6.  JsonLineStream         — фрейминг JSON-сообщений поверх libp2p-потоков
 *   7.  FoxMaConn/FoxTransport — свой транспорт libp2p поверх WebRTC DataChannel
 *   8.  Link primitives        — создание WebRTC-оффер/ансвер (сигналинг данных)
 *   9.  Rendezvous             — поиск собеседников: BC + LAN + ГЛОБАЛЬНЫЙ Nostr
 *   9.5 Reconnector            — автопереподключение P2P после обрыва сети
 *   10. Codes                  — подключение по коду (для разных устройств)
 *   11. Chat/Signal            — протоколы чата и сигналинга звонков поверх libp2p
 *   12. Presence               — присутствие через GossipSub (PubSub)
 *   13. Call                   — аудио/видео звонки (WebRTC + STUN/TURN)
 *   14. Boot                   — запуск узла и привязка событий
 *
 * КАК ЭТО РАБОТАЕТ БЕЗ СЕРВЕРОВ:
 *   - Каждый узел создаёт собственный libp2p-узел с Ed25519-ключом (Peer ID).
 *   - Два узла на одной машине находят друг друга через BroadcastChannel
 *     (это аналог "поиска по имени" — Peer ID скрыт от пользователя).
 *   - Для установки P2P-соединения без серверов используется обмен WebRTC
 *     оффер/ансвер через тот же BroadcastChannel (или вручную — по коду).
 *   - ГЛОБАЛЬНАЯ СЕТЬ: обмен сигналингом дублируется через публичные
 *     Nostr-релеи (эфемерные события, подписка на тег своего Peer ID) —
 *     поиск по @юзернейму работает между любыми странами/сетями;
 *     STUN-кандидаты нескольких серверов + бесплатный TURN (openrelay)
 *     пробивают симметричные NAT. Медиа/переписка остаются прямым P2P.
 *   - После открытия WebRTC DataChannel поверх него поднимается полноценное
 *     соединение libp2p (Noise-шифрование + Yamux-мультиплексирование):
 *     работают протокольные потоки, PubSub, идентификация — всё как в сети.
 *   - Аудио/видео звонок — отдельное RTCPeerConnection; его сигналинг
 *     (SDP оффер/ансвер) передаётся ВНУТРИ открытого канала libp2p.
 * ============================================================================
 */

/* ==========================================================================
 * 1. ИМПОРТЫ И КОНФИГ
 * ========================================================================== */

// Ядро libp2p: P2P-стек (транспорты, шифрование, мультиплексирование, сервисы)
import { createLibp2p } from 'https://esm.sh/libp2p@3.3.11'
// Noise — обмен ключами и шифрование каждого соединения (аутентификация Peer ID)
import { noise } from 'https://esm.sh/@libp2p/noise@17.0.3'
// Yamux — мультиплексирование: много логических потоков поверх одного соединения
import { yamux } from 'https://esm.sh/@libp2p/yamux@8.0.3'
// Identify — обмен информацией о поддерживаемых протоколах между парами
import { identify } from 'https://esm.sh/@libp2p/identify@4.1.14'
// GossipSub — реализация PubSub для тем (общий эфир/комнаты/присутствие)
import { gossipsub } from 'https://esm.sh/@libp2p/gossipsub@17.1.2'
// crypto.keys — генерация/сериализация Ed25519-ключей узла
import { keys } from 'https://esm.sh/@libp2p/crypto@5.1.23'
// multiaddr — парсинг сетевых адресов libp2p
import { multiaddr } from 'https://esm.sh/@multiformats/multiaddr@13.0.3'
// Базовый класс соединения транспорта (реализует буферизацию и события потока)
import { AbstractMultiaddrConnection } from 'https://esm.sh/@libp2p/utils@7.4.1'
// schnorr (BIP-340) — подпись NIP-01 событий для публичных Nostr-релеев
import { schnorr } from 'https://esm.sh/@noble/curves@1.4.0/secp256k1'

/** Глобальные символы libp2p (Symbol.for — они общие для всех копий библиотеки) */
const transportSymbol = Symbol.for('@libp2p/transport')
const serviceCapabilities = Symbol.for('@libp2p/service-capabilities')

/* -------------------------------------------------------------------------
 * Идентификатор «устройства» текущей вкладки.
 * Проблема: две вкладки одного origin делят один localStorage — а значит один
 * Ed25519-ключ и один профиль. Одинаковый Peer ID = rendezvous принимает
 * чужие сообщения за свои (from === selfId()) и «не находит» собеседника
 * ни поиском, ни кодом. Решение: все ключи хранилища пространственно-разделены
 * по id вкладки. Каждая вкладка = отдельный узел сети.
 * id хранится в ДВУХ каналах: sessionStorage (основной) и window.name
 * (запасной): часть мобильных браузеров чистит sessionStorage при
 * восстановлении/перезагрузке вкладки, а window.name переживает её.
 * ------------------------------------------------------------------------- */
const INST_ID = (() => {
  const gen = () => (typeof crypto !== 'undefined' && crypto.randomUUID)
    ? crypto.randomUUID()
    : Date.now().toString(16) + Math.random().toString(16).slice(2)
  const fromName = () => {
    const m = /^foxosis#(.+)$/.exec(window.name || '')
    return m ? m[1] : null
  }
  const toSession = (id) => { try { sessionStorage.setItem('foxosis.inst', id); return true } catch { return false } }
  const fromSession = () => { try { return sessionStorage.getItem('foxosis.inst') } catch { return null } }

  // НАТИВНОЕ ПРИЛОЖЕНИЕ (Capacitor: APK/IPA): WebView при перезапуске стирает
  // sessionStorage и window.name — «вкладочный» id менялся бы каждый раз:
  // аккаунт и чаты «вылетали», а пир выглядел новым устройством. В приложении
  // вкладок нет — id храним в localStorage (переживает перезапуск).
  const w = typeof window !== 'undefined' ? window : null
  const isNative = !!(w && (
    w.androidBridge ||                                   // Android: JavascriptInterface
    (w.webkit && w.webkit.messageHandlers && w.webkit.messageHandlers.capacitor) || // iOS WKWebView
    (w.Capacitor && typeof w.Capacitor.isNativePlatform === 'function' && w.Capacitor.isNativePlatform())
  ))
  if (isNative) {
    try {
      let id = localStorage.getItem('foxosis/inst-id')
      if (!id) { id = gen(); localStorage.setItem('foxosis/inst-id', id) }
      return id
    } catch { return 'default' }
  }

  try {
    let id = fromSession() || fromName()
    if (id) {
      // поддерживаем оба канала синхронно (второй мог потеряться)
      toSession(id)
      try { window.name = 'foxosis#' + id } catch {}
      return id
    }
    id = gen()
    const sOk = toSession(id)
    try { window.name = 'foxosis#' + id } catch {}
    // проверка: каналы молча игнорируют записи — тогда живём на 'default'
    if (!sOk && fromSession() !== id && fromName() !== id) return 'default'
    return id
  } catch {
    return fromName() || 'default' // хранилище недоступно — запасной вариант
  }
})()

/** Ключ localStorage с учётом экземпляра вкладки */
const instKey = (base) => `foxosis/inst/${INST_ID}/${base}`

// Нативное приложение: раз INST_ID раньше менялся при каждом перезапуске,
// старые хранилища лежат под ключами foxosis/inst/<случайный>/... — переносим
// самое свежее «живое» аккаунт-хранилище (и ключи) под стабильный id,
// иначе после обновления приложение снова покажет пустой аккаунт.
;(() => {
  const isNative = typeof window !== 'undefined' && !!(window.androidBridge ||
    (window.webkit && window.webkit.messageHandlers && window.webkit.messageHandlers.capacitor) ||
    (window.Capacitor && typeof window.Capacitor.isNativePlatform === 'function' && window.Capacitor.isNativePlatform()))
  if (!isNative) return
  const prefix = 'foxosis/inst/'
  try {
    // 1) хранилище мессенджера (профиль + чаты): берём САМОЕ НОВОЕ с авторизацией
    const target = instKey('store/v1')
    if (!localStorage.getItem(target)) {
      const cands = []
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i)
        if (k && k.startsWith(prefix) && k.endsWith('/store/v1') && k !== target) cands.push(k)
      }
      for (let pass = 0; pass < 2 && !localStorage.getItem(target); pass++) {
        // сначала ищем с авторизованным профилем (с конца — конец = свежее),
        // затем просто самое свежее непустое хранилище
        for (let i = cands.length - 1; i >= 0; i--) {
          const raw = localStorage.getItem(cands[i])
          if (!raw) continue
          let data = null
          try { data = JSON.parse(raw) } catch { continue }
          if (!data) continue
          const authed = data.profile && data.profile.authed
          const hasChats = data.chats && Object.keys(data.chats).length
          if (pass === 0 ? authed : (authed || hasChats)) {
            localStorage.setItem(target, raw)
            break
          }
        }
      }
    }
    // 2) ключи libp2p (Ed25519) и Nostr — переносим самое свежее значение
    for (const base of ['key/v1', 'nostr/v1']) {
      const t = instKey(base)
      if (localStorage.getItem(t)) continue
      for (let i = localStorage.length - 1; i >= 0; i--) {
        const k = localStorage.key(i)
        if (k && k.startsWith(prefix) && k.endsWith('/' + base) && k !== t) {
          const v = localStorage.getItem(k)
          if (v) { localStorage.setItem(t, v); break }
        }
      }
    }
  } catch { /* миграция не обязательна — без неё просто пусто */ }
})()

/** Конфигурация приложения — все "магические числа" собраны в одном месте */
const CFG = {
  // Протоколы приложения (handshake multistream-select по этим идентификаторам)
  PROTO_CHAT: '/foxosis/chat/1.0.0',      // текстовый чат (JSON-строки)
  PROTO_SIGNAL: '/foxosis/signal/1.0.0',  // сигналинг звонков (SDP и события)

  // "Виртуальный" адрес нашего транспорта: реальные сокеты НЕ открываются,
  // номер порта используется только как метка для фильтрации адресов libp2p.
  LISTEN_PORT: 40217,
  LISTEN_ADDR: '/ip4/0.0.0.0/tcp/40217',

  // Локальный rendezvous: канал BroadcastChannel для поиска узлов одной машины
  BC_CHANNEL: 'foxosis-rendezvous-v1',
  // LAN-ретранслятор (server.js): WebSocket-путь для поиска МЕЖДУ устройствами
  WS_PATH: '/ws',
  HELLO_MS: 2500,      // период рассылки "я здесь" (мс)
  HELLO_TTL: 8000,     // через сколько мс считать узел офлайном (мс)

  // Аватар профиля: квадратное фото, сжатое до webp-дата-URL.
  // Шлётся отдельным сообщением t:'ava' (один раз при смене/подключении),
  // НЕ в каждом hello — чтобы не раздувать периодическую рассылку.
  AVA_SIZE: 96,        // px, сторона квадрата аватара
  AVA_QUALITY: 0.72,   // качество webp

  // Топик PubSub для демонстрации/расширения (общий эфир, комнаты)
  TOPIC_PRESENCE: 'foxosis/presence',

  // Ключи и данные в localStorage (разделены по вкладкам — см. INST_ID)
  KEY_STORAGE: instKey('key/v1'),
  STORE_KEY: instKey('store/v1'),
  // старые общие ключи (до разделения по вкладкам) — мигрируем при загрузке
  LEGACY_KEY_STORAGE: 'foxosis/key/v1',
  LEGACY_STORE_KEY: 'foxosis/store/v1',
  MAX_MESSAGES: 300,   // сколько сообщений храним на чат (старые отрезаем)

  // Тайминги
  CONNECT_TIMEOUT_MS: 10000,  // ожидание открытия WebRTC-канала
  ICE_GATHER_MS: 5000,        // сколько ждать сбора ICE-кандидатов (TURN дольше)
  SIG_RETRY_MS: 1500,         // повтор оффера/ансвера, пока не открыт канал
  SIG_COMPRESS_MIN: 700,      // JSON длиннее — сжимаем (deflate-raw):
                              // сжатый SDP ~в 3 раза меньше
  KEEPALIVE_MS: 60000,        // период "пингов" для поддержания потоков

  // ---- ГЛОБАЛЬНАЯ СЕТЬ (интернет, разные NAT) ----------------------------
  // STUN: несколько независимых публичных серверов (если один заблокан —
  // возьмётся reflexive-кандидат другого). Проверяются вживую при разработке.
  // TURN: бесплатный openrelay (тестовый) — даёт relay-кандидаты, когда обе
  // стороны за Symmetric NAT и прямое соединение невозможно. В продакшене
  // замените на свой Xirsys/Metered — публичный TURN перегружен и медленный.
  ICE_SERVERS: [
    {
      urls: [
        'stun:stun.l.google.com:19302',
        'stun:stun1.l.google.com:19302',
        'stun:stun.cloudflare.com:3478',
        'stun:global.stun.twilio.com:3478'
      ]
    },
    {
      urls: [
        'turn:openrelay.metered.ca:80?transport=udp',
        'turn:openrelay.metered.ca:443?transport=tcp',
        'turns:openrelay.metered.ca:443?transport=tcp'
      ],
      username: 'openrelayproject',
      credential: 'openrelayproject'
    }
  ],

  // Публичные Nostr-релеи — ГЛОБАЛЬНЫЙ сигнальный канал (вместо MQTT).
  // Ищем собеседника по @юзернейму из любой точки мира без своего сервера:
  // подписка REQ по тегу t = свой Peer ID, hello — в общий тег. Все релея
  // держим ПАРАЛЛЕЛЬНО (дубли отсекает mid), при обрыве — свой backoff.
  // Проверено вживую: полный круг NIP-01 (подпись → EVENT → приём подпиской)
  // на всех трёх; 14 hello @2.5 с и burst офферов 8 @1.5 с — без провалов.
  // Эфемерный kind 21337: релея ретранслируют, но НЕ хранят события —
  // история не накапливается, повторная подписка чиста.
  NOSTR_RELAYS: [
    'wss://relay.primal.net',
    'wss://nos.lol',
    'wss://nostr.bitcoiner.social'
  ],
  NOSTR_ROOT: 'foxosis/v1',    // корень тегов приложения
  NOSTR_KIND: 21337,           // эфемерный kind (20000–29999) — без хранения
  UNAME_KIND: 30078,           // адресный (d-тег) — релеи ХРАНЯТ: заявка @юзернейма
  NOSTR_BACKOFF_MS: [1000, 2000, 5000, 15000, 30000], // паузы между попытками

  // Дополнительные СВОИ ретрансляторы server.js в интернете (VPS) — полные
  // ws/wss-адреса. Плюс разовый оверрайд: localStorage['foxosis/relay'].
  RELAY_URLS: [],

  // Автопереподключение P2P-сессий после обрыва связи (моргание сети)
  RECONNECT_MIN_MS: 2000,     // первая попытка через 2 с
  RECONNECT_MAX_MS: 30000,    // потолок backoff — 30 с
  RECONNECT_TRIES: 8          // сколько попыток подряд, пока узел «в сети»
}

/** Полный multiaddr собеседника для нашего транспорта */
const foxAddr = (peerStr) => `${CFG.LISTEN_ADDR}/p2p/${peerStr}`

/* ==========================================================================
 * 2. УТИЛИТЫ
 * ========================================================================== */

const ENCODER = new TextEncoder()
const DECODER = new TextDecoder()

/** Лог в журнал события (модалка профиля) + консоль разработчика */
function log (text, cls = '') {
  console.log('[FoxOsis]', text)
  try {
    const el = document.getElementById('log')
    if (!el) return
    const line = document.createElement('div')
    line.className = 'l ' + cls
    line.textContent = `${new Date().toLocaleTimeString()}  ${text}`
    el.appendChild(line)
    el.scrollTop = el.scrollHeight
    while (el.childElementCount > 300) el.removeChild(el.firstChild)
  } catch { /* журнала ещё нет — тихо игнорируем */ }
}

/** Короткое представление Peer ID: 12D3KooW…x7f2 */
function shortId (id) {
  if (!id) return '?'
  return id.length > 18 ? `${id.slice(0, 10)}…${id.slice(-4)}` : id
}

/* ---- сжатие сигналинга (сжатый SDP заметно меньше в публичном канале) ---- */

/** Uint8Array → base64 чанками (без переполнения стека) */
function bytesToB64 (bytes) {
  let s = ''
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000))
  }
  return btoa(s)
}

/** base64 → Uint8Array */
function b64ToBytes (b64) {
  const s = atob(b64)
  const out = new Uint8Array(s.length)
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i)
  return out
}

/** Сжать строку (deflate-raw). null — если сжатие недоступно/не выгодно. */
async function deflateText (text) {
  if (typeof CompressionStream === 'undefined') return null
  try {
    const stream = new Blob([text]).stream().pipeThrough(new CompressionStream('deflate-raw'))
    const buf = await new Response(stream).arrayBuffer()
    const b64 = bytesToB64(new Uint8Array(buf))
    return b64.length < text.length ? b64 : null
  } catch { return null }
}

/** Распаковать то, что сделал deflateText. null — не удалось. */
async function inflateText (b64) {
  if (typeof DecompressionStream === 'undefined') return null
  try {
    const stream = new Blob([b64ToBytes(b64)]).stream().pipeThrough(new DecompressionStream('deflate-raw'))
    return await new Response(stream).text()
  } catch { return null }
}

/** Генератор отложенных промисов (resolve/reject наружу) */
function createDeferred () {
  let resolve, reject
  const promise = new Promise((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

/** Ограничивает ожидание промиса таймаутом */
function withTimeout (promise, ms, label = 'Ожидание') {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label}: превышено время (${ms} мс)`)), ms)
    promise.then(
      v => { clearTimeout(timer); resolve(v) },
      e => { clearTimeout(timer); reject(e) }
    )
  })
}

/** base64url для хранения ключей и кодов (без символов +/ =) */
function b64encode (bytes) {
  let s = ''
  for (const b of bytes) s += String.fromCharCode(b)
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
function b64decode (str) {
  let s = str.replace(/-/g, '+').replace(/_/g, '/')
  if (s.length % 4 !== 0) s += '='.repeat(4 - (s.length % 4))
  const bin = atob(s)
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}

/** Дождаться сбора ICE-кандидатов (не-trickle: кандидаты прямо в SDP) */
function waitIceGathering (pc, timeoutMs = CFG.ICE_GATHER_MS) {
  if (pc.iceGatheringState === 'complete') return Promise.resolve()
  return new Promise(resolve => {
    const done = () => { clearTimeout(timer); pc.removeEventListener('icegatheringstatechange', onState); resolve() }
    const onState = () => { if (pc.iceGatheringState === 'complete') done() }
    const timer = setTimeout(done, timeoutMs) // STUN может молчать — не ждём вечно
    pc.addEventListener('icegatheringstatechange', onState)
  })
}

/** Формат времени сообщения: 14:32 */
function fmtClock (ts) {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}
/** Подпись дня для ленты: Сегодня / Вчера / 14.03.2026 */
function dayKey (ts) { const d = new Date(ts); return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}` }
function fmtDaySep (ts) {
  const now = new Date(); const d = new Date(ts)
  const yest = new Date(now); yest.setDate(now.getDate() - 1)
  if (dayKey(ts) === dayKey(now.getTime())) return 'Сегодня'
  if (dayKey(ts) === dayKey(yest.getTime())) return 'Вчера'
  return d.toLocaleDateString()
}
/** Время в строке списка чатов: 14:32 / Вчера / 14.03 */
function fmtRowTime (ts) {
  const now = new Date(); const d = new Date(ts)
  const yest = new Date(now); yest.setDate(now.getDate() - 1)
  if (dayKey(ts) === dayKey(now.getTime())) return fmtClock(ts)
  if (dayKey(ts) === dayKey(yest.getTime())) return 'Вчера'
  return `${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}`
}

/** Инициалы для аватарки: "Алиса" -> "А", "Иван Петров" -> "ИП" */
function initials (name) {
  const parts = String(name || '?').trim().split(/\s+/).filter(Boolean)
  if (parts.length === 0) return '?'
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase()
  return (parts[0][0] + parts[1][0]).toUpperCase()
}
/** Устойчивый цвет аватарки по строке (id или имени) */
const AVATAR_COLORS = ['#e17076', '#f5a623', '#7bc862', '#65aadd', '#a695e7', '#ee7aae', '#6ec9cb', '#faa774']
function colorFor (str) {
  let h = 0
  for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) >>> 0
  return AVATAR_COLORS[h % AVATAR_COLORS.length]
}
/** id сообщения */
function rndId () { return (self.crypto?.randomUUID?.() || Math.random().toString(36).slice(2)) }

/** Показать на элементе-аватарке картинку (dataURL) или убрать её —
 *  тогда останется буквенно-цветная заглушка (letters+colorFor). */
function applyAva (el, avaUrl) {
  if (avaUrl && typeof avaUrl === 'string' && avaUrl.startsWith('data:image/')) {
    el.style.backgroundImage = `url("${avaUrl}")`
    el.style.backgroundSize = 'cover'
    el.style.backgroundPosition = 'center'
  } else {
    el.style.backgroundImage = 'none'
  }
}

/** Сжать выбранное фото в квадратный webp dataURL ≤ 3.5 КБ.
 *  Центральный кроп, убывающие размеры/качество — пока не уместимся
 *  в лимит, который брокер стабильно пропускает (до 4 КБ проверено). */
async function compressAva (file) {
  const url = URL.createObjectURL(file)
  try {
    const img = await new Promise((res, rej) => {
      const i = new Image()
      i.onload = () => res(i)
      i.onerror = () => rej(new Error('не удалось прочитать изображение'))
      i.src = url
    })
    if (!img.width || !img.height) throw new Error('пустое изображение')
    let best = ''
    const tries = [
      [CFG.AVA_SIZE, CFG.AVA_QUALITY],
      [CFG.AVA_SIZE, 0.6],
      [72, 0.6],
      [72, 0.5],
      [56, 0.5]
    ]
    for (const [size, quality] of tries) {
      const canvas = document.createElement('canvas')
      canvas.width = canvas.height = size
      const ctx = canvas.getContext('2d')
      const s = Math.min(img.width, img.height) // центральный квадратный кроп
      ctx.drawImage(img, (img.width - s) / 2, (img.height - s) / 2, s, s, 0, 0, size, size)
      let d = canvas.toDataURL('image/webp', quality)
      if (!d.startsWith('data:image/webp')) d = canvas.toDataURL('image/jpeg', quality)
      if (!best || d.length < best.length) best = d
      if (d.length <= 3500) return d
    }
    return best
  } finally {
    URL.revokeObjectURL(url)
  }
}

/* ==========================================================================
 * 3. Store — профиль и история чатов в localStorage («Недавние»)
 * ========================================================================== */

const Store = {
  data: {
    profile: { name: '', username: '', passHash: '', authed: false, ava: '' },
    chats: {},
    lastPeer: null
  },
  lastSaveOk: true, // false = браузер запретил запись (проверяет Auth)

  /** Загрузка из localStorage (с защитой от повреждённых данных) */
  load () {
    try {
      let raw = localStorage.getItem(CFG.STORE_KEY)
      // миграция старых общих данных: первый, кто открыл вкладку, забирает
      // их себе (иначе две вкладки снова делили бы один профиль)
      if (!raw) {
        const legacy = localStorage.getItem(CFG.LEGACY_STORE_KEY)
        if (legacy) {
          localStorage.setItem(CFG.STORE_KEY, legacy)
          localStorage.removeItem(CFG.LEGACY_STORE_KEY)
          raw = legacy
          log('данные профиля и чатов мигрированы в хранилище вкладки', 'sys')
        }
      }
      if (raw) {
        const parsed = JSON.parse(raw)
        this.data = Object.assign(this.data, parsed)
        this.data.profile = Object.assign(
          { name: '', username: '', passHash: '', authed: false, ava: '' },
          this.data.profile || {}
        )
        this.data.chats = this.data.chats || {}
      }
    } catch (e) {
      log('не удалось прочитать хранилище: ' + e.message, 'warn')
      this.data = { profile: { name: '', username: '', passHash: '', authed: false }, chats: {}, lastPeer: null }
    }
  },

  /** Сохранение (вызывается после каждой мутации).
   *  Возвращает false, если браузер запретил запись (приватный режим,
   *  встроенный браузер, переполнение) — вызывающий код обязан предупредить
   *  пользователя, что данные не переживут перезагрузку. */
  save () {
    try {
      localStorage.setItem(CFG.STORE_KEY, JSON.stringify(this.data))
      this.lastSaveOk = true
      return true
    } catch (e) {
      this.lastSaveOk = false
      log('НЕ СОХРАНЕНО (браузер запретил запись): ' + e.message, 'err')
      return false
    }
  },

  /** Ленивое создание записи чата для собеседника */
  ensureChat (peerId, name, username) {
    let chat = this.data.chats[peerId]
    if (!chat) {
      chat = this.data.chats[peerId] = {
        name: name || shortId(peerId),
        username: username || '',
        ava: '',
        messages: [],
        unread: 0,
        ts: Date.now()
      }
      this.save()
    } else {
      let changed = false
      if (name && chat.name !== name) { chat.name = name; changed = true }
      if (username && chat.username !== username) { chat.username = username; changed = true }
      if (changed) this.save()
    }
    return chat
  },

  /** Обновить отображаемое имя/юзернейм собеседника (presence/'hi') */
  renameContact (peerId, name, username) {
    if (!name && !username) return
    const chat = this.data.chats[peerId]
    if (chat) {
      let changed = false
      if (name && chat.name !== name) { chat.name = name; changed = true }
      if (username && chat.username !== username) { chat.username = username; changed = true }
      if (changed) this.save()
    } else if (name) this.ensureChat(peerId, name, username)
  },

  /** Добавить сообщение в историю (с отсечкой старых) */
  appendMessage (peerId, msg) {
    const chat = this.ensureChat(peerId)
    chat.messages.push(msg)
    if (chat.messages.length > CFG.MAX_MESSAGES) {
      chat.messages = chat.messages.slice(-CFG.MAX_MESSAGES)
    }
    chat.ts = msg.ts || Date.now()
    this.save()
    return msg
  },

  /** Найти сообщение по id (для обновления галочек доставки/прочтения) */
  findMessage (peerId, msgId) {
    const chat = this.data.chats[peerId]
    return chat ? chat.messages.find(m => m.id === msgId) : null
  },

  setProfileName (name) {
    this.data.profile.name = name
    this.save()
  },

  /** Полное сохранение профиля (регистрация/вход) */
  setProfile ({ name, username, passHash, authed }) {
    if (name !== undefined) this.data.profile.name = name
    if (username !== undefined) this.data.profile.username = username
    if (passHash !== undefined) this.data.profile.passHash = passHash
    if (authed !== undefined) this.data.profile.authed = authed
    this.save()
  },

  /** Наш аватар (dataURL). '' = включить буквенно-цветную заглушку. */
  setOwnAva (dataUrl) {
    this.data.profile.ava = typeof dataUrl === 'string' ? dataUrl : ''
    this.save()
  },

  /** Аватар собеседника из входящего t:'ava'. Без чата — создаём запись. */
  setContactAva (peerId, dataUrl) {
    if (!peerId || typeof dataUrl !== 'string') return
    const chat = this.ensureChat(peerId)
    if (chat.ava === dataUrl) return
    chat.ava = dataUrl
    this.save()
  }
}

/* ==========================================================================
 * 4. KeyStore — Ed25519-ключ узла: наш "луковый логин", живёт в localStorage
 * ========================================================================== */

const KeyStore = {
  async load () {
    try {
      let stored = localStorage.getItem(CFG.KEY_STORAGE)
      // миграция старого общего ключа: его забирает первая открывшаяся вкладка —
      // так две вкладки больше не получают одинаковый Peer ID
      if (!stored) {
        const legacy = localStorage.getItem(CFG.LEGACY_KEY_STORAGE)
        if (legacy) {
          localStorage.setItem(CFG.KEY_STORAGE, legacy)
          localStorage.removeItem(CFG.LEGACY_KEY_STORAGE)
          stored = legacy
          log('ключ узла мигрирован в хранилище вкладки', 'sys')
        }
      }
      if (stored) {
        const priv = keys.privateKeyFromProtobuf(b64decode(stored))
        if (priv.type === 'Ed25519') {
          log('ключ узла загружен из localStorage', 'sys')
          return priv
        }
      }
    } catch (e) {
      log('сохранённый ключ повреждён, создаём новый: ' + e.message, 'warn')
    }
    const priv = await keys.generateKeyPair('Ed25519')
    log('сгенерирован новый Ed25519-ключ узла', 'sys')
    return priv
  },

  save (priv) {
    try {
      localStorage.setItem(CFG.KEY_STORAGE, b64encode(keys.privateKeyToProtobuf(priv)))
    } catch (e) {
      log('не удалось сохранить ключ: ' + e.message, 'warn')
    }
  }
}

/* ==========================================================================
 * 5. UI — весь DOM: список «Недавние», окно чата, звонки, модалки
 * ========================================================================== */

const UI = {
  els: {},            // кэш getElementById
  modalMode: null,    // 'welcome' | 'profile' | null
  toastTimer: null,

  /* ---------- инициализация: кэшируем элементы, вешаем обработчики ------- */
  init () {
    const ids = [
      'bootWarn', 'btnProfile', 'btnNewChat', 'btnBackList', 'searchInput',
      'sideList', 'sideNew', 'netStatus',
      'chatList', 'chatListEmpty', 'onlineList', 'onlineEmpty',
      'codesPanel', 'codeBox', 'btnMakeCode', 'btnAcceptCode', 'btnCopyCode', 'btnClearCode',
      'chatEmpty', 'chatActive', 'btnBack', 'chatAva', 'chatName', 'chatStatus',
      'btnCallVoice', 'btnCallVideo', 'messages', 'msgInput', 'btnSend',
      'callOverlay', 'remoteVideo', 'localVideo', 'callAvaBig', 'callName', 'callStatus', 'btnHangup',
      'incoming', 'incAva', 'incName', 'incType', 'btnAccept', 'btnDecline',
      'modal', 'modalTitle', 'modalSub', 'modalAva', 'nameInput', 'myId', 'btnCopyId', 'btnSaveName',
      'btnLogout', 'avaFile',
      'splash', 'btnEnter', 'auth', 'tabReg', 'tabLogin',
      'formReg', 'regName', 'regUser', 'regPass', 'btnRegister',
      'formLogin', 'logUser', 'logPass', 'btnLogin',
      'authError', 'authError2',
      'toast'
    ]
    for (const id of ids) this.els[id] = document.getElementById(id)
    const e = this.els

    // --- экраны заставки и авторизации ---
    e.btnEnter.onclick = () => Auth.showAuth()
    e.tabReg.onclick = () => Auth.tab('reg')
    e.tabLogin.onclick = () => Auth.tab('login')
    e.btnRegister.onclick = () => Auth.register()
    e.btnLogin.onclick = () => Auth.login()
    e.regUser.oninput = () => { e.regUser.value = e.regUser.value.toLowerCase().replace(/[^a-z0-9_]/g, '') }
    e.logUser.oninput = () => { e.logUser.value = e.logUser.value.toLowerCase().replace(/[^a-z0-9_]/g, '') }
    e.regPass.onkeydown = (ev) => { if (ev.key === 'Enter') Auth.register() }
    e.logPass.onkeydown = (ev) => { if (ev.key === 'Enter') Auth.login() }
    e.btnLogout.onclick = () => Auth.logout()

    // --- шапка левой колонки ---
    e.btnProfile.onclick = () => this.showModal('profile')
    e.btnNewChat.onclick = () => this.showSide('new')
    e.btnBackList.onclick = () => this.showSide('list')
    e.searchInput.oninput = () => this.renderChatList()
    if (e.netStatus) e.netStatus.onclick = () => this.showModal('profile')

    // --- Android WebView: глушим длинное нажатие (меню «Копировать/
    //     Поделиться/Веб-поиск») везде, кроме полей ввода и журнала ---
    document.addEventListener('contextmenu', (ev) => {
      const t = ev.target
      if (t && t.closest && t.closest('input, textarea, .modal-log')) return
      ev.preventDefault()
    })

    // --- списки (делегирование кликов) ---
    e.chatList.onclick = (ev) => {
      const row = ev.target.closest('.chat-row')
      if (row) this.openChat(row.dataset.peer)
    }
    e.onlineList.onclick = (ev) => {
      const row = ev.target.closest('.chat-row')
      if (row) {
        this.showSide('list')
        this.openChat(row.dataset.peer)   // авто-подключение произойдёт в openChat
      }
    }

    // --- чат ---
    e.btnBack.onclick = () => document.body.classList.remove('chat-open')
    e.btnSend.onclick = () => this._sendFromInput()
    e.msgInput.onkeydown = (ev) => { if (ev.key === 'Enter') this._sendFromInput() }

    // --- звонки ---
    e.btnCallVoice.onclick = () => Call.start('voice')
    e.btnCallVideo.onclick = () => Call.start('video')
    e.btnHangup.onclick = () => Call.hangup()
    e.btnAccept.onclick = () => Call.accept()
    e.btnDecline.onclick = () => Call.decline()

    // --- коды подключения ---
    e.btnMakeCode.onclick = () => Codes.create()
    e.btnAcceptCode.onclick = () => Codes.accept()
    e.btnCopyCode.onclick = () => this.copyText(e.codeBox.value, 'Код скопирован')
    e.btnClearCode.onclick = () => { e.codeBox.value = '' }

    // --- модалка ---
    e.btnSaveName.onclick = () => this.saveName()
    e.btnCopyId.onclick = () => this.copyText(e.myId.textContent, 'Peer ID скопирован')
    e.nameInput.oninput = () => this._updateModalAvatar()
    e.nameInput.onkeydown = (ev) => { if (ev.key === 'Enter') this.saveName() }
    // аватар: клик по картинке → выбор файла → сжатие → рассылка окружению
    e.modalAva.onclick = () => this.pickAvatar()
    e.avaFile.onchange = () => {
      const f = e.avaFile.files && e.avaFile.files[0]
      e.avaFile.value = '' // сброс, чтобы тот же файл можно было выбрать снова
      if (f) this.onAvatarFile(f)
    }
    e.modal.onclick = (ev) => {
      // закрыть по клику вне карточки можно только в режиме профиля
      if (this.modalMode === 'profile' && ev.target === e.modal) this.hideModal()
    }
  },

  _sendFromInput () {
    const text = this.els.msgInput.value
    if (!text.trim()) return
    this.els.msgInput.value = ''
    // именно sendChatText (сохранение, доставка, галочки), а не Channel.send:
    // у Channel.send первый аргумент — это peerId, а не текст
    sendChatText(text).catch(e => log('ошибка отправки: ' + e.message, 'err'))
  },

  /* ---------- переключение режимов левой колонки ----------------------- */
  showSide (mode) {
    this.els.sideList.hidden = mode !== 'list'
    this.els.sideNew.hidden = mode !== 'new'
    if (mode === 'new') this.renderOnlineList()
  },

  /* ---------- список «Недавние» (+ поиск по @юзернейму) ---------------- */
  renderChatList () {
    const e = this.els
    const raw = (e.searchInput.value || '').toLowerCase().trim()
    const filter = raw.replace(/^@/, '')   // ввод «@nick» и «nick» эквивалентны

    const rows = []
    const seen = new Set()

    // 1) сохранённые чаты — ищем по имени, @юзернейму и peerId
    const entries = Object.entries(Store.data.chats)
      .filter(([, c]) => !filter ||
        (c.name || '').toLowerCase().includes(filter) ||
        (c.username || '').toLowerCase().includes(filter))
      .sort((a, b) => (b[1].ts || 0) - (a[1].ts || 0))
    for (const [peerId, chat] of entries) {
      seen.add(peerId)
      rows.push({ peerId, chat, presence: Rendezvous.lookup(peerId) })
    }

    // 2) собеседники «в сети», с которыми чата ещё нет: поиск по присутствию —
    //    именно он находит @юзернейм, даже если вы ещё никогда не переписывались
    if (filter) {
      for (const p of Rendezvous.onlinePeers()) {
        if (seen.has(p.peerId)) continue
        const hit = (p.name || '').toLowerCase().includes(filter) ||
                    (p.username || '').toLowerCase().includes(filter)
        if (!hit) continue
        seen.add(p.peerId)
        rows.push({ peerId: p.peerId, chat: null, presence: p })
      }
    }

    e.chatList.innerHTML = ''
    e.chatListEmpty.hidden = rows.length > 0

    for (const item of rows) {
      const { peerId, chat, presence } = item
      const online = isConnectedTo(peerId) || Rendezvous.isOnline(peerId)
      const displayName = (chat && chat.name) || (presence && presence.name) || shortId(peerId)
      const username = (chat && chat.username) || (presence && presence.username) || ''

      const row = document.createElement('div')
      row.className = 'chat-row' + (App.currentPeer === peerId ? ' active' : '')
      row.dataset.peer = peerId

      // аватарка (+ зелёная точка, если собеседник в сети)
      const ava = document.createElement('div')
      ava.className = 'avatar'
      ava.style.background = colorFor(peerId)
      // фото аватара (t:'ava') вместо букв, если собеседник его прислал
      const savedAva = chat && chat.ava
      ava.textContent = savedAva ? '' : initials(displayName)
      applyAva(ava, savedAva)
      if (online) {
        const dot = document.createElement('span')
        dot.className = 'dot'
        ava.appendChild(dot)
      }

      const main = document.createElement('div')
      main.className = 'row-main'
      const top = document.createElement('div')
      top.className = 'row-top'
      const nameEl = document.createElement('span')
      nameEl.className = 'row-name'
      nameEl.textContent = displayName
      if (username) {
        // @юзернейм рядом с именем — по нему вас нашли
        const handle = document.createElement('span')
        handle.className = 'handle'
        handle.textContent = '@' + username
        nameEl.appendChild(handle)
      }
      const timeEl = document.createElement('span')
      timeEl.className = 'row-time'
      timeEl.textContent = chat && chat.ts ? fmtRowTime(chat.ts) : (online ? 'в сети' : '')
      if (!chat && online) timeEl.style.color = 'var(--online)'
      top.append(nameEl, timeEl)

      const bottom = document.createElement('div')
      bottom.className = 'row-bottom'
      const prev = document.createElement('span')
      prev.className = 'row-preview'
      const last = chat && chat.messages[chat.messages.length - 1]
      if (last) {
        if (last.side === 'me') {
          const tick = document.createElement('span')
          tick.className = 'tick'
          tick.textContent = !last.delivered ? '' : (last.read ? '✓✓' : '✓')
          prev.appendChild(tick)
          prev.appendChild(document.createTextNode('Вы: ' + last.text))
        } else {
          prev.textContent = last.text
        }
      } else if (!chat && username) {
        // результат поиска: нашли по @юзернейму — показываем его
        const found = document.createElement('span')
        found.className = 'found'
        found.textContent = '@' + username
        prev.appendChild(found)
        prev.appendChild(document.createTextNode(' · найден по запросу'))
      } else if (!chat) {
        prev.textContent = 'в сети — нажмите, чтобы написать'
      } else {
        prev.textContent = 'пустой чат'
        prev.style.fontStyle = 'italic'
      }
      bottom.appendChild(prev)

      if (chat && chat.unread > 0) {
        const badge = document.createElement('span')
        badge.className = 'badge-unread'
        badge.textContent = chat.unread > 99 ? '99+' : String(chat.unread)
        bottom.appendChild(badge)
      }

      main.append(top, bottom)
      row.append(ava, main)
      e.chatList.appendChild(row)
    }
  },

  /* ---------- список «В сети сейчас» ----------------------------------- */
  renderOnlineList () {
    const e = this.els
    e.onlineList.innerHTML = ''
    const online = Rendezvous.onlinePeers() // [{peerId, name}]
    e.onlineEmpty.hidden = online.length > 0

    for (const { peerId, name, username } of online) {
      const chat = Store.data.chats[peerId]
      const displayName = (chat && chat.name) || name || 'Без имени'
      const handle = (chat && chat.username) || username || ''

      const row = document.createElement('div')
      row.className = 'chat-row'
      row.dataset.peer = peerId

      const ava = document.createElement('div')
      ava.className = 'avatar'
      ava.style.background = colorFor(peerId)
      ava.textContent = (chat && chat.ava) ? '' : initials(displayName)
      applyAva(ava, chat && chat.ava)
      const dot = document.createElement('span')
      dot.className = 'dot'
      ava.appendChild(dot)

      const main = document.createElement('div')
      main.className = 'row-main'
      const top = document.createElement('div')
      top.className = 'row-top'
      const nameEl = document.createElement('span')
      nameEl.className = 'row-name'
      nameEl.textContent = displayName
      if (handle) {
        const h = document.createElement('span')
        h.className = 'handle'
        h.textContent = '@' + handle
        nameEl.appendChild(h)
      }
      const timeEl = document.createElement('span')
      timeEl.className = 'row-time'
      timeEl.textContent = 'в сети'
      timeEl.style.color = 'var(--online)'
      top.append(nameEl, timeEl)

      const bottom = document.createElement('div')
      bottom.className = 'row-bottom'
      const prev = document.createElement('span')
      prev.className = 'row-preview'
      // покажем @юзернейм — по нему собеседника можно найти поиском
      prev.textContent = handle ? '@' + handle : shortId(peerId)
      bottom.appendChild(prev)

      main.append(top, bottom)
      row.append(ava, main)
      e.onlineList.appendChild(row)
    }
  },

  /* ---------- открытие/закрытие чата ---------------------------------- */
  openChat (peerId, opts = {}) {
    if (!peerId) return
    App.currentPeer = peerId
    Store.data.lastPeer = peerId
    Store.save()

    // подтягиваем имя/@юзернейм из присутствия, если чат только открывается
    const presence = Rendezvous.lookup(peerId)
    const chat = Store.ensureChat(
      peerId,
      presence && presence.name,
      presence && presence.username
    )
    chat.unread = 0
    Store.save()

    this.els.chatEmpty.hidden = true
    this.els.chatActive.hidden = false
    document.body.classList.add('chat-open') // для мобильной вёрстки
    this.renderChatList()
    this.renderMessages()
    this.updateChatHeader()

    // если собеседник в сети и соединения ещё нет — подключаемся автоматически
    if (opts.reconnect !== false && !isConnectedTo(peerId) && Rendezvous.isOnline(peerId)) {
      log(`автоподключение к ${shortId(peerId)}`, 'sys')
      Rendezvous.connect(peerId).catch(() => {})
    }
  },

  /** Шапка чата: имя + @юзернейм + статус (в сети / подключение / не в сети) */
  updateChatHeader () {
    const peer = App.currentPeer
    if (!peer) return
    const chat = Store.data.chats[peer]
    const presence = Rendezvous.lookup(peer)
    const name = (chat && chat.name) || (presence && presence.name) || shortId(peer)
    const username = (chat && chat.username) || (presence && presence.username) || ''

    this.els.chatName.textContent = name
    if (username) {
      const handle = document.createElement('span')
      handle.className = 'handle'
      handle.textContent = '@' + username
      this.els.chatName.appendChild(handle)
    }
    this.els.chatAva.textContent = (chat && chat.ava) ? '' : initials(name)
    this.els.chatAva.style.background = colorFor(peer)
    applyAva(this.els.chatAva, chat && chat.ava)
    this.updateChatStatus()
  },

  /** Актуальный статус подключения собеседника */
  updateChatStatus () {
    const peer = App.currentPeer
    if (!peer || this.els.chatActive.hidden) return
    const el = this.els.chatStatus
    if (isConnectedTo(peer)) {
      el.textContent = 'в сети'
      el.className = 'status on'
    } else if (Rendezvous.isOnline(peer)) {
      el.textContent = 'подключение…'
      el.className = 'status warn'
    } else {
      el.textContent = 'не в сети'
      el.className = 'status'
    }
  },

  /** Строка состояния глобального канала: сколько релеев живы и сколько
   *  собеседников найдено. Главный индикатор для APK: если «нет связи» —
   *  телефон не достучался до публичных релеев (или их заблокировали). */
  updateNetStatus () {
    const el = this.els.netStatus
    if (!el) return
    const txt = el.querySelector('.txt')
    if (!txt) return
    let cls = 'warn'
    let text = 'глобальный канал запускается…'
    if (NostrRendezvous.started) {
      const up = NostrRendezvous.relays.filter((r) => r.sub).length
      const total = NostrRendezvous.relays.length
      const online = Rendezvous.onlinePeers().length
      if (up === 0) {
        cls = 'err'
        text = 'нет связи с глобальными релеями — контакты не найдутся'
      } else {
        cls = online > 0 ? 'ok' : 'warn'
        text = `релея ${up}/${total} · в сети: ${online}` +
          (online > 0 ? '' : ' — ждём собеседников (обе стороны должны быть онлайн)')
      }
    }
    el.className = 'net-status ' + cls
    txt.textContent = text
  },

  /* ---------- лента сообщений ------------------------------------------ */
  /** Полная перерисовка ленты текущего чата */
  renderMessages () {
    const peer = App.currentPeer
    const box = this.els.messages
    box.innerHTML = ''
    if (!peer) return
    const chat = Store.data.chats[peer]
    if (!chat) return

    let lastDay = null
    for (const msg of chat.messages) {
      const dk = dayKey(msg.ts)
      if (dk !== lastDay) { box.appendChild(this._daySeparator(msg.ts)); lastDay = dk }
      box.appendChild(this._bubble(peer, msg))
    }
    box.scrollTop = box.scrollHeight
  },

  /** Добавить одно сообщение в конец ленты (с разделителем дня при нужде) */
  appendMessageToView (peer, msg) {
    if (peer !== App.currentPeer) return
    const box = this.els.messages
    const dk = dayKey(msg.ts)
    // разделитель нужен, только если у последнего элемента другой день
    // (и сам разделитель, и пузырёк несут data-dk с ключом дня)
    const last = box.lastElementChild
    if (!last || last.dataset.dk !== dk) box.appendChild(this._daySeparator(msg.ts))
    box.appendChild(this._bubble(peer, msg))
    box.scrollTop = box.scrollHeight
  },

  _daySeparator (ts) {
    const sep = document.createElement('div')
    sep.className = 'day-sep'
    sep.dataset.dk = dayKey(ts)
    sep.textContent = fmtDaySep(ts)
    return sep
  },

  /** Пузырь сообщения: текст + время/галочки (обтекаются снизу справа) */
  _bubble (peer, msg) {
    const wrap = document.createElement('div')
    wrap.className = 'msg ' + (msg.side === 'me' ? 'me' : 'peer')
    wrap.dataset.id = msg.id
    wrap.dataset.dk = dayKey(msg.ts) // ключ дня — чтобы не дублировать разделители

    const bubble = document.createElement('div')
    bubble.className = 'bubble'

    const text = document.createElement('span')
    text.className = 'text'
    text.textContent = msg.text

    const meta = document.createElement('span')
    meta.className = 'meta'
    meta.appendChild(document.createTextNode(fmtClock(msg.ts)))
    if (msg.side === 'me') {
      const tick = document.createElement('span')
      tick.className = 'tick'
      // галочки: нет — не отправлено, ✓ — доставлено, ✓✓ — прочитано
      tick.textContent = !msg.delivered ? '' : (msg.read ? '✓✓' : '✓')
      meta.appendChild(tick)
    }

    bubble.append(text, meta)
    wrap.appendChild(bubble)
    return wrap
  },

  /** Обновить галочки у сообщений (после доставки/прочтения) */
  refreshMeta (peer) {
    if (peer === App.currentPeer) this.renderMessages()
    this.renderChatList()
  },

  /* ---------- звонки: оверлеи ------------------------------------------ */
  openCallOverlay (peerId, kind, status) {
    const chat = Store.data.chats[peerId]
    const name = chat ? chat.name : shortId(peerId)
    this.els.callName.textContent = name
    this.els.callAvaBig.textContent = (chat && chat.ava) ? '' : initials(name)
    this.els.callAvaBig.style.background = colorFor(peerId)
    applyAva(this.els.callAvaBig, chat && chat.ava)
    this.els.callStatus.textContent = status
    this.els.callOverlay.classList.toggle('voice', kind === 'voice')
    this.els.callOverlay.hidden = false
  },
  setCallStatus (text) { this.els.callStatus.textContent = text },
  closeCall () {
    this.els.callOverlay.hidden = true
    this.els.localVideo.srcObject = null
    this.els.remoteVideo.srcObject = null
  },
  showIncoming (peerId, kind) {
    const chat = Store.data.chats[peerId]
    const name = chat ? chat.name : shortId(peerId)
    this.els.incName.textContent = name
    this.els.incAva.textContent = (chat && chat.ava) ? '' : initials(name)
    this.els.incAva.style.background = colorFor(peerId)
    applyAva(this.els.incAva, chat && chat.ava)
    this.els.incType.textContent = kind === 'voice' ? 'Входящий голосовой звонок…' : 'Входящий видеозвонок…'
    this.els.incoming.hidden = false
  },
  hideIncoming () { this.els.incoming.hidden = true },

  /* ---------- модалка (имя / профиль) ---------------------------------- */
  showModal (mode) {
    this.modalMode = mode
    const e = this.els
    e.modalTitle.textContent = 'Профиль'
    e.modalSub.textContent = 'Имя видно собеседникам. Peer ID — технический идентификатор узла.'
    e.btnSaveName.textContent = 'Сохранить'
    e.nameInput.value = Store.data.profile.name || ''
    e.myId.textContent = App.node ? App.node.peerId.toString() : 'генерация…'
    this._updateModalAvatar()
    e.modal.hidden = false
  },
  hideModal () {
    this.els.modal.hidden = true
    this.modalMode = null
  },
  _updateModalAvatar () {
    const name = this.els.nameInput.value || Store.data.profile.name || 'Гость'
    const own = Store.data.profile.ava
    this.els.modalAva.textContent = own ? '' : initials(name)
    this.els.modalAva.style.background = colorFor(name + 'me')
    applyAva(this.els.modalAva, own)
  },

  /** Клик по аватару в профиле — выбрать фото, сжать и разослать */
  pickAvatar () {
    this.els.avaFile.click()
  },
  async onAvatarFile (file) {
    if (!file) return
    if (!/^image\//.test(file.type)) { this.toast('Нужен файл изображения'); return }
    try {
      const d = await compressAva(file)
      Store.setOwnAva(d)
      this._updateModalAvatar()
      this.renderChatList()
      this.updateChatHeader()
      Presence.announce()                       // обновить присутствие (имя/юзернейм)
      Rendezvous._post({ t: 'ava', ava: d })    // разослать новый аватар окружению
      this.toast('Аватар обновлён')
      log('аватар обновлён (' + Math.round(d.length / 1024 * 10) / 10 + ' КБ)', 'ok')
    } catch (e) {
      this.toast('Не удалось обработать фото: ' + e.message)
      log('аватар: ' + e.message, 'err')
    }
  },
  saveName () {
    const name = this.els.nameInput.value.trim() || 'Гость'
    Store.setProfileName(name)
    this.els.modal.hidden = true
    this.modalMode = null
    this.renderChatList()
    Presence.announce() // сообщить своё имя окружению
    this.toast(`Имя сохранено: ${name}`)
    log(`имя профиля: ${name}`, 'ok')
  },

  /* ---------- мелочи ---------------------------------------------------- */
  toast (text) {
    const el = this.els.toast
    if (!el) return
    el.textContent = text
    el.classList.add('show')
    clearTimeout(this.toastTimer)
    this.toastTimer = setTimeout(() => el.classList.remove('show'), 2600)
  },
  async copyText (text, okMsg) {
    if (!text) return
    try {
      if (navigator.clipboard && window.isSecureContext) await navigator.clipboard.writeText(text)
      else {
        const ta = document.createElement('textarea')
        ta.value = text
        document.body.appendChild(ta)
        ta.select()
        document.execCommand('copy')
        ta.remove()
      }
      this.toast(okMsg || 'Скопировано')
    } catch (e) {
      this.toast('Не удалось скопировать')
      log('copy: ' + e.message, 'warn')
    }
  }
}

/* ==========================================================================
 * 5.5 Auth — экраны заставки и регистрации/входа
 *     Заставка (стрелочка в шарике) → плавная анимация → карточка с
 *     именем, @юзернеймом (по нему собеседники ищут вас поиском) и паролем.
 *     Пароль хранится только локально: SHA-256 в localStorage.
 *     Аккаунтов «в облаке» нет — это P2P-мессенджер без серверов.
 * ========================================================================== */

/** Запасной SHA-256 на чистом JS: crypto.subtle есть только в secure context
 *  (https и localhost) — на телефоне по http его нет, и без этого варианта
 *  регистрация там падала бы. Алгоритм — FIPS 180-4, совпадает с subtle. */
function sha256js (msg) {
  const K = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
  ])
  const rotr = (x, n) => (x >>> n) | (x << (32 - n))
  const l = msg.length
  const total = Math.ceil((l + 9) / 64) * 64            // кратно 64 с учётом 0x80 и длины
  const pad = new Uint8Array(total)
  pad.set(msg)
  pad[l] = 0x80
  const dv = new DataView(pad.buffer)
  const bits = l * 8
  dv.setUint32(total - 8, Math.floor(bits / 0x100000000)) // 64-битная длина в битах, BE
  dv.setUint32(total - 4, bits >>> 0)

  const H = new Uint32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19])
  const w = new Uint32Array(64)
  for (let off = 0; off < total; off += 64) {
    for (let i = 0; i < 16; i++) w[i] = dv.getUint32(off + i * 4)
    for (let i = 16; i < 64; i++) {
      const s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3)
      const s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10)
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0
    }
    let a = H[0], b = H[1], c = H[2], d = H[3], e = H[4], f = H[5], g = H[6], h = H[7]
    for (let i = 0; i < 64; i++) {
      const S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)
      const ch = (e & f) ^ (~e & g)
      const t1 = (h + S1 + ch + K[i] + w[i]) >>> 0
      const S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)
      const maj = (a & b) ^ (a & c) ^ (b & c)
      const t2 = (S0 + maj) >>> 0
      h = g; g = f; f = e; e = (d + t1) >>> 0
      d = c; c = b; b = a; a = (t1 + t2) >>> 0
    }
    H[0] = (H[0] + a) >>> 0; H[1] = (H[1] + b) >>> 0
    H[2] = (H[2] + c) >>> 0; H[3] = (H[3] + d) >>> 0
    H[4] = (H[4] + e) >>> 0; H[5] = (H[5] + f) >>> 0
    H[6] = (H[6] + g) >>> 0; H[7] = (H[7] + h) >>> 0
  }
  return Array.from(H).map(x => x.toString(16).padStart(8, '0')).join('')
}

const Auth = {
  /** SHA-256 пароля в hex (локальное «хэширование», не для сети) */
  async hash (password) {
    const bytes = new TextEncoder().encode(password)
    // crypto.subtle доступен только в secure context (https/localhost)
    if (globalThis.crypto && crypto.subtle) {
      try {
        const digest = await crypto.subtle.digest('SHA-256', bytes)
        return Array.from(new Uint8Array(digest))
          .map(b => b.toString(16).padStart(2, '0'))
          .join('')
      } catch { /* нет контекста — используем JS-вариант ниже */ }
    }
    return sha256js(bytes)
  },

  /** Допустимый юзернейм: латиница/цифры/подчёркивание, 3–20 символов */
  validUsername (u) {
    return /^[a-z0-9_]{3,20}$/.test(u)
  },

  _err (msg, form) {
    const el = form === 'login' ? this.els.authError2 : this.els.authError
    if (!el) return
    if (!msg) { el.hidden = true; el.textContent = ''; return }
    el.textContent = msg
    el.hidden = false
  },

  /** Показать экран авторизации (после клика по стрелочке) */
  showAuth () {
    const e = this.els
    e.splash.classList.add('gone')
    e.auth.classList.add('shown')
    // если аккаунт уже создавался — сразу вкладка «Вход» (юзернейм подставлен)
    const hasAccount = !!(Store.data.profile.username && Store.data.profile.passHash)
    this.tab(hasAccount ? 'login' : 'reg')
    if (hasAccount) e.logUser.value = Store.data.profile.username || ''

    // заранее проверяем: может ли браузер вообще сохранять данные —
    // иначе пользователь зарегистрируется и «потеряет» аккаунт при перезагрузке
    let storageOk = true
    try {
      localStorage.setItem('foxosis/probe', '1')
      localStorage.removeItem('foxosis/probe')
    } catch { storageOk = false }
    if (!storageOk) {
      this._err('Браузер запрещает сохранение данных (приватный режим или встроенный браузер?) — аккаунт не переживёт перезагрузку.',
        hasAccount ? 'login' : 'reg')
    }

    setTimeout(() => {
      const first = hasAccount ? e.logUser : e.regName
      if (first) first.focus()
    }, 450)
  },

  /** Переключение вкладок «Регистрация» / «Вход» */
  tab (which) {
    const e = this.els
    e.formReg.hidden = which !== 'reg'
    e.formLogin.hidden = which !== 'login'
    e.tabReg.classList.toggle('active', which === 'reg')
    e.tabLogin.classList.toggle('active', which === 'login')
    this._err(null, 'reg')
    this._err(null, 'login')
  },

  /** Регистрация: имя + @юзернейм + пароль */
  async register () {
    const e = this.els
    this._err(null, 'reg')
    const name = (e.regName.value || '').trim()
    const username = (e.regUser.value || '').trim().toLowerCase()
    const pass = e.regPass.value || ''

    if (!name) return this._err('Введите имя — его увидят собеседники.', 'reg')
    if (!this.validUsername(username)) {
      return this._err('Юзернейм: 3–20 символов — латиница, цифры и «_».', 'reg')
    }
    if (pass.length < 4) return this._err('Пароль должен быть не короче 4 символов.', 'reg')

    const passHash = await this.hash(pass)

    // Проверяем занятость ника в публичных релеях (fail-open: без интернета
    // или до подключения — пропускаем, регистрация не должна вставать).
    // Отпечаток acc = hash(пароля): та же учётка (ник+пароль) на другом
    // устройстве — не помеха, чужой пароль при совпадении ника — блок.
    const btn = e.btnRegister
    const btnText = btn.textContent
    btn.disabled = true
    btn.textContent = 'Проверяем ник…'
    let chk = { status: 'free', name: '' }
    try { chk = await NostrRendezvous.checkUname(username, passHash) } catch {}
    btn.disabled = false
    btn.textContent = btnText
    if (chk.status === 'taken') {
      log(`@${username} занят другим пользователем — регистрация отклонена`, 'warn')
      return this._err('@' + username + ' уже занят другим пользователем. Если это ваш ник — войдите тем же паролем во вкладке «Вход».', 'reg')
    }

    Store.setProfile({ name, username, passHash, authed: true })
    e.regPass.value = ''
    // заявка в релеи: другие устройства увидят, что ник занят
    try { NostrRendezvous.publishUname(username, name, passHash) } catch {}
    if (!Store.lastSaveOk) {
      // честно предупреждаем: сессия продолжится, но при перезагрузке
      // аккаунт и чаты не вернутся — включите обычный браузер, не приватный
      this._err('Браузер запретил сохранение: аккаунт живёт только до перезагрузки.', 'reg')
      UI.toast('Внимание: аккаунт НЕ сохраняется этим браузером')
    }
    log(`регистрация: @${username} (${name})` + (Store.lastSaveOk ? '' : ' (без сохранения!)'), Store.lastSaveOk ? 'ok' : 'err')
    this.enter()
  },

  /** Вход: юзернейм + пароль сверяются с локальной записью; если локальной
   *  записи нет (переустановка/новое устройство) — ищем СВОЮ учётку по
   *  заявке ника в публичных релеях: пароль и есть ключ восстановления. */
  async login () {
    const e = this.els
    this._err(null, 'login')
    const username = (e.logUser.value || '').trim().toLowerCase()
    const pass = e.logPass.value || ''
    const p = Store.data.profile

    if (!this.validUsername(username)) {
      return this._err('Юзернейм: 3–20 символов — латиница, цифры и «_».', 'login')
    }
    const passHash = await this.hash(pass)

    if (p.username && p.passHash) {
      // локальный аккаунт есть — сверяем как раньше
      if (username !== p.username) {
        return this._err('Неверный юзернейм (на этом устройстве сохранён @' + p.username + ').', 'login')
      }
      if (passHash !== p.passHash) return this._err('Неверный пароль.', 'login')
    } else {
      // локальной записи нет — ищем учётку (ник+пароль) в публичных релеях
      const btn = e.btnLogin
      const btnText = btn.textContent
      btn.disabled = true
      btn.textContent = 'Ищем аккаунт…'
      let chk = { status: 'free', name: '' }
      try { chk = await NostrRendezvous.checkUname(username, passHash) } catch {}
      btn.disabled = false
      btn.textContent = btnText
      if (chk.status !== 'own') {
        return this._err('Аккаунт не найден: на этом устройстве записи нет, а в сети учётку с таким @ником и паролем не нашли. Зарегистрируйтесь — ник освободится, если пароль тот же.', 'login')
      }
      // восстановление: та же учётка найдена в сети — пересоздаём локально
      Store.setProfile({ name: chk.name || username, username, passHash })
      log(`аккаунт восстановлен из сети: @${username}`, 'ok')
    }

    Store.setProfile({ authed: true })
    e.logPass.value = ''
    // освежаем заявку ника (для аккаунтов, зарегистрированных до её появления)
    try { NostrRendezvous.publishUname(username, Store.data.profile.name, passHash) } catch {}
    if (!Store.lastSaveOk) UI.toast('Внимание: вход не сохраняется этим браузером')
    log(`вход: @${username}`, Store.lastSaveOk ? 'ok' : 'err')
    this.enter()
  },

  /** Успешный вход/регистрация — убрать экраны, показать приложение */
  enter () {
    const e = this.els
    Store.data.profile.authed = true
    Store.save()
    e.splash.classList.add('gone')
    e.auth.classList.remove('shown')
    document.body.classList.add('authorized')
    Presence.announce()          // рассказать сети имя и @юзернейм
    UI.renderChatList()
    UI.toast(`Вы вошли как @${Store.data.profile.username}`)
    log(`авторизация: @${Store.data.profile.username} (${Store.data.profile.name})`, 'ok')
  },

  /** Выход из аккаунта (профиль остаётся — можно войти снова) */
  logout () {
    Store.setProfile({ authed: false })
    document.body.classList.remove('authorized')
    const e = this.els
    e.auth.classList.remove('shown')
    e.splash.classList.remove('gone')
    UI.hideModal()
    log('выход из аккаунта', 'sys')
  },

  /** Первичная разметка экранов при запуске */
  initial () {
    const e = this.els
    if (Store.data.profile.authed && Store.data.profile.name) {
      // уже авторизованы — сразу в приложение, без заставки
      e.splash.classList.add('gone')
      e.auth.classList.remove('shown')
      document.body.classList.add('authorized')
      return
    }
    e.splash.classList.remove('gone')
    e.auth.classList.remove('shown')
    document.body.classList.remove('authorized')
  },

  get els () { return UI.els }
}

/* ==========================================================================
 * 6. JsonLineStream — доставка JSON-объектов поверх libp2p-потока
 *    (одно JSON-сообщение = одна строка, разделитель "\n").
 *    Нужен потому, что поток байтов не гарантирует границы сообщений.
 * ========================================================================== */

class JsonLineStream {
  /**
   * @param {object} stream   libp2p Stream (send/addEventListener('message'))
   * @param {(msg:any)=>void} onMessage  колбэк на каждый полученный JSON
   * @param {()=>void} onClose           колбэк при закрытии потока
   */
  constructor (stream, onMessage, onClose) {
    this.stream = stream
    this.onMessage = onMessage
    this.onCloseCb = onClose
    this.partial = ''     // неполная строка (если сообщение разрезано кусками)
    this.queue = []       // очередь на случай backpressure (send() вернул false)
    this.closed = false

    stream.addEventListener('message', (evt) => this._feed(evt.data))
    stream.addEventListener('close', () => this._close())
    stream.addEventListener('drain', () => this._flush())
  }

  /** Приём байтов: декодируем, разбиваем по \n, парсим JSON */
  _feed (data) {
    if (this.closed) return
    // libp2p отдаёт Uint8Array либо Uint8ArrayList (объект-список буферов);
    // у второго нет toUint8Array(), но есть subarray() -> Uint8Array.
    // Держимся также за Blob/строку/сырой ArrayBuffer — на случай смены API.
    let bytes
    if (data instanceof Uint8Array) bytes = data
    else if (data instanceof ArrayBuffer) bytes = new Uint8Array(data)
    else if (data && typeof data.subarray === 'function') bytes = data.subarray()
    else if (data && typeof data.toUint8Array === 'function') bytes = data.toUint8Array()
    else if (typeof data === 'string') bytes = ENCODER.encode(data)
    else if (typeof Blob !== 'undefined' && data instanceof Blob) {
      // Blob асинхронен — декодируем отложенно, порядок кусков сохраняем
      data.arrayBuffer()
        .then(buf => { if (!this.closed) this._feed(new Uint8Array(buf)) })
        .catch(() => {})
      return
    } else {
      log('неожиданный тип данных в потоке: ' + Object.prototype.toString.call(data), 'err')
      return
    }

    this.partial += DECODER.decode(bytes, { stream: true })
    let idx
    while ((idx = this.partial.indexOf('\n')) !== -1) {
      const line = this.partial.slice(0, idx)
      this.partial = this.partial.slice(idx + 1)
      if (!line.trim()) continue
      try {
        this.onMessage(JSON.parse(line))
      } catch (e) {
        log('ошибка разбора входящего JSON: ' + e.message, 'err')
      }
    }
  }

  /** Отправка объекта (одна строка JSON + \n) */
  send (obj) {
    if (this.closed) throw new Error('поток закрыт')
    this._write(ENCODER.encode(JSON.stringify(obj) + '\n'))
  }

  _write (bytes) {
    try {
      if (this.stream.send(bytes)) return   // буфер пуст — отправлено сразу
      this.queue.push(bytes)                 // буфер полон — ждём 'drain'
    } catch (e) {
      this._close()
      throw e
    }
  }

  _flush () {
    while (!this.closed && this.queue.length > 0) {
      const bytes = this.queue.shift()
      if (!this.stream.send(bytes)) { this.queue.unshift(bytes); return }
    }
  }

  _close () {
    if (this.closed) return
    this.closed = true
    this.queue = []
    if (this.onCloseCb) this.onCloseCb()
  }
}

/* ==========================================================================
 * 7. ТРАНСПОРТ libp2p ПОВЕРХ WebRTC DataChannel
 *
 *    Обычные транспорты libp2p (WebSockets и т.д.) требуют реальный сокет.
 *    Браузер не умеет слушать сокеты, поэтому мы поступаем так:
 *      1) приложение самостоятельно устанавливает WebRTC-соединение
 *         (сигналинг — BroadcastChannel или код);
 *      2) открытый DataChannel "протаскивается" в libp2p через свой транспорт:
 *         dial()  -> upgradeOutbound() (мы инициировали соединение)
 *         accept  -> upgradeInbound()  (нас вызвали)
 *    libp2p поверх получает шифрование Noise, мультиплексирование Yamux,
 *    протокольные потоки и PubSub — то есть полностью обычное P2P-соединение.
 * ========================================================================== */

/** Состояние транспорта: ожидающие dial и захваченный upgrader */
const dcRegistry = {
  outbound: new Map(),  // peerId -> открытый DataChannel (ждёт node.dial)
  upgrader: null,       // передаётся в createListener()
  logger: null          // ComponentLogger из libp2p
}

/** MultiaddrConnection, заматывающий RTCDataChannel */
class FoxMaConn extends AbstractMultiaddrConnection {
  constructor ({ dc, remoteAddr, direction, log }) {
    super({ remoteAddr, direction, log })
    this.dc = dc
    this.pollTimer = null
    dc.binaryType = 'arraybuffer'

    // входящие байты -> внутренние буферы AbstractMessageStream
    dc.addEventListener('message', (evt) => {
      if (evt.data instanceof ArrayBuffer) this.onData(new Uint8Array(evt.data))
      else if (typeof evt.data === 'string') this.onData(ENCODER.encode(evt.data))
    })
    // закрытие канала -> закрытие соединения
    dc.addEventListener('close', () => {
      if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null }
      this.onTransportClosed()
    })
    dc.addEventListener('error', () => {
      log('ошибка WebRTC DataChannel', 'err')
    })
  }

  /** Передать данные в канал; сообщает libp2p про backpressure */
  sendData (data) {
    let sent = 0
    // обычно это Uint8ArrayList (несколько кусков), но защитимся и от одного
    // Uint8Array — итерировать его как список было бы ошибкой (даст числа)
    const chunks = data instanceof Uint8Array ? [data] : data
    for (const chunk of chunks) {          // data: Uint8ArrayList (несколько кусков)
      const buf = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk)
      this.dc.send(buf)                    // браузер сам кладёт в буфер отправки
      sent += buf.byteLength
    }
    const canSendMore = this.dc.bufferedAmount < 4 * 1024 * 1024
    if (!canSendMore && !this.pollTimer) this._startDrainPoll()
    return { sentBytes: sent, canSendMore }
  }

  /** Опрос буфера отправки до пустоты -> событие 'drain' для libp2p */
  _startDrainPoll () {
    this.pollTimer = setInterval(() => {
      if (this.dc.readyState !== 'open') { clearInterval(this.pollTimer); this.pollTimer = null; return }
      if (this.dc.bufferedAmount === 0) {
        clearInterval(this.pollTimer)
        this.pollTimer = null
        this.safeDispatchEvent('drain')
      }
    }, 30)
  }

  sendReset () { try { this.dc.close() } catch {} }         // аварийный сброс
  async sendClose () { this.dc.close() }                     // graceful close
  sendPause () {}                                            // исходящий backpressure не поддерживаем
  sendResume () {}
}

/** "Слушающий" объект транспорта: хранит адрес и upgrader для входящих */
class FoxListener extends EventTarget {
  constructor (logger) {
    super()
    this.addr = null
    this.log = logger.forComponent('foxosis:dc-listener')
  }
  async listen (ma) {
    this.addr = ma
    this.log('виртуальный listener активен: %s', ma.toString())
    this.dispatchEvent(new Event('listening'))
  }
  getAddrs () { return this.addr ? [this.addr] : [] }
  async close () { this.addr = null; this.dispatchEvent(new Event('close')) }
  updateAnnounceAddrs () { /* адреса не меняются */ }
}

/** Сам транспорт libp2p */
class FoxTransport {
  constructor (components) {
    this.logger = components.logger
    this.log = components.logger.forComponent('foxosis:dc-transport')
    dcRegistry.logger = components.logger
  }
  // NB: у полей класса обязательно стоят точки с запятой — иначе ASI склеит
  // строку "... = true" со следующей, начинающейся на "[", и свойства запишутся
  // не туда (классы — строгий режим, будет TypeError).
  [transportSymbol] = true;
  [Symbol.toStringTag] = '@foxosis/webrtc-dc';
  [serviceCapabilities] = ['@libp2p/transport'];

  /** Исходящее соединение: забираем подготовленный DataChannel и апгрейдим */
  async dial (ma, options) {
    // multiaddr v13 не имеет getPeerId() — Peer ID лежит в компоненте "p2p"
    const p2pComp = ma.getComponents().find(c => c.name === 'p2p')
    const peerStr = p2pComp ? p2pComp.value : null
    this.log('dial %s', ma.toString())
    if (!peerStr) throw new Error('в адресе нет Peer ID: ' + ma.toString())

    const dc = dcRegistry.outbound.get(peerStr)
    if (dc == null) {
      throw new Error('нет подготовленного WebRTC-канала — сначала подключитесь («Подключиться» или по коду)')
    }
    dcRegistry.outbound.delete(peerStr)

    const conn = new FoxMaConn({
      dc,
      remoteAddr: ma,
      direction: 'outbound',
      log: this.logger.forComponent('foxosis:maconn')
    })
    // Noise + Yamux поднимаются здесь: возвращается готовое Connection
    const upgraded = await options.upgrader.upgradeOutbound(conn, options)
    this.log('соединение к %s апгрейднуто', peerStr)
    return upgraded
  }

  /** Захватываем upgrader — он нужен для входящих соединений */
  createListener (options) {
    dcRegistry.upgrader = options.upgrader
    return new FoxListener(this.logger)
  }

  /** Наш транспорт работает только с "виртуальными" адресами FoxOsis */
  listenFilter (maddrs) { return maddrs.filter(isFoxAddr) }
  dialFilter (maddrs) { return maddrs.filter(isFoxAddr) }
}

/** Фабрика транспорта (как webSockets() в официальных модулях) */
function foxTransport () {
  return (components) => new FoxTransport(components)
}

/** Принадлежит ли адрес нашему транспорту (по "магическому" порту) */
function isFoxAddr (ma) {
  try { return ma.toString().includes(`/tcp/${CFG.LISTEN_PORT}`) } catch { return false }
}

/** Завершить входящее соединение: DataChannel -> libp2p Connection */
async function acceptInboundConnection (dc, peerStr) {
  const upgrader = dcRegistry.upgrader
  if (!upgrader) throw new Error('libp2p-upgrader ещё не готов')
  const conn = new FoxMaConn({
    dc,
    remoteAddr: multiaddr(foxAddr(peerStr)),
    direction: 'inbound',
    log: dcRegistry.logger.forComponent('foxosis:maconn')
  })
  await upgrader.upgradeInbound(conn, { signal: AbortSignal.timeout(30_000) })
  log(`входящее соединение libp2p принято: ${shortId(peerStr)}`, 'ok')
}

/* ==========================================================================
 * 8. ПРИМИТИВЫ СИГНАЛИНГА WebRTC-DataChannel (оффер/ансвер)
 *    Используются и локальным rendezvous, и подключением по коду.
 * ========================================================================== */

/** Подготовка сессии: события закрытия + deferred открытия канала */
function wireSession (session) {
  session.opened = createDeferred()
  // Отклонение promise никем не перехвачено -> "uncaught" в консоли.
  // Настоящие потребители подключаются к тому же promise позже, поэтому
  // здесь достаточно пустого обработчика-заглушки.
  session.opened.promise.catch(() => {})
  session.close = () => {
    try { session.dc && session.dc.close() } catch {}
    try { session.pc.close() } catch {}
    session.opened.reject(new Error('сессия закрыта'))
  }
  session.pc.addEventListener('connectionstatechange', () => {
    const st = session.pc.connectionState
    if (st === 'failed') session.opened.reject(new Error('WebRTC-соединение не удалось установить'))
    if (st === 'closed') session.opened.reject(new Error('WebRTC-соединение закрыто'))
  })
  return session
}

/** Роль "звонящего": создаём DataChannel и оффер */
async function beginOutboundLink (peerStr) {
  const pc = new RTCPeerConnection({ iceServers: CFG.ICE_SERVERS })
  const session = wireSession({ pc, dc: null, peerStr, role: 'caller' })

  const dc = pc.createDataChannel('foxosis', { ordered: true })
  dc.binaryType = 'arraybuffer'
  session.dc = dc
  dc.addEventListener('open', () => session.opened.resolve(dc), { once: true })
  dc.addEventListener('close', () => session.opened.reject(new Error('канал закрылся до открытия')), { once: true })

  const offer = await pc.createOffer()
  await pc.setLocalDescription(offer)
  await waitIceGathering(pc)   // не-trickle: кандидаты уже внутри SDP
  return { session, offerSdp: pc.localDescription.sdp, opened: session.opened.promise }
}

/** Роль "отвечающего": принимаем оффер, ждём входящий DataChannel */
async function beginInboundLink (peerStr, offerSdp) {
  const pc = new RTCPeerConnection({ iceServers: CFG.ICE_SERVERS })
  const session = wireSession({ pc, dc: null, peerStr, role: 'callee' })

  pc.addEventListener('datachannel', (evt) => {
    const dc = evt.channel
    session.dc = dc
    dc.binaryType = 'arraybuffer'
    dc.addEventListener('open', () => session.opened.resolve(dc), { once: true })
    dc.addEventListener('close', () => session.opened.reject(new Error('канал закрылся до открытия')), { once: true })
  }, { once: true })

  await pc.setRemoteDescription({ type: 'offer', sdp: offerSdp })
  const answer = await pc.createAnswer()
  await pc.setLocalDescription(answer)
  await waitIceGathering(pc)
  return { session, answerSdp: pc.localDescription.sdp, opened: session.opened.promise }
}

/** Принять ansver на наш оффер (у "звонящего") */
async function completeOutboundLink (session, answerSdp) {
  await session.pc.setRemoteDescription({ type: 'answer', sdp: answerSdp })
}

/**
 * Общая точка "канал готов": подключаем DataChannel к libp2p.
 * role='outbound' — мы инициировали, притворяемся dialer'ом;
 * role='inbound'  — нас вызвали, принимаем соединение как listener.
 */
async function onLinked (peerStr, dc, role) {
  log(`WebRTC-канал открыт (${role === 'outbound' ? 'мы позвонили' : 'к нам позвонили'}) -> ${shortId(peerStr)}`, 'ok')

  if (role === 'outbound') {
    dcRegistry.outbound.set(peerStr, dc)
    try {
      await App.node.dial(multiaddr(foxAddr(peerStr)))
    } catch (e) {
      dcRegistry.outbound.delete(peerStr)
      try { dc.close() } catch {}
      log('ошибка входа в libp2p: ' + e.message, 'err')
      throw e
    }
    // dial мог не забрать канал (соединение уже существовало) — приберём дубль
    if (dcRegistry.outbound.has(peerStr)) {
      dcRegistry.outbound.delete(peerStr)
      try { dc.close() } catch {}
    }
  } else {
    try {
      await acceptInboundConnection(dc, peerStr)
    } catch (e) {
      log('ошибка входящего подключения libp2p: ' + e.message, 'err')
      try { dc.close() } catch {}
      throw e
    }
  }
}

/* ==========================================================================
 * 8.5 LanRendezvous — клиент LAN-ретранслятора (WebSocket к server.js)
 *     BroadcastChannel связывает только вкладки одного браузера. Чтобы ПК и
 *     телефон видели друг друга по @юзернейму, сообщения присутствия и
 *     SDP-сигналинг идут через лёгкий ретранслятор: сервер пересылает
 *     байты между сокетами, но переписка и звонки остаются прямым P2P
 *     (WebRTC, DTLS). Ретранслятора нет (статический хостинг)? Приложение
 *     работает как раньше — поиск только в рамках одной машины.
 * ========================================================================== */

const LanRendezvous = {
  ws: null,
  retryTimer: null,
  started: false,
  warned: false,
  idx: 0,         // какой кандидат адреса ретранслятора пробуем
  missed: 0,      // сколько кандидатов подряд не ответили
  opened: false,  // открылся ли ТЕКУЩИЙ сокет
  validated: false, // пришёл ли ростер (сокет = наш ретранслятор)
  rosterTimer: null,

  /** WebSocket-канал к ретранслятору открыт? */
  get connected () { return !!(this.ws && this.ws.readyState === WebSocket.OPEN) },

  start () {
    this.started = true
    this._open()
  },

  /** Адреса ретранслятора по приоритету:
   *  1) тот же хост И порт, что у страницы (обычный случай: node server.js);
   *  2) тот же хост на стандартном порту 8099 — когда страницу отдаёт другой
   *     сервер без WebSocket (Live Server :5500 и т.п.), иначе ПК и телефон
   *     попадают на разные ретрансляторы и не видят друг друга;
   *  3) свои публичные ретрансляторы из CFG.RELAY_URLS (server.js на VPS)
   *     и разовый оверрайд localStorage['foxosis/relay'] — интернет-режим. */
  _candidates () {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
    const list = []
    const push = (host) => {
      if (!host) return
      const url = `${proto}//${host}${CFG.WS_PATH}`
      if (!list.includes(url)) list.push(url)
    }
    const pushUrl = (u) => {
      if (u && typeof u === 'string' && /^wss?:\/\//.test(u) && !list.includes(u)) list.push(u)
    }
    push(location.host)
    push(`${location.hostname}:8099`)
    for (const u of CFG.RELAY_URLS) pushUrl(u)
    try { pushUrl(localStorage.getItem('foxosis/relay')) } catch { /* приватный режим */ }
    return list
  },

  _open () {
    if (!this.started || this.ws) return
    if (!location.host || location.protocol === 'file:') return // только по http(s)
    const list = this._candidates()
    if (this.idx >= list.length) this.idx = 0
    const url = list[this.idx]
    let ws
    try {
      ws = new WebSocket(url)
    } catch {
      this._advance(true)
      return
    }
    this.ws = ws
    this.opened = false
    this.validated = false
    ws.onopen = () => {
      this.opened = true
      // регистрация у ретранслятора. Настоящий ретранслятор ОБЯЗАН ответить
      // ростером на первое hello — а «чужие» WebSocket (например, livereload
      // у Live Server) молчат или отвечают иначе. Не пришёл ростер за 2 с —
      // сокет негодный: закрываем и пробуем следующий адрес.
      this._send({
        t: 'hello',
        from: myId(),
        name: Store.data.profile.name || '',
        username: Store.data.profile.username || ''
      })
      clearTimeout(this.rosterTimer)
      this.rosterTimer = setTimeout(() => {
        log('сокет не ответил ростером — пробуем другой адрес', 'warn')
        try { ws.close() } catch {}
      }, 2000)
    }
    ws.onmessage = (ev) => this._onMessage(ev.data)
    ws.onclose = () => {
      clearTimeout(this.rosterTimer)
      const ok = this.opened && this.validated
      if (this.ws === ws) this.ws = null
      if (!ok) {
        this._advance(true) // адрес не подошёл (или чужой WebSocket)
        return
      }
      // подключение было — возвращаемся на тот же (проверенный) адрес
      this._schedule(1500)
    }
    ws.onerror = () => { /* onclose отработает */ }
  },

  /** Кандидат не ответил: почти сразу пробуем следующий; после полного
   *  круга — честное предупреждение и пауза перед новым кругом. */
  _advance (schedule) {
    const n = Math.max(this._candidates().length, 1)
    this.idx = (this.idx + 1) % n
    this.missed += 1
    if (this.missed >= n && !this.warned) {
      this.warned = true
      log('LAN-ретранслятор недоступен — поиск идёт через глобальный канал (мир)', 'warn')
    }
    if (schedule) this._schedule(this.missed >= n ? 4000 : 250)
  },

  _schedule (ms) {
    if (!this.started) return
    clearTimeout(this.retryTimer)
    this.retryTimer = setTimeout(() => this._open(), ms)
  },

  _send (obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try { this.ws.send(JSON.stringify(obj)) } catch { /* сокет закрылся */ }
    }
  },

  /** Отправить сообщение рандевью на ДРУГИЕ устройства (своим вкладкам оно
   *  уже ушло по BroadcastChannel). Сервер маршрутизирует по `to`,
   *  а если `to` нет — рассылает всем, кроме отправителя. */
  send (msg) {
    this._send({ t: 'relay', to: msg.to || '', data: msg })
  },

  _onMessage (raw) {
    let m
    try { m = JSON.parse(raw) } catch { return }

    // ростер: узлы, подключёншиеся до нас — видны в поиске сразу
    if (m.t === 'roster' && Array.isArray(m.peers)) {
      clearTimeout(this.rosterTimer)
      if (!this.validated) {
        // доказательство, что на том конце именно наш ретранслятор
        this.validated = true
        this.missed = 0
        this.warned = false
        log('LAN-ретранслятор подключён', 'ok')
      }
      let added = false
      for (const p of m.peers) {
        if (!p || !p.from || p.from === myId()) continue
        const had = Rendezvous.lookup(p.from)
        Rendezvous.remember(p.from, p.name || '', p.username || '')
        if (!had || (p.name && had.name !== p.name)) {
          Store.renameContact(p.from, p.name || '', p.username || '')
          added = true
        }
      }
      if (added) UI.renderChatList()
      if (!UI.els.sideNew.hidden) UI.renderOnlineList()
      return
    }

    // ретранслированное сообщение присутствия/сигналинга — как из BC
    if (m.t === 'relay' && m.data) Rendezvous._handle(m.data)
  }
}

/* ==========================================================================
 * 8.6 NostrRendezvous — ГЛОБАЛЬНЫЙ сигнальный канал (интернет)
 *     Публичные Nostr-релеи вместо MQTT: по одному WebSocket на каждый релей
 *     из CFG.NOSTR_RELAYS, все параллельно. NIP-01: событие kind=NOSTR_KIND
 *     (эфемерный 21337 — релеи его не хранят, только доставляют тем, кто
 *     подписан в момент публикации; надёжность дают повторы hello/offer).
 *     Подписка REQ {'#t': [hello, sig/<myId>], kinds: [21337]}:
 *       foxosis/v1/sig/<peerId> — направленный сигналинг (offer/answer/relay)
 *                                  каждый узел подписывается на СВОЙ топик;
 *       foxosis/v1/hello        — общий «я здесь» (поиск по @юзернейму).
 *     Одно и то же подписанное событие уходит во ВСЕ открытые релеи —
 *     дубли приёма отсекаются по id события, дальше по mid в Rendezvous.
 *     Релеи — только СИГНАЛЬНЫЙ тумблер: сообщения и звонки идут прямым
 *     P2P (WebRTC, DTLS+Noise), релеи их не видят (кроме SDP с IP-кандидатами).
 *     Порядок работы: сначала BC (вкладки), потом LAN (server.js), потом
 *     Nostr (мир) — все каналы дублируют одно и то же сообщение, дубли
 *     отсекаются по mid.
 * ========================================================================== */

/** hex ⇄ байты (ключ и id события NIP-01) */
const hexToBytes = (h) => {
  const out = new Uint8Array(h.length >> 1)
  for (let i = 0; i < out.length; i++) out[i] = parseInt(h.substr(i * 2, 2), 16)
  return out
}
const bytesToHex = (b) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')

const NostrRendezvous = {
  relays: [],        // [{url, ws, timer, backoffIdx, sub, mutedUntil}]
  started: false,
  key: null,         // {priv: Uint8Array(32), pub: hex} — своя пара на вкладку
  pending: [],       // сообщения, ждущие первого открытого релея (≤20)
  warned: false,     // уже жаловались на полную недоступность релеев
  okWarned: false,   // уже жаловались на отказ релея (OK false)
  _ready: false,     // был хотя бы один EOSE (первое подключение/восстановление)
  _seen: new Map(),  // id события -> время (одно событие приходит со всех релеев)
  _rx: null,         // цепочка приёма: порядок обработки при async-сжатии
  _tx: null,         // цепочка отправки: порядок при async-сжатии

  start () {
    this.started = true
    this._loadKey()
    this.relays = CFG.NOSTR_RELAYS.map((url) => ({
      url, ws: null, timer: null, backoffIdx: 0, sub: false, mutedUntil: 0
    }))
    for (const r of this.relays) this._open(r)
    log('глобальный канал запущен — ' + this.relays.length + ' публичных релея (Nostr)', 'sys')
  },

  /** Хватает ли хотя бы одной живой подписки (для тестов/UI) */
  get connected () { return this.relays.some((r) => r.sub) },

  /** secp256k1-ключ подписи событий: свой на каждую вкладку, в localStorage */
  _loadKey () {
    try {
      const stored = localStorage.getItem(instKey('nostr/v1'))
      if (stored && /^[0-9a-f]{64}$/.test(stored)) {
        const priv = hexToBytes(stored)
        this.key = { priv, pub: bytesToHex(schnorr.getPublicKey(priv)) }
        return
      }
    } catch { /* хранилище недоступно — сгенерируем ниже */ }
    for (let i = 0; i < 8; i++) {
      const priv = new Uint8Array(32)
      crypto.getRandomValues(priv)
      try {
        const pub = bytesToHex(schnorr.getPublicKey(priv))
        this.key = { priv, pub }
        try { localStorage.setItem(instKey('nostr/v1'), bytesToHex(priv)) } catch {}
        return
      } catch { /* невалидный ключ (крайне редко) — пробуем снова */ }
    }
    throw new Error('не удалось создать ключ подписи Nostr')
  },

  /** Топики, на которые подписаны: общий hello + свой сигнальный */
  _topics () {
    return [CFG.NOSTR_ROOT + '/hello', CFG.NOSTR_ROOT + '/sig/' + myId()]
  },

  /** Открыть WebSocket к релею и подписаться */
  _open (r) {
    if (!this.started || r.ws) return
    let ws
    try { ws = new WebSocket(r.url) } catch { this._advance(r); return }
    r.ws = ws
    ws.onopen = () => {
      if (r.ws !== ws) return
      // REQ сразу при открытии: фильтр — свой топик + общий hello
      try {
        ws.send(JSON.stringify(['REQ', 'foxosis-1', { '#t': this._topics(), kinds: [CFG.NOSTR_KIND], limit: 20 }]))
      } catch { /* сокет умер мгновенно — onclose разрулит */ }
    }
    ws.onmessage = (ev) => { if (r.ws === ws) this._onMsg(r, ev.data) }
    ws.onclose = () => {
      if (r.ws !== ws) return
      r.ws = null
      r.sub = false
      if (!this.relays.some((x) => x.ws)) this._ready = false // полный обвал — EOSE снова «первый»
      if (this.started) this._advance(r)
    }
    ws.onerror = () => { /* onclose последует */ }
  },

  /** Переподключение с нарастающей паузой (и одна жалоба на полный простой) */
  _advance (r) {
    const steps = CFG.NOSTR_BACKOFF_MS
    const delay = steps[Math.min(r.backoffIdx, steps.length - 1)]
    r.backoffIdx++
    if (!this.warned && r.backoffIdx >= steps.length && this.relays.every((x) => !x.ws)) {
      this.warned = true
      log('нет связи с публичными релеями — глобальный поиск приостановлен (локальные каналы работают)', 'warn')
    }
    r.timer = setTimeout(() => { r.timer = null; this._open(r) }, delay)
  },

  /** Кадр от релея: EVENT / EOSE / OK / CLOSED / NOTICE (NIP-01) */
  _onMsg (r, raw) {
    let frame
    try { frame = JSON.parse(raw) } catch { return }
    if (!Array.isArray(frame) || typeof frame[0] !== 'string') return

    if (frame[0] === 'EVENT') {
      const ev = frame[2]
      this._rx = (this._rx || Promise.resolve())
        .then(() => this._onEvent(ev))
        .catch(() => {})
      return
    }
    if (frame[0] === 'EOSE') {
      r.sub = true
      r.backoffIdx = 0
      this.warned = false
      if (!this._ready) {
        this._ready = true
        log('глобальный канал подключён — ищем собеседников по всему миру', 'ok')
        const queued = this.pending
        this.pending = []
        for (const m of queued) this.send(m)
        try { Rendezvous._tick() } catch {}      // свежий hello сразу в эфир
        try { Rendezvous._republish() } catch {} // незавершённый offer/answer — повтором
        this._flushClaim()                       // заявка юзернейма — после обвала сети
      }
      return
    }
    if (frame[0] === 'OK') {
      if (frame[2] === false) {
        // релей отказал (rate limit/policy) — глушим публикации в него на 15 с
        r.mutedUntil = Date.now() + 15000
        if (!this.okWarned) {
          this.okWarned = true
          log('релей отклонил событие: ' + (frame[3] || 'без причины') + ' — остальные релеи работают', 'sys')
        }
      }
      return
    }
    if (frame[0] === 'CLOSED') { // подписку закрыли — закрываем WS, onclose пере-REQ
      try { r.ws && r.ws.close() } catch {}
    }
    // NOTICE — информационные тексты, игнорируем
  },

  /** Принятое событие → проверка, дедуп, распаковка, в Rendezvous */
  async _onEvent (ev) {
    if (!ev || typeof ev !== 'object' || ev.kind !== CFG.NOSTR_KIND || typeof ev.content !== 'string') return
    if (ev.id) {
      if (this._seen.has(ev.id)) return // то же событие с другого релея
      this._seen.set(ev.id, Date.now())
      if (this._seen.size > 600) {
        const cut = Date.now() - 120000
        for (const [k, ts] of this._seen) if (ts < cut) this._seen.delete(k)
      }
    }
    // релей обязан доставлять только наши топики — но проверяем и сами
    const want = this._topics()
    const tags = Array.isArray(ev.tags) ? ev.tags : []
    if (!tags.some((t) => Array.isArray(t) && t[0] === 't' && want.includes(t[1]))) return

    let data = null
    try { data = JSON.parse(ev.content) } catch { return }
    if (data && data.__z === 1 && typeof data.d === 'string') {
      const raw = await inflateText(data.d)
      if (raw == null) return
      try { data = JSON.parse(raw) } catch { return }
    }
    if (!data || typeof data !== 'object') return
    if (typeof Rendezvous !== 'undefined' && Rendezvous) Rendezvous._handle(data)
  },

  /** Отправить рандевью-сообщение в интернет:
   *  направленное (есть to) — в топик получателя; иначе — в общий hello.
   *  Пока ни одно релей не открыто — короткая очередь (порция hello не
   *  должна раздуваться: отбрасываем старые, если накопилось > 20).
   *  Крупный JSON (> SIG_COMPRESS_MIN) сжимается: укладываемся в лимиты
   *  релеев, а сжатый SDP — в разумный размер. Цепочка _tx сохраняет
   *  порядок отправки при async-сжатии. */
  send (msg) {
    if (!this.relays.some((r) => r.ws && r.ws.readyState === 1)) {
      this.pending.push(msg)
      if (this.pending.length > 20) this.pending.shift()
      return
    }
    this._tx = (this._tx || Promise.resolve()).then(() => this._sendNow(msg))
  },

  async _sendNow (msg) {
    const now = Date.now()
    const socks = this.relays.filter((r) => r.ws && r.ws.readyState === 1 && r.mutedUntil < now)
    if (!socks.length) {
      this.pending.push(msg)
      if (this.pending.length > 20) this.pending.shift()
      return
    }
    const topic = msg.to
      ? CFG.NOSTR_ROOT + '/sig/' + msg.to
      : CFG.NOSTR_ROOT + '/hello'
    let body = JSON.stringify(msg)
    if (body.length >= CFG.SIG_COMPRESS_MIN) {
      const packed = await deflateText(body)
      if (packed) body = JSON.stringify({ __z: 1, d: packed })
    }
    let frame
    try {
      frame = JSON.stringify(['EVENT', this._sign(body, topic)])
    } catch (e) {
      log('Nostr: не удалось подписать событие: ' + e.message, 'warn')
      return
    }
    for (const r of socks) {
      try { r.ws.send(frame) } catch { /* умер giữa отправки — onclose переподключит */ }
    }
  },

  /** NIP-01: id = sha256([0, pubkey, created_at, kind, tags, content]),
   *  sig = BIP-340 schnorr-подпись по id. sha256js работает и вне
   *  secure context (http на телефоне), как и весь остальной код. */
  _sign (content, topic) {
    return this._signAny(CFG.NOSTR_KIND, [['t', topic]], content)
  },

  /** Подпись произвольного события (kind + свои теги) */
  _signAny (kind, tags, content) {
    const created_at = Math.floor(Date.now() / 1000)
    const pub = this.key.pub
    const idHex = sha256js(new TextEncoder().encode(
      JSON.stringify([0, pub, created_at, kind, tags, content])
    ))
    const sig = bytesToHex(schnorr.sign(hexToBytes(idHex), this.key.priv))
    return { id: idHex, pubkey: pub, created_at, kind, tags, content, sig }
  },

  /** Заявка на @юзернейм: адресное событие kind 30078 с d='uname/<ник>'.
   *  Релеи хранят его (в отличие от эфемерного hello), поэтому любое
   *  устройство может проверить: занят ли ник. Публикуем сразу по открытым
   *  сокетам, иначе — после первого EOSE (заявка лежит в _claim). */
  publishUname (username, name, acc) {
    if (!this.key) { try { this._loadKey() } catch { return } }
    try {
      this._claim = this._signAny(CFG.UNAME_KIND, [['d', 'uname/' + username]],
        JSON.stringify({ username, name: name || '', acc: acc || '', ts: Date.now() }))
    } catch { return }
    if (this._flushClaim(true)) log('заявка на @' + username + ' опубликована в публичных релеях', 'ok')
  },

  /** Отправить незакрытую заявку юзернейма на все живые сокеты.
   *  Возвращает true, если отправили хотя бы в один релей. */
  _flushClaim (quiet) {
    if (!this._claim) return false
    const now = Date.now()
    const socks = this.relays.filter((r) => r.ws && r.ws.readyState === 1 && r.mutedUntil < now)
    if (!socks.length) return false
    const frame = JSON.stringify(['EVENT', this._claim])
    for (const r of socks) {
      try { r.ws.send(frame) } catch { /* сокет умер — onclose переподключит */ }
    }
    if (!quiet) log('заявка на @юзернейм повторно отправлена в ' + socks.length + ' релеях', 'sys')
    return true
  },

  /** Проверка @юзернейма в релеях: REQ {'#d':['uname/<ник>'], kinds:[30078]}.
   *  Возвращает {status, name}:
   *   'free'  — заявок нет, ник свободен;
   *   'own'   — есть только НАША учётка (совпал отпечаток пароля acc или наш
   *             ключ подписи) — ник наш, можно пользоваться/восстановиться;
   *   'taken' — чужая заявка: ник занят ДРУГИМ пользователем.
   *  Fail-open: без сокетов или ответа за 2.5 с — 'free' (ничего не блокируем,
   *  офлайн-сборка должна работать). */
  checkUname (username, acc) {
    return new Promise((resolve) => {
      const socks = this.relays.filter((r) => r.ws && r.ws.readyState === 1)
      if (!this.key || !socks.length) return resolve({ status: 'free', name: '' })
      const sub = 'foxosis-uname-' + rndId()
      const filter = { kinds: [CFG.UNAME_KIND], '#d': ['uname/' + username], limit: 10 }
      let done = false
      let eoses = 0
      let ownName = ''
      const handlers = new Map()
      const finish = (status) => {
        if (done) return
        done = true
        clearTimeout(timer)
        for (const r of socks) {
          const h = handlers.get(r)
          if (h) { try { r.ws.removeEventListener('message', h) } catch {} }
          try { if (r.ws && r.ws.readyState === 1) r.ws.send(JSON.stringify(['CLOSE', sub])) } catch {}
        }
        resolve({ status, name: ownName })
      }
      const timer = setTimeout(() => finish(ownName ? 'own' : 'free'), 2500)
      for (const r of socks) {
        const h = (ev) => {
          let f
          try { f = JSON.parse(ev.data) } catch { return }
          if (!Array.isArray(f) || f[1] !== sub) return
          if (f[0] === 'EVENT') {
            const e = f[2]
            if (!e || typeof e !== 'object') return
            let body = null
            try { body = JSON.parse(e.content) } catch {}
            const isOwn = e.pubkey === this.key.pub || !!(body && acc && body.acc === acc)
            if (isOwn) {
              // своя учётка — запомним имя профиля из заявки (для восстановления)
              if (body && typeof body.name === 'string' && body.name) ownName = ownName || body.name
              return
            }
            finish('taken')                       // чужая заявка — ник занят
          } else if (f[0] === 'EOSE') {
            eoses++
            if (eoses >= socks.length) finish(ownName ? 'own' : 'free')
          }
        }
        handlers.set(r, h)
        try {
          r.ws.addEventListener('message', h)
          r.ws.send(JSON.stringify(['REQ', sub, filter]))
        } catch { /* сокет умер — таймер даст 'free' */ }
      }
    })
  }
}

/* ==========================================================================
 * 9. RENDEZVOUS — автоматический поиск собеседников
 *    Три канала: BroadcastChannel (вкладки одного браузера), LAN-ретранслятор
 *    (server.js: ПК ↔ телефон в одной сети) и Nostr-релеи (мир: сети/страны).
 *    Рассылаем "hello" с именем; оффер/ансвер для WebRTC — тоже через них.
 * ========================================================================== */

class LocalRendezvous {
  constructor () {
    this.channel = null
    this.catalog = new Map()   // peerId -> { name, ts }
    this.pending = new Map()   // peerId -> активная link-сессия (оффер/ансвер)
    this.seen = new Map()      // mid -> время: отсечение дублей (BC + LAN)
    this.timer = null
    this.onLinked = null       // коллбэк из boot()
    this._avaAsked = new Set() // у кого уже спрашивали аватар (раз за сессию)
  }

  start (onLinked) {
    this.onLinked = onLinked
    try {
      this.channel = new BroadcastChannel(CFG.BC_CHANNEL)
    } catch (e) {
      log('BroadcastChannel недоступен: ' + e.message, 'err')
      return
    }
    this.channel.onmessage = (evt) => this._handle(evt.data)
    this._tick()
    this.timer = setInterval(() => this._tick(), CFG.HELLO_MS)
    log('локальный rendezvous запущен — собеседники находятся автоматически', 'sys')
  }

  selfId () { return myId() }

  _post (msg) {
    const full = {
      ...msg,
      from: this.selfId(),
      name: Store.data.profile.name || '',
      username: Store.data.profile.username || '',
      mid: rndId() // id сообщения: приёмник отсекает дубли BC + LAN
    }
    try {
      this.channel.postMessage(full)
    } catch { /* канал закрыт */ }
    // дублируем на другие устройства — через LAN-ретранслятор (server.js)
    LanRendezvous.send(full)
    // и в интернет — через публичные Nostr-релеи (глобальный поиск/сигналинг)
    NostrRendezvous.send(full)
  }

  /** Периодический "я здесь" + чистка устаревших записей */
  _tick () {
    this._post({ t: 'hello' })
    const now = Date.now()
    let changed = false
    for (const [id, rec] of this.catalog) {
      if (now - rec.ts > CFG.HELLO_TTL) { this.catalog.delete(id); changed = true }
    }
    if (changed && !UI.els.sideNew.hidden) UI.renderOnlineList()
  }

  _handle (m) {
    if (!m || typeof m !== 'object') return

    // одно и то же сообщение приходит и по BroadcastChannel (вкладки одного
    // origin), и по LAN-ретранслятору (другие устройства) — дубль отсекаем
    if (m.mid) {
      if (this.seen.has(m.mid)) return
      this.seen.set(m.mid, Date.now())
      if (this.seen.size > 400) {
        const cutoff = Date.now() - 60000
        for (const [k, ts] of this.seen) if (ts < cutoff) this.seen.delete(k)
      }
    }

    const from = m.from
    if (!from || from === this.selfId()) return

    // обновляем каталог присутствия (имя, @юзернейм, время последнего пинга)
    this.remember(from, m.name || '', m.username || '')

    if (m.t === 'hello') {
      // новое имя/юзернейм могло прийти — подправить список
      if (m.name || m.username) {
        const before = Store.data.chats[from]
        Store.renameContact(from, m.name, m.username)
        const after = Store.data.chats[from]
        if (!before || before.name !== after.name || before.username !== after.username) {
          UI.renderChatList()   // новый собеседник появился в «Недавних»
          UI.updateChatHeader() // …или переименован открытый чат
        }
      }
      // один раз за сессию спросить аватар собеседника
      if (!this._avaAsked.has(from)) {
        this._avaAsked.add(from)
        if (this._avaAsked.size > 400) this._avaAsked.delete(this._avaAsked.values().next().value)
        this._post({ t: 'ava?', to: from })
      }
      // есть ОТКРЫТЫЙ чат, соединения нет, собеседник «в сети» — чиним сами
      // (после обрыва/смены сети). Фоновые чаты НЕ трогаем: иначе обе
      // стороны одновременно лезут с офферами и гонка крутится по кругу.
      if (App.currentPeer === from && !isConnectedTo(from)) Reconnector.schedule(from)
      return
    }
    // аватар собеседника: прислали (broadcast или ответ на наш запрос)
    if (m.t === 'ava') {
      if ((!m.to || m.to === this.selfId()) && typeof m.ava === 'string' && m.ava) {
        const chat = Store.data.chats[from]
        if (chat && chat.ava !== m.ava) {
          chat.ava = m.ava
          Store.save()
          UI.renderChatList()
          if (!UI.els.sideNew.hidden) UI.renderOnlineList()
          UI.updateChatHeader()
        }
      }
      return
    }
    // нас спросили о нашем аватаре — отвечаем только спросившему
    if (m.t === 'ava?') {
      const own = Store.data.profile.ava
      if (own) this._post({ t: 'ava', to: from, ava: own })
      return
    }
    if (m.t === 'offer' && (!m.to || m.to === this.selfId())) return this._onOffer(from, m)
    if (m.t === 'answer' && (!m.to || m.to === this.selfId())) return this._onAnswer(from, m)
  }

  /** Запомнить собеседника в каталоге присутствия (общий для BC и PubSub) */
  remember (peerId, name, username) {
    if (!peerId || peerId === this.selfId()) return
    const prev = this.catalog.get(peerId) || {}
    this.catalog.set(peerId, {
      name: name || prev.name || '',
      username: username || prev.username || '',
      ts: Date.now()
    })
    // Дубль @юзернейма: кто-то в сети использует НАШ ник — предупреждаем
    // один раз (в P2P без сервера уникальность не гарантирована, только заметна)
    const mine = Store.data.profile.username
    if (username && mine && username === mine && !this._dupWarned) {
      this._dupWarned = true
      log(`@${username} также открыт на другом устройстве`, 'sys')
      try { UI.toast('@' + username + ' ещё открыт на другом устройстве — ок, если это вы') } catch {}
    }
  }

  /** Метаданные собеседника из каталога присутствия (или null) */
  lookup (peerId) {
    return this.catalog.get(peerId) || null
  }

  /** Переслать незавершённый сигналинг (после восстановления Nostr-канала):
   *  оффер ждёт ansver, ansver ждёт открытия канала — копии доползут. */
  _republish () {
    for (const [peerStr, slot] of this.pending) {
      const caller = slot.caller
      if (caller && caller.offerSdp && !caller.answered && !caller.cancelled) {
        this._post({ t: 'offer', to: peerStr, sdp: caller.offerSdp })
      }
      const callee = slot.callee
      if (callee && callee.answerSdp && !callee.cancelled) {
        this._post({ t: 'answer', to: peerStr, sdp: callee.answerSdp })
      }
    }
  }

  /** Публичный список онлайн: [{peerId, name, username}] */
  onlinePeers () {
    const out = []
    for (const [peerId, rec] of this.catalog) {
      if (peerId === this.selfId()) continue
      const chat = Store.data.chats[peerId]
      out.push({
        peerId,
        name: (chat && chat.name) || rec.name || shortId(peerId),
        username: (chat && chat.username) || rec.username || ''
      })
    }
    return out.sort((a, b) => a.name.localeCompare(b.name, 'ru'))
  }

  isOnline (peerId) { return this.catalog.has(peerId) }

  /** Пользователь нажал «Подключиться» (клик по контакту) */
  async connect (peerStr) {
    if (!peerStr) throw new Error('пустой идентификатор собеседника')
    if (isConnectedTo(peerStr)) { log('уже подключены', 'sys'); return }
    const busy = this._getSlot(peerStr)
    if (busy && (busy.caller || busy.callee)) throw new Error('подключение уже идёт')

    UI.updateChatStatus()
    log(`создаю WebRTC-оффер для ${shortId(peerStr)}…`, 'sys')

    // Помечаем роль СРАЗУ (до сбора ICE-кандидатов, который занимает ~3 с):
    // чужой оффер, пришедший за это время, обязан увидеть нашу роль и
    // корректно разрешить гонку одновременных подключений.
    const attempt = { session: null, cancelled: false }
    this._setSlot(peerStr, 'caller', attempt)

    let built
    try {
      built = await beginOutboundLink(peerStr)
    } catch (e) {
      this._clearSlot(peerStr, 'caller', attempt)
      log('не удалось создать оффер: ' + e.message, 'err')
      throw e
    }

    // Гонка: пока собирали ICE, чужой оффер отменил наше исходящее
    if (attempt.cancelled) {
      built.session.close()
      this._clearSlot(peerStr, 'caller', attempt)
      return
    }

    attempt.session = built.session
    attempt.offerSdp = built.offerSdp
    this._post({ t: 'offer', to: peerStr, sdp: built.offerSdp })
    // Эфемерный Nostr (kind 21337) релеи не хранят: пока собеседник не
    // подписан — оффер не дойдёт, повторяем его каждые SIG_RETRY_MS
    // (новый mid на каждую копию, чтобы дедуп не отсёк повтор потерянного).
    attempt.retryTimer = setInterval(() => {
      if (attempt.cancelled || attempt.answered || isConnectedTo(peerStr)) {
        clearInterval(attempt.retryTimer)
        attempt.retryTimer = null
        return
      }
      this._post({ t: 'offer', to: peerStr, sdp: built.offerSdp })
    }, CFG.SIG_RETRY_MS)

    try {
      const dc = await withTimeout(built.opened, CFG.CONNECT_TIMEOUT_MS, 'Подключение')
      if (attempt.retryTimer) { clearInterval(attempt.retryTimer); attempt.retryTimer = null }
      this._clearSlot(peerStr, 'caller', attempt)
      await this.onLinked(peerStr, dc, 'outbound')
      UI.updateChatStatus()
    } catch (e) {
      if (attempt.retryTimer) { clearInterval(attempt.retryTimer); attempt.retryTimer = null }
      this._clearSlot(peerStr, 'caller', attempt)
      if (attempt.session) {
        try { attempt.session.close() } catch {}
      }
      UI.updateChatStatus()
      // отмена при гонке — не ошибка, вторую сторону ведёт их же оффер
      if (!attempt.cancelled) log('не удалось подключиться: ' + e.message, 'err')
      throw e
    }
  }

  /** К нам пришёл оффер */
  async _onOffer (from, m) {
    if (isConnectedTo(from)) return

    const slot = this._getSlot(from) || this._setSlot(from, 'callee', null)
    // уже отвечаем на их же оффер — повторный не трогаем, но ansver
    // пересылаем: он мог потеряться (обрыв релея/сети)
    if (slot.callee) {
      if (slot.callee.answerSdp) {
        this._post({ t: 'answer', to: from, sdp: slot.callee.answerSdp })
      }
      return
    }

    if (slot.caller) {
      // ГОНКА одновременных подключений: вызывающим остаётся больший Peer ID
      if (this.selfId() > from) {
        log('гонка подключений: ждём отработки нашего оффера', 'sys')
        return
      }
      log('гонка подключений: отменяем свой оффер, принимаем чужой', 'sys')
      const attempt = slot.caller
      slot.caller = null
      attempt.cancelled = true           // connect() увидит флаг и завершится
      if (attempt.session) { try { attempt.session.close() } catch {} }
    }

    // Становимся callee: НЕ удаляем слот (в нём может висеть наш caller),
    // просто занимаем вторую роль и ждём открытия канала.
    const attempt = { session: null, cancelled: false, answerSdp: null, retryTimer: null }
    slot.callee = attempt
    try {
      const { session, answerSdp, opened } = await beginInboundLink(from, m.sdp)
      attempt.session = session
      attempt.answerSdp = answerSdp
      this._post({ t: 'answer', to: from, sdp: answerSdp })
      // ansver тоже QoS0 — пересылаем, пока канал не открылся (его копия
      // доберётся, даже если первая потерялась; дедуп по mid внутри _handle)
      attempt.retryTimer = setInterval(() => {
        if (attempt.cancelled || isConnectedTo(from)) {
          clearInterval(attempt.retryTimer)
          attempt.retryTimer = null
          return
        }
        this._post({ t: 'answer', to: from, sdp: answerSdp })
      }, CFG.SIG_RETRY_MS)
      const dc = await withTimeout(opened, CFG.CONNECT_TIMEOUT_MS, 'Подключение')
      if (attempt.retryTimer) { clearInterval(attempt.retryTimer); attempt.retryTimer = null }
      this._clearSlot(from, 'callee', attempt)
      await this.onLinked(from, dc, 'inbound')
      UI.updateChatStatus()
    } catch (e) {
      if (attempt.retryTimer) { clearInterval(attempt.retryTimer); attempt.retryTimer = null }
      this._clearSlot(from, 'callee', attempt)
      log('ошибка приёма подключения: ' + e.message, 'err')
    }
  }

  /** Нам прислали ansver на наш оффер */
  async _onAnswer (from, m) {
    const slot = this._getSlot(from)
    const attempt = slot && slot.caller
    // слота нет — ответ на уже отменённый/завершённый оффер; session ещё
    // не собрана — ansver пришёл раньше, чем мы выложили свой оффер (гонка)
    if (!attempt || !attempt.session || attempt.cancelled) return
    if (attempt.answered) return // копия ansver (ретрай) — уже применена
    // ansver получен — оффер больше пересылать не нужно
    attempt.answered = true
    if (attempt.retryTimer) { clearInterval(attempt.retryTimer); attempt.retryTimer = null }
    try {
      log('получен ansver — завершаю handshake', 'sys')
      await completeOutboundLink(attempt.session, m.sdp)
      // открытие канала дальше ждёт тот, кто вызвал connect()
    } catch (e) {
      this._clearSlot(from, 'caller', attempt)
      try { attempt.session.close() } catch {}
      log('ошибка приёма ansver: ' + e.message, 'err')
    }
  }

  /* --- слоты ожидающих сессий: по одной на роль (caller/callee) ----------
     Во время гонки у одного собеседника могут одновременно существовать
     ДВЕ сессии (наш исходящий оффер + входящий на их оффер), поэтому
     одна общая запись на собеседника их перетирала и ansver терялся. */
  _getSlot (peerStr) { return this.pending.get(peerStr) }

  _setSlot (peerStr, role, attempt) {
    let s = this.pending.get(peerStr)
    if (!s) { s = { caller: null, callee: null }; this.pending.set(peerStr, s) }
    s[role] = attempt
    return s
  }

  _clearSlot (peerStr, role, attempt) {
    const s = this.pending.get(peerStr)
    if (!s) return
    if (s[role] === attempt) s[role] = null
    if (!s.caller && !s.callee) this.pending.delete(peerStr)
  }
}

const Rendezvous = new LocalRendezvous()

/* ==========================================================================
 * 9.5 Reconnector — автопереподключение P2P после обрыва
 *     Мобильная сеть «моргает»: Wi-Fi ↔ LTE, потеря сигнала, сон телефона.
 *     Без этой секции пользователь видит «соединение разорвано» и должен
 *     нажимать чат вручную. Логика: после peer:disconnect ставим таймер с
 *     экспоненциальным backoff (2с → 30с, макс. RECONNECT_TRIES раз подряд),
 *     пока собеседник «в сети» (его hello продолжает приходить). Успешное
 *     подключение или уход собеседника офлайн — цепочку гасим.
 * ========================================================================== */

const Reconnector = {
  timers: new Map(),   // peerId -> { timeout, tries }

  /** Поставить/переставить автопереподключение к собеседнику */
  schedule (peerStr) {
    if (!peerStr || peerStr === myId()) return
    if (isConnectedTo(peerStr)) return
    const slot = this.timers.get(peerStr) || { timeout: null, tries: 0 }
    if (slot.timeout) return // уже запланировано — не плодим таймеры
    // с этой стороны уже идёт сессия (оффер/ансвер) — не накладываем свой
    const busy = Rendezvous._getSlot(peerStr)
    if (busy && (busy.caller || busy.callee)) return
    const tries = slot.tries
    if (tries >= CFG.RECONNECT_TRIES) return
    const delay = Math.min(
      CFG.RECONNECT_MIN_MS * Math.pow(2, tries),
      CFG.RECONNECT_MAX_MS
    )
    slot.timeout = setTimeout(async () => {
      slot.timeout = null
      if (isConnectedTo(peerStr)) { this.timers.delete(peerStr); return }
      // ушёл офлайн — автоперепессия бессмысленна, ждём его hello
      if (!Rendezvous.isOnline(peerStr)) { this.timers.delete(peerStr); return }
      slot.tries += 1
      log(`автопереподключение к ${shortId(peerStr)} (попытка ${slot.tries})`, 'sys')
      try {
        await Rendezvous.connect(peerStr)
        this.timers.delete(peerStr)          // успех — цепочку снимаем
      } catch {
        this.schedule(peerStr)                // не вышло — следующий шаг backoff
      }
    }, delay)
    this.timers.set(peerStr, slot)
  },

  /** Соединение поднято (в т.ч. вручную) — автопереподключение не нужно */
  cancel (peerStr) {
    const slot = this.timers.get(peerStr)
    if (slot && slot.timeout) clearTimeout(slot.timeout)
    this.timers.delete(peerStr)
  },

  /** Сброс счётчика попыток: собеседник снова виден (пришёл hello) */
  reset (peerStr) {
    const slot = this.timers.get(peerStr)
    if (slot) slot.tries = 0
  }
}

/* ==========================================================================
 * 10. CODES — подключение по коду (для разных устройств/сетей)
 *     Код = base64url(JSON) с префиксом FOX1.: в нём лежат SDP и Peer ID,
 *     поэтому поле «ID собеседника» для этой схемы не нужно.
 * ========================================================================== */

function encodeCode (obj) {
  return 'FOX1.' + b64encode(ENCODER.encode(JSON.stringify(obj)))
}
function decodeCode (text) {
  const t = String(text || '').trim()
  if (!t.startsWith('FOX1.')) throw new Error('не похоже на код FoxOsis')
  let obj
  try { obj = JSON.parse(DECODER.decode(b64decode(t.slice(5)))) } catch { throw new Error('код повреждён') }
  if (obj.v !== 1 || !obj.k || !obj.sdp) throw new Error('код повреждён')
  return obj
}

const Codes = {
  pending: null, // исходящая link-сессия в ожидании ответного кода

  /** Создать код-приглашение (оффер) */
  async create () {
    if (this.pending) { this.pending.close(); this.pending = null }
    try {
      const { session, offerSdp } = await beginOutboundLink('')
      this.pending = session
      // чтобы не держать "висящий" rejection, если ответ так и не придёт
      session.opened.promise.catch(() => {})
      UI.els.codeBox.value = encodeCode({ v: 1, k: 'offer', from: myId(), sdp: offerSdp })
      UI.els.codeBox.focus()
      UI.els.codeBox.select()
      UI.toast('Код-приглашение создан — отправьте его собеседнику')
      log('создан код-приглашение', 'ok')
    } catch (e) {
      UI.toast('Не удалось создать код')
      log('create code: ' + e.message, 'err')
    }
  },

  /** Принять код: это либо оффер (мы отвечаем), либо ansver (мы завершаем) */
  async accept () {
    let msg
    try {
      msg = decodeCode(UI.els.codeBox.value)
    } catch (e) {
      UI.toast('Код не распознан')
      log('decode: ' + e.message, 'warn')
      return
    }

    if (msg.k === 'offer') {
      try {
        const { session, answerSdp, opened } = await beginInboundLink(msg.from, msg.sdp)
        Store.ensureChat(msg.from) // чат заведём заранее — id пришёл в коде
        UI.els.codeBox.value = encodeCode({ v: 1, k: 'answer', from: myId(), to: msg.from, sdp: answerSdp })
        UI.els.codeBox.focus()
        UI.els.codeBox.select()
        UI.toast('Ответный код готов — отправьте его обратно')
        log('ответный код создан для ' + shortId(msg.from), 'ok')
        opened.then((dc) => this._finish(session, dc)).catch(e => log('канал не открылся: ' + e.message, 'err'))
      } catch (e) {
        UI.toast('Ошибка приёма кода')
        log('accept offer: ' + e.message, 'err')
      }
      return
    }

    if (msg.k === 'answer') {
      const session = this.pending
      if (!session) { UI.toast('Сначала нажмите «Создать код»'); return }
      if (msg.to && msg.to !== myId()) { UI.toast('Код адресован другому узлу'); return }
      this.pending = null
      session.peerStr = msg.from
      Store.ensureChat(msg.from)
      try {
        await completeOutboundLink(session, msg.sdp)
        const dc = await withTimeout(session.opened.promise, CFG.CONNECT_TIMEOUT_MS, 'Подключение')
        await this._finish(session, dc)
        UI.toast('Подключение по коду установлено')
      } catch (e) {
        session.close()
        UI.toast('Не удалось подключиться по коду')
        log('accept answer: ' + e.message, 'err')
      }
      return
    }
    log('неизвестный тип кода: ' + msg.k, 'warn')
  },

  /**
   * Общий финал: канал готов -> в libp2p.
   * Роль берём прямо из сессии: 'caller' = мы звонили (dialer/outbound),
   * 'callee' = к нам пришли (listener/inbound).
   */
  async _finish (session, dc) {
    const role = session.role === 'caller' ? 'outbound' : 'inbound'
    await onLinked(session.peerStr, dc, role)
  }
}

/* ==========================================================================
 * 11. ProtocolChannel / Chat / Signal — протоколы поверх libp2p
 *     Каждая сторона открывает ИСХОДЯЩИЙ поток (dialProtocol) и слушает
 *     ВХОДЯЩИЙ (node.handle); отправка — по своему исходящему потоку.
 * ========================================================================== */

class ProtocolChannel {
  /**
   * @param {string} proto         идентификатор протокола
   * @param {(peerId:string, msg:any)=>void} onMessage  входящие JSON-сообщения
   */
  constructor (proto, onMessage) {
    this.proto = proto
    this.onMessage = onMessage
    this.out = new Map()        // peerId -> JsonLineStream (наш исходящий)
    this.in = new Map()         // peerId -> JsonLineStream (входящий)
    this.attaching = new Map()  // peerId -> Promise (защита от двойного dial)
  }

  /** Открыть исходящий поток к собеседнику (идемпотентно) */
  async attach (peerStr) {
    const cur = this.out.get(peerStr)
    if (cur && !cur.closed) return cur
    const inflight = this.attaching.get(peerStr)
    if (inflight) return inflight

    const p = (async () => {
      const stream = await App.node.dialProtocol(
        multiaddr(foxAddr(peerStr)),
        this.proto,
        { signal: AbortSignal.timeout(10_000) }
      )
      this._bind('out', peerStr, stream)
      log(`поток ${this.proto} открыт к ${shortId(peerStr)}`, 'sys')
      return this.out.get(peerStr)
    })()

    this.attaching.set(peerStr, p)
    try { return await p } finally { this.attaching.delete(peerStr) }
  }

  /** Регистрируем входящий поток (вызывается из node.handle) */
  handleInbound (stream, connection) {
    const peerStr = connection.remotePeer.toString()
    this._bind('in', peerStr, stream)
    log(`входящий поток ${this.proto} от ${shortId(peerStr)}`, 'sys')
  }

  _bind (dir, peerStr, stream) {
    const map = dir === 'out' ? this.out : this.in
    const line = new JsonLineStream(
      stream,
      (msg) => this.onMessage(peerStr, msg),
      () => { if (map.get(peerStr) === line) map.delete(peerStr) }
    )
    map.set(peerStr, line)
    return line
  }

  /** Отправить JSON собеседнику (с автооткрытием потока) */
  async send (peerStr, obj) {
    let ch = this.out.get(peerStr)
    if (!ch || ch.closed) ch = await this.attach(peerStr)
    if (!ch || ch.closed) ch = this.in.get(peerStr) // запасной путь
    if (!ch || ch.closed) throw new Error(`нет активного потока ${this.proto}`)
    ch.send(obj)
  }
}

/** Текстовый чат */
const Chat = new ProtocolChannel(CFG.PROTO_CHAT, (peerStr, msg) => onChatMessage(peerStr, msg))
/** Сигналинг звонков (SDP оффер/ансвер, busy, hangup) */
const Signal = new ProtocolChannel(CFG.PROTO_SIGNAL, (peerStr, msg) => {
  Call.onSignal(peerStr, msg).catch(e => log('ошибка сигналинга: ' + e.message, 'err'))
})

/** Обработка входящих сообщений чата */
function onChatMessage (peerStr, msg) {
  if (!msg || typeof msg !== 'object') return
  switch (msg.k) {
    case 'msg': {
      // пришло новое сообщение; имя могло прийти в 'hi' раньше
      const chat = Store.ensureChat(peerStr)
      if (chat.messages.some(m => m.id === msg.id)) return // дедупликация
      const rec = {
        id: msg.id || rndId(),
        side: 'peer',
        text: String(msg.text || ''),
        ts: msg.ts || Date.now(),
        delivered: true,
        read: false
      }
      Store.appendMessage(peerStr, rec)

      const isOpen = App.currentPeer === peerStr
      if (isOpen) {
        rec.read = true
        Store.save()
        UI.appendMessageToView(peerStr, rec)
        UI.renderChatList()
        // подтверждаем прочтение — отправитель увидит ✓✓
        Chat.send(peerStr, { k: 'read', id: rec.id }).catch(() => {})
      } else {
        chat.unread = (chat.unread || 0) + 1
        Store.save()
        UI.renderChatList()
        UI.toast(`${chat.name}: ${rec.text}`)
      }
      break
    }

    case 'read': {
      // собеседник прочитал наше сообщение
      const m = Store.findMessage(peerStr, msg.id)
      if (m && !m.read) { m.read = true; Store.save(); UI.refreshMeta(peerStr) }
      break
    }

    case 'hi': {
      // представление при подключении: имя и @юзернейм собеседника
      if (msg.name || msg.username) {
        Store.renameContact(peerStr, msg.name, msg.username)
        Rendezvous.remember(peerStr, msg.name || '', msg.username || '')
        UI.renderChatList()
        UI.updateChatHeader()
      }
      break
    }

    case 'ping':
      Chat.send(peerStr, { k: 'pong', ts: Date.now() }).catch(() => {})
      break

    case 'pong':
      break // активность потока — отдельно логировать нечего

    default:
      log(`чат: неизвестный тип "${msg.k}" от ${shortId(peerStr)}`, 'warn')
  }
}

/** Отправка сообщения пользователем */
async function sendChatText (text) {
  const t = text.trim()
  if (!t) return
  const peer = App.currentPeer
  if (!peer) { UI.toast('Сначала выберите чат'); return }

  const msg = {
    id: rndId(),
    side: 'me',
    text: t,
    ts: Date.now(),
    delivered: false,
    read: false
  }
  Store.appendMessage(peer, msg)
  UI.appendMessageToView(peer, msg)
  UI.renderChatList()
  await tryDeliver(peer, msg)
}

/** Доставка одного сообщения: если нет соединения — попробовать подключиться */
async function tryDeliver (peer, msg) {
  const doSend = async () => {
    await Chat.send(peer, { k: 'msg', id: msg.id, text: msg.text, ts: msg.ts })
    msg.delivered = true
    Store.save()
    UI.refreshMeta(peer)
  }

  if (isConnectedTo(peer)) {
    try { await doSend(); return } catch (e) { log('доставка не удалась: ' + e.message, 'err') }
  }
  // соединения нет: если собеседник в сети — подключаемся и отправляем
  if (Rendezvous.isOnline(peer)) {
    try {
      await Rendezvous.connect(peer)
      await doSend()
    } catch (e) {
      log('не удалось отправить: ' + e.message, 'err')
      UI.toast('Соединение не установлено — сообщение отправится позже')
    }
  } else {
    UI.toast('Собеседник не в сети — сообщение отправится при появлении')
  }
}

/** Дослать недоставленные сообщения при появлении соединения */
function flushUndelivered (peer) {
  const chat = Store.data.chats[peer]
  if (!chat) return
  const pending = chat.messages.filter(m => m.side === 'me' && !m.delivered)
  if (pending.length === 0) return
  log(`досылаю ${pending.length} недоставленных сообщений…`, 'sys')
  let chain = Promise.resolve()
  for (const m of pending) chain = chain.then(() => tryDeliver(peer, m).catch(() => {}))
}

/* ==========================================================================
 * 12. Presence — присутствие через GossipSub (PubSub, как просили в ТЗ)
 *     Основной статус «в сети» даёт BroadcastChannel/соединения, а PubSub
 *     демонстрирует публикации в теме и готов для комнат/общего эфира.
 * ========================================================================== */

const Presence = {
  start () {
    const ps = App.node.services.pubsub
    if (!ps) { log('pubsub недоступен — присутствие только через соединения', 'warn'); return }

    ps.addEventListener('message', (evt) => {
      const d = evt.detail || {}
      let payload = null
      try { payload = JSON.parse(DECODER.decode(d.data)) } catch { return }
      let from = d.from
      if (from && typeof from !== 'string') from = from.toString ? String(from.toString()) : ''
      if (!from || from === myId()) return

      if (payload.k === 'hello') {
        log(`[PubSub] hello от ${shortId(from)}`, 'ok')
        // и имя, и @юзернейм — по ним ищут собеседников в списке
        Rendezvous.remember(from, payload.name || '', payload.username || '')
        if (payload.name || payload.username) Store.renameContact(from, payload.name, payload.username)
        UI.renderChatList()
      } else if (payload.k === 'room' && typeof payload.text === 'string') {
        // заготовка для «общего эфира»/комнат: показываем прямо в чате
        UI.appendMessageToView(from, { id: rndId(), side: 'peer', text: payload.text, ts: Date.now(), delivered: true, read: true })
      }
    })

    try {
      ps.subscribe(CFG.TOPIC_PRESENCE)
      log(`PubSub: подписка на тему ${CFG.TOPIC_PRESENCE}`, 'sys')
    } catch (e) {
      log('pubsub subscribe: ' + e.message, 'warn')
    }
  },

  /** Опубликовать приветствие (имя узла) в тему присутствия */
  async announce () {
    const ps = App.node && App.node.services.pubsub
    if (!ps) return
    try {
      await ps.publish(
        CFG.TOPIC_PRESENCE,
        ENCODER.encode(JSON.stringify({
          k: 'hello',
          name: Store.data.profile.name,
          username: Store.data.profile.username,
          from: myId(),
          ts: Date.now()
        }))
      )
    } catch {
      /* нет подписчиков на тему — для P2P без ретрансляторов это нормально */
    }
  },

  /** При новом соединении: переотправляем подписку и hello */
  onPeerConnected () {
    const ps = App.node && App.node.services.pubsub
    if (!ps) return
    // повторная подписка заставляет gossipsub заново отправить subscription
    // уже открытым пирам — так hello гарантированно дойдёт.
    try { ps.unsubscribe(CFG.TOPIC_PRESENCE); ps.subscribe(CFG.TOPIC_PRESENCE) } catch {}
    this.announce()
  }
}

/* ==========================================================================
 * 13. Call — аудио/видео звонки (WebRTC RTCPeerConnection + STUN/TURN)
 *     Сигналинг (SDP) передаётся ВНУТРИ канала libp2p по PROTO_SIGNAL.
 *     ICE: несколько публичных STUN (Google/Cloudflare/Twilio) + бесплатный
 *     TURN openrelay — прямое соединение или ретрансляция через relay-сервер,
 *     что позволяет звонить через разные сети и симметричные NAT.
 * ========================================================================== */

class CallManager {
  constructor () {
    this.state = 'idle'   // idle | outgoing | incoming | active
    this.peer = null
    this.kind = 'video'   // voice | video
    this.pc = null
    this.localStream = null
    this.remoteStream = null
    this.pendingOffer = null // сохранённый входящий оффер до нажатия «Принять»
  }

  /** Есть ли доступ к камере/микрофону (в secure context: https или localhost) */
  _canCapture () {
    return !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia)
  }

  /** «Позвонить» из шапки чата */
  async start (kind) {
    if (this.state !== 'idle') { UI.toast('Звонок уже идёт'); return }
    const peer = App.currentPeer
    if (!peer) { UI.toast('Сначала выберите чат'); return }
    // по http (например, на телефоне в локалке) браузер не отдаёт камеру/микрофон
    if (!this._canCapture()) {
      UI.toast('Звонки требуют HTTPS или localhost — по http браузер не даёт камеру')
      return
    }

    // звонок требует сигнального канала — подключаемся при необходимости
    if (!isConnectedTo(peer)) {
      if (!Rendezvous.isOnline(peer)) { UI.toast('Собеседник не в сети'); return }
      try {
        UI.setCallStatus('Подключение…')
        await Rendezvous.connect(peer)
      } catch {
        UI.toast('Не удалось подключиться к собеседнику')
        return
      }
    }

    this.state = 'outgoing'
    this.peer = peer
    this.kind = kind
    UI.openCallOverlay(peer, kind, 'Запрос камеры…')
    log(`исходящий ${kind === 'voice' ? 'голосовой' : 'видео'}-звонок -> ${shortId(peer)}`, 'sys')

    try {
      // 1) доступ к камере/микрофону
      this.localStream = await navigator.mediaDevices.getUserMedia({
        video: kind === 'video',
        audio: true
      })
      UI.els.localVideo.srcObject = this.localStream
      UI.setCallStatus('Вызов…')

      // 2) создаём RTCPeerConnection и добавляем треки
      this._createPc(peer)
      this._addLocalTracks()

      // 3) оффер -> локальное описание -> сбор ICE -> отправка через libp2p
      const offer = await this.pc.createOffer()
      await this.pc.setLocalDescription(offer)
      await waitIceGathering(this.pc)
      await Signal.send(peer, { k: 'call-offer', kind, sdp: this.pc.localDescription.sdp })
      UI.setCallStatus('Вызов…')
      log('оффер звонка отправлен по каналу libp2p', 'ok')
    } catch (e) {
      UI.toast('Не удалось начать вызов: ' + e.message)
      log('start call: ' + e.message, 'err')
      this._teardown()
    }
  }

  /** Входящий оффер пришёл — показываем окно входящего звонка */
  async _onOffer (peerStr, msg) {
    if (this.state === 'active' || this.state === 'incoming') {
      Signal.send(peerStr, { k: 'busy' }).catch(() => {})
      log('входящий вызов проигнорирован — занято', 'warn')
      return
    }
    if (this.state === 'outgoing') {
      // ГОНКА одновременных вызовов: больший Peer ID остаётся "звонящим"
      if (myId() < peerStr) {
        Signal.send(peerStr, { k: 'busy' }).catch(() => {})
        log('одновременные вызовы — наш вызов продолжается', 'sys')
        return
      }
      this._teardown() // уступаем: принимаем чужой вызов
    }

    this.state = 'incoming'
    this.peer = peerStr
    this.kind = msg.kind === 'voice' ? 'voice' : 'video'
    this.pendingOffer = msg
    UI.showIncoming(peerStr, this.kind)
    UI.els.chatStatus.textContent = 'входящий звонок…'
    UI.els.chatStatus.className = 'status warn'
    log(`входящий звонок от ${shortId(peerStr)} (${this.kind})`, 'sys')
  }

  /** Пользователь нажал «Принять» */
  async accept () {
    if (this.state !== 'incoming' || !this.pendingOffer) return
    if (!this._canCapture()) {
      UI.hideIncoming()
      this.state = 'idle'
      UI.toast('Звонки требуют HTTPS или localhost — по http браузер не даёт камеру')
      return
    }
    const { peer, kind, sdp } = { peer: this.peer, kind: this.kind, sdp: this.pendingOffer.sdp }
    this.pendingOffer = null
    UI.hideIncoming()
    UI.openCallOverlay(peer, kind, 'Запрос камеры…')
    log('принимаем вызов…', 'sys')

    try {
      // 1) свой микрофон/камера
      this.localStream = await navigator.mediaDevices.getUserMedia({ video: kind === 'video', audio: true })
      UI.els.localVideo.srcObject = this.localStream

      // 2) соединение: remote description (оффер), потом свои треки
      this._createPc(peer)
      await this.pc.setRemoteDescription({ type: 'offer', sdp })
      this._addLocalTracks()

      // 3) ansver -> ICE -> отправка обратно через libp2p
      const answer = await this.pc.createAnswer()
      await this.pc.setLocalDescription(answer)
      await waitIceGathering(this.pc)
      await Signal.send(peer, { k: 'call-answer', sdp: this.pc.localDescription.sdp })
      this.state = 'active'
      UI.setCallStatus('Соединение…')
      log('ansver отправлен — идёт обмен медиапотоками', 'ok')
    } catch (e) {
      UI.toast('Не удалось принять вызов: ' + e.message)
      log('accept: ' + e.message, 'err')
      this._teardown()
    }
  }

  /** Пользователь нажал «Отклонить» */
  decline () {
    if (this.state !== 'incoming') return
    const peer = this.peer
    this.pendingOffer = null
    UI.hideIncoming()
    if (peer) Signal.send(peer, { k: 'busy' }).catch(() => {})
    this._teardown()
    log('вызов отклонён', 'sys')
  }

  /** Нам прислали ansver */
  async _onAnswer (peerStr, msg) {
    if (!this.pc || this.state !== 'outgoing') { log('ansver вне звонка — игнорируем', 'warn'); return }
    if (peerStr !== this.peer) { log('ansver от другого узла — игнорируем', 'warn'); return }
    try {
      await this.pc.setRemoteDescription({ type: 'answer', sdp: msg.sdp })
      this.state = 'active'
      UI.setCallStatus('Соединение…')
      log('ansver принят', 'ok')
    } catch (e) {
      log('ошибка ansver: ' + e.message, 'err')
      this._teardown()
    }
  }

  /** Входящие события сигнального канала */
  async onSignal (peerStr, msg) {
    if (!msg || typeof msg !== 'object') return
    switch (msg.k) {
      case 'call-offer': return this._onOffer(peerStr, msg)
      case 'call-answer': return this._onAnswer(peerStr, msg)
      case 'busy':
        log(`${shortId(peerStr)} занят`, 'warn')
        if (this.state === 'outgoing') {
          UI.toast('Собеседник занят')
          this._teardown()
        }
        return
      case 'hangup':
        log(`${shortId(peerStr)} положил трубку`, 'sys')
        if (this.state !== 'idle') {
          UI.toast('Звонок завершён собеседником')
          this._teardown(false)
        }
        return
      default:
        log('сигнал: неизвестный тип ' + msg.k, 'warn')
    }
  }

  /** Повесить трубку (кнопка) */
  hangup () {
    if (this.state === 'idle') return
    const peer = this.peer
    if (peer) Signal.send(peer, { k: 'hangup' }).catch(() => {})
    this._teardown()
    log('звонок завершён локально', 'sys')
  }

  /* ---------- внутренности --------------------------------------------- */

  _createPc (peer) {
    this.pc = new RTCPeerConnection({ iceServers: CFG.ICE_SERVERS })
    this.remoteStream = new MediaStream()
    UI.els.remoteVideo.srcObject = this.remoteStream

    // входящие медиапотоки (видео/аудио собеседника)
    this.pc.addEventListener('track', (evt) => {
      this.remoteStream.addTrack(evt.track)
      log(`получен медиатрек: ${evt.track.kind}`, 'ok')
      UI.setCallStatus('Соединено')
    })
    this.pc.addEventListener('connectionstatechange', () => {
      const st = this.pc && this.pc.connectionState
      if (!st) return
      log('WebRTC: ' + st, st === 'connected' ? 'ok' : '')
      if (st === 'connected' && this.state === 'active') UI.setCallStatus('Соединено')
      if (st === 'failed') { UI.toast('Соединение не удалось'); this._teardown() }
    })
    this.pc.addEventListener('iceconnectionstatechange', () => {
      const st = this.pc && this.pc.iceConnectionState
      if (st === 'failed') {
        // STUN/TURN не смогли пройти NAT — кандидатов (в т.ч. relay) нет
        log('ICE failed — ни прямое соединение, ни TURN не прошли NAT', 'err')
        UI.toast('Не удалось пройти через NAT')
        this._teardown()
      } else if (st === 'disconnected') {
        log('ICE disconnected — связь нестабильна', 'warn')
        UI.setCallStatus('Нестабильная связь…')
      }
    })
    void peer
  }

  _addLocalTracks () {
    if (!this.localStream || !this.pc) return
    for (const track of this.localStream.getTracks()) {
      const already = this.pc.getSenders().some(s => s.track === track)
      if (!already) this.pc.addTrack(track, this.localStream)
    }
  }

  /** Полная очистка состояния звонка */
  _teardown (notifyUi = true) {
    const pc = this.pc
    this.pc = null
    if (pc) { try { pc.close() } catch {} }

    if (this.localStream) {
      for (const t of this.localStream.getTracks()) { try { t.stop() } catch {} }
      this.localStream = null
    }
    if (this.remoteStream) {
      for (const t of this.remoteStream.getTracks()) { try { t.stop() } catch {} }
      this.remoteStream = null
    }

    this.pendingOffer = null
    this.state = 'idle'
    this.peer = null

    if (notifyUi) {
      UI.closeCall()
      UI.hideIncoming()
      UI.updateChatStatus()
    } else {
      UI.closeCall()
      UI.hideIncoming()
      UI.updateChatStatus()
    }
  }
}

const Call = new CallManager()

/* ==========================================================================
 * 14. Общее состояние + события соединений + BOOT
 * ========================================================================== */

/** Глобальное состояние приложения */
const App = {
  node: null,         // экземпляр libp2p
  currentPeer: null,  // открытый сейчас чат (Peer ID)
  ready: false
}

function myId () { return App.node ? App.node.peerId.toString() : '' }
function isConnectedTo (peerStr) {
  if (!App.node || !peerStr) return false
  try { return App.node.getConnections().some(c => c.remotePeer.toString() === peerStr) } catch { return false }
}
function connectedPeers () {
  if (!App.node) return []
  try { return App.node.getConnections().map(c => c.remotePeer.toString()) } catch { return [] }
}

/** Собеседник открыл соединение */
function onPeerConnect (peerObj) {
  const peerStr = peerObj.toString()
  log(`соединение установлено: ${shortId(peerStr)}`, 'ok')
  UI.updateChatStatus()
  UI.renderChatList()

  // сразу открываем протокольные потоки и представляемся (имя + @юзернейм)
  Chat.attach(peerStr)
    .then(() => Chat.send(peerStr, {
      k: 'hi',
      name: Store.data.profile.name,
      username: Store.data.profile.username
    }))
    .catch(e => log('chat stream: ' + e.message, 'warn'))
  Signal.attach(peerStr).catch(e => log('signal stream: ' + e.message, 'warn'))

  Presence.onPeerConnected()
  flushUndelivered(peerStr)   // дослать всё, что копилось офлайн
  Reconnector.cancel(peerStr) // соединение есть — автопереподключение снять
}

/** Соединение разорвано */
function onPeerDisconnect (peerObj) {
  const peerStr = peerObj.toString()
  log(`соединение разорвано: ${shortId(peerStr)}`, 'warn')
  UI.updateChatStatus()
  UI.renderChatList()
  UI.renderOnlineList()

  if (App.currentPeer === peerStr) UI.updateChatStatus()
  if (Call.peer === peerStr && Call.state !== 'idle') {
    UI.toast('Связь с собеседником потеряна')
    Call._teardown()
  }

  // «моргание» сети: собеседник мог остаться онлайн — переподключаемся сами
  if (Store.data.chats[peerStr]) Reconnector.schedule(peerStr)
}

/** Пинг потоков, чтобы соединения и NAT не засыпали */
function keepAliveTick () {
  for (const peer of connectedPeers()) {
    const ch = Chat.out.get(peer)
    if (ch && !ch.closed) {
      try { ch.send({ k: 'ping', ts: Date.now() }) } catch { /* поток сам закроется */ }
    }
  }
}

/** Точка входа */
async function boot () {
  UI.init()
  Store.load()

  // экраны: заставка → регистрация/вход, либо сразу приложение
  Auth.initial()
  if (Store.data.profile.authed && Store.data.profile.name) {
    log(`профиль: ${Store.data.profile.name} (@${Store.data.profile.username || '—'})`, 'sys')
  } else {
    log('нужна регистрация: имя, @юзернейм и пароль', 'sys')
  }

  log('запуск FoxOsis…', 'sys')

  // 1) стабильный ключ узла (= мой "логин" на всех сессиях)
  const privateKey = await KeyStore.load()
  KeyStore.save(privateKey)

  // 2) поднимаем libp2p-узел:
  //    - свой транспорт поверх WebRTC DataChannel;
  //    - Noise (шифрование+аутентификация), Yamux (мультиплексирование);
  //    - identify и GossipSub (PubSub) как сервисы.
  App.node = await createLibp2p({
    privateKey,
    addresses: { listen: [CFG.LISTEN_ADDR] },
    transports: [foxTransport()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    // Фильтр соединений по умолчанию запрещает дозвон до «приватных» адресов
    // (10/8, 192.168/16, 127.0.0.1, 0.0.0.0…), а наш виртуальный адрес —
    // как раз 0.0.0.0: реального сокета нет, роль адреса выполняет метка
    // порта, которую проверяет FoxTransport.dialFilter/listenFilter.
    // Поэтому разрешаем дозвон всем адресам — адресацию мы фильтруем сами.
    connectionGater: {
      denyDialMultiaddr: () => false
    },
    services: {
      identify: identify(),
      pubsub: gossipsub()
    }
  })

  UI.els.myId.textContent = myId()
  log(`libp2p-узел запущен: ${shortId(myId())}`, 'ok')

  // 3) обработчики протоколов приложения (входящие потоки)
  await App.node.handle(CFG.PROTO_CHAT, (stream, connection) => Chat.handleInbound(stream, connection))
  await App.node.handle(CFG.PROTO_SIGNAL, (stream, connection) => Signal.handleInbound(stream, connection))

  // 4) события соединений
  App.node.addEventListener('peer:connect', (evt) => onPeerConnect(evt.detail))
  App.node.addEventListener('peer:disconnect', (evt) => onPeerDisconnect(evt.detail))

  // 5) rendezvous: вкладки одной машины (BC) + другие устройства (LAN/WS)
  //    + весь мир (публичные Nostr-релеи)
  Rendezvous.start(onLinked)
  LanRendezvous.start()
  NostrRendezvous.start()

  // 5.1) восстановление после «моргания» сети: вкладка вернулась из фона,
  //      появился интернет — сбрасываем backoff и переподключаем ОТКРЫТЫЙ чат
  //      (фоновые — не трогаем, чтобы не плодить встречные офферы)
  const kickReconnect = () => {
    const peer = App.currentPeer
    if (!peer || isConnectedTo(peer)) return
    const slot = Reconnector.timers.get(peer)
    if (slot) slot.tries = 0
    Reconnector.schedule(peer)
  }
  window.addEventListener('online', kickReconnect)
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) kickReconnect()
  })

  // 6) PubSub-присутствие
  Presence.start()

  // 7) фоновые задачи
  setInterval(keepAliveTick, CFG.KEEPALIVE_MS)
  setInterval(() => {
    UI.updateChatStatus()
    UI.updateNetStatus()
    if (!UI.els.sideNew.hidden) UI.renderOnlineList()
  }, 2000)

  // 8) восстановить последний открытый чат
  if (Store.data.lastPeer && Store.data.chats[Store.data.lastPeer]) {
    UI.openChat(Store.data.lastPeer, { reconnect: false })
  }
  UI.renderChatList()

  App.ready = true
  log('готово — можно переписываться и звонить', 'ok')
}

/** Хук для отладки и автотестов: window.Fox */
window.Fox = {
  get myId () { return myId() },
  get ready () { return App.ready },
  get authed () { return !!Store.data.profile.authed },
  get currentPeer () { return App.currentPeer },
  get connected () { return connectedPeers() },
  get messages () { return (Store.data.chats[App.currentPeer] || { messages: [] }).messages },
  get callState () { return Call.state },
  get chats () { return Store.data.chats },
  get profile () { return Store.data.profile },
  send: (t) => sendChatText(t),
  openChat: (id) => UI.openChat(id),
  call: (kind) => Call.start(kind),
  hangup: () => Call.hangup(),
  acceptCall: () => Call.accept(),
  declineCall: () => Call.decline(),
  rename: (n) => { Store.setProfileName(n); Presence.announce() },
  logout: () => Auth.logout(),
  UI, Store, Rendezvous, LanRendezvous, NostrRendezvous, Reconnector, Call, Chat, Signal, Presence, Auth, App
}

// Запуск приложения с диагностикой фатальных ошибок
boot().catch((err) => {
  console.error(err)
  log('ФАТАЛЬНАЯ ОШИБКА: ' + (err && err.message ? err.message : err), 'err')
  try {
    UI.toast('Ошибка запуска: ' + (err && err.message ? err.message : err))
    UI.showModal('profile') // чтобы журнал был виден
  } catch { /* UI ещё не готов */ }
})
