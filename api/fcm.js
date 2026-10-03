/* Vercel serverless: уведомление в закрытое Android-приложение (FCM HTTP v1).
 * Тело: { token, title, body, kind }
 * Ключ сервис-аккаунта Firebase — в переменной окружения
 * FIREBASE_SERVICE_ACCOUNT (содержимое json-файла одной строкой). */
const crypto = require('crypto')

let saCache
function serviceAccount () {
  if (saCache !== undefined) return saCache
  try { saCache = process.env.FIREBASE_SERVICE_ACCOUNT ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT.replace(/^\uFEFF/, '')) : null } catch { saCache = null }
  return saCache
}

const b64url = (buf) => Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')

function derToRaw (der) {
  let i = 2
  if (der[1] & 0x80) i = 2 + (der[1] & 0x7f)
  const readInt = () => {
    if (der[i] !== 0x02) throw new Error('bad DER')
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

function assertion (sa) {
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
  return data + '.' + b64url(alg === 'ES256' ? derToRaw(der) : der)
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Content-Type', 'application/json; charset=utf-8')
  if (req.method === 'OPTIONS') { res.statusCode = 204; res.end(); return }
  if (req.method !== 'POST') { res.statusCode = 405; res.end('{"err":"POST only"}'); return }
  const sa = serviceAccount()
  if (!sa) { res.statusCode = 503; res.end('{"err":"FIREBASE_SERVICE_ACCOUNT не задан"}'); return }
  const body = req.body || {}
  const token = String(body.token || '')
  if (!/^[A-Za-z0-9_:.-]{20,}$/.test(token)) { res.statusCode = 400; res.end('{"err":"битый токен"}'); return }
  try {
    const r0 = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: 'grant_type=' + encodeURIComponent('urn:ietf:params:oauth:grant-type:jwt-bearer') + '&assertion=' + assertion(sa),
      signal: AbortSignal.timeout(15000)
    })
    const tok = await r0.json()
    if (!r0.ok || !tok.access_token) throw new Error('oauth: ' + (tok.error_description || tok.error || r0.status))
    const r = await fetch('https://fcm.googleapis.com/v1/projects/' + (sa.project_id || 'foxosis') + '/messages:send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + tok.access_token },
      body: JSON.stringify({
        message: {
          token,
          notification: {
            title: String(body.title || 'FoxOsis').slice(0, 120),
            body: String(body.body || '').slice(0, 500)
          },
          data: { kind: String(body.kind || 'msg').slice(0, 16) },
          android: { priority: 'HIGH' }
        }
      }),
      signal: AbortSignal.timeout(15000)
    })
    const out = await r.json().catch(() => ({}))
    res.statusCode = r.ok ? 200 : r.status
    res.end(JSON.stringify(r.ok ? { ok: true, name: out.name || '' } : { err: (out.error && out.error.message) || 'fcm error' }))
  } catch (e) {
    res.statusCode = 502
    res.end(JSON.stringify({ err: String(e.message || e) }))
  }
}
