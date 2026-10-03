/* Vercel serverless-функция: форвард Web Push на push-сервис получателя.
 * (браузер не может слать пуш напрямую — push-сервисы не отдают CORS;
 *  payload зашифрован для получателя — сервер его не читает).
 * Тело: { ep, h: { Authorization }, b: base64 } */
const PUSH_OK_HOST = /(^|\.)(notify\.windows\.com|fcm\.googleapis\.com|web\.push\.apple\.com|updates\.push\.services\.mozilla\.com|push\.services\.firefox\.com|mtalk\.google\.com)$/

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Content-Type', 'application/json; charset=utf-8')
  if (req.method === 'OPTIONS') { res.statusCode = 204; res.end(); return }
  if (req.method !== 'POST') { res.statusCode = 405; res.end('{"err":"POST only"}'); return }
  const body = req.body || {}
  let host = ''
  try { host = new URL(body.ep).hostname } catch {}
  if (!host || !PUSH_OK_HOST.test(host)) {
    res.statusCode = 400
    res.end('{"err":"bad ep"}')
    return
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
    res.statusCode = r.status === 201 ? 201 : r.status
    res.end(JSON.stringify({ ok: r.status === 201, st: r.status }))
  } catch (e) {
    res.statusCode = 502
    res.end(JSON.stringify({ err: String(e.message || e) }))
  }
}
