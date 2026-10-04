/* Отправка файла в Telegram (Bot API sendDocument через curl.exe).
 * Настройка один раз: заполни tg-config.json (token, chat, proxy).
 * Запуск:  node scripts\tg.js путь\к\файлу.apk "текст"
 * Прямой доступ к api.telegram.org часто блокируется — поэтому
 * используется локальный прокси (v2rayN, поле "proxy"). */
const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')

const cfgPath = path.join(__dirname, '..', 'tg-config.json')
if (!fs.existsSync(cfgPath)) { console.error('Нет tg-config.json'); process.exit(1) }
let cfg = {}
try { cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8').replace(/^\uFEFF/, '')) } catch (e) {
  console.error('tg-config.json не разбирается: ' + e.message); process.exit(1)
}
if (!cfg.token || !cfg.chat) { console.error('В tg-config.json пустые token/chat'); process.exit(1) }

const file = process.argv[2]
if (!file || !fs.existsSync(file)) { console.error('Файл не найден: ' + file); process.exit(1) }
const caption = (process.argv[3] || '').slice(0, 1024)
const abs = path.resolve(file)

function buildArgs (proxy) {
  const a = ['-sS', '-m', '180', '-w', '\n%{http_code}']
  if (proxy) a.push('-x', proxy)
  a.push('-F', 'chat_id=' + cfg.chat)
  if (caption) a.push('-F', 'caption=' + caption)
  a.push('-F', 'document=@' + abs)
  a.push('https://api.telegram.org/bot' + cfg.token + '/sendDocument')
  return a
}

function attempt (proxy) {
  const r = spawnSync('curl.exe', buildArgs(proxy), { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 })
  const out = (r.stdout || '') + (r.stderr || '')
  const m = out.match(/(\d{3})\s*$/)
  const code = m ? m[1] : '000'
  const body = m ? out.slice(0, out.length - m[0].length) : out
  return { code, body }
}

// 1) через прокси (если задан), 2) при сбое — напрямую
let res = cfg.proxy ? attempt(cfg.proxy) : null
if (!res || res.code !== '200') {
  if (cfg.proxy) console.error('прокси не сработал (' + res.code + ') — пробую напрямую…')
  res = attempt('')
}
if (res.code === '200' && /"ok"\s*:\s*true/.test(res.body)) {
  console.log('TG OK: ' + path.basename(abs) + ' (' + fs.statSync(abs).size + ' байт)')
  process.exit(0)
}
console.error('TG HTTP ' + res.code + ': ' + res.body.slice(0, 500))
process.exit(1)
