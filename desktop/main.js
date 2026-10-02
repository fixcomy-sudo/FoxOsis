/* ==========================================================================
 * FoxOsis — десктоп-запуск (Windows exe / dev):
 *   сервер из server.js + вся статика внутри процесса (память),
 *   при старте открывает браузер на http://localhost:<port>/
 * Запуск в dev: node desktop/main.js
 * ========================================================================== */
const { spawn } = require('child_process')
const { createServer, PORT: DEF_PORT } = require('../server.js')
const assets = require('./assets.js')

const PORT = Number(process.env.PORT || DEF_PORT)

function openBrowser () {
  if (process.env.FOXOISIS_NOOPEN) return   // тестовый режим: не открывать окно
  const url = `http://localhost:${PORT}/`
  let cmd
  if (process.platform === 'win32') {
    cmd = spawn('cmd', ['/c', 'start', '', url], { detached: true, stdio: 'ignore' })
  } else if (process.platform === 'darwin') {
    cmd = spawn('open', [url], { detached: true, stdio: 'ignore' })
  } else {
    cmd = spawn('xdg-open', [url], { detached: true, stdio: 'ignore' })
  }
  cmd.on('error', () => {})
  cmd.unref()
}

const server = createServer({ files: assets })

server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.log(`Порт ${PORT} уже занят — FoxOsis, похоже, уже запущен.`)
    console.log('Открываю вкладку…')
    openBrowser()
    process.exit(0)
  }
  console.error('Ошибка сервера:', e.message)
  process.exit(1)
})

server.listen(PORT, () => {
  console.log('==========================================')
  console.log('  FoxOsis запущен:  http://localhost:' + PORT + '/')
  console.log('  Для телефона в той же сети:')
  console.log('     http://<IP-этого-ПК>:' + PORT + '/')
  console.log('  Окно можно свернуть. Закрытие окна = стоп.')
  console.log('==========================================')
  openBrowser()
})

process.on('SIGINT', () => { console.log('\nОстанавливаюсь…'); process.exit(0) })
process.on('SIGTERM', () => { process.exit(0) })
