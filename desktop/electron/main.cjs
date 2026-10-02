/* ==========================================================================
 * FoxOsis — десктоп (Electron): полноценное окно приложения, как на телефоне.
 *   - внутри крутится встроенный сервер (тот же код, что server.js) с
 *     ПОЛНЫМ приложением index.html + app.bundle.js (глобальные релеи Nostr,
 *     P2P-чаты, звонки — всё как в APK/IPA);
 *   - сервер LAN дополнительно служит ретранслятором для телефонов в одной
 *     сети (и дублирует публичные релеи CFG.RELAY_URLS — это не «локалка»).
 * ========================================================================== */
const { app, BrowserWindow, session, Menu, shell } = require('electron')
const path = require('path')
const { createServer, assets, PORT: DEF_PORT } = require('../out/core.cjs')

let win = null

/** Поднять сервер: обычный порт, а если занят (dev-сервер) — свободный */
function startServer () {
  return new Promise((resolve, reject) => {
    const attempt = (port) => {
      const s = createServer({ files: assets })
      s.once('error', (e) => {
        if (e.code === 'EADDRINUSE' && port === DEF_PORT) attempt(0)
        else reject(e)
      })
      s.listen(port, () => resolve(`http://127.0.0.1:${s.address().port}/`))
    }
    attempt(DEF_PORT)
  })
}

function createWindow (url) {
  win = new BrowserWindow({
    width: 1280, height: 820,
    minWidth: 720, minHeight: 520,
    backgroundColor: '#0e1621',
    title: 'FoxOsis',
    icon: path.join(__dirname, 'icon.png'),
    autoHideMenuBar: true,
    webPreferences: { spellcheck: true }
  })
  Menu.setApplicationMenu(null)
  win.loadURL(url)
  // ссылки с target=_blank / внешние — в системный браузер, не в окно
  win.webContents.setWindowOpenHandler(({ url: u }) => { shell.openExternal(u); return { action: 'deny' } })
  win.on('closed', () => { win = null })
}

const gotLock = app.requestSingleInstanceLock()
if (!gotLock) {
  app.quit()
} else {
  app.on('second-instance', () => {
    if (win) { if (win.isMinimized()) win.restore(); win.focus() }
  })

  app.whenReady().then(async () => {
    // Разрешения: камера/микрофон для звонков, уведомления, буфер обмена
    const ALLOW = ['media', 'mediaKeySystem', 'notifications', 'fullscreen',
      'pointerLock', 'clipboard-sanitized-write', 'clipboard-read']
    session.defaultSession.setPermissionRequestHandler((wc, permission, cb) => {
      cb(ALLOW.includes(permission))
    })
    session.defaultSession.setPermissionCheckHandler((wc, permission) =>
      ALLOW.includes(permission))

    try {
      const url = await startServer()
      createWindow(url)
    } catch (e) {
      console.error('FoxOsis: сервер не запустился —', e.message)
      app.exit(1)
    }
  })

  app.on('window-all-closed', () => { app.quit() })
}
