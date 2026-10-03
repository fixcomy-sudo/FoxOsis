/* ==========================================================================
 * FoxOsis — Service Worker: Web Push + уведомления ОС с кнопками
 * «Принять / Сбросить» для входящих звонков.
 *
 * Расшифровку payload (RFC 8291) выполняет сам браузер — до service
 * worker'а доходит ГОТОВЫЙ текст. SW только показывает уведомление и
 * передаёт клик в приложение. Работает при ЗАКРЫТОЙ вкладке: пуш приходит
 * от push-сервиса (FCM/WNS), SW показывает уведомление ОС.
 * ========================================================================== */

/* Push пришёл (страница может быть закрыта) — показываем уведомление ОС */
self.addEventListener('push', (event) => {
  console.log('[sw] push-событие, данных: ' + (event.data ? 'есть' : 'нет'))
  event.waitUntil((async () => {
    let data = { title: 'FoxOsis', body: 'Новое уведомление — откройте приложение', kind: 'msg' }
    if (event.data) {
      try {
        const parsed = event.data.json()
        if (parsed && typeof parsed === 'object') data = Object.assign(data, parsed)
        console.log('[sw] json: ' + JSON.stringify(parsed).slice(0, 120))
      } catch (e) {
        try {
          const t = event.data.text()
          if (t) data.body = t
          console.log('[sw] text: ' + t.slice(0, 120))
        } catch (e2) { console.log('[sw] payload не разобран: ' + e2.message) }
      }
    }
    const opts = {
      body: String(data.body || '').slice(0, 500),
      icon: './logo.jpeg',
      badge: './logo.jpeg',
      tag: data.kind === 'call' ? 'foxosis-call' : 'foxosis-msg',
      renotify: true,
      requireInteraction: data.kind === 'call',
      data: { url: './' + (data.kind === 'call' ? '#inc=1' : '') }
    }
    if (data.kind === 'call') {
      opts.actions = [
        { action: 'accept', title: 'Принять' },
        { action: 'decline', title: 'Сбросить' }
      ]
    }
    try {
      await self.registration.showNotification(String(data.title || 'FoxOsis').slice(0, 100), opts)
      console.log('[sw] уведомление показано, tag=' + opts.tag)
    } catch (e) {
      console.log('[sw] showNotification ОШИБКА: ' + e.message)
    }
  })())
})

/* Клик по уведомлению: будим/открываем приложение, передаём действие */
self.addEventListener('notificationclick', (event) => {
  event.notification.close()
  const act = event.action || ''
  event.waitUntil((async () => {
    const url = './' + (act === 'accept' ? '#inc=accept' : act === 'decline' ? '#inc=decline' : '')
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
    for (const c of wins) {
      try {
        await c.focus()
        c.postMessage({ foxosisInc: act || 'open' })
        return
      } catch (e) { /* не смогли сфокусировать — откроем новое окно */ }
    }
    await self.clients.openWindow(url)
  })())
})

self.addEventListener('install', (event) => { event.waitUntil(self.skipWaiting()) })
self.addEventListener('activate', (event) => { event.waitUntil(self.clients.claim()) })
