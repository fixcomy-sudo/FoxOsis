/* Ядро десктоп-сборки: встроенный сервер + статика приложения в памяти.
 * Используется Electron-оболочкой и (отдельным бандлом) standalone-exe. */
const { createServer, PORT } = require('../server.js')
const assets = require('./assets.js')

module.exports = { createServer, PORT, assets }
