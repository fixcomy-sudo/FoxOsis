// Сборка app.js + все https://esm.sh-зависимости в один самодостаточный
// app.bundle.js — приложение открывается БЕЗ интернета (ни один CDN не нужен).
// Использование: node scripts/bundle.mjs
import * as esbuild from 'esbuild'
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const cacheDir = path.join(root, 'node_modules', '.cache', 'esm')
fs.mkdirSync(cacheDir, { recursive: true })

const fetchMod = async (url) => {
  const key = crypto.createHash('sha1').update(url).digest('hex') + '.js'
  const file = path.join(cacheDir, key)
  if (fs.existsSync(file)) return fs.readFileSync(file, 'utf8')
  const r = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0 (FoxOsis build)' } })
  if (!r.ok) throw new Error(`не удалось скачать ${url} -> HTTP ${r.status}`)
  const src = await r.text()
  fs.writeFileSync(file, src)
  return src
}

const plugin = {
  name: 'cdn-imports',
  setup (build) {
    // абсолютные https/data-импорты (esm.sh)
    build.onResolve({ filter: /^(https?:|data:)/ }, (a) => {
      if (a.path.startsWith('data:')) return { path: a.path, namespace: 'dataurl' }
      return { path: a.path, namespace: 'https' }
    })
    // корневые пути вида /v135/... внутри модулей esm.sh
    build.onResolve({ filter: /^\// }, (a) => {
      if (a.importer && a.importer.startsWith('http')) {
        return { path: new URL(a.path, a.importer).href, namespace: 'https' }
      }
    })
    // относительные ./ ../ внутри модулей esm.sh — резолвим от URL импортёра
    build.onResolve({ filter: /^\.\.?[/]/ }, (a) => {
      if (a.importer && a.importer.startsWith('http')) {
        return { path: new URL(a.path, a.importer).href, namespace: 'https' }
      }
    })
    build.onLoad({ filter: /.*/, namespace: 'dataurl' }, async (a) => {
      const comma = a.path.indexOf(',')
      const meta = a.path.slice(5, comma)
      const body = a.path.slice(comma + 1)
      const src = /;base64/.test(meta)
        ? Buffer.from(body, 'base64').toString('utf8')
        : decodeURIComponent(body)
      return { contents: src, loader: 'js' }
    })
    build.onLoad({ filter: /.*/, namespace: 'https' }, async (a) => {
      const src = await fetchMod(a.path)
      return { contents: src, loader: 'js' }
    })
  }
}

const res = await esbuild.build({
  entryPoints: [path.join(root, 'app.js')],
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2020',
  minify: true,
  legalComments: 'none',
  outfile: path.join(root, 'app.bundle.js'),
  plugins: [plugin],
  logLevel: 'info'
})
if (res.errors.length) process.exit(1)
const size = fs.statSync(path.join(root, 'app.bundle.js')).length
console.log('OK app.bundle.js = ' + (size / 1024 / 1024).toFixed(2) + ' МБ')
