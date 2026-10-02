/* Сгенерировать assets.js — статика приложения в памяти (base64) для exe */
const fs = require('fs')
const path = require('path')

const root = path.join(__dirname, '..')
const files = ['index.html', 'app.bundle.js', 'logo.jpeg']

const entries = files.map((name) => {
  const buf = fs.readFileSync(path.join(root, name))
  return `  ${JSON.stringify('/' + name)}: Buffer.from(${JSON.stringify(buf.toString('base64'))}, 'base64')`
})

const out = '/* Генерируется desktop/make_assets.js — НЕ редактировать вручную */\n' +
  'module.exports = {\n' + entries.join(',\n') + '\n}\n'

fs.writeFileSync(path.join(__dirname, 'assets.js'), out)
console.log('assets.js:', files.join(', '), '=', fs.statSync(path.join(__dirname, 'assets.js')).size, 'байт')
