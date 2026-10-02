/* Сгенерировать desktop/electron/icon.png 256x256 из logo.jpeg */
const puppeteer = require('C:/Users/fixco/AppData/Local/Temp/opencode/foxtest/node_modules/puppeteer-core')
const fs = require('fs')
const path = require('path')

async function main () {
  const src = 'data:image/jpeg;base64,' +
    fs.readFileSync(path.join(__dirname, '..', 'logo.jpeg')).toString('base64')
  const browser = await puppeteer.launch({
    executablePath: 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
    headless: 'new'
  })
  const page = await browser.newPage()
  await page.goto('about:blank')
  const dataUrl = await page.evaluate(async (s) => {
    const img = new Image()
    img.src = s
    await img.decode()
    const c = document.createElement('canvas')
    c.width = c.height = 256
    const ctx = c.getContext('2d')
    ctx.drawImage(img, 0, 0, 256, 256)
    return c.toDataURL('image/png')
  }, src)
  await browser.close()
  const out = path.join(__dirname, 'electron', 'icon.png')
  fs.writeFileSync(out, Buffer.from(dataUrl.split(',')[1], 'base64'))
  console.log('icon.png:', fs.statSync(out).size, 'байт')
}
main().catch(e => { console.error(e); process.exit(1) })
