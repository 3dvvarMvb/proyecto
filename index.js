const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

(async () => {
  // Lanzamos navegador
  const browser = await puppeteer.launch({
    headless: true, // Cambia a false si quieres ver el navegador
    defaultViewport: null,
    args: ['--start-maximized']
  });

  const page = await browser.newPage();

  // Captura de respuestas con contenido JSON o XML
  page.on('response', async (response) => {
    const url = response.url();
    const headers = response.headers();
    const contentType = headers['content-type'] || '';

    // Mostrar todas las respuestas JSON o XML
    if (contentType.includes('application/json') || contentType.includes('application/xml')) {
      console.log('📡 Capturada URL:', url);

      try {
        const text = await response.text();
        const ext = contentType.includes('json') ? 'json' : 'xml';
        const filePath = path.join(__dirname, `data/events.${ext}`);

        fs.writeFileSync(filePath, text);
        console.log(`✅ Datos guardados en ${filePath}`);
      } catch (err) {
        console.error('⚠️ Error al leer o guardar datos:', err.message);
      }
    }
  });

  // También podemos escuchar las peticiones salientes (opcional)
  page.on('request', (req) => {
    const url = req.url();
    if (url.includes('waze') && (url.includes('events') || url.includes('traffic'))) {
      console.log('➡️ Petición Waze:', url);
    }
  });

  // Ir al mapa
  await page.goto('https://www.waze.com/es-419/live-map/', { waitUntil: 'networkidle2' });

  // Esperar a que cargue todo
  console.log('⏳ Esperando 15 segundos para capturar eventos...');
  await new Promise(resolve => setTimeout(resolve, 15000));

  await browser.close();
})();
