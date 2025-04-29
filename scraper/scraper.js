const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Configuración
const REGION_BOUNDS = {
  top: -33.3,
  bottom: -33.6,
  left: -70.85,
  right: -70.5
};
const TARGET_EVENTS = 10000;
const SCRAPE_INTERVAL = 30000; // 30 segundos entre requests

// Función para verificar duplicados
function isDuplicate(event, existingEvents) {
  return existingEvents.some(existing => {
    if (event.id && existing.id && event.id === existing.id) return true;
    const sameLocation = event.latitude === existing.latitude && 
                       event.longitude === existing.longitude;
    const sameType = event.type === existing.type;
    return sameLocation && sameType;
  });
}

// Función para obtener datos de Waze
async function fetchWazeData() {
  const url = 'https://www.waze.com/live-map/api/georss';
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Referer': 'https://www.waze.com/live-map',
    'Accept': 'application/json'
  };

  const params = {
    top: REGION_BOUNDS.top,
    bottom: REGION_BOUNDS.bottom,
    left: REGION_BOUNDS.left,
    right: REGION_BOUNDS.right,
    env: 'row',
    types: 'alerts,traffic'
  };

  try {
    console.log('Consultando API de Waze...');
    const response = await axios.get(url, { headers, params });

    if (!response.data || (!response.data.alerts && !response.data.jams)) {
      console.log('No se encontraron eventos en este ciclo.');
      return [];
    }

    const alerts = response.data.alerts || [];
    const jams = response.data.jams || [];

    const processedAlerts = alerts.map(alert => ({
      id: alert.uuid,
      timestamp: alert.pubMillis,
      latitude: alert.location.y,
      longitude: alert.location.x,
      type: alert.type,
      subtype: alert.subtype,
      street: alert.street,
      city: alert.city,
      country: alert.country,
      reliability: alert.reliability,
      reportRating: alert.reportRating,
      confidence: alert.confidence
    }));

    const processedJams = jams.map(jam => ({
      id: jam.uuid,
      timestamp: jam.pubMillis,
      latitude: jam.line[0].y,
      longitude: jam.line[0].x,
      type: 'jam',
      street: jam.street,
      city: jam.city,
      country: jam.country,
      speedKMH: jam.speedKMH,
      length: jam.length,
      delay: jam.delay
    }));

    return [...processedAlerts, ...processedJams];
  } catch (error) {
    console.error('Error en la consulta a la API:', error.message);
    return [];
  }
}

// Función para manejar el archivo de eventos
function handleEventFile() {
  const filename = 'eventos.json';
  const filePath = path.join(__dirname, 'data', filename);

  // Crear directorio si no existe
  if (!fs.existsSync(path.dirname(filePath))) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
  }

  // Cargar eventos existentes o crear archivo nuevo
  let existingEvents = [];
  if (fs.existsSync(filePath)) {
    try {
      const data = fs.readFileSync(filePath, 'utf8');
      existingEvents = JSON.parse(data);
      console.log(`${existingEvents.length} eventos cargados del archivo existente.`);
    } catch (error) {
      console.error('Error leyendo archivo existente:', error.message);
    }
  }

  return {
    getEvents: () => existingEvents,
    saveEvents: (events) => {
      try {
        fs.writeFileSync(filePath, JSON.stringify(events, null, 2));
        console.log(`Datos guardados. Total acumulado: ${events.length} eventos.`);
      } catch (error) {
        console.error('Error guardando los datos:', error.message);
      }
    }
  };
}

async function postToStorage(events, retries = 10) {
    const url = 'http://storage:5000/events';
    for (let i = 1; i <= retries; i++) {
      try {
        await axios.post(url, events, { headers: { 'Content-Type':'application/json' } });
        console.log('Datos enviados exitosamente.');
        return;
      } catch (err) {
        console.error(`Intento ${i} fallido: ${err.message}`);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
    console.error('No se pudo conectar a storage tras varios intentos.');
  }

// Función principal con bucle
async function main() {
  const eventFile = handleEventFile();
  let allEvents = eventFile.getEvents();

  console.log(`Objetivo: recolectar ${TARGET_EVENTS} eventos únicos.`);
  console.log(`Iniciando con ${allEvents.length} eventos existentes.`);

  while (allEvents.length < TARGET_EVENTS) {
    try {
      const newEvents = await fetchWazeData();
      const uniqueNewEvents = newEvents.filter(event => !isDuplicate(event, allEvents));

      if (uniqueNewEvents.length > 0) {
        allEvents = [...allEvents, ...uniqueNewEvents];
        eventFile.saveEvents(allEvents);
      }

      console.log(`Progreso: ${allEvents.length}/${TARGET_EVENTS} (${Math.round((allEvents.length/TARGET_EVENTS)*100)}%)`);

      // Esperar antes de la próxima consulta si no hemos alcanzado el objetivo
      if (allEvents.length < TARGET_EVENTS) {
        console.log(`Esperando ${SCRAPE_INTERVAL/1000} segundos para el próximo ciclo...`);
        await new Promise(resolve => setTimeout(resolve, SCRAPE_INTERVAL));
      }
    } catch (error) {
      console.error('Error en el ciclo principal:', error.message);
      // Esperar antes de reintentar si hay error
      await new Promise(resolve => setTimeout(resolve, SCRAPE_INTERVAL));
    }
  }

  console.log(`Objetivo alcanzado: ${allEvents.length} eventos recolectados.`);
  console.log('Proceso completado.');

  try {
    console.log('Enviando datos a storage...');
    // Enviar datos a storage
    await new Promise(resolve => setTimeout(resolve, 10000)); // Esperar 10 segundos antes de enviar
    await postToStorage(allEvents); 
  } catch (error) {
    console.error('Error enviando datos a storage:', error.message);
  }
}
main();