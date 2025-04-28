const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Configuración de la región metropolitana (Santiago de Chile)
const REGION_BOUNDS = {
  top: -33.3,
  bottom: -33.6,
  left: -70.85,
  right: -70.5
};

// Función para verificar si un evento ya existe
function isDuplicate(event, existingEvents) {
  return existingEvents.some(existing => {
    // Comparar por ID si está disponible
    if (event.id && existing.id && event.id === existing.id) return true;
    
    // Comparar por coordenadas y tipo si no hay ID
    const sameLocation = 
      event.latitude === existing.latitude && 
      event.longitude === existing.longitude;
    const sameType = event.type === existing.type;
    
    // Considerar como duplicado si es el mismo tipo en la misma ubicación
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
    console.log('📡 Consultando eventos de tráfico en la Región Metropolitana...');
    const response = await axios.get(url, { headers, params });

    if (!response.data || (!response.data.alerts && !response.data.jams)) {
      console.log('⚠️ No se encontraron eventos.');
      return [];
    }

    const alerts = response.data.alerts || [];
    const jams = response.data.jams || [];

    // Procesar alertas
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
      confidence: alert.confidence,
      magvar: alert.magvar
    }));

    // Procesar atascos de tráfico
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
      delay: jam.delay,
      level: jam.level,
      line: jam.line,
      segments: jam.segments
    }));

    return [...processedAlerts, ...processedJams];
  } catch (error) {
    console.error('❌ Error consultando la API:', error.message);
    throw error;
  }
}

// Función para cargar eventos existentes
function loadExistingEvents(filename = 'eventos.json') {
  const filePath = path.join(__dirname, 'data', filename);
  
  if (!fs.existsSync(filePath)) {
    return [];
  }

  try {
    const data = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    console.error('❌ Error leyendo archivo existente:', error.message);
    return [];
  }
}

// Función para guardar datos en JSON
function saveToJson(data, filename = 'eventos.json') {
  try {
    const filePath = path.join(__dirname, 'data', filename);
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
    console.log(`📝 ${data.length} eventos guardados en ${filePath}`);
  } catch (error) {
    console.error('❌ Error guardando los datos:', error.message);
    throw error;
  }
}

// Función principal
async function main() {
  try {
    // Cargar eventos existentes
    const existingEvents = loadExistingEvents();
    console.log(`ℹ️ ${existingEvents.length} eventos cargados de archivo anterior`);

    // Obtener nuevos eventos
    const newEvents = await fetchWazeData();
    console.log(`ℹ️ ${newEvents.length} nuevos eventos obtenidos`);

    // Filtrar duplicados
    const uniqueNewEvents = newEvents.filter(event => !isDuplicate(event, existingEvents));
    console.log(`ℹ️ ${uniqueNewEvents.length} eventos nuevos no duplicados`);

    // Combinar con existentes
    const allEvents = [...existingEvents, ...uniqueNewEvents];
    
    // Guardar solo si hay eventos nuevos
    if (uniqueNewEvents.length > 0) {
      saveToJson(allEvents);
    } else {
      console.log('ℹ️ No hay eventos nuevos para guardar');
    }
  } catch (error) {
    console.error('Error en el proceso principal:', error);
  }
}

// Ejecutar el scraper
main();