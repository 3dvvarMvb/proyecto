-- Cargar datos desde CSV
data = LOAD 'data/eventos.csv' USING PigStorage(',') AS (
    id:chararray, timestamp:chararray, latitude:float, longitude:float,
    type:chararray, subtype:chararray, street:chararray,
    city:chararray, country:chararray, reliability:float,
    reportrating:float, confidence:float, speedkmh:float,
    length:float, delay:float
);

-- Filtro: datos válidos y confiables
cleaned = FILTER data BY
    latitude IS NOT NULL AND latitude != 0.0 AND
    longitude IS NOT NULL AND longitude != 0.0 AND
    type IS NOT NULL AND TRIM(type) != '';

-- Normalizar tipo de incidente
normalized = FOREACH cleaned GENERATE
    id, timestamp, latitude, longitude,
    (CASE 
        WHEN type == 'ACCIDENT' THEN 'accidente'
        WHEN type == 'JAM' THEN 'atasco'
        WHEN type == 'ROAD_CLOSED' THEN 'corte'
        ELSE 'otro'
    END) AS tipo_normalizado,
    subtype, street, city, country, reliability, reportrating, confidence, speedkmh, length, delay;

-- Eliminar duplicados exactos (por todos los campos)
deduplicated = DISTINCT normalized;

-- Extraer solo fecha desde timestamp para agrupar
-- (ej: 2024-04-21 14:00:00 → 2024-04-21)
formatted = FOREACH deduplicated GENERATE
    tipo_normalizado,
    city,
    SUBSTRING(timestamp, 0, 10) AS fecha,
    subtype AS descripcion;

-- Agrupar por tipo, comuna y día
grouped = GROUP formatted BY (city, tipo_normalizado, fecha);

-- Contar incidentes similares por comuna/tipo/día
resumen = FOREACH grouped GENERATE
    FLATTEN(group) AS (comuna, tipo, fecha),
    COUNT(formatted) AS cantidad_incidentes;

-- Guardar resultados
STORE resumen INTO 'results/incidentes_por_dia' USING PigStorage(',');
