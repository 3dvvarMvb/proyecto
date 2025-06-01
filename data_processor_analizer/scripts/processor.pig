-- Registrar piggybank para JSON
REGISTER /usr/local/pig/piggybank.jar;
DEFINE JsonLoader org.apache.pig.piggybank.storage.JsonLoader;

-- Cargar datos
data = LOAD 'data/events.json' USING JsonLoader(
    'id:int, timestamp:chararray, latitude:float, longitude:float, type:chararray, subtype:chararray, street:chararray, city:chararray, country:chararray, reliability:float, reportrating:float, confidence:float, speedkmh:float, length:float, delay:float'
);

-- Filtrar datos válidos y confiables
cleaned = FILTER data BY latitude IS NOT NULL AND longitude IS NOT NULL AND type IS NOT NULL AND reliability > 0.8;

-- Normalizar tipos usando CASE
normalized = FOREACH cleaned GENERATE
    id, timestamp, latitude, longitude,
    (CASE 
        WHEN type == 'ACCIDENT' THEN 'accidente'
        WHEN type == 'JAM' THEN 'atasco'
        WHEN type == 'ROAD_CLOSED' THEN 'corte'
        ELSE 'otro'
    END) AS tipo_normalizado,
    subtype, street, city, country, reliability, reportrating, confidence, speedkmh, length, delay;

-- Agrupar por comuna y tipo
grouped = GROUP normalized BY (city, tipo_normalizado);

-- Contar incidentes por grupo
counts = FOREACH grouped GENERATE
    FLATTEN(group) AS (city, tipo), COUNT(normalized) AS total;

-- Guardar resultados en texto CSV
STORE counts INTO 'results/summary_by_city_type' USING PigStorage(',');
