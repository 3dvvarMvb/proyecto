-- Cargar datos desde CSV
data = LOAD 'data/eventos.csv' USING PigStorage(',') AS (
    id:chararray, timestamp:chararray, latitude:float, longitude:float,
    type:chararray, subtype:chararray, street:chararray,
    city:chararray, country:chararray, reliability:float,
    reportrating:float, confidence:float, speedkmh:float,
    length:float, delay:float
);

-- Filtro: datos validos
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
        WHEN type == 'jam' THEN 'atasco'
        WHEN type == 'POLICE' THEN 'policia_controlando'
        WHEN type == 'HAZARD' THEN 'precaucion'
        WHEN type == 'ROAD_CLOSED' THEN 'corte'
        ELSE 'otro'
    END) AS tipo_normalizado,
    subtype, street, city, country, reliability, reportrating, confidence, speedkmh, length, delay;

-- Eliminar duplicados exactos (por todos los campos)
deduplicated = DISTINCT normalized;

-- Extraer solo fecha desde timestamp para agrupar
formatted = FOREACH deduplicated GENERATE
    tipo_normalizado,
    street,
    city,
    SUBSTRING(timestamp, 0, 10) AS fecha,
    subtype AS descripcion;

-- Agrupar por tipo, comuna y día
grouped = GROUP formatted BY (city, street, tipo_normalizado, fecha);

-- Contar incidentes similares 
resumen = FOREACH grouped GENERATE
    FLATTEN(group) AS (comuna, street, tipo, fecha),
    COUNT(formatted) AS cantidad_incidentes;

-- Eliminar el directorio de salida si ya existe
sh rm -rf ./results/incidentes_por_dia;

-- Crear la carpeta 'results' si no existe
SET default_parallel 1;
sh mkdir -p ./results;

-- Guardar resultados en la carpeta local 'results'
STORE resumen INTO './results/incidentes_por_dia' USING PigStorage(',');

-- ANÁLISIS ADICIONAL PARA MOSTRAR 

-- 1. Comuna con mas incidentes en total
comuna_incidentes = GROUP resumen BY comuna;
comuna_incidentes_count = FOREACH comuna_incidentes GENERATE
    group AS comuna,
    SUM(resumen.cantidad_incidentes) AS total_incidentes;
    
comuna_incidentes_max = ORDER comuna_incidentes_count BY total_incidentes DESC;
comuna_max = LIMIT comuna_incidentes_max 1;

-- 2. Calle con mas incidentes en total
calle_incidentes = GROUP resumen BY street;
calle_incidentes_count = FOREACH calle_incidentes GENERATE
    group AS calle,
    SUM(resumen.cantidad_incidentes) AS total_incidentes;
    
calle_incidentes_max = ORDER calle_incidentes_count BY total_incidentes DESC;
calle_max = LIMIT calle_incidentes_max 1;

-- 3. Media de accidentes en todas las comunas
accidentes = FILTER resumen BY tipo == 'accidente';
accidentes_comuna = GROUP accidentes BY comuna;
media_accidentes_comuna = FOREACH accidentes_comuna GENERATE
    group AS comuna,
    AVG(accidentes.cantidad_incidentes) AS media_accidentes;
    
media_accidentes_todas = FOREACH (GROUP media_accidentes_comuna ALL) GENERATE
    AVG(media_accidentes_comuna.media_accidentes) AS media_accidentes_total;

-- 4. Media de controles policiales en todas las comunas
policia = FILTER resumen BY tipo == 'policia_controlando';
policia_comuna = GROUP policia BY comuna;
media_policia_comuna = FOREACH policia_comuna GENERATE
    group AS comuna,
    AVG(policia.cantidad_incidentes) AS media_policia;
    
media_policia_todas = FOREACH (GROUP media_policia_comuna ALL) GENERATE
    AVG(media_policia_comuna.media_policia) AS media_policia_total;

-- 5. Desviacion estandar aproximada de incidentes por comuna
-- Paso 1: Calcular la media por comuna
incidentes_media_comuna = FOREACH comuna_incidentes GENERATE
    group AS comuna,
    AVG(resumen.cantidad_incidentes) AS media;

-- Paso 2: Unir con los datos originales para calcular diferencias
incidentes_join = JOIN resumen BY comuna, incidentes_media_comuna BY comuna;
incidentes_diff = FOREACH incidentes_join GENERATE
    resumen::comuna AS comuna,
    (resumen::cantidad_incidentes - incidentes_media_comuna::media) * (resumen::cantidad_incidentes - incidentes_media_comuna::media) AS diff_squared;

-- Paso 3: Agrupar de nuevo y calcular la desviación
incidentes_var = GROUP incidentes_diff BY comuna;
incidentes_std = FOREACH incidentes_var GENERATE
    group AS comuna,
    SQRT(SUM(incidentes_diff.diff_squared) / COUNT(incidentes_diff.diff_squared)) AS desviacion;

-- 6. Comunas con mayor cantidad de tipos de incidentes diferentes
tipos_por_comuna = GROUP resumen BY (comuna, tipo);
tipos_count = FOREACH tipos_por_comuna GENERATE
    group.comuna AS comuna,
    group.tipo AS tipo;

tipos_distintos = GROUP tipos_count BY comuna;
tipos_distintos_count = FOREACH tipos_distintos GENERATE
    group AS comuna,
    COUNT(tipos_count) AS num_tipos_distintos;
    
tipos_distintos_max = ORDER tipos_distintos_count BY num_tipos_distintos DESC;
top_comunas_tipos = LIMIT tipos_distintos_max 5;

-- Guardar resultados finales en directorios
sh rm -rf ./results/analisis_estadistico;
sh mkdir -p ./results/analisis_estadistico;

-- 1. Comuna con más incidentes
STORE comuna_max INTO './results/analisis_estadistico/comuna_max' USING PigStorage(',');

-- 2. Calle con más incidentes
STORE calle_max INTO './results/analisis_estadistico/calle_max' USING PigStorage(',');

-- 3. Media de accidentes
STORE media_accidentes_todas INTO './results/analisis_estadistico/media_accidentes' USING PigStorage(',');

-- 4. Media de controles policiales
STORE media_policia_todas INTO './results/analisis_estadistico/media_policia' USING PigStorage(',');

-- 5. Desviación estándar
STORE incidentes_std INTO './results/analisis_estadistico/desviacion' USING PigStorage(',');

-- 6. Top comunas con más tipos de incidentes diferentes
STORE top_comunas_tipos INTO './results/analisis_estadistico/top_comunas' USING PigStorage(',');

-- Imprimir mensaje de éxito
sh echo "Análisis estadístico completado. Los resultados están en los directorios de ./results/analisis_estadistico/";