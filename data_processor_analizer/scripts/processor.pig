-- Cargar datos desde un archivo JSON exportado
REGISTER /usr/local/pig/piggybank.jar;
DEFINE JsonLoader org.apache.pig.piggybank.storage.JsonLoader;

data = LOAD 'data/events.json' USING JsonLoader('id:int, timestamp:chararray, latitude:float, longitude:float, type:chararray, subtype:chararray, street:chararray, city:chararray, country:chararray, reliability:float, reportrating:float, confidence:float, speedkmh:float, length:float, delay:float');

-- Filtrar datos por un criterio específico
filtered_data = FILTER data BY reliability > 0.8;

-- Mostrar los datos filtrados
DUMP filtered_data;