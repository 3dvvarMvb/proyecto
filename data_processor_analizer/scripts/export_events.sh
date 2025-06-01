#!/bin/bash

# Exportar datos desde el endpoint y guardarlos en un archivo JSON
curl -s http://storage:5000/events/export -o data/events.json

# Verificar si el archivo fue creado correctamente
if [ -f data/events.json ]; then
    echo "Datos exportados exitosamente a data/events.json"
else
    echo "Error al exportar los datos"
    exit 1
fi
