#!/bin/bash

echo "Usando archivo local de eventos desde /data/eventos.json..."
if [ -f /data/eventos.json ]; then
    echo " Archivo encontrado."
    cp /data/eventos.json data/eventos.json
else
    echo " Archivo /data/eventos.json no encontrado."
    exit 1
fi

echo " Convirtiendo JSON a CSV..."
python3 scripts/json_to_csv.py
