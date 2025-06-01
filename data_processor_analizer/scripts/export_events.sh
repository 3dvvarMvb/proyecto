#!/bin/bash

echo "Usando archivo local de eventos desde /data/eventos.json..."

# Verificar que exista
if [ -f /data/eventos.json ]; then
    echo " Archivo encontrado."
    cp /data/eventos.json data/events.json
else
    echo " Archivo /data/eventos.json no encontrado."
    exit 1
fi