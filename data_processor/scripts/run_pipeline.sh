#!/bin/bash
set -e  # Exit on error

echo "Iniciando pipeline de procesamiento..."

# Paso 1: Obtener datos y almacenarlos en carpetas locales
python3 /scripts/Obetener_data.py || exit 1

# Paso 2: Procesar con Pig
pig -x local /scripts/processor.pig || exit 1

echo "Pipeline completado exitosamente"