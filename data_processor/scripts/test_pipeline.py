#!/usr/bin/env python3
"""
Script de prueba para verificar el pipeline de procesamiento
"""

import sys
import os

# Agregar el directorio scripts al path
sys.path.append('scripts')

# Importar el script principal
from Obetener_data import main, logger

if __name__ == "__main__":
    logger.info("=== INICIANDO PRUEBA DEL PIPELINE ===")
    
    try:
        # Ejecutar el pipeline completo
        success = main()
        
        if success:
            logger.info("=== PRUEBA COMPLETADA EXITOSAMENTE ===")
            sys.exit(0)
        else:
            logger.error("=== PRUEBA FALLÓ ===")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error en la prueba: {e}")
        sys.exit(1)
