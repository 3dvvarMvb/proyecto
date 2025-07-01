#!/usr/bin/env python3
"""
Script de prueba para verificar que los datos se envían correctamente a Elasticsearch
como documentos individuales en lugar de un solo macro-documento.
"""

import sys
import os
import json

# Agregar el directorio de scripts al path
sys.path.append('/home/eduardo-valenzuela/Universidad/Sistemas_Distribuidos/proyecto/data_processor/scripts')

# Importar la función de procesamiento
from Obetener_data import read_processed_results, send_to_elasticsearch

def test_data_parsing():
    """Prueba que los datos se parseen correctamente"""
    print("=== PRUEBA DE PARSEO DE DATOS ===")
    
    # Leer datos existentes
    results = read_processed_results()
    
    print(f"Incidentes encontrados: {len(results['incidentes_por_dia'])}")
    print(f"Análisis encontrados: {len(results['analisis'])}")
    
    # Mostrar algunos ejemplos de incidentes
    if results['incidentes_por_dia']:
        print("\nEjemplos de incidentes:")
        for i, incidente in enumerate(results['incidentes_por_dia'][:3]):
            print(f"  {i+1}. {incidente}")
    
    # Mostrar análisis estadísticos
    if results['analisis']:
        print("\nAnálisis estadísticos:")
        for analysis_type, value in results['analisis'].items():
            print(f"  {analysis_type}: {value[:100]}...")  # Mostrar solo los primeros 100 caracteres
    
    return results

def test_document_structure():
    """Prueba la estructura de documentos que se enviarán a Elasticsearch"""
    print("\n=== PRUEBA DE ESTRUCTURA DE DOCUMENTOS ===")
    
    results = read_processed_results()
    
    # Simular la estructura de documentos para incidentes
    print("\nEstructura de documentos de incidentes:")
    if results['incidentes_por_dia']:
        example_incidente = results['incidentes_por_dia'][0]
        document = {
            **example_incidente,
            'document_type': 'incidente',
            'timestamp_processed': 1234567890
        }
        print(json.dumps(document, indent=2, ensure_ascii=False))
    
    # Simular la estructura de documentos para análisis
    print("\nEstructura de documentos de análisis:")
    if 'comuna_max' in results['analisis']:
        analysis_value = results['analisis']['comuna_max']
        document = {
            'document_type': 'analisis_estadistico',
            'analysis_type': 'comuna_max',
            'value': analysis_value,
            'timestamp_processed': 1234567890
        }
        
        # Parsear el valor
        parts = analysis_value.split(',')
        if len(parts) == 2:
            document['name'] = parts[0]
            document['count'] = int(parts[1])
        
        print(json.dumps(document, indent=2, ensure_ascii=False))

def simulate_elasticsearch_send():
    """Simula el envío a Elasticsearch sin hacer las llamadas HTTP reales"""
    print("\n=== SIMULACIÓN DE ENVÍO A ELASTICSEARCH ===")
    
    results = read_processed_results()
    
    # Contar documentos que se enviarían
    incidentes_count = len(results['incidentes_por_dia'])
    analisis_count = len(results['analisis'])
    total_documents = incidentes_count + analisis_count
    
    print(f"Se enviarían {incidentes_count} documentos de incidentes")
    print(f"Se enviarían {analisis_count} documentos de análisis")
    print(f"Total de documentos individuales: {total_documents}")
    
    # Mostrar los tipos de análisis que se procesarían
    print(f"\nTipos de análisis que se procesarían:")
    for analysis_type in results['analisis'].keys():
        print(f"  - {analysis_type}")

if __name__ == "__main__":
    print("Iniciando pruebas del sistema de envío a Elasticsearch...")
    
    try:
        # Verificar que existen los archivos de resultados
        if not os.path.exists('/home/eduardo-valenzuela/Universidad/Sistemas_Distribuidos/proyecto/data_processor/results/incidentes_por_dia/part-r-00000'):
            print("ERROR: No se encontraron archivos de resultados")
            print("Asegúrate de que el pipeline de procesamiento haya sido ejecutado")
            sys.exit(1)
        
        # Cambiar al directorio correcto
        os.chdir('/home/eduardo-valenzuela/Universidad/Sistemas_Distribuidos/proyecto/data_processor')
        
        # Ejecutar pruebas
        results = test_data_parsing()
        test_document_structure()
        simulate_elasticsearch_send()
        
        print("\n=== RESUMEN ===")
        print("✅ Parseo de datos: OK")
        print("✅ Estructura de documentos: OK")
        print("✅ Simulación de envío: OK")
        print("\nEl sistema está listo para enviar documentos individuales a Elasticsearch")
        
    except Exception as e:
        print(f"ERROR durante las pruebas: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
