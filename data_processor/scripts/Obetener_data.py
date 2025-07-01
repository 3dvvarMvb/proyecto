#!/usr/bin/env python3
# Script para obtener datos de ElasticSearch, procesarlos con Apache Pig
# y enviar los resultados procesados a Elasticsearch para visualizar en Kibana

import os
import json
import requests
import logging
from dotenv import load_dotenv
import csv
import glob
import time
import subprocess
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# URLs y endpoints
STORAGE_URL = os.getenv('STORAGE_URL', 'http://storage:5000')
WAZE_EVENTS_ENDPOINT = f"{STORAGE_URL}/get-waze-events?scroll=true"
PROCESSED_EVENTS_ENDPOINT = f"{STORAGE_URL}/upload/json"

def fetch_waze_events():
    """Obtiene eventos de Waze desde el módulo de almacenamiento"""
    try:
        logger.info(f"Solicitando eventos desde: {WAZE_EVENTS_ENDPOINT}")
        response = requests.get(WAZE_EVENTS_ENDPOINT)
        response.raise_for_status()
        response_data = response.json()
        
        # Extraer los datos del objeto de respuesta
        if 'data' in response_data:
            events = response_data['data']
            logger.info(f"Recibidos {len(events)} eventos (método: {response_data.get('method', 'unknown')})")
            return events
        else:
            # Compatibilidad con formato anterior
            logger.info(f"Recibidos {len(response_data)} eventos (formato legacy)")
            return response_data
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener eventos: {e}")
        return None
    except Exception as e:
        logger.error(f"Error al procesar respuesta: {e}")
        return None

def save_json_data(data, filename='data/eventos.json'):
    """Guarda los datos en formato JSON"""
    try:
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        logger.info(f"Datos guardados en {filename}")
        return True
    except Exception as e:
        logger.error(f"Error al guardar JSON: {e}")
        return False

def convert_json_to_csv(json_file='data/eventos.json', csv_file='data/eventos.csv'):
    """Convierte el archivo JSON a CSV para procesamiento con Pig"""
    try:
        # Leer archivo JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Escribir como CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'id', 'timestamp', 'latitude', 'longitude', 'type', 'subtype',
                'street', 'city', 'country', 'reliability', 'reportrating',
                'confidence', 'speedkmh', 'length', 'delay'
            ])
            for d in data:
                writer.writerow([
                    d.get('id'), d.get('timestamp'), d.get('latitude'), d.get('longitude'),
                    d.get('type'), d.get('subtype'), d.get('street'), d.get('city'),
                    d.get('country'), d.get('reliability'), d.get('reportRating'),
                    d.get('confidence'), d.get('speedKMH'), d.get('length'), d.get('delay')
                ])
        logger.info(f"Datos convertidos a CSV en {csv_file}")
        return True
    except Exception as e:
        logger.error(f"Error al convertir a CSV: {e}")
        return False

def run_shell_command(command, working_dir=None):
    """Ejecuta un comando de shell y retorna el resultado"""
    try:
        logger.info(f"Ejecutando comando: {command}")
        
        # Configurar el directorio de trabajo
        cwd = working_dir if working_dir else os.getcwd()
        
        # Ejecutar el comando
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300  # Timeout de 5 minutos
        )
        
        # Log de la salida
        if result.stdout:
            logger.info(f"Salida del comando:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Errores del comando:\n{result.stderr}")
        
        # Verificar el código de retorno
        if result.returncode == 0:
            logger.info(f"Comando ejecutado exitosamente")
            return True, result.stdout, result.stderr
        else:
            logger.error(f"Comando falló con código {result.returncode}")
            return False, result.stdout, result.stderr
            
    except subprocess.TimeoutExpired:
        logger.error(f"Comando excedió el tiempo límite (300s)")
        return False, "", "Timeout"
    except Exception as e:
        logger.error(f"Error al ejecutar comando: {e}")
        return False, "", str(e)

def run_pig_processor():
    """Ejecuta el script processor.pig para procesar los datos"""
    try:
        logger.info("Iniciando procesamiento con Apache Pig")
        
        # Verificar que el archivo CSV existe
        csv_file = 'data/eventos.csv'
        if not os.path.exists(csv_file):
            logger.error(f"Archivo CSV no encontrado: {csv_file}")
            return False
        
        # Verificar que el script Pig existe
        pig_script = '/scripts/processor.pig'
        if not os.path.exists(pig_script):
            logger.error(f"Script Pig no encontrado: {pig_script}")
            return False
        
        # Crear directorio de resultados si no existe
        os.makedirs('results', exist_ok=True)
        
        # Configurar variables de entorno para Pig
        env_vars = {
            'PIG_HOME': '/usr/local/pig',
            'HADOOP_HOME': '/usr/local/hadoop',
            'PATH': f"/usr/local/pig/bin:/usr/local/hadoop/bin:{os.environ.get('PATH', '')}"
        }
        
        # Actualizar el entorno actual
        for key, value in env_vars.items():
            os.environ[key] = value
        
        # Comando para ejecutar Pig
        pig_command = f"pig -x local {pig_script}"
        
        logger.info("Ejecutando Apache Pig para procesar datos...")
        success, stdout, stderr = run_shell_command(pig_command)
        
        if success:
            logger.info("Procesamiento con Pig completado exitosamente")
            
            # Verificar que se generaron los resultados esperados
            expected_results = [
                'results/incidentes_por_dia/part-r-00000',
                'results/analisis_estadistico/comuna_max/part-r-00000',
                'results/analisis_estadistico/calle_max/part-r-00000'
            ]
            
            results_found = 0
            for result_file in expected_results:
                if os.path.exists(result_file):
                    results_found += 1
                    logger.info(f"Resultado generado: {result_file}")
                else:
                    logger.warning(f"Resultado esperado no encontrado: {result_file}")
            
            if results_found > 0:
                logger.info(f"Se generaron {results_found} archivos de resultados")
                return True
            else:
                logger.error("No se generaron archivos de resultados")
                return False
        else:
            logger.error("Error en el procesamiento con Pig")
            return False
            
    except Exception as e:
        logger.error(f"Error al ejecutar processor.pig: {e}")
        return False

def read_processed_results():
    """Lee los resultados procesados por Pig desde la carpeta results"""
    results = {
        'incidentes_por_dia': [],
        'analisis': {}
    }
    
    # Leer resultados principales
    try:
        with open('results/incidentes_por_dia/part-r-00000', 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 5:  # Asegurar formato correcto
                    results['incidentes_por_dia'].append({
                        'comuna': parts[0],
                        'calle': parts[1],
                        'tipo': parts[2],
                        'fecha': parts[3],
                        'cantidad': int(parts[4])
                    })
    except Exception as e:
        logger.error(f"Error al leer incidentes_por_dia: {e}")
    
    # Leer resultados de análisis estadístico
    analysis_dirs = [
        'comuna_max', 'calle_max', 'media_accidentes', 
        'media_policia', 'desviacion', 'top_comunas'
    ]
    
    for analysis in analysis_dirs:
        try:
            filepath = f'results/analisis_estadistico/{analysis}/part-r-00000'
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                results['analisis'][analysis] = content
        except Exception as e:
            logger.error(f"Error al leer {analysis}: {e}")
    
    return results

def send_document_to_elasticsearch(document, index_name):
    """Envía un documento individual a ElasticSearch"""
    url = f'{PROCESSED_EVENTS_ENDPOINT}?index={index_name}'
    try:
        response = requests.post(
            url,
            json=document
        )
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al enviar documento a ElasticSearch: {e}")
        return False

def send_to_elasticsearch(data):
    """Envía los resultados procesados a ElasticSearch como documentos individuales"""
    success_count = 0
    error_count = 0
    
    # Enviar incidentes individuales
    logger.info(f"Enviando {len(data['incidentes_por_dia'])} incidentes como documentos individuales...")
    for incidente in data['incidentes_por_dia']:
        # Agregar metadatos al documento
        document = {
            **incidente,
            'document_type': 'incidente',
            'timestamp_processed': int(time.time())
        }
        
        if send_document_to_elasticsearch(document, 'procesed-waze_events'):
            success_count += 1
        else:
            error_count += 1
    
    # Enviar análisis estadísticos individuales
    logger.info(f"Enviando {len(data['analisis'])} análisis estadísticos como documentos individuales...")
    for analysis_type, analysis_value in data['analisis'].items():
        # Crear documento para cada análisis estadístico
        document = {
            'document_type': 'analisis_estadistico',
            'analysis_type': analysis_type,
            'value': analysis_value,
            'timestamp_processed': int(time.time())
        }
        
        # Parsear valores específicos según el tipo de análisis
        if analysis_type in ['comuna_max', 'calle_max']:
            # Formato: "Nombre,Valor"
            parts = analysis_value.split(',')
            if len(parts) == 2:
                document['name'] = parts[0]
                document['count'] = int(parts[1])
        elif analysis_type in ['media_accidentes', 'media_policia']:
            # Formato: valor numérico
            try:
                document['numeric_value'] = float(analysis_value)
            except ValueError:
                document['numeric_value'] = 0
        elif analysis_type == 'top_comunas':
            # Formato: múltiples líneas "Comuna,Valor"
            comunas = []
            for line in analysis_value.split('\n'):
                parts = line.split(',')
                if len(parts) == 2:
                    comunas.append({'comuna': parts[0], 'count': int(parts[1])})
            document['top_comunas'] = comunas
        elif analysis_type == 'desviacion':
            # Formato: múltiples líneas "Comuna,Desviacion"
            desviaciones = []
            for line in analysis_value.split('\n'):
                parts = line.split(',')
                if len(parts) == 2:
                    try:
                        desviaciones.append({'comuna': parts[0], 'desviacion': float(parts[1])})
                    except ValueError:
                        continue
            document['desviaciones'] = desviaciones
        
        if send_document_to_elasticsearch(document, 'procesed-waze_events'):
            success_count += 1
        else:
            error_count += 1
    
    total_documents = len(data['incidentes_por_dia']) + len(data['analisis'])
    logger.info(f"Enviados {success_count}/{total_documents} documentos exitosamente")
    
    if error_count > 0:
        logger.warning(f"{error_count} documentos fallaron al enviarse")
    
    return error_count == 0

def main():
    """Proceso principal - Pipeline completo de procesamiento de datos"""
    try:
        # Verificar si ya existen resultados previos
        if os.path.exists('results/incidentes_por_dia/part-r-00000'):
            logger.info("Se detectaron resultados previos - enviando solo a Elasticsearch")
            return send_results_to_elasticsearch()
        
        # Pipeline completo desde el inicio
        logger.info("Iniciando pipeline completo de procesamiento de datos Waze")
        
        # Fase 1: Obtener y preparar datos
        logger.info("=== FASE 1: Obtención y preparación de datos ===")
        
        # Obtener eventos desde ElasticSearch
        events = fetch_waze_events()
        if not events:
            logger.error("No se pudieron obtener eventos, finalizando")
            return False
        
        # Guardar eventos localmente en formato JSON
        if not save_json_data(events):
            logger.error("Error al guardar datos JSON, finalizando")
            return False
        
        # Convertir JSON a CSV para procesamiento con Pig
        if not convert_json_to_csv():
            logger.error("Error al convertir a CSV, finalizando")
            return False
        
        # Fase 2: Procesamiento con Apache Pig
        logger.info("=== FASE 2: Procesamiento con Apache Pig ===")
        
        # Ejecutar procesamiento con Apache Pig
        if not run_pig_processor():
            logger.error("Error al ejecutar processor.pig, finalizando")
            return False
        
        # Fase 3: Envío de resultados a Elasticsearch
        logger.info("=== FASE 3: Envío de resultados a Elasticsearch ===")
        
        # Automáticamente ejecutar envío a Elasticsearch
        return send_results_to_elasticsearch()
            
    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}")
        return False

def send_results_to_elasticsearch():
    """Lee los resultados procesados y los envía a Elasticsearch"""
    try:
        logger.info("Iniciando envío de resultados a Elasticsearch")
        
        # Verificar que existen los resultados
        if not os.path.exists('results/incidentes_por_dia/part-r-00000'):
            logger.error("No se encontraron archivos de resultados para enviar")
            return False
        
        # Leer resultados procesados
        processed_results = read_processed_results()
        
        if not processed_results['incidentes_por_dia']:
            logger.warning("No se encontraron resultados procesados válidos")
            return False
        
        logger.info(f"Resultados leídos: {len(processed_results['incidentes_por_dia'])} incidentes, {len(processed_results['analisis'])} análisis")
        
        # Enviar resultados a ElasticSearch para Kibana
        elasticsearch_success = send_to_elasticsearch(processed_results)
        
        if elasticsearch_success:
            logger.info("Pipeline completo ejecutado exitosamente - Resultados enviados a Elasticsearch")
            log_pipeline_summary(processed_results)
            return True
        else:
            logger.error("Error al enviar resultados a Elasticsearch")
            return False
            
    except Exception as e:
        logger.error(f"Error al enviar resultados: {e}")
        return False

def log_pipeline_summary(results):
    """Registra un resumen del pipeline ejecutado"""
    try:
        logger.info("=== RESUMEN DEL PIPELINE ===")
        logger.info(f"Total de incidentes procesados: {len(results['incidentes_por_dia'])}")
        logger.info(f"Análisis estadísticos generados: {len(results['analisis'])}")
        
        # Mostrar algunos análisis clave si están disponibles
        if 'comuna_max' in results['analisis']:
            logger.info(f"Comuna con más incidentes: {results['analisis']['comuna_max']}")
        if 'calle_max' in results['analisis']:
            logger.info(f"Calle con más incidentes: {results['analisis']['calle_max']}")
        
        logger.info("Pipeline completado exitosamente")
        logger.info("Datos disponibles en:")
        logger.info("  - Elasticsearch: índice 'procesed-waze_events'")
        logger.info("  - Kibana: visualizaciones disponibles")
        
    except Exception as e:
        logger.warning(f"Error al generar resumen: {e}")

if __name__ == "__main__":
    logger.info("Iniciando script de procesamiento de datos Waze")
    try:
        success = main()
        if success:
            logger.info("Script ejecutado exitosamente")
            sys.exit(0)
        else:
            logger.error("Script falló")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Script interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error crítico en el script: {e}")
        sys.exit(1)
