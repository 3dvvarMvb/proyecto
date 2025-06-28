from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import json
import os
import time
import redis
from utils.elastic_handler import ElasticsearchHandler
from utils.data_processor import process_json_data, process_csv_data

app = Flask(__name__)
CORS(app)

# Conectar con Elasticsearch
es_host = os.environ.get('ELASTICSEARCH_HOST', 'elasticsearch')
es_port = os.environ.get('ELASTICSEARCH_PORT', '9200')
es_handler = ElasticsearchHandler(f"http://{es_host}:{es_port}")

# Conectar con Redis
redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = os.environ.get('REDIS_PORT', '6379')
redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

# Endpoint para recibir y enviar eventos

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar salud del servicio"""
    try:
        # Verificar conexión con Redis
        redis_ping = redis_client.ping()
        
        return jsonify({
            "status": "UP",
            "elasticsearch": "connected",
            "redis": "connected" if redis_ping else "disconnected"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "PARTIAL",
            "elasticsearch": "connected",
            "redis": "disconnected",
            "redis_error": str(e)
        }), 200

@app.route('/upload/json', methods=['POST'])
def upload_json():
    """Endpoint para recibir y procesar datos JSON"""
    try:
        # Verificar si se recibió un archivo o datos en JSON
        if 'file' in request.files:
            file = request.files['file']
            data = json.load(file)
        else:
            data = request.json
        
        if not data:
            return jsonify({"error": "No se proporcionaron datos JSON válidos"}), 400
        
        # Obtener parámetros
        index_name = request.args.get('index', 'default_index')
        
        # Procesar y cargar datos en Elasticsearch
        processed_data = process_json_data(data)
        result = es_handler.bulk_index(index_name, processed_data)
        
        return jsonify({
            "message": "Datos cargados exitosamente en Elasticsearch",
            "index": index_name,
            "count": len(processed_data),
            "details": result
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload/csv', methods=['POST'])
def upload_csv():
    """Endpoint para recibir y procesar datos CSV"""
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No se proporcionó ningún archivo CSV"}), 400
        
        file = request.files['file']
        
        # Obtener parámetros
        index_name = request.args.get('index', 'default_index')
        delimiter = request.args.get('delimiter', ',')
        
        # Procesar y cargar datos en Elasticsearch
        df = pd.read_csv(file, delimiter=delimiter)
        processed_data = process_csv_data(df)
        result = es_handler.bulk_index(index_name, processed_data)
        
        return jsonify({
            "message": "Datos CSV cargados exitosamente en Elasticsearch",
            "index": index_name,
            "count": len(processed_data),
            "details": result
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/indices', methods=['GET'])
def get_indices():
    """Endpoint para listar índices disponibles en Elasticsearch"""
    try:
        indices = es_handler.list_indices()
        return jsonify({"indices": indices}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/indices/<index_name>', methods=['DELETE'])
def delete_index(index_name):
    """Endpoint para eliminar un índice"""
    try:
        result = es_handler.delete_index(index_name)
        return jsonify({"message": f"Índice '{index_name}' eliminado", "result": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get-waze-events', methods=['GET'])
def get_waze_events():
    """Endpoint para obtener todos los datos del índice waze-events de Elasticsearch"""
    try:
        # Obtener parámetros opcionales
        limit = request.args.get('limit', type=int)
        use_scroll = request.args.get('scroll', 'true').lower() == 'true'
        
        if limit and limit <= 10000:
            # Para consultas pequeñas, usar búsqueda normal
            query = {
                "query": {
                    "match_all": {}
                },
                "size": limit
            }
            
            result = es_handler.search('default_index', query)
            documents = [hit['_source'] for hit in result['hits']['hits']]
            
            return jsonify({
                "data": documents,
                "total": result['hits']['total']['value'],
                "method": "search"
            }), 200
        
        elif use_scroll:
            # Para consultas grandes, usar Scroll API
            all_documents = []
            
            # Iniciar scroll
            query = {
                "query": {
                    "match_all": {}
                },
                "size": 1000  # Tamaño de batch por scroll
            }
            
            result = es_handler.search('default_index', query, scroll='2m')
            scroll_id = result['_scroll_id']
            
            # Procesar primer batch
            documents = [hit['_source'] for hit in result['hits']['hits']]
            all_documents.extend(documents)
            
            # Continuar scrolling hasta obtener todos los documentos
            while len(documents) > 0:
                result = es_handler.scroll(scroll_id, scroll='2m')
                documents = [hit['_source'] for hit in result['hits']['hits']]
                all_documents.extend(documents)
            
            # Limpiar scroll
            es_handler.clear_scroll(scroll_id)
            
            return jsonify({
                "data": all_documents,
                "total": len(all_documents),
                "method": "scroll"
            }), 200
        
        else:
            # Búsqueda limitada por defecto
            query = {
                "query": {
                    "match_all": {}
                },
                "size": 10000  # Máximo permitido por defecto
            }
            
            result = es_handler.search('default_index', query)
            documents = [hit['_source'] for hit in result['hits']['hits']]
            
            return jsonify({
                "data": documents,
                "total": result['hits']['total']['value'],
                "returned": len(documents),
                "method": "search_limited",
                "note": "Use ?scroll=true para obtener todos los documentos o ?limit=N para limitar resultados"
            }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/events-cache', methods=['POST'])
def events_cache():
    """Guardar evento en caché Redis"""
    try:
        data = request.get_json()
        event = data.get("event", {})
        
        if not event:
            return jsonify({"error": "No se proporcionó evento válido"}), 400
        
        # Generar key única para el evento
        event_id = event.get('id') or event.get('uuid') or str(hash(str(event)))
        key = f"event:{event_id}"
        
        # Guardar en Redis como JSON string
        redis_client.set(key, json.dumps(event))
        
        # Agregar la key a un set para tracking
        redis_client.sadd("event_keys", key)
        
        return jsonify({
            "message": "Evento guardado en caché",
            "key": key,
            "event_id": event_id
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/events-cache/keys', methods=['GET'])
def get_last_keys():
    """Obtener todas las keys de eventos desde Redis"""
    try:
        # Obtener todas las keys del set
        keys = list(redis_client.smembers("event_keys"))
        
        # Verificar que las keys aún existen en Redis (por si expiraron)
        existing_keys = []
        for key in keys:
            if redis_client.exists(key):
                existing_keys.append(key)
            else:
                # Remover del set si ya no existe
                redis_client.srem("event_keys", key)
        
        return jsonify({
            "keys": existing_keys,
            "total": len(existing_keys)
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/events-cache/<event_key>', methods=['GET'])
def get_cached_event(event_key):
    """Obtener un evento específico del caché"""
    try:
        # Asegurar que la key tenga el prefijo correcto
        if not event_key.startswith("event:"):
            event_key = f"event:{event_key}"
        
        event_data = redis_client.get(event_key)
        
        if event_data is None:
            return jsonify({"error": "Evento no encontrado en caché"}), 404
        
        return jsonify({
            "key": event_key,
            "event": json.loads(event_data)
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/events-cache/<event_key>', methods=['DELETE'])
def delete_cached_event(event_key):
    """Eliminar un evento específico del caché"""
    try:
        # Asegurar que la key tenga el prefijo correcto
        if not event_key.startswith("event:"):
            event_key = f"event:{event_key}"
        
        # Eliminar del Redis y del set de tracking
        deleted = redis_client.delete(event_key)
        redis_client.srem("event_keys", event_key)
        
        if deleted:
            return jsonify({"message": f"Evento {event_key} eliminado del caché"}), 200
        else:
            return jsonify({"error": "Evento no encontrado"}), 404
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/events-cache', methods=['DELETE'])
def clear_events_cache():
    """Limpiar todo el caché de eventos"""
    try:
        # Obtener todas las keys
        keys = list(redis_client.smembers("event_keys"))
        
        # Eliminar todas las keys de eventos
        if keys:
            redis_client.delete(*keys)
        
        # Limpiar el set de tracking
        redis_client.delete("event_keys")
        
        return jsonify({
            "message": "Caché de eventos limpiado",
            "deleted_keys": len(keys)
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)