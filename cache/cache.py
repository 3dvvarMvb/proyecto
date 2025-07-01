import os
import time
import redis
import requests
import threading
import json

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))

def clear_redis_cache(r):
    """Elimina todas las claves de Redis al iniciar."""
    r.flushdb()
    print("Cache de Redis limpiada al iniciar.")

def wait_for_redis(host, port, timeout=60):
    start = time.time()
    while True:
        try:
            client = redis.Redis(host=host, port=port, db=0)
            client.ping()
            print("Redis está listo.")
            return client
        except Exception as e:
            if time.time() - start > timeout:
                print("Timeout esperando Redis.")
                raise e
            print("Esperando Redis...")
            time.sleep(2)

def poll_storage_keys(r, interval=15):
    print("Iniciando polling a storage para obtener documentos del índice procesed-waze_events...")
    while True:
        try:
            # Hacer polling al endpoint get-waze-events con el índice específico
            resp = requests.get("http://storage:5000/get-waze-events?index=procesed-waze_events&scroll=true", timeout=30)
            if resp.ok:
                data = resp.json()
                documents = data.get("data", [])
                total = data.get("total", 0)
                method = data.get("method", "unknown")
                
                print(f"Recibidos {len(documents)} documentos desde storage (total: {total}, método: {method})")
                
                # Procesar cada documento
                cached_count = 0
                for doc in documents:
                    try:
                        # Generar una clave única para cada documento
                        if 'document_type' in doc:
                            if doc['document_type'] == 'incidente':
                                # Para incidentes: usar comuna, calle, tipo y fecha como clave
                                key = f"incidente:{doc.get('comuna', 'unknown')}:{doc.get('calle', 'unknown')}:{doc.get('tipo', 'unknown')}:{doc.get('fecha', 'unknown')}"
                            elif doc['document_type'] == 'analisis_estadistico':
                                # Para análisis: usar el tipo de análisis como clave
                                key = f"analisis:{doc.get('analysis_type', 'unknown')}"
                            else:
                                # Clave genérica
                                key = f"document:{hash(str(doc))}"
                        else:
                            # Documento sin tipo específico
                            key = f"document:{hash(str(doc))}"
                        
                        # Guardar en Redis con TTL
                        r.setex(key, CACHE_TTL, json.dumps(doc))
                        cached_count += 1
                        
                    except Exception as doc_error:
                        print(f"Error al procesar documento: {doc_error}")
                        continue
                
                print(f"Cacheados {cached_count} documentos en Redis.")
                
                # Mostrar estadísticas del cache
                total_keys = r.dbsize()
                print(f"Total de claves en Redis: {total_keys}")
                
            else:
                print(f"Error al consultar storage: {resp.status_code} - {resp.text}")
        except requests.exceptions.Timeout:
            print("Timeout al consultar storage - reintentando...")
        except Exception as e:
            print(f"Error conectando a storage: {e}")
        
        time.sleep(interval)

def show_cache_stats(r):
    """Muestra estadísticas del cache Redis"""
    try:
        total_keys = r.dbsize()
        print(f"=== ESTADÍSTICAS DEL CACHE ===")
        print(f"Total de claves en Redis: {total_keys}")
        
        # Obtener algunas claves de ejemplo
        sample_keys = r.keys("*")[:10]  # Primeras 10 claves
        print(f"Ejemplos de claves:")
        for key in sample_keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            print(f"  - {key_str}")
        
        # Contar por tipo de documento
        incidente_keys = r.keys("incidente:*")
        analisis_keys = r.keys("analisis:*")
        document_keys = r.keys("document:*")
        
        print(f"Incidentes cacheados: {len(incidente_keys)}")
        print(f"Análisis cacheados: {len(analisis_keys)}")
        print(f"Documentos genéricos: {len(document_keys)}")
        print("=" * 30)
        
    except Exception as e:
        print(f"Error al mostrar estadísticas: {e}")

def get_cached_document(r, key):
    """Obtiene un documento específico del cache"""
    try:
        data = r.get(key)
        if data:
            return json.loads(data)
        return None
    except Exception as e:
        print(f"Error al obtener documento {key}: {e}")
        return None

if __name__ == "__main__":
    print("Iniciando Cliente Redis")
    r = wait_for_redis(REDIS_HOST, REDIS_PORT)
    print("Cliente Redis inicializado.")
    clear_redis_cache(r)
    
    # Iniciar el hilo de polling
    threading.Thread(target=poll_storage_keys, args=(r, 15), daemon=True).start()
    
    # Mostrar estadísticas cada 60 segundos
    print("Sistema de cache iniciado. Mostrando estadísticas cada 60 segundos...")
    while True:
        time.sleep(60)
        show_cache_stats(r)