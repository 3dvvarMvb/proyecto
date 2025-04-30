import os
import time
import redis
import requests
#from collections import OrderedDict
print(">>> INICIO DEL SCRIPT <<<")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))
CACHE_CAPACITY = 200  # Capacidad máxima de claves

def clear_redis_cache():
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

print("iniciando Cliente Redis")
r = wait_for_redis(REDIS_HOST, REDIS_PORT)
print("Cliente Redis inicializado.")

def remove_keys_policy(keys):
    """
    Atiende cada key recibida como una petición de uso reciente.
    Si la key no existe y el cache está lleno, elimina la menos usada (LRU).
    """
    for key in keys:
        # Si la key ya existe, actualiza su TTL para simular "uso reciente"
        if r.exists(key):
            r.expire(key, CACHE_TTL)
            print(f"TTL actualizado para {key} (uso reciente)")
        else:
            # Si el cache está lleno, elimina la menos recientemente usada
            current_keys = r.keys("event:*")
            if len(current_keys) >= CACHE_CAPACITY:
                # Encuentra la menos recientemente usada (por TTL más bajo)
                lru_key = min(current_keys, key=lambda k: r.ttl(k))
                r.delete(lru_key)
                print(f"Cache lleno. Clave LRU eliminada: {lru_key.decode()}")
            # Inserta la nueva key con TTL
            r.setex(key, CACHE_TTL, "placeholder")
            print(f"Clave nueva agregada al cache: {key}")
            
def poll_storage_keys(interval=15):
    print("Iniciando polling a storage para control de claves...")
    while True:
        try:
            resp = requests.get("http://storage:5000/events-cache/keys", timeout=5)
            if resp.ok:
                data = resp.json()
                keys = data.get("keys", [])
                print(f"Recibidas {len(keys)} keys desde storage.")
                remove_keys_policy(keys)
            else:
                print("Error al consultar storage:", resp.text)
        except Exception as e:
            print("Error conectando a storage:", e)
        time.sleep(interval)


if __name__ == "__main__":
    # Puedes lanzar el polling en un hilo aparte si tienes más lógica principal
    print("Iniciando el servicio de cache...")

    clear_redis_cache()

    poll_storage_keys(interval=15)