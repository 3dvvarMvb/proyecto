import os
import time
import redis
import requests
import threading
import json

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))

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

def poll_storage_keys(interval=15):
    print("Iniciando polling a storage para control de claves...")
    while True:
        try:
            resp = requests.get("http://storage:5000/events-cache/keys", timeout=5)
            if resp.ok:
                data = resp.json()
                keys = data.get("keys", [])
                print(f"Recibidas {len(keys)} keys desde storage.")
            else:
                print("Error al consultar storage:", resp.text)
        except Exception as e:
            print("Error conectando a storage:", e)
        time.sleep(interval)

if __name__ == "__main__":
    clear_redis_cache()
    threading.Thread(target=poll_storage_keys, args=(15,), daemon=True).start()