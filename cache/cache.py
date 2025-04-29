import os
import time
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))

# Conexión a Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

def print_metrics():
    info = r.info()
    evictions = info.get("evicted_keys", 0)
    used_memory = info.get("used_memory_human", "N/A")
    keys = r.keys("event:*")
    print("------ CACHE METRICS ------")
    print(f"Total event:* keys: {len(keys)}")
    print(f"Evictions: {evictions}")
    print(f"Used memory: {used_memory}")
    print(f"Max memory: {info.get('maxmemory_human', 'N/A')}")
    print("---------------------------")

def monitor_cache(interval=10):
    print("Iniciando monitoreo de cache Redis...")
    while True:
        print_metrics()
        time.sleep(interval)
        

if __name__ == "__main__":
    monitor_cache()