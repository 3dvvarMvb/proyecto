import os
import time
import redis
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

metrics = {
    "hits": 0,
    "misses": 0,
    "requests": 0,
    "total_time_ms": 0,
    "evictions": 0,
    "eviction_policy": ""
}
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))
CACHE_CAPACITY = 20  # Capacidad máxima de claves

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

def remove_keys_policy(keys, policy="lifo"):
    metrics["eviction_policy"] = policy
    for key in keys:
        start_time = time.time()
        metrics["requests"] += 1
        
        if r.exists(key):
            r.expire(key, CACHE_TTL)
            metrics["hits"] += 1
            print(f"TTL actualizado para {key} (uso reciente)")
        else:
            metrics["misses"] += 1
            current_keys = r.keys("event:*")
            if len(current_keys) >= CACHE_CAPACITY:
                if policy.lower() == "lifo":
                    lru_key = current_keys[-1]
                else:
                    lru_key = min(current_keys, key=lambda k: r.ttl(k))
                
                r.delete(lru_key)
                metrics["evictions"] += 1
                print(f"Cache lleno. Clave eliminada ({policy}): {lru_key.decode()}")
            
            r.setex(key, CACHE_TTL, "placeholder")
            print(f"Clave nueva agregada al cache: {key}")
        
        elapsed = (time.time() - start_time) * 1000
        metrics["total_time_ms"] += elapsed

def print_metrics():
    print("\n------ CACHE METRICS ------")
    hit_rate = metrics["hits"] / metrics["requests"] if metrics["requests"] else 0
    miss_rate = metrics["misses"] / metrics["requests"] if metrics["requests"] else 0
    avg_time = metrics["total_time_ms"] / metrics["requests"] if metrics["requests"] else 0
    
    print(f"Hits: {metrics['hits']}, Misses: {metrics['misses']}")
    print(f"Hit rate: {hit_rate:.2%}, Miss rate: {miss_rate:.2%}")
    print(f"Avg response time: {avg_time:.2f} ms")
    print(f"Evictions: {metrics['evictions']} ({metrics['eviction_policy'].upper()} policy)")
    print("---------------------------\n")

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

# --- NUEVO: Endpoints HTTP para cache ---

import json

@app.route("/cache", methods=["GET"])
def cache_get():
    event_id = request.args.get("event_id")
    if not event_id:
        return jsonify({"error": "event_id requerido"}), 400
    key = f"event:{event_id}"
    start = time.time()
    metrics["requests"] += 1
    value = r.get(key)
    elapsed = (time.time() - start) * 1000
    metrics["total_time_ms"] += elapsed
    if value:
        metrics["hits"] += 1
        try:
            return jsonify({"event": json.loads(value)}), 200
        except Exception:
            return jsonify({"event": value.decode()}), 200
    else:
        metrics["misses"] += 1
        return jsonify({"event": None}), 404

@app.route("/cache", methods=["POST"])
def cache_set():
    data = request.get_json()
    if not data or "event" not in data or "id" not in data["event"]:
        return jsonify({"error": "event con id requerido"}), 400
    event = data["event"]
    ttl = data.get("ttl", CACHE_TTL)
    key = f"event:{event['id']}"
    try:
        r.setex(key, ttl, json.dumps(event).encode("utf-8"))
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cache/metrics", methods=["GET"])
def cache_metrics():
    hit_rate = metrics["hits"] / metrics["requests"] if metrics["requests"] else 0
    miss_rate = metrics["misses"] / metrics["requests"] if metrics["requests"] else 0
    avg_time = metrics["total_time_ms"] / metrics["requests"] if metrics["requests"] else 0
    return jsonify({
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": hit_rate,
        "miss_rate": miss_rate,
        "avg_time_ms": avg_time,
        "evictions": metrics["evictions"],
        "eviction_policy": metrics["eviction_policy"]
    })

if __name__ == "__main__":
    import threading
    def metrics_loop():
        while True:
            print_metrics()
            time.sleep(30)
    threading.Thread(target=metrics_loop, daemon=True).start()
    threading.Thread(target=poll_storage_keys, args=(15,), daemon=True).start()
    clear_redis_cache()
    app.run(host="0.0.0.0", port=5000)