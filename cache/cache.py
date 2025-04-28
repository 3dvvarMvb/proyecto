from flask import Flask, request, jsonify
import redis
import os
import time

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))
CACHE_POLICY = os.getenv("CACHE_POLICY", "ttl")  # "ttl" o "lru"

time.sleep(15)  # Esperar a que Redis esté listo

app = Flask(__name__)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Métricas
metrics = {
    "hits": 0,
    "misses": 0,
    "evictions": 0,
    "total_time_ms": 0,
    "requests": 0,
    "ttl_sum": 0,
    "ttl_count": 0
}

def _key(event_id):
    return f"event:{event_id}"

@app.route("/cache/<event_id>", methods=["GET"])
def get_event(event_id):
    start = time.time()
    value = r.get(_key(event_id))
    metrics["requests"] += 1
    if value:
        metrics["hits"] += 1
        ttl = r.ttl(_key(event_id))
        if ttl > 0:
            metrics["ttl_sum"] += (CACHE_TTL - ttl)
            metrics["ttl_count"] += 1
        elapsed = (time.time() - start) * 1000
        metrics["total_time_ms"] += elapsed
        return value, 200, {'Content-Type': 'application/json'}
    else:
        metrics["misses"] += 1
        elapsed = (time.time() - start) * 1000
        metrics["total_time_ms"] += elapsed
        return jsonify({"error": "not found"}), 404

@app.route("/cache", methods=["POST"])
def set_event():
    data = request.get_json()
    event_id = data.get("id")
    if not event_id:
        return jsonify({"error": "id requerido"}), 400
    if CACHE_POLICY == "ttl":
        r.set(_key(event_id), jsonify(data).data, ex=CACHE_TTL)
    elif CACHE_POLICY == "lru":
        # LRU se configura en Redis, aquí solo set
        r.set(_key(event_id), jsonify(data).data)
    return jsonify({"status": "ok"}), 200

@app.route("/cache/metrics", methods=["GET"])
def get_metrics():
    hit_rate = metrics["hits"] / metrics["requests"] if metrics["requests"] else 0
    miss_rate = metrics["misses"] / metrics["requests"] if metrics["requests"] else 0
    avg_time = metrics["total_time_ms"] / metrics["requests"] if metrics["requests"] else 0
    avg_ttl = metrics["ttl_sum"] / metrics["ttl_count"] if metrics["ttl_count"] else 0
    # Evictions: usa info de Redis
    info = r.info()
    evictions = info.get("evicted_keys", 0)
    return jsonify({
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": hit_rate,
        "miss_rate": miss_rate,
        "avg_response_time_ms": avg_time,
        "evictions": evictions,
        "avg_ttl_used": avg_ttl
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)