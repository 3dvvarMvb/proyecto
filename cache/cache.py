from flask import Flask, request, jsonify
import redis
import os
import time
import json
import matplotlib.pyplot as plt
import atexit

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

def save_metrics_and_plot():
    # Obtener métricas actuales
    info = r.info()
    evictions = info.get("evicted_keys", 0)
    metrics_data = {
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": metrics["hits"] / metrics["requests"] if metrics["requests"] else 0,
        "miss_rate": metrics["misses"] / metrics["requests"] if metrics["requests"] else 0,
        "avg_response_time_ms": metrics["total_time_ms"] / metrics["requests"] if metrics["requests"] else 0,
        "evictions": evictions,
        "avg_ttl_used": metrics["ttl_sum"] / metrics["ttl_count"] if metrics["ttl_count"] else 0
    }
    # Guardar en JSON
    with open("cache_metrics.json", "w") as f:
        json.dump(metrics_data, f, indent=4)

    # Graficar
    labels = ["Hits", "Misses", "Evictions"]
    values = [metrics["hits"], metrics["misses"], evictions]
    plt.figure(figsize=(6,4))
    plt.bar(labels, values, color=["green", "red", "orange"])
    plt.title("Cache Metrics")
    plt.ylabel("Count")
    plt.savefig("cache_metrics_bar.png")
    plt.close()

    # Gráfico de tasas
    rates_labels = ["Hit Rate", "Miss Rate"]
    rates_values = [metrics_data["hit_rate"], metrics_data["miss_rate"]]
    plt.figure(figsize=(6,4))
    plt.bar(rates_labels, rates_values, color=["blue", "purple"])
    plt.title("Cache Hit/Miss Rates")
    plt.ylabel("Rate")
    plt.savefig("cache_rates_bar.png")
    plt.close()


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

# Registrar función para que se ejecute al terminar el proceso
atexit.register(save_metrics_and_plot)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)