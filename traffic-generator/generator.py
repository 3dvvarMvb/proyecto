from cassandra.cluster import Cluster
import random
import time
import requests
import json

CACHE_URL = "http://cache-service:5000/cache"
STORAGE_URL = "http://storage:5000/events-cache"
CASSANDRA_HOST = "cassandra"

metrics = {
    "hits": 0,
    "misses": 0,
    "requests": 0,
    "total_time_ms": 0
}

def wait_for_cassandra(host, timeout=60):
    start = time.time()
    while True:
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            session.shutdown()
            cluster.shutdown()
            print("Cassandra está listo.")
            break
        except Exception as e:
            if time.time() - start > timeout:
                print("Timeout esperando Cassandra.")
                raise e
            print("Esperando Cassandra...")
            time.sleep(3)

wait_for_cassandra(CASSANDRA_HOST)

def query_cassandra(event_id):
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('waze')
    row = session.execute("SELECT * FROM events WHERE id=%s", (event_id,))
    result = row.one()
    session.shutdown()
    if result:
        event_dict = dict(result._asdict())
        for k, v in event_dict.items():
            if not isinstance(v, (str, int, float, bool, list, dict, type(None))):
                event_dict[k] = str(v)
        return event_dict
    return None

def get_from_cache(event_id):
    start = time.time()
    try:
        resp = requests.get(CACHE_URL, params={"event_id": event_id}, timeout=2)
        metrics["requests"] += 1
        elapsed = (time.time() - start) * 1000
        metrics["total_time_ms"] += elapsed
        print(f"Tiempo de consulta cache-service: {elapsed:.2f} ms")
        if resp.status_code == 200:
            metrics["hits"] += 1
            return resp.json()["event"]
        else:
            metrics["misses"] += 1
            return None
    except Exception as e:
        print(f"Error consultando cache-service: {e}")
        metrics["misses"] += 1
        return None

def set_in_cache(event, ttl=3600):
    try:
        resp = requests.post(CACHE_URL, json={"event": event, "ttl": ttl}, timeout=2)
        return resp.ok
    except Exception as e:
        print(f"Error guardando en cache-service: {e}")
        return False

def notify_storage(event):
    try:
        requests.post(STORAGE_URL, json={"event": event})
    except Exception as e:
        print(f"Error notificando a storage: {e}")

def process_query(event_id):
    result = get_from_cache(event_id)
    if result:
        print(f"Cache hit para id={event_id}")
        notify_storage({"id": event_id})  # Notifica acceso a storage
        return result
    print(f"Cache miss para id={event_id}, consultando Cassandra")
    result = query_cassandra(event_id)
    if result:
        set_in_cache(result)
        notify_storage(result)  # Notifica inserción a storage
    return result

def print_metrics():
    print("------ CACHE METRICS (GENERATOR) ------")
    hit_rate = metrics["hits"] / metrics["requests"] if metrics["requests"] else 0
    miss_rate = metrics["misses"] / metrics["requests"] if metrics["requests"] else 0
    avg_time = metrics["total_time_ms"] / metrics["requests"] if metrics["requests"] else 0
    print(f"Hits: {metrics['hits']}, Misses: {metrics['misses']}, Hit rate: {hit_rate:.2f}, Miss rate: {miss_rate:.2f}, Avg response time: {avg_time:.2f} ms")
    print("---------------------------------------")

def generate_uniform(event_ids, interval_min, interval_max):
    while True:
        eid = random.choice(event_ids)
        process_query(eid)
        print_metrics()
        time.sleep(random.uniform(interval_min, interval_max))

def generate_poisson(event_ids, rate):
    while True:
        eid = random.choice(event_ids)
        process_query(eid)
        print_metrics()
        interval = random.expovariate(rate)
        time.sleep(interval)

def fill_cache(event_ids, porcentaje=0.2):
    print("Llenando el cache con un subconjunto de eventos iniciales de Cassandra...")
    subset = random.sample(event_ids, int(len(event_ids) * porcentaje))
    for eid in subset:
        event = query_cassandra(eid)
        if event:
            if set_in_cache(event):
                print(f"Evento {eid} guardado en cache.")
            notify_storage(event)
    print("Cache parcialmente lleno")

if __name__ == "__main__":
    MODEL = 'uniform'  # 'uniform' o 'poisson'
    INTERVAL_MIN = 5.0  # intervalo mínimo (s)
    INTERVAL_MAX = 10.0  # intervalo máximo (s)
    POISSON_RATE = 0.5  # tasa λ para Poisson

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('waze')
    rows = session.execute("SELECT id FROM events;")
    event_ids = [row.id for row in rows]
    session.shutdown()

    fill_cache(event_ids, porcentaje=0.2)  # Solo 20% de los eventos en cache al inicio

    if MODEL == 'uniform':
        print(f"Usando modelo uniforme con intervalo [{INTERVAL_MIN}, {INTERVAL_MAX}] segundos")
        generate_uniform(event_ids, INTERVAL_MIN, INTERVAL_MAX)
    else:
        print(f"Usando modelo Poisson con tasa λ={POISSON_RATE}")
        generate_poisson(event_ids, POISSON_RATE)