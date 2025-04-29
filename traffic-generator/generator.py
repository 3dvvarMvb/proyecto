from cassandra.cluster import Cluster
import random, time, requests, argparse,threading
import redis

REDIS_HOST = "redis"
REDIS_PORT = 6379

CACHE_URL = "http://cache-service:5000/cache"  # Cambia a cache-service (nombre del servicio Docker)
CASSANDRA_HOST = "cassandra"

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

# Antes de cualquier conexión:
wait_for_cassandra(CASSANDRA_HOST)

# Conexión global a Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

metrics = {
    "hits": 0,
    "misses": 0,
    "requests": 0,
    "total_time_ms": 0
}

def query_cassandra(event_id):
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('waze')
    row = session.execute("SELECT * FROM events WHERE id=%s", (event_id,))
    result = row.one()
    session.shutdown()
    return dict(result._asdict()) if result else None

def _key(event_id):
    return f"event:{event_id}"

def get_from_cache(event_id):
    start = time.time()
    try:
        value = redis_client.get(_key(event_id))
        metrics["requests"] += 1
        elapsed = (time.time() - start) * 1000
        metrics["total_time_ms"] += elapsed
        print(f"Tiempo de consulta cache Redis: {elapsed:.2f} ms")
        
        if value:
            metrics["hits"] += 1
            import json
            return json.loads(value)
        else:
            metrics["misses"] += 1
            return None
    except Exception as e:
        print(f"Error consultando cache Redis: {e}")
        metrics["misses"] += 1
    return None

def set_in_cache(event):
    try:
        import json
        redis_client.set(_key(event["id"]), json.dumps(event))
        return True
    except Exception as e:
        #print(f"Error al guardar en cache Redis: {e}")
        return False

def process_query(event_id):
    result = get_from_cache(event_id)
    if result:
        print(f"Cache hit para id={event_id}")
        return result
    print(f"Cache miss para id={event_id}, consultando Cassandra")
    result = query_cassandra(event_id)
    if result:
        set_in_cache(result)
    return result

def generate_uniform(event_ids, interval_min, interval_max):
    while True:
        # Genera un intervalo aleatorio entre interval_min y interval_max   
        eid = random.choice(event_ids)
        time.sleep(random.uniform(interval_min, interval_max))
        process_query(eid)
        print_metrics()


def generate_poisson(event_ids, rate):
    while True:
        # Genera un intervalo aleatorio usando la distribución de Poisson
        eid = random.choice(event_ids)
        interval = random.expovariate(rate)
        time.sleep(interval)
        process_query(eid)
        print_metrics()


def print_metrics():
        print("------ CACHE METRICS ------")
        hit_rate = metrics["hits"] / metrics["requests"] if metrics["requests"] else 0
        miss_rate = metrics["misses"] / metrics["requests"] if metrics["requests"] else 0
        avg_time = metrics["total_time_ms"] / metrics["requests"] if metrics["requests"] else 0
        print(f"Hits: {metrics['hits']}, Misses: {metrics['misses']}, Hit rate: {hit_rate:.2f}, Miss rate: {miss_rate:.2f}, Avg response time: {avg_time:.2f} ms")
        print("---------------------------")

def fill_cache(event_ids):
    print("Llenando el cache con todos los eventos de Cassandra...")
    for eid in event_ids:
        event = query_cassandra(eid)
        if event:
            if set_in_cache(event)==True:
                print(f"Evento {eid} guardado en cache.")
        else:
            print("Cache llenado con todos los eventos de Cassandra.")
            break
    print("Cache lleno")

if __name__ == "__main__":
    # Configuración editable en el código
    MODEL = 'uniform'  # 'uniform' o 'poisson'
    INTERVAL_MIN = 1.0  # intervalo mínimo (s)
    INTERVAL_MAX = 5.0  # intervalo máximo (s)
    POISSON_RATE = 1.0  # tasa λ para Poisson

    # Obtén todos los ids de eventos de Cassandra
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('waze')
    rows = session.execute("SELECT id FROM events;")
    event_ids = [row.id for row in rows]
    session.shutdown()

    fill_cache(event_ids)  # Llenar el cache antes de empezar a generar eventos

    # Generador de eventos
    if MODEL == 'uniform':
        print(f"Usando modelo uniforme con intervalo [{INTERVAL_MIN}, {INTERVAL_MAX}] segundos")
        generate_uniform(event_ids, INTERVAL_MIN, INTERVAL_MAX)

    else:
        print(f"Usando modelo Poisson con tasa λ={POISSON_RATE}")
        generate_poisson(event_ids, POISSON_RATE)
        