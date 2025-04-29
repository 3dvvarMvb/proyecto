from cassandra.cluster import Cluster
import random, time, requests, argparse,threading

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

def query_cassandra(event_id):
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('waze')
    row = session.execute("SELECT * FROM events WHERE id=%s", (event_id,))
    result = row.one()
    session.shutdown()
    return dict(result._asdict()) if result else None

def get_from_cache(event_id):
    try:
        resp = requests.get(f"{CACHE_URL}/{event_id}", timeout=2)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error consultando cache: {e}")
    return None

def set_in_cache(event):
    try:
        requests.post(CACHE_URL, json=event, timeout=2)
    except Exception as e:
        print(f"Error al guardar en cache: {e}")

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
    for eid in event_ids:
        time.sleep(random.uniform(interval_min, interval_max))
        process_query(eid)

def generate_poisson(event_ids, rate):
    for eid in event_ids:
        interval = random.expovariate(rate)
        time.sleep(interval)
        process_query(eid)

def print_metrics_loop(interval=5):
    while True:
        try:
            resp = requests.get(f"{CACHE_URL}/metrics", timeout=2)
            if resp.status_code == 200:
                metrics = resp.json()
                print(f"[CACHE METRICS] Usage: {metrics['cache_usage']}/{metrics['cache_max_size']} | "
                      f"Hit Rate: {metrics['hit_rate']:.2f} | Miss Rate: {metrics['miss_rate']:.2f} | "
                      f"Evictions: {metrics['evictions']}")
            else:
                print("[CACHE METRICS] No response")
        except Exception as e:
            print(f"[CACHE METRICS] Error: {e}")
        time.sleep(interval)

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

    process_query(event_ids[0])  # Consulta inicial para evitar que el primer evento tarde mucho
    # Generador de eventos
    if MODEL == 'uniform':
        print(f"Usando modelo uniforme con intervalo [{INTERVAL_MIN}, {INTERVAL_MAX}] segundos")
        generator_thread = threading.Thread(target=generate_uniform, args=(event_ids, INTERVAL_MIN, INTERVAL_MAX), daemon=True)
    else:
        print(f"Usando modelo Poisson con tasa λ={POISSON_RATE}")
        generator_thread = threading.Thread(target=generate_poisson, args=(event_ids, POISSON_RATE), daemon=True)

    metrics_thread = threading.Thread(target=print_metrics_loop, daemon=True)
    metrics_thread.start()
    generator_thread.start()