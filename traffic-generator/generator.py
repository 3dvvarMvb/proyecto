from cassandra.cluster import Cluster
import random, time, requests, argparse

CACHE_URL = "http://cache-service:5000/cache"  # Cambia a cache-service (nombre del servicio Docker)
CASSANDRA_HOST = "cassandra"

time.sleep(15)  # Espera a que Cassandra esté listo

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de tráfico")
    parser.add_argument('--model', choices=['uniform','poisson'], default='uniform')
    parser.add_argument('--min', type=float, default=1.0, help="intervalo mínimo (s)")
    parser.add_argument('--max', type=float, default=5.0, help="intervalo máximo (s)")
    parser.add_argument('--rate', type=float, default=1.0, help="tasa λ para Poisson")
    args = parser.parse_args()

    # Obtén todos los ids de eventos de Cassandra
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('waze')
    rows = session.execute("SELECT id FROM events;")
    event_ids = [row.id for row in rows]
    session.shutdown()

    if args.model == 'uniform':
        generate_uniform(event_ids, args.min, args.max)
    else:
        generate_poisson(event_ids, args.rate)