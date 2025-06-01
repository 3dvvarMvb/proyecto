from flask import Flask, request, jsonify
from cassandra.cluster import Cluster
import time

CASSANDRA_HOST = "cassandra"

app = Flask(__name__)

last_keys = []

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

def ensure_keyspace(cluster, name):
    session = cluster.connect()
    session.execute(f"""
      CREATE KEYSPACE IF NOT EXISTS {name}
      WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)
    session.shutdown()

def ensure_table(session):
    session.execute("""
      CREATE TABLE IF NOT EXISTS events (
        id text PRIMARY KEY,
        timestamp bigint,
        latitude double,
        longitude double,
        type text,
        subtype text,
        street text,
        city text,
        country text,
        reliability double,
        reportrating double,
        confidence double,
        speedkmh double,
        length double,
        delay double
      );
    """)

def connect_with_retry(keyspace, hosts=['cassandra'], retries=20, delay=5):
    last_exc = None
    print("[storage] Esperando a que Cassandra esté listo...")
    time.sleep(10)  # Espera inicial extra
    for i in range(1, retries+1):
        try:
            cluster = Cluster(hosts)
            ensure_keyspace(cluster, keyspace)
            session = cluster.connect(keyspace)
            ensure_table(session)
            print(f"[storage] Conectado a Cassandra hosts={hosts} keyspace={keyspace}")
            return session
        except Exception as e:
            last_exc = e
            print(f"[storage] Error conectando a Cassandra: {e}. Reintentando en {delay}s ({i}/{retries})")
            time.sleep(delay)
    raise last_exc

# Antes de cualquier conexión:
wait_for_cassandra(CASSANDRA_HOST)
session = connect_with_retry('waze')

# Endpoint para recibir eventos
@app.route('/events', methods=['POST'])
def receive_events():
    events = request.get_json()
    inserted = 0
    for ev in events:
        try:
            # forzar siempre id a str
            id_val = ev.get('id')
            if id_val is None:
                continue
            id_val = str(id_val)

            # defaults para los campos opcionales
            timestamp     = ev.get('timestamp')
            latitude      = ev.get('latitude')
            longitude     = ev.get('longitude')
            ev_type       = ev.get('type')
            subtype       = ev.get('subtype', '')
            street        = ev.get('street', '')
            city          = ev.get('city', '')
            country       = ev.get('country', '')
            reliability   = ev.get('reliability', 0.0)
            report_rating = ev.get('reportRating', 0.0)    # JSON usa reportRating, tabla reportrating
            confidence    = ev.get('confidence', 0.0)
            speedkmh      = ev.get('speedKMH', 0.0)        # JSON usa speedKMH, tabla speedkmh
            length        = ev.get('length', 0.0)
            delay         = ev.get('delay', 0.0)

            session.execute(
                """
                INSERT INTO events (
                  id, timestamp, latitude, longitude, type,
                  subtype, street, city, country,
                  reliability, reportrating, confidence,
                  speedkmh, length, delay
                ) VALUES (
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s, %s,
                  %s, %s, %s
                )
                """,
                (
                  id_val,
                  ev.get('timestamp'),
                  ev.get('latitude'),
                  ev.get('longitude'),
                  ev.get('type'),
                  ev.get('subtype'),
                  ev.get('street'),
                  ev.get('city'),
                  ev.get('country'),
                  ev.get('reliability'),
                  ev.get('reportRating'),
                  ev.get('confidence'),
                  ev.get('speedKMH'),
                  ev.get('length'),
                  ev.get('delay'),
                )
            )
            inserted += 1
        except Exception as e:
            app.logger.error(f"Error inserting {id_val}: {e}")
    return jsonify({'inserted': inserted}), 200

@app.route('/events-cache', methods=['POST'])
def events_cache():
    global last_keys
    data = request.get_json()
    event = data.get("event", {})
    policy = data.get("policy", "lru")  # Por defecto LRU si no se especifica
    
    key = f"event:{event.get('id', '')}" if event else str(data)
    
    if policy.lower() == 'lifo':
        # Para LIFO: insertamos al principio
        last_keys.insert(0, key)
    else:
        # Para LRU: insertamos al final (comportamiento original)
        last_keys.append(key)
    
    print(f"Key recibida: {key} (política: {policy}, total: {len(last_keys)})")
    
    if len(last_keys) >= 10:
        print(f"Límite de 10 alcanzado, reiniciando arreglo last_keys con política {policy}.")
        last_keys = []
    
    return jsonify({"keys": last_keys, "added": key, "policy": policy})

@app.route('/events-cache/keys', methods=['GET'])
def get_last_keys():
    return jsonify({"keys": last_keys})

@app.route('/events/export', methods=['GET'])
def export_events():
    try:
        rows = session.execute("SELECT * FROM events")
        events = []
        for row in rows:
            events.append({
                'id': row.id,
                'timestamp': row.timestamp,
                'latitude': row.latitude,
                'longitude': row.longitude,
                'type': row.type,
                'subtype': row.subtype,
                'street': row.street,
                'city': row.city,
                'country': row.country,
                'reliability': row.reliability,
                'reportrating': row.reportrating,
                'confidence': row.confidence,
                'speedkmh': row.speedkmh,
                'length': row.length,
                'delay': row.delay
            })
        return jsonify(events), 200
    except Exception as e:
        app.logger.error(f"Error exporting events: {e}")
        return jsonify({'error': 'Failed to export events'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)