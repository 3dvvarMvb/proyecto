import json
import csv

# Leer archivo JSON exportado
with open('data/eventos.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Escribir como CSV
with open('data/eventos.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'id', 'timestamp', 'latitude', 'longitude', 'type', 'subtype',
        'street', 'city', 'country', 'reliability', 'reportrating',
        'confidence', 'speedkmh', 'length', 'delay'
    ])
    for d in data:
        writer.writerow([
            d.get('id'), d.get('timestamp'), d.get('latitude'), d.get('longitude'),
            d.get('type'), d.get('subtype'), d.get('street'), d.get('city'),
            d.get('country'), d.get('reliability'), d.get('reportRating'),
            d.get('confidence'), d.get('speedKMH'), d.get('length'), d.get('delay')
        ])
