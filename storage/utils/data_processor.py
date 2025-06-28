import pandas as pd
import json
from datetime import datetime

def process_json_data(data):
    """
    Procesa datos JSON para preparar para indexación en Elasticsearch
    Puede ser una lista de documentos o un documento único
    """
    if isinstance(data, list):
        # Es una lista de documentos
        processed_data = []
        for doc in data:
            # Añadir timestamp si no existe
            if '@timestamp' not in doc:
                doc['@timestamp'] = datetime.now().isoformat()
            processed_data.append(doc)
        return processed_data
    elif isinstance(data, dict):
        # Es un solo documento
        if '@timestamp' not in data:
            data['@timestamp'] = datetime.now().isoformat()
        return [data]
    else:
        raise ValueError("Formato de datos JSON no válido")

def process_csv_data(df):
    """
    Procesa un DataFrame de pandas para preparar para indexación en Elasticsearch
    """
    # Convertir DataFrame a una lista de diccionarios
    records = df.to_dict(orient='records')
    
    # Añadir timestamp a cada registro
    for record in records:
        if '@timestamp' not in record:
            record['@timestamp'] = datetime.now().isoformat()
    
    return records

def flatten_nested_json(data, prefix=''):
    """
    Aplana documentos JSON anidados para facilitar la indexación en Elasticsearch
    """
    flattened = {}
    for key, value in data.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            flattened.update(flatten_nested_json(value, new_key))
        elif isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], dict):
                # Lista de objetos
                for i, item in enumerate(value):
                    flattened.update(flatten_nested_json(item, f"{new_key}.{i}"))
            else:
                # Lista de valores simples
                flattened[new_key] = value
        else:
            flattened[new_key] = value
    
    return flattened