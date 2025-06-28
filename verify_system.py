#!/usr/bin/env python3
"""
Script de verificación del pipeline completo
"""

import requests
import json
import sys

def check_services():
    """Verificar que todos los servicios estén funcionando"""
    print("=== VERIFICANDO SERVICIOS ===")
    
    # Storage service
    try:
        response = requests.get("http://localhost:5000/health", timeout=5)
        print(f"✅ Storage service: {response.json()}")
    except Exception as e:
        print(f"❌ Storage service: {e}")
        return False
    
    # Elasticsearch
    try:
        response = requests.get("http://localhost:9200/_cluster/health", timeout=5)
        print(f"✅ Elasticsearch: {response.json()['status']}")
    except Exception as e:
        print(f"❌ Elasticsearch: {e}")
        return False
    
    # Redis (a través del storage)
    try:
        response = requests.get("http://localhost:5000/events-cache/keys", timeout=5)
        cache_data = response.json()
        print(f"✅ Redis (via storage): {cache_data['total']} keys")
    except Exception as e:
        print(f"❌ Redis: {e}")
        return False
    
    return True

def check_data():
    """Verificar datos en el sistema"""
    print("\n=== VERIFICANDO DATOS ===")
    
    # Datos en Elasticsearch
    try:
        response = requests.get("http://localhost:5000/get-waze-events?scroll=true", timeout=10)
        data = response.json()
        print(f"✅ Eventos en Elasticsearch: {data.get('total', 0)} eventos")
        return data.get('data', [])
    except Exception as e:
        print(f"❌ Error obteniendo eventos: {e}")
        return []

def test_cache_storage():
    """Probar almacenamiento en caché"""
    print("\n=== PROBANDO ALMACENAMIENTO EN CACHÉ ===")
    
    test_event = {
        "event": {
            "id": "test_event_123",
            "type": "test",
            "timestamp": 1672531200,
            "data": {"test": "value"}
        }
    }
    
    try:
        # Enviar evento de prueba
        response = requests.post("http://localhost:5000/events-cache", 
                               json=test_event, timeout=5)
        print(f"✅ Evento enviado a caché: {response.json()}")
        
        # Verificar que se guardó
        response = requests.get("http://localhost:5000/events-cache/keys", timeout=5)
        cache_data = response.json()
        print(f"✅ Keys en caché: {cache_data['total']}")
        
        return True
    except Exception as e:
        print(f"❌ Error probando caché: {e}")
        return False

def check_elasticsearch_indices():
    """Verificar índices en Elasticsearch"""
    print("\n=== VERIFICANDO ÍNDICES ELASTICSEARCH ===")
    
    try:
        response = requests.get("http://localhost:5000/indices", timeout=5)
        indices = response.json()
        print(f"✅ Índices disponibles: {indices}")
        return True
    except Exception as e:
        print(f"❌ Error obteniendo índices: {e}")
        return False

def main():
    print("🔍 INICIANDO VERIFICACIÓN COMPLETA DEL SISTEMA")
    
    # 1. Verificar servicios
    if not check_services():
        print("❌ Algunos servicios no están funcionando")
        return False
    
    # 2. Verificar datos
    events = check_data()
    if not events:
        print("⚠️  No hay eventos para procesar")
    
    # 3. Probar caché
    if not test_cache_storage():
        print("❌ Problemas con el almacenamiento en caché")
        return False
    
    # 4. Verificar índices
    if not check_elasticsearch_indices():
        print("❌ Problemas con Elasticsearch")
        return False
    
    print("\n✅ VERIFICACIÓN COMPLETADA EXITOSAMENTE")
    print(f"📊 Resumen:")
    print(f"   - Eventos disponibles: {len(events)}")
    print(f"   - Servicios funcionando: ✅")
    print(f"   - Cache funcionando: ✅")
    print(f"   - Elasticsearch funcionando: ✅")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
