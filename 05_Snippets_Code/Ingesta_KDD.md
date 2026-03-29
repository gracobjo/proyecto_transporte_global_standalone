# Ingesta KDD - Snapshot de Datos

## 📝 Descripción
Script principal de ingesta que genera un snapshot del sistema cada 15 minutos.

## 💻 Código
```python
import json
import random
from datetime import datetime
from kafka import KafkaProducer

def generar_snapshot():
    """Genera snapshot del estado actual del sistema."""
    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "clima": obtener_clima(),
        "nodos": simular_estados_nodos(),
        "aristas": simular_estados_aristas(),
        "camiones": simular_camiones()
    }
    return snapshot

def obtener_clima():
    """Obtiene clima de API OpenWeatherMap."""
    # Implementación...
    pass

def simular_estados_nodos():
    """Simula estados de nodos."""
    estados = ["OK", "Congestionado", "Bloqueado"]
    return [{"id": nodo, "estado": random.choice(estados)} for nodo in nodos]

def publicar_kafka(snapshot):
    """Publica snapshot a Kafka."""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('transporte_raw', snapshot)
```

## 📌 Uso
```bash
python ingesta_kdd.py
```

## 🔧 Requisitos
- Python 3.x
- kafka-python
- requests

## 🏷️ Tags
#ingesta #kafka #simlog
