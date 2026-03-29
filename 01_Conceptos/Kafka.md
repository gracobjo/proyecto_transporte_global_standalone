# Apache Kafka

## 🧠 Definición
Sistema de mensajería distribuida de alto rendimiento utilizado como cola de mensajes para la ingesta de datos en tiempo real.

## ⚙️ Cómo funciona
- Productores publican mensajes en **temas (topics)**
- Consumidores leen de los topics
- Mensajes organizados en particiones
- Retención configurable

## 📊 Uso en SIMLOG
- Topic `transporte_raw` - Datos crudos de ingesta
- Topic `transporte_filtered` - Datos filtrados

## 📊 Ejemplo
```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('transporte_raw', {'data': 'valor'})
```

## 🔗 Relacionado
- [[Spark]]
- [[Ingesta]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#kafka #streaming #mensajeria
