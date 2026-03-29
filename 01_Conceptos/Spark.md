# Apache Spark

## 🧠 Definición
Motor de procesamiento distribuido para análisis de datos a gran escala. Soporta procesamiento batch y streaming.

## ⚙️ Cómo funciona
- RDDs (Resilient Distributed Datasets)
- DataFrames y Datasets
- Transformaciones y acciones
- Spark SQL para consultas
- GraphFrames para grafos

## 📊 Uso en SIMLOG
- Procesamiento de grafos con GraphFrames
- Pipeline KDD (Knowledge Discovery in Data)
- Procesamiento streaming

## 📊 Ejemplo
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SIMLOG") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.json("datos/*.json")
```

## 🔗 Relacionado
- [[GraphFrames]]
- [[Kafka]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#spark #bigdata #procesamiento
