# Apache Hive

## 🧠 Definición
Data warehouse built on top of Hadoop for querying and analyzing large datasets using SQL-like language (HiveQL).

## ⚙️ Cómo funciona
- Almacena datos en HDFS
- HiveQL similar a SQL
- Consultas convertidas a MapReduce/Spark
- Particionamiento y bucketing
- UDFs personalizables

## 📊 Uso en SIMLOG
- Almacenamiento de **histórico**
- Análisis de tendencias
- Reportes y agregados
- Datos particionados por fecha

## 📊 Ejemplo HiveQL
```sql
CREATE TABLE eventos (
    id_evento string,
    timestamp timestamp,
    tipo string,
    nodo_origen string
)
PARTITIONED BY (fecha string)
STORED AS PARQUET;
```

## 🔗 Relacionado
- [[Cassandra]]
- [[Spark]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#hive #hadoop #datawarehouse
