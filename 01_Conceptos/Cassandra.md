# Apache Cassandra

## 🧠 Definición
Base de datos NoSQL distribuida, altamente escalable y de alta disponibilidad. Optimizada para escrituras rápidas.

## ⚙️ Cómo funciona
- Arquitectura peer-to-peer
- Modelo de datos basado en tablas
- Gossip protocol para comunicación
- Consistency level configurable
- CQL (Cassandra Query Language)

## 📊 Uso en SIMLOG
- Almacenamiento de **estado actual**
- Tablas: nodos, aristas, camiones, PageRank
- Consultas rápidas para dashboard

## 📊 Ejemplo CQL
```sql
CREATE TABLE nodos (
    id_nodo text PRIMARY KEY,
    nombre text,
    latitud float,
    longitud float,
    estado text,
    pagerank float
);
```

## 🔗 Relacionado
- [[Hive]]
- [[Spark]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#cassandra #nosql #basedatos
