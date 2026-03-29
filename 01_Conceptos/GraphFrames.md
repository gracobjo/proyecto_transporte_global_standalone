# GraphFrames

## 🧠 Definición
Biblioteca de Spark para procesamiento de grafos, proporcionando DataFrame-based API sobre GraphX.

## ⚙️ Cómo funciona
- Grafos representados como DataFrames
- Vertices (vértices) y Edges (aristas)
- Algoritmos: PageRank, ShortestPath, ConnectedComponents
- Motifs: búsqueda de patrones en grafos

## 📊 Uso en SIMLOG
- Construcción del grafo de red de transporte
- Autosanación del grafo
- Cálculo de rutas alternativas
- PageRank para nodos críticos

## 📊 Ejemplo
```python
from graphframes import GraphFrame

vertices = spark.createDataFrame([
    ("A", "Madrid"), ("B", "Barcelona")
], ["id", "nombre"])

aristas = spark.createDataFrame([
    ("A", "B", 500), ("B", "A", 500)
], ["src", "dst", "distancia"])

grafo = GraphFrame(vertices, aristas)
resultado = grafo.pageRank.resetProbability(0.15).run()
```

## 🔗 Relacionado
- [[Spark]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#graphframes #grafos #spark
