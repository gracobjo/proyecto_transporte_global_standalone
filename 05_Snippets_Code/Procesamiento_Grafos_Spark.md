# Procesamiento Grafos con GraphFrames

## 📝 Descripción
Script de procesamiento de grafos usando Spark y GraphFrames.

## 💻 Código
```python
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql.functions import col

def procesar_grafos(ruta_datos):
    """Procesa grafo de transporte."""
    spark = SparkSession.builder \
        .appName("SIMLOG_Grafos") \
        .master("local[*]") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.5-s_2.12") \
        .getOrCreate()
    
    # Cargar vertices
    vertices = spark.read.json(f"{ruta_datos}/nodos.json")
    
    # Cargar aristas
    aristas = spark.read.json(f"{ruta_datos}/aristas.json")
    
    # Crear grafo
    grafo = GraphFrame(vertices, aristas)
    
    # PageRank
    resultados = grafo.pageRank.resetProbability(0.15).run()
    
    # ShortestPath
    resultados_sp = grafo.shortestPaths.landmarks(["Madrid"]).run()
    
    return resultados

def autosanacion(grafo):
    """Elimina rutas bloqueadas y penaliza congestión."""
    aristas_filtradas = grafo.edges.filter(col("estado") != "Bloqueado")
    return grafo.dropEdges(aristas_filtradas)
```

## 🔧 Requisitos
- Spark 3.5+
- GraphFrames 0.8.2

## 🔗 Relacionado
- [[GraphFrames]]
- [[Spark]]

## 🏷️ Tags
#grafos #spark #graphframes
