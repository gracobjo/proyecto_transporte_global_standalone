# PageRank - Nodos Críticos

## 🤖 Descripción
Algoritmo de PageRank para identificar los nodos más importantes/criticos en la red de transporte.

## 🧠 Tipo de modelo
- Algoritmo de grafos
- Análisis de centrality

## 📊 Arquitectura
- Entrada: Grafo de nodos y aristas
- Proceso: Iteraciones de PageRank
- Salida: Puntuación de importancia por nodo

## ⚙️ Hiperparámetros
| Parámetro | Valor | Descripción |
|-----------|-------|-------------|
| resetProbability | 0.15 | Probabilidad de reinicio |
| maxIter | 20 | Iteraciones máximas |
| tol | 0.0001 | Tolerancia de convergencia |

## 📈 Métricas
- **Pagerank score**: Importancia relativa de cada nodo
- **Nodos críticos**: top 10 por puntuación

## 💻 Implementación
```python
from graphframes import GraphFrame

resultados = grafo.pageRank \
    .resetProbability(0.15) \
    .maxIter(20) \
    .run()

# Obtener top nodos críticos
top_nodos = resultados.vertices \
    .orderBy(desc("pagerank")) \
    .limit(10)
```

## 🔗 Relacionado
- [[GraphFrames]]
- [[Gemelo_Digital]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#pagerank #grafos #analisis
