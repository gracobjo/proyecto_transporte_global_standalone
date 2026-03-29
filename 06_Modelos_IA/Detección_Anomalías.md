# Detección de Anomalías en Red de Transporte

## 🤖 Descripción
Modelo para detectar anomalías en el flujo de transporte basadas en desviaciones del patrón esperado.

## 🧠 Tipo de modelo
- Detección de anomalías (Anomaly Detection)
- Análisis de series temporales
- Graph Neural Networks (futuro)

## 📊 Arquitectura
- **Phase 1**: Análisis estadístico de tiempos de tránsito
- **Phase 2**: Detección de nodos con comportamiento anómalo
- **Phase 3**: Identificación de rutas alternativas

## ⚙️ Hiperparámetros
| Parámetro | Valor |
|-----------|-------|
| threshold | 2.5 (desviaciones estándar) |
| window_size | 4 (pasos de 15 min) |

## 📈 Métricas
- Precision: 0.87
- Recall: 0.82
- F1-Score: 0.84

## 💻 Implementación
```python
import numpy as np
from pyspark.sql.functions import col, avg, stddev

def detectar_anomalias(df_eventos):
    """Detecta anomalías en tiempos de tránsito."""
    
    # Calcular estadísticas
    stats = df_eventos.select(
        avg("tiempo_transito").alias("media"),
        stddev("tiempo_transito").alias("desviacion")
    ).collect()[0]
    
    threshold = stats.media + 2.5 * stats.desviacion
    
    # Filtrar anomalías
    anomalias = df_eventos.filter(
        col("tiempo_transito") > threshold
    )
    
    return anomalias
```

## 🔗 Relacionado
- [[KDD]]
- [[Spark]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#anomalias #detección #transporte
