# Reconfiguración logística ante fallo crítico

## Objetivo

Implementar un mecanismo de resiliencia que:

- detecte caídas de nodos y rutas a partir de eventos operativos,
- actualice el grafo logístico activo,
- recalcule rutas alternativas automáticamente,
- emita alertas en tiempo real,
- mantenga persistencia multicapa:
  - Cassandra para estado actual y alertas activas,
  - Hive para histórico de alertas resueltas y eventos del grafo.

## Arquitectura

### Flujo lógico

1. La ingesta o un productor operativo publica `eventos_grafo` con eventos como `NODE_DOWN`, `NODE_UP`, `ROUTE_DOWN`, `ROUTE_UP`.
2. `procesamiento/procesamiento_grafos.py` lee el último snapshot y pasa los eventos a `procesamiento/reconfiguracion_grafo.py`.
3. El módulo de reconfiguración:
   - actualiza `estado_nodos` y `estado_rutas`,
   - desactiva rutas incidentes a nodos caídos,
   - construye un `GraphFrame` activo,
   - recalcula rutas alternativas con BFS/shortest paths sobre el grafo filtrado,
   - genera `alertas_activas`,
   - detecta alertas resueltas para moverlas al histórico.
4. Cassandra recibe el estado actual del grafo y las alertas vivas.
5. Hive recibe:
   - `alertas_historicas` cuando una alerta se resuelve,
   - `eventos_grafo` con el historial de cambios de topología.
6. Streamlit visualiza:
   - nodos activos en verde,
   - nodos caídos en rojo con X,
   - rutas desactivadas en gris,
   - rutas alternativas en azul.

### Componentes implicados

- `procesamiento/reconfiguracion_grafo.py`
- `procesamiento/procesamiento_grafos.py`
- `persistencia_hive.py`
- `cassandra/esquema_logistica.cql`
- `servicios/estado_y_datos.py`
- `app_visualizacion.py`

## Modelo de datos

### Cassandra: estado actual y baja latencia

Tablas:

- `estado_nodos`
  - `id_nodo`
  - `status` (`ACTIVE` / `DOWN`)
  - `cause`
  - `updated_at`
  - `last_event`

- `estado_rutas`
  - `src`, `dst`
  - `status`
  - `cause`
  - `updated_at`
  - `last_event`
  - `manual_down`

- `alertas_activas`
  - `alerta_id`
  - `tipo_alerta`
  - `entidad_id`
  - `severidad`
  - `mensaje`
  - `causa`
  - `timestamp_inicio`
  - `timestamp_ultima_actualizacion`
  - `estado`
  - `ruta_original`
  - `ruta_alternativa`

### Hive: histórico y analítica

Tablas:

- `alertas_historicas`
  - inicio/fin de alerta
  - severidad
  - causa
  - duración
  - ruta original / alternativa

- `eventos_grafo`
  - tipo de evento
  - entidad afectada
  - estado anterior / nuevo
  - causa
  - detalles serializados

## Caso de uso

### Reconfiguración logística ante fallo crítico

1. Un nodo logístico recibe `NODE_DOWN`.
2. El sistema marca el nodo como `DOWN`.
3. Se desactivan automáticamente las rutas que dependen de ese nodo.
4. Se insertan alertas activas en Cassandra.
5. Se recalculan rutas alternativas para los camiones afectados.
6. La UI actualiza el mapa operativo con el nuevo estado.
7. Cuando llega `NODE_UP`, el sistema reactiva el nodo.
8. Las alertas que dejan de estar activas se eliminan de Cassandra y se escriben en Hive.

## Justificación técnica

### Cassandra para baja latencia

- Permite consultar el estado actual del grafo sin esperar a agregaciones.
- Es adecuada para tablas de hechos vivos como `estado_nodos`, `estado_rutas` y `alertas_activas`.
- Encaja con el dashboard operativo y con refrescos rápidos en Streamlit.

### Hive para histórico

- Conserva el historial de resolución sin contaminar el estado actual.
- Facilita reporting, auditoría y análisis temporal de incidentes.
- Se integra con el histórico ya existente del proyecto.

### Grafos para resiliencia

- El problema es topológico: cuando un nodo o una ruta falla, cambia la conectividad.
- GraphFrames permite representar el grafo activo y recalcular conectividad/rutas.
- La separación entre grafo completo y grafo activo facilita la autosanación del sistema.

## Pruebas previstas

- caída de nodo y desactivación de rutas incidentes,
- recalculo de ruta alternativa,
- resolución de alerta y paso a histórico Hive,
- actualización del estado actual preparado para Cassandra,
- simulación de fallo en cascada con pérdida de conectividad.
