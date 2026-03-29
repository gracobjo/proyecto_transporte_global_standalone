# Dataset SIMLOG - Datos de Transporte

## 📋 Descripción
Conjunto de datos del sistema SIMLOG para simulación y monitorización de la red de transporte en España.

## 📊 Estructura

### Datos de Nodos
| Campo | Tipo | Descripción |
|-------|------|-------------|
| id_nodo | string | Identificador único del nodo |
| nombre | string | Nombre del hub |
| latitud | float | Coordenada latitud |
| longitud | float | Coordenada longitud |
| tipo | string | Tipo de nodo (hub, distribución) |

### Datos de Aristas
| Campo | Tipo | Descripción |
|-------|------|-------------|
| src | string | Nodo origen |
| dst | string | Nodo destino |
| distancia | float | Distancia en km |
| estado | string | OK/Congestionado/Bloqueado |
| motivo | string | Razón del estado |

### Datos de Camiones
| Campo | Tipo | Descripción |
|-------|------|-------------|
| id_camion | string | Identificador del camión |
| origen | string | Nodo de origen |
| destino | string | Nodo de destino |
| posicion_lat | float | Latitud actual |
| posicion_lon | float | Longitud actual |
| ruta | array | Lista de nodos en ruta |
| paso_actual | int | Paso actual en la ruta |

### Datos de Clima
| Campo | Tipo | Descripción |
|-------|------|-------------|
| ciudad | string | Nombre de la ciudad |
| temperatura | float | Temperatura en °C |
| humedad | int | Humedad relativa % |
| descripcion | string | Descripción del clima |
| visibilidad | float | Visibilidad en metros |

## 📁 Fuentes
- **Clima**: OpenWeatherMap API
- **Posiciones**: Simulación GPS
- **Estados**: Simulación de incidentes (DGT)

## 🔧 Preprocesamiento
1. Enrichment con datos de clima
2. Simulación de estados de tráfico
3. Cálculo de posiciones GPS interpoladas

## 📅 Frecuencia
- Snapshot cada 15 minutos
- Ciclos de 1 hora (4 pasos)

## 🔗 Relacionado
- [[SIMLOG_Transporte_Global]]
- [[Kafka]]
- [[Cassandra]]
- [[Hive]]

## 🏷️ Tags
#dataset #transporte #simlog
