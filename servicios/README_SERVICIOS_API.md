# API REST SIMLOG (OpenAPI / Swagger)

Servicio **FastAPI** que publica las capacidades de lectura del proyecto para integraciones externas.

## Instalación

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate   # o tu venv
pip install -r requirements.txt
```

## Arranque

```bash
cd ~/proyecto_transporte_global
uvicorn servicios.api_simlog:app --host 0.0.0.0 --port 8090
```

- **Swagger UI (interactivo):** http://localhost:8090/docs  
- **ReDoc:** http://localhost:8090/redoc  
- **Esquema OpenAPI (JSON):** http://localhost:8090/openapi.json  

## CORS

Por defecto se permite `*` (cualquier origen). Para restringir:

```bash
export SIMLOG_CORS_ORIGINS="https://mi-app.example.com,http://localhost:3000"
uvicorn servicios.api_simlog:app --host 0.0.0.0 --port 8090
```

## Endpoints (resumen)

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/health` | Liveness |
| GET | `/api/v1/info` | Metadatos del proyecto |
| GET | `/api/v1/servicios/estado` | HDFS / Kafka / Cassandra (puertos) |
| GET | `/api/v1/servicios/verificacion` | Comprobaciones técnicas (HDFS ls, topics, CQL) |
| GET | `/api/v1/topologia/nodos` | Nodos estáticos (`config_nodos`) |
| GET | `/api/v1/topologia/aristas` | Aristas estáticas |
| GET | `/api/v1/datos/nodos` | `nodos_estado` (Cassandra) |
| GET | `/api/v1/datos/aristas` | `aristas_estado` |
| GET | `/api/v1/datos/tracking` | `tracking_camiones` |
| GET | `/api/v1/datos/pagerank` | `pagerank_nodos` |
| GET | `/api/v1/clima/hubs` | Clima por hub (OpenWeather) |
| GET | `/api/v1/kdd/fases` | Fases KDD documentadas |

Las rutas bajo `/api/v1/datos/*` devuelven listas vacías si Cassandra no está disponible o no hay datos (mismo criterio que el dashboard Streamlit).

## FAQ IA API

Microservicio complementario para preguntas frecuentes sobre el proyecto, sin dependencia de servicios externos:

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
uvicorn servicios.api_faq_ia:app --host 0.0.0.0 --port 8091
```

- **Swagger UI (interactivo):** http://localhost:8091/docs
- **ReDoc:** http://localhost:8091/redoc
- **Esquema OpenAPI (JSON):** http://localhost:8091/openapi.json

### Endpoints FAQ (resumen)

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/health` | Liveness del servicio FAQ IA |
| GET | `/api/v1/faq/questions` | Lista de preguntas cargadas desde la KB |
| POST | `/api/v1/faq/ask` | Resuelve una pregunta y devuelve respuesta, confianza, sugerencias y fuentes |

Base de conocimiento:

- `servicios/faq_knowledge_base.json`

## Integración con otras aplicaciones

1. Importar el contrato desde `openapi.json` (generadores de cliente: OpenAPI Generator, `openapi-typescript`, etc.).
2. Consumir JSON desde los endpoints anteriores; el esquema Pydantic aparece documentado en `/docs`.

No se exponen por defecto acciones de **escritura** (ingesta/procesamiento); esas operaciones siguen en scripts, Airflow o el dashboard.
